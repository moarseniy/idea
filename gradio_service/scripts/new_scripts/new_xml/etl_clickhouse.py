#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL для ClickHouse:
- формирует DDL через ваш ddlgenerator_clickhouse,
- по желанию пересоздаёт таблицы,
- парсит XML по final_spec,
- грузит JSONEachRow с безопасной нормализацией чисел (десятичные без 'e').

Флаги:
- recreate_tables: дропает таблицы перед созданием (чтобы приняли новые типы)
- decimal_min_precision: расширяет p в decimal(p,s) минимум до указанного
- force_nullable: делает все пользовательские поля Nullable, но:
  * в ORDER BY, PK/FK/seq — оставляет NOT NULL (в CH ключи не могут быть Nullable)
"""

from __future__ import annotations

import re
import json
import time
import decimal
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from xml.etree import ElementTree as ET
from collections import defaultdict

import requests

from ddlgenerator_clickhouse import generate_clickhouse_ddl

# requests без прокси
_SESSION = requests.Session()
_NO_PROXY = {"http": None, "https": None}

# -----------------------------
# Типы / правки спецификации
# -----------------------------

_DEC_CANON_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.I)

def _parse_decimal(canon: str) -> Optional[Tuple[int, int]]:
    m = _DEC_CANON_RE.match(canon.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

def _adjust_spec_types(final_spec: Dict[str, Any],
                       decimal_min_precision: int = 28,
                       force_nullable: bool = True) -> Dict[str, Any]:
    """
    Копия final_spec с:
      - p := max(p, decimal_min_precision) для decimal(p,s),
      - force_nullable=True для всех пользовательских колонок кроме:
        * pk_surrogate, fk_parent, sequence_within_parent
        * колонок из order_by (CH требует non-null для ключей)
    """
    import copy
    spec = copy.deepcopy(final_spec)
    for t in spec["tables"]:
        order_by = set(t.get("order_by") or [])
        for c in t["columns"]:
            # widen decimal
            dt = _parse_decimal(c["type"])
            if dt:
                p, s = dt
                if p < decimal_min_precision:
                    c["type"] = f"decimal({decimal_min_precision},{s})"

            if force_nullable:
                role = c.get("role")
                keep_notnull = (
                    role in {"pk_surrogate", "fk_parent", "sequence_within_parent"}
                    or c["name"] in order_by
                )
                c["nullable"] = False if keep_notnull else True
    return spec


# ---------------------------------
# Применение DDL / управление БД
# ---------------------------------

def _ch_params(user: Optional[str], password: Optional[str]) -> Dict[str, str]:
    p = {"user": user or "default"}
    if password:
        p["password"] = password
    return p

def _apply_ddl(ch_url: str, ddl: str, user: Optional[str], password: Optional[str]):
    # простая проверка доступности
    ping = _SESSION.get(ch_url, params={**_ch_params(user, password), "query": "SELECT 1"},
                        proxies=_NO_PROXY, timeout=15)
    ping.raise_for_status()
    # по одному стейтменту
    for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
        resp = _SESSION.post(ch_url, params=_ch_params(user, password),
                             data=(stmt + ";").encode("utf-8"),
                             proxies=_NO_PROXY, timeout=60)
        try:
            resp.raise_for_status()
        except Exception as e:
            raise RuntimeError(f"Ошибка применения DDL:\n{stmt}\nОтвет:\n{resp.text}") from e

def _drop_tables(ch_url: str, final_spec: Dict[str, Any], database: str,
                 user: Optional[str], password: Optional[str]):
    for t in final_spec["tables"]:
        stmt = f"DROP TABLE IF EXISTS {database}.{t['table']}"
        r = _SESSION.post(ch_url, params=_ch_params(user, password),
                          data=(stmt + ";").encode("utf-8"),
                          proxies=_NO_PROXY, timeout=30)
        r.raise_for_status()

def _truncate_tables(ch_url: str, final_spec: Dict[str, Any], database: str,
                     user: Optional[str], password: Optional[str]):
    for t in final_spec["tables"]:
        stmt = f"TRUNCATE TABLE {database}.{t['table']}"
        r = _SESSION.post(ch_url, params=_ch_params(user, password),
                          data=(stmt + ";").encode("utf-8"),
                          proxies=_NO_PROXY, timeout=30)
        r.raise_for_status()


# -----------------------------
# Парсинг XML (общая логика)
# -----------------------------

def _ns_local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _split(p: str) -> List[str]:
    return [seg for seg in p.split("/") if seg]

def _first_text_rel(root_el: ET.Element, rel_path: str) -> Optional[str]:
    parts = _split(rel_path)
    cur = [root_el]
    for name in parts:
        nxt = []
        for el in cur:
            for ch in el:
                if _ns_local(ch.tag) == name:
                    nxt.append(ch)
        if not nxt:
            return None
        cur = nxt
    return (cur[0].text or None)

def _index_tables(final_spec: Dict[str, Any]):
    t_by_rowpath: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    t_by_name: Dict[str, Dict[str, Any]] = {}
    for t in final_spec["tables"]:
        rowp = tuple(_split(t["extract"]["row_xpath"]))
        t["__rowp_tuple"] = rowp
        t["__col_by_name"] = {c["name"]: c for c in t["columns"]}
        seq_col = None
        for c in t["columns"]:
            if c.get("role") == "sequence_within_parent":
                seq_col = c["name"]
                break
        t["__seq_col"] = seq_col
        t_by_rowpath[rowp] = t
        t_by_name[t["table"]] = t
    return t_by_rowpath, t_by_name

def _iter_rows_raw_from_xml(final_spec: Dict[str, Any], xml_path: str):
    """
    Генератор (table_name, row_raw) — значения строками/None + id/fk/seq.
    """
    t_by_rowpath, t_by_name = _index_tables(final_spec)
    stack: List[str] = []
    ctx_stacks: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    id_counters: Dict[str, int] = defaultdict(int)

    parent_rowp_by_table: Dict[str, Tuple[str, ...]] = {}
    for t in final_spec["tables"]:
        p_tab = (t.get("parent") or {}).get("table")
        if p_tab:
            parent_rowp_by_table[t["table"]] = t_by_name[p_tab]["__rowp_tuple"]

    for ev, el in ET.iterparse(xml_path, events=("start", "end")):
        if ev == "start":
            stack.append(_ns_local(el.tag))
            key = tuple(stack)
            T = t_by_rowpath.get(key)
            if T:
                id_counters[T["table"]] += 1
                rid = id_counters[T["table"]]

                parent_fk_col = (T.get("parent") or {}).get("fk_column")
                parent_fk_val = None
                seq_val = None
                if T.get("parent", {}).get("table"):
                    prow = parent_rowp_by_table[T["table"]]
                    parents = ctx_stacks.get(prow) or []
                    if parents:
                        pctx = parents[-1]
                        parent_fk_val = pctx["id"]
                        if T["__seq_col"]:
                            pctx["seq_counters"][T["table"]] += 1
                            seq_val = pctx["seq_counters"][T["table"]]

                ctx = {
                    "table": T["table"],
                    "id": rid,
                    "parent_fk_col": parent_fk_col,
                    "parent_fk_val": parent_fk_val,
                    "seq_col": T["__seq_col"],
                    "seq_val": seq_val,
                    "el": el,
                    "seq_counters": defaultdict(int),
                }
                ctx_stacks[key].append(ctx)
            continue

        # end
        key = tuple(stack)
        T = t_by_rowpath.get(key)
        if T:
            ctx = ctx_stacks[key].pop()
            row: Dict[str, Any] = {}
            row["id"] = ctx["id"]
            if ctx["parent_fk_col"]:
                row[ctx["parent_fk_col"]] = ctx["parent_fk_val"]
            if ctx["seq_col"] is not None:
                row[ctx["seq_col"]] = ctx["seq_val"]

            for fld in T["extract"]["fields"]:
                colname = fld["column"]
                txt = _first_text_rel(ctx["el"], fld["rel_xpath"])
                if txt is not None:
                    txt = txt.strip()
                    if txt == "":
                        txt = None
                row[colname] = txt

            yield (T["table"], row)
            ctx["el"].clear()

        stack.pop()


# -----------------------------
# Нормализация для ClickHouse
# -----------------------------

_BOOL_TRUE = {"1", "true", "t", "y", "yes", "да", "истина"}
_BOOL_FALSE = {"0", "false", "f", "n", "no", "нет", "ложь"}

def _to_bool(v: Optional[str]) -> Optional[bool]:
    if v is None:
        return None
    low = v.strip().lower()
    if low in _BOOL_TRUE:
        return True
    if low in _BOOL_FALSE:
        return False
    return None

def _to_int(v: Optional[str]) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None

def _to_float(v: Optional[str]) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v.replace(",", "."))
    except Exception:
        return None

def _to_decimal_number_for_ch(v: Optional[str], scale: int) -> Optional[float]:
    """
    ClickHouse JSONEachRow для Decimal НЕ принимает экспоненту.
    Возвращаем float без 'e': quantize до scale и cast -> float.
    """
    if v is None or v == "":
        return None
    try:
        dec = Decimal(v.replace(",", "."))
        q = Decimal("1").scaleb(-scale)
        dec_q = dec.quantize(q, rounding=decimal.ROUND_HALF_UP)
        # cast to float — json.dumps даст обычную десятичную форму для типичных значений
        return float(dec_q)
    except Exception:
        return None

def _to_date(v: Optional[str]) -> Optional[str]:
    if v is None or v == "":
        return None
    return v

def _to_ts_utc(v: Optional[str], with_ms: bool) -> Optional[str]:
    if v is None or v == "":
        return None
    from datetime import datetime, timezone
    vv = v.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(vv)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        if with_ms:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _convert_row_for_ch(table_spec: Dict[str, Any], row_raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for c in table_spec["columns"]:
        name = c["name"]
        v = row_raw.get(name)
        ctype = c["type"].lower()
        if name == "id" or c.get("role") in {"pk_surrogate", "fk_parent", "sequence_within_parent"}:
            out[name] = int(v) if v is not None else None
            continue

        if ctype == "string" or ctype == "json":
            out[name] = v
        elif ctype in ("int32", "int64"):
            out[name] = _to_int(v)
        elif ctype == "float64":
            # float нормально заходит (в CH — Float64)
            out[name] = _to_float(v)
        elif ctype == "bool":
            out[name] = _to_bool(v)
        elif ctype == "date":
            out[name] = _to_date(v)
        elif ctype == "timestamp":
            out[name] = _to_ts_utc(v, with_ms=False)
        elif ctype.startswith("timestamp64"):
            out[name] = _to_ts_utc(v, with_ms=True)
        else:
            m = _DEC_CANON_RE.match(ctype)
            if m:
                s = int(m.group(2))
                out[name] = _to_decimal_number_for_ch(v, s)
            else:
                out[name] = v
    return out


# -----------------------------
# Вставка JSONEachRow
# -----------------------------

def _insert_json_each_row(ch_url: str, database: str, table: str,
                          rows: List[Dict[str, Any]],
                          user: Optional[str], password: Optional[str]):
    if not rows:
        return
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    params = {
        **_ch_params(user, password),
        "query": f"INSERT INTO {database}.{table} FORMAT JSONEachRow",
        # чуть мягче парсер
        "input_format_skip_unknown_fields": 1,
        "input_format_null_as_default": 1,
        "date_time_input_format": "best_effort",
    }
    r = _SESSION.post(ch_url, params=params, data=payload.encode("utf-8"),
                      proxies=_NO_PROXY, timeout=120)
    try:
        r.raise_for_status()
    except Exception as e:
        sample = rows[0] if rows else {}
        raise RuntimeError(
            "Ошибка вставки в ClickHouse.\n"
            f"Стейтмент: INSERT INTO {database}.{table} FORMAT JSONEachRow\n"
            f"Ответ сервера:\n{r.text}\n"
            f"Пример строки JSONEachRow:\n{json.dumps(sample, ensure_ascii=False)}"
        ) from e


# -----------------------------
# Публичная функция ETL (CH)
# -----------------------------

def load_xml_to_clickhouse(
    final_spec: Dict[str, Any],
    xml_path: str,
    ch_url: str = "http://localhost:8123",
    database: str = "analytics",
    batch_size: int = 5000,
    create_database: bool = True,
    truncate: bool = True,
    include_unique_comments: bool = True,
    types_yaml_path: Optional[str] = "config/types.yaml",  # совместимость
    user: Optional[str] = "default",
    password: Optional[str] = None,
    decimal_min_precision: int = 28,
    force_nullable: bool = True,
    recreate_tables: bool = True,
) -> None:
    """
    Полный цикл для ClickHouse.
    """
    # правим спецификацию под CH (nullable/order_by/decimal)
    ddl_spec = _adjust_spec_types(final_spec,
                                  decimal_min_precision=decimal_min_precision,
                                  force_nullable=force_nullable)

    # генерим DDL (create database + create tables)
    ddl = generate_clickhouse_ddl(
        ddl_spec,
        database=database,
        types_yaml_path=types_yaml_path,
        include_unique_comments=include_unique_comments,
    )

    # создаём БД/таблицы
    if create_database:
        if recreate_tables:
            _drop_tables(ch_url, final_spec, database, user=user, password=password)
        _apply_ddl(ch_url, ddl, user=user, password=password)

    if truncate:
        _truncate_tables(ch_url, final_spec, database, user=user, password=password)

    # собираем строки «сырыми»
    rows_by_table: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for tname, row_raw in _iter_rows_raw_from_xml(final_spec, xml_path):
        rows_by_table[tname].append(row_raw)

    # вставляем по порядку загрузки
    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]
    t_by_name = {t["table"]: t for t in ddl_spec["tables"]}  # заметим: для конвертации берём ddl_spec (с типами!)

    for tname in order:
        T = t_by_name[tname]
        raw_rows = rows_by_table.get(tname, [])
        batch: List[Dict[str, Any]] = []
        for rr in raw_rows:
            batch.append(_convert_row_for_ch(T, rr))
            if len(batch) >= batch_size:
                _insert_json_each_row(ch_url, database, tname, batch, user=user, password=password)
                batch.clear()
        if batch:
            _insert_json_each_row(ch_url, database, tname, batch, user=user, password=password)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Load XML into ClickHouse using final_spec")
    ap.add_argument("--xml", required=True)
    ap.add_argument("--url", default="http://localhost:8123")
    ap.add_argument("--db", default="analytics")
    ap.add_argument("--spec", required=True)
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--no-create", action="store_true")
    ap.add_argument("--no-truncate", action="store_true")
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument("--dec-min-p", type=int, default=28)
    ap.add_argument("--force-nullable", action="store_true")
    ap.add_argument("--user", default="default")
    ap.add_argument("--password", default=None)
    args = ap.parse_args()

    with open(args.spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    load_xml_to_clickhouse(
        spec, args.xml, ch_url=args.url, database=args.db,
        batch_size=args.batch,
        create_database=not args.no_create,
        truncate=not args.no_truncate,
        user=args.user, password=args.password,
        decimal_min_precision=args.dec_min_p,
        force_nullable=args.force_nullable,
        recreate_tables=args.recreate,
    )
