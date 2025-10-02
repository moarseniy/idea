#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ETL для PostgreSQL:
- строит DDL из final_spec (через ваш ddlgenerator_postgres),
- по желанию пересоздаёт таблицы,
- парсит XML по final_spec['tables'][*]['extract'],
- грузит данными батчами через psycopg2.extras.execute_values.

Ключевые флаги:
- recreate_tables: перед применением DDL дропает все таблицы (чтобы приняли новые типы)
- decimal_min_precision: поднимает p в decimal(p,s) до заданного минимума (например, 28)
- force_nullable: все пользовательские колонки делает NULLABLE (кроме id/fk/seq и колонок в order_by)
"""

from __future__ import annotations

import re
import json
import decimal
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from xml.etree import ElementTree as ET
from collections import defaultdict

import psycopg2
import psycopg2.extras

# ваш генератор DDL
from ddlgenerator_postgres import generate_postgres_ddl


# -----------------------------
# Вспомогательные для типов
# -----------------------------

_DEC_CANON_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.I)

def _parse_decimal(canon: str) -> Optional[Tuple[int, int]]:
    m = _DEC_CANON_RE.match(canon.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))

def _adjust_spec_types(final_spec: Dict[str, Any],
                       decimal_min_precision: int = 28,
                       force_nullable: bool = True,
                       for_clickhouse: bool = False) -> Dict[str, Any]:
    """
    Возвращает КОПИЮ final_spec с:
    - поднятой точностью decimal(p,s): p = max(p, decimal_min_precision)
    - nullable=True для всех пользовательских колонок (кроме ключевых/порядковых и order_by),
      а в режиме CH — столбцы из ORDER BY и ключи ОСТАЮТСЯ not null (CH так требует).
    """
    import copy
    spec = copy.deepcopy(final_spec)

    # быстрые индексы: name->table
    t_by_name = {t["table"]: t for t in spec["tables"]}

    for t in spec["tables"]:
        order_by = set(t.get("order_by") or [])
        for col in t["columns"]:
            role = col.get("role")
            # decimal widen
            dt = _parse_decimal(col["type"])
            if dt:
                p, s = dt
                if p < decimal_min_precision:
                    col["type"] = f"decimal({decimal_min_precision},{s})"

            if force_nullable:
                # какие колонки НЕ трогаем:
                keep_notnull = (
                    role in {"pk_surrogate", "fk_parent", "sequence_within_parent"}
                    or col["name"] in order_by
                )
                if for_clickhouse:
                    # в CH ключевые и ORDER BY — точно not null
                    col["nullable"] = False if keep_notnull else True
                else:
                    # в PG можно оставить FK not null, остальным — nullable
                    col["nullable"] = False if keep_notnull else True
            # иначе оставляем как было

    return spec


# ---------------------------------
# Применение DDL/управление схемой
# ---------------------------------

def _drop_tables_pg(conn, final_spec: Dict[str, Any], schema: str):
    with conn.cursor() as cur:
        for t in final_spec["tables"]:
            cur.execute(f"DROP TABLE IF EXISTS {schema}.{t['table']} CASCADE;")

def _truncate_tables_pg(conn, final_spec: Dict[str, Any], schema: str):
    with conn.cursor() as cur:
        for t in final_spec["tables"]:
            cur.execute(f"TRUNCATE TABLE {schema}.{t['table']} CASCADE;")

def _ensure_schema_and_tables(conn, final_spec: Dict[str, Any],
                              schema: str,
                              emit_unique: bool,
                              ddl_spec: Optional[Dict[str, Any]] = None):
    """
    Генерим DDL (из скорректированного ddl_spec если передан) и применяем.
    """
    spec_to_use = ddl_spec or final_spec
    ddl = generate_postgres_ddl(spec_to_use, schema=schema, emit_unique=emit_unique)
    with conn.cursor() as cur:
        # Разобьём на стейтменты по ';' (простая эвристика — в генерируемом DDL ок)
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            cur.execute(stmt + ";")
    conn.commit()


# -----------------------------
# Парсинг XML в «сырые» строки
# -----------------------------

def _ns_local(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag

def _split(p: str) -> List[str]:
    return [seg for seg in p.split("/") if seg]

def _first_text_rel(root_el: ET.Element, rel_path: str) -> Optional[str]:
    """
    Находит ПЕРВОЕ вхождение текста по относительному пути внутри root_el.
    Путь без namespaces. Если нет — None.
    """
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
    # берём text первого найденного листа/элемента
    # (если это внутренний узел с детьми — берём его .text как есть)
    return (cur[0].text or None)

def _index_tables(final_spec: Dict[str, Any]):
    """
    Готовим структуру для парсинга:
      - по row_xpath -> дескриптор таблицы,
      - по имени таблицы -> дескриптор.
    """
    t_by_rowpath: Dict[Tuple[str, ...], Dict[str, Any]] = {}
    t_by_name: Dict[str, Dict[str, Any]] = {}

    for t in final_spec["tables"]:
        rowp = tuple(_split(t["extract"]["row_xpath"]))
        t["__rowp_tuple"] = rowp
        # подготовим быстрый индекс полей
        col_by_name = {c["name"]: c for c in t["columns"]}
        t["__col_by_name"] = col_by_name
        # имя seq-колонки (если есть)
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
    Генератор: даёт (table_name, row_dict_raw) для каждой найденной строки сущности.
    row_dict_raw — словарь значений СТРОКАМИ (или None), плюс служебные: id, fk, seq при наличии.
    """
    t_by_rowpath, t_by_name = _index_tables(final_spec)
    stack: List[str] = []
    # стеки активных контекстов по row_xpath
    ctx_stacks: Dict[Tuple[str, ...], List[Dict[str, Any]]] = defaultdict(list)
    # счётчики id по таблицам
    id_counters: Dict[str, int] = defaultdict(int)

    # для быстрого поиска родителя: по entity_path -> таблица -> row_xpath
    parent_rowp_by_table: Dict[str, Tuple[str, ...]] = {}
    for t in final_spec["tables"]:
        p_tab = (t.get("parent") or {}).get("table")
        if p_tab:
            parent_rowp_by_table[t["table"]] = t_by_name[p_tab]["__rowp_tuple"]

    for ev, el in ET.iterparse(xml_path, events=("start", "end")):
        if ev == "start":
            stack.append(_ns_local(el.tag))
            key = tuple(stack)

            # начало строки сущности?
            T = t_by_rowpath.get(key)
            if T:
                # выставим id
                id_counters[T["table"]] += 1
                rid = id_counters[T["table"]]
                # вычислим FK/SEQ (если есть родитель и seq)
                parent_fk_col = (T.get("parent") or {}).get("fk_column")
                parent_fk_val = None
                seq_val = None

                if T.get("parent", {}).get("table"):
                    prow = parent_rowp_by_table[T["table"]]
                    parent_ctx_stack = ctx_stacks.get(prow) or []
                    if parent_ctx_stack:
                        parent_ctx = parent_ctx_stack[-1]
                        parent_fk_val = parent_ctx["id"]
                        # seq
                        if T["__seq_col"]:
                            parent_ctx["seq_counters"][T["table"]] += 1
                            seq_val = parent_ctx["seq_counters"][T["table"]]

                # создаём контекст
                ctx = {
                    "table": T["table"],
                    "id": rid,
                    "parent_fk_col": parent_fk_col,
                    "parent_fk_val": parent_fk_val,
                    "seq_col": T["__seq_col"],
                    "seq_val": seq_val,
                    "el": el,  # на end возьмём значения
                    # у родителя храним счетчики seq для детей
                    "seq_counters": defaultdict(int),
                }
                ctx_stacks[key].append(ctx)

            continue

        # END:
        key = tuple(stack)
        T = t_by_rowpath.get(key)
        if T:
            # закрываем контекст
            ctx = ctx_stacks[key].pop()
            # строим строку
            row: Dict[str, Any] = {}
            row["id"] = ctx["id"]
            if ctx["parent_fk_col"]:
                row[ctx["parent_fk_col"]] = ctx["parent_fk_val"]
            if ctx["seq_col"] is not None:
                row[ctx["seq_col"]] = ctx["seq_val"]

            # пользовательские поля по спецификации extract
            for fld in T["extract"]["fields"]:
                colname = fld["column"]
                txt = _first_text_rel(ctx["el"], fld["rel_xpath"])
                # нормализуем пустые строки -> None
                if txt is not None:
                    txt = txt.strip()
                    if txt == "":
                        txt = None
                row[colname] = txt

            yield (T["table"], row)

            # чистим элемент (экономия памяти при больших XML)
            ctx["el"].clear()

        stack.pop()

# -----------------------------
# Конвертация значений в PG-типы
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

def _to_decimal(v: Optional[str], scale: int) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        dec = Decimal(v.replace(",", "."))
        q = Decimal("1").scaleb(-scale)  # 10^-scale
        return dec.quantize(q, rounding=decimal.ROUND_HALF_UP)
    except Exception:
        return None

def _to_date(v: Optional[str]) -> Optional[str]:
    # оставим как ISO-строку 'YYYY-MM-DD' — PG сам приведёт
    if v is None or v == "":
        return None
    return v

def _to_ts_utc(v: Optional[str], with_ms: bool) -> Optional[str]:
    # приводим к UTC-строке 'YYYY-MM-DD HH:MM:SS[.mmm]'
    if v is None or v == "":
        return None
    from datetime import datetime, timezone
    vv = v.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(vv)
        if dt.tzinfo is None:
            # трактуем как UTC если без зоны
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        if with_ms:
            return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _convert_row_for_pg(table_spec: Dict[str, Any], row_raw: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    col_by = table_spec["__col_by_name"]
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
                out[name] = _to_decimal(v, s)
            else:
                out[name] = v
    return out


# -----------------------------
# Вставка батчами в PostgreSQL
# -----------------------------

def _bulk_insert(conn, schema: str, table: str, columns: List[str], rows: List[Dict[str, Any]]):
    if not rows:
        return
    cols_sql = ", ".join(columns)
    sql = f"INSERT INTO {schema}.{table} ({cols_sql}) VALUES %s"
    # порядок колонок в значениях
    values = [[r.get(c) for c in columns] for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)


# -----------------------------
# Публичная функция ETL
# -----------------------------

def load_xml_to_postgres(
    final_spec: Dict[str, Any],
    xml_path: str,
    conn_str: str = "postgresql://postgres:postgres@localhost:5432/analytics",
    schema: str = "public",
    batch_size: int = 5000,
    create_schema: bool = True,
    truncate: bool = True,
    emit_unique: bool = False,
    types_yaml_path: Optional[str] = "config/types.yaml",  # оставлен для совместимости
    decimal_min_precision: int = 28,
    force_nullable: bool = True,
    recreate_tables: bool = True,
) -> None:
    """
    Выполняет полный цикл:
      - (опц.) пересоздание таблиц под актуальную схему,
      - (опц.) truncate,
      - загрузка XML батчами в порядке load_order.
    """
    # скорректируем спецификацию под DDL
    ddl_spec = _adjust_spec_types(final_spec,
                                  decimal_min_precision=decimal_min_precision,
                                  force_nullable=force_nullable,
                                  for_clickhouse=False)

    conn = psycopg2.connect(conn_str)
    try:
        if create_schema:
            if recreate_tables:
                _drop_tables_pg(conn, final_spec, schema)
            _ensure_schema_and_tables(conn, final_spec, schema, emit_unique, ddl_spec=ddl_spec)

        if truncate:
            _truncate_tables_pg(conn, final_spec, schema)

        # собираем строки «сырыми» (строками), потом конвертим под PG типы
        rows_by_table: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for tname, row_raw in _iter_rows_raw_from_xml(final_spec, xml_path):
            rows_by_table[tname].append(row_raw)

        # вставляем по порядку загрузки
        order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]
        t_by_name = {t["table"]: t for t in final_spec["tables"]}

        for tname in order:
            T = t_by_name[tname]
            cols = [c["name"] for c in T["columns"]]
            raw_rows = rows_by_table.get(tname, [])
            # батчами
            batch: List[Dict[str, Any]] = []
            for rr in raw_rows:
                batch.append(_convert_row_for_pg(T, rr))
                if len(batch) >= batch_size:
                    _bulk_insert(conn, schema, tname, cols, batch)
                    batch.clear()
            if batch:
                _bulk_insert(conn, schema, tname, cols, batch)

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Load XML into PostgreSQL using final_spec")
    ap.add_argument("--xml", required=True)
    ap.add_argument("--conn", default="postgresql://postgres:postgres@localhost:5432/analytics")
    ap.add_argument("--schema", default="public")
    ap.add_argument("--spec", required=True, help="Path to final_spec.json")
    ap.add_argument("--batch", type=int, default=5000)
    ap.add_argument("--no-create", action="store_true")
    ap.add_argument("--no-truncate", action="store_true")
    ap.add_argument("--emit-unique", action="store_true")
    ap.add_argument("--recreate", action="store_true")
    ap.add_argument("--dec-min-p", type=int, default=28)
    ap.add_argument("--force-nullable", action="store_true")
    args = ap.parse_args()

    with open(args.spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    load_xml_to_postgres(
        spec, args.xml, conn_str=args.conn, schema=args.schema,
        batch_size=args.batch,
        create_schema=not args.no_create,
        truncate=not args.no_truncate,
        emit_unique=args.emit_unique,
        decimal_min_precision=args.dec_min_p,
        force_nullable=args.force_nullable,
        recreate_tables=args.recreate,
    )
