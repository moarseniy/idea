#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
copy_into_pg(final_spec, xml_path, conn, ...)

Только вставка данных в уже созданные таблицы PostgreSQL.
DDL и TRUNCATE делай снаружи (пример кода ниже).
"""

from __future__ import annotations

import re
import json
import decimal
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from xml.etree import ElementTree as ET
from collections import defaultdict

import psycopg2.extras


# -----------------------------
# Парсинг XML в «сырые» строки
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
    Генератор: (table_name, row_dict_raw) — значения строками/None + id/fk/seq.
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
# Конвертация значений в PG-типы
# -----------------------------

_DEC_CANON_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.I)
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
        q = Decimal("1").scaleb(-scale)
        return dec.quantize(q, rounding=decimal.ROUND_HALF_UP)
    except Exception:
        return None

def _to_date(v: Optional[str]) -> Optional[str]:
    return None if (v is None or v == "") else v

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
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if with_ms else dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _convert_row_for_pg(table_spec: Dict[str, Any], row_raw: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for c in table_spec["columns"]:
        name = c["name"]
        v = row_raw.get(name)
        ctype = c["type"].lower()
        if name == "id" or c.get("role") in {"pk_surrogate", "fk_parent", "sequence_within_parent"}:
            out[name] = int(v) if v is not None else None
            continue

        if ctype in ("string", "json"):
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
    values = [[r.get(c) for c in columns] for r in rows]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)


# -----------------------------
# Публичная функция COPY (PG)
# -----------------------------

def xml_copy_into_pg(
    final_spec: Dict[str, Any],
    xml_path: str,
    conn,
    schema: str = "public",
    batch_size: int = 5000,
) -> None:
    """
    Только загрузка данных. Считаем, что таблицы уже созданы по согласованной схеме.
    """
    rows_by_table: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for tname, row_raw in _iter_rows_raw_from_xml(final_spec, xml_path):
        rows_by_table[tname].append(row_raw)

    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]
    t_by_name = {t["table"]: t for t in final_spec["tables"]}

    for tname in order:
        T = t_by_name[tname]
        cols = [c["name"] for c in T["columns"]]
        raw_rows = rows_by_table.get(tname, [])
        batch: List[Dict[str, Any]] = []
        for rr in raw_rows:
            batch.append(_convert_row_for_pg(T, rr))
            if len(batch) >= batch_size:
                _bulk_insert(conn, schema, tname, cols, batch)
                batch.clear()
        if batch:
            _bulk_insert(conn, schema, tname, cols, batch)
    conn.commit()
