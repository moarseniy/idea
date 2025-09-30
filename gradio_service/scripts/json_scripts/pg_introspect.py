# pg_introspect.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict, List, Tuple
import psycopg2

def table_exists(conn, schema: str, table: str) -> bool:
    q = """
    select 1
    from information_schema.tables
    where table_schema = %s and table_name = %s
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return cur.fetchone() is not None

def list_tables(conn, schema: str) -> List[str]:
    q = """
    select table_name
    from information_schema.tables
    where table_schema = %s and table_type='BASE TABLE'
    order by table_name
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema,))
        return [r[0] for r in cur.fetchall()]

def describe_columns(conn, schema: str, table: str) -> List[Dict[str, Any]]:
    q = """
    select
      c.column_name,
      c.is_nullable,
      c.data_type,
      c.udt_name,
      c.character_maximum_length,
      c.numeric_precision,
      c.numeric_scale
    from information_schema.columns c
    where c.table_schema=%s and c.table_name=%s
    order by c.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        out = []
        for (name, is_null, data_type, udt, charlen, prec, scale) in cur.fetchall():
            out.append({
                "name": name,
                "nullable": (is_null == "YES"),
                "data_type": data_type,    # человекочитаемо
                "udt_name": udt,           # низкоуровневое (int4, numeric, timestamptz и т.п.)
                "char_len": charlen,
                "numeric_precision": prec,
                "numeric_scale": scale,
            })
        return out

def primary_key(conn, schema: str, table: str) -> List[str]:
    q = """
    select a.attname
    from pg_index i
    join pg_class t on t.oid = i.indrelid
    join pg_namespace n on n.oid = t.relnamespace
    join unnest(i.indkey) with ordinality as k(attnum, ord) on true
    join pg_attribute a on a.attrelid = t.oid and a.attnum = k.attnum
    where i.indisprimary and n.nspname=%s and t.relname=%s
    order by k.ord
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        return [r[0] for r in cur.fetchall()]

def foreign_keys(conn, schema: str, table: str) -> List[Dict[str, Any]]:
    q = """
    select
      tc.constraint_name,
      kcu.column_name,
      ccu.table_schema as foreign_table_schema,
      ccu.table_name   as foreign_table_name,
      ccu.column_name  as foreign_column_name,
      kcu.ordinal_position
    from information_schema.table_constraints tc
    join information_schema.key_column_usage kcu
      on tc.constraint_name = kcu.constraint_name
     and tc.table_schema    = kcu.table_schema
    join information_schema.constraint_column_usage ccu
      on ccu.constraint_name = tc.constraint_name
     and ccu.table_schema    = tc.table_schema
    where tc.constraint_type = 'FOREIGN KEY'
      and tc.table_schema=%s and tc.table_name=%s
    order by tc.constraint_name, kcu.ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema, table))
        rows = cur.fetchall()
    # сгруппируем по констрейнту
    fks: Dict[str, Dict[str, Any]] = {}
    for (name, col, f_sch, f_tab, f_col, pos) in rows:
        fk = fks.setdefault(name, {
            "name": name,
            "columns": [],
            "ref_table": (f_sch, f_tab),
            "ref_columns": []
        })
        fk["columns"].append(col)
        fk["ref_columns"].append(f_col)
    return list(fks.values())

def row_count(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]

# -------- Валидация против итогового профиля --------

def validate_schema_against_profile(conn, profile: Dict[str, Any], schema: str, max_errors: int = 50) -> str:
    """
    Проверяет:
      - что все сущности из профиля существуют как таблицы
      - что PK = (rec_id, idx1..idxN)
      - что все колонки профиля присутствуют (имена), и PK-колонки тоже
      - для depth>0: что есть FK (rec_id, idx1..idxN-1) -> parent(rec_id, idx1..idxN-1)
    Возвращает 'SUCCESS' либо компактный отчёт с ошибками.
    """
    errors: List[str] = []
    def err(s: str):
        if len(errors) < max_errors:
            errors.append(s)

    # индекс: путь->имя родителя для проверки FK
    path_to_name = {tuple(e.get("path", [])): e.get("name") for e in profile.get("entities", [])}
    name_to_entity = {e["name"]: e for e in profile.get("entities", [])}

    for e in profile.get("entities", []):
        t = e["name"]
        d = e.get("depth", 0)

        if not table_exists(conn, schema, t):
            err(f"table missing: {t}")
            continue

        # PK
        pk_expected = ["rec_id"] + [f"idx{i}" for i in range(1, d + 1)]
        pk_actual = primary_key(conn, schema, t)
        if pk_actual != pk_expected:
            err(f"{t}: PK mismatch, expected {pk_expected}, got {pk_actual}")

        # columns presence
        cols_db = {c["name"] for c in describe_columns(conn, schema, t)}
        cols_expected = set(pk_expected) | {c["name"] for c in e.get("columns", [])}
        missing = cols_expected - cols_db
        extra   = cols_db - cols_expected
        if missing:
            err(f"{t}: missing columns {sorted(missing)}")
        # extra не считаем ошибкой (могут быть служебные), но можно подсветить:
        # if extra: err(f"{t}: extra columns {sorted(extra)}")

        # FK для детей
        if d > 0:
            parent_path = tuple(e.get("path", []))[:-1]
            parent_name = path_to_name.get(parent_path)
            if parent_name:
                want_from = ["rec_id"] + [f"idx{i}" for i in range(1, d)]
                want_to   = want_from
                fks = foreign_keys(conn, schema, t)
                ok = any(
                    fk["ref_table"][1] == parent_name and
                    fk["columns"]      == want_from and
                    fk["ref_columns"]  == want_to
                    for fk in fks
                )
                if not ok:
                    err(f"{t}: FK to parent {parent_name} on {want_from} missing")

    if not errors:
        return "SUCCESS"
    tail = "" if len(errors) <= max_errors else f" (+{len(errors)-max_errors} more)"
    return "ERROR: " + " | ".join(errors) + tail
