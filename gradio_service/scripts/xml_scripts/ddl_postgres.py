#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ddl_postgres.py

Генерирует PostgreSQL DDL из final_spec (см. модуль final_profile).
Вызов из кода:
    from ddl_postgres import generate_postgres_ddl
    sql = generate_postgres_ddl(final_spec, schema="public")
"""

from __future__ import annotations
import os
import re
import json
from typing import Dict, Any, Optional, List

# --- fallback types in case config/types.yaml is unavailable
DEFAULT_TYPES = {
    "canonical": {
        "string":       {"pg": "text",               "ch": "String",             "py": "str"},
        "int32":        {"pg": "integer",            "ch": "Int32",              "py": "int"},
        "int64":        {"pg": "bigint",             "ch": "Int64",              "py": "int"},
        "float64":      {"pg": "double precision",   "ch": "Float64",            "py": "float"},
        "decimal(p,s)": {"pg": "numeric({p},{s})",   "ch": "Decimal({p},{s})",   "py": "decimal.Decimal"},
        "bool":         {"pg": "boolean",            "ch": "Bool",               "py": "bool"},
        "date":         {"pg": "date",               "ch": "Date32",             "py": "datetime.date"},
        "timestamp":    {"pg": "timestamptz",        "ch": "DateTime('UTC')",    "py": "datetime.datetime"},
        "timestamp64(ms)": {"pg": "timestamptz",     "ch": "DateTime64(3, 'UTC')","py": "datetime.datetime"},
        "json":         {"pg": "jsonb",              "ch": "String",             "py": "typing.Any"},
    },
    "synonyms": {
        "text": "string",
        "varchar": "string",
        "bigint": "int64",
        "integer": "int32",
        "int4": "int32",
        "int8": "int64",
        "double": "float64",
        "double precision": "float64",
        "numeric": "decimal(p,s)",
        "decimal": "decimal(p,s)",
        "timestamptz": "timestamp",
        "timestampz": "timestamp",
        "datetime": "timestamp",
        "datetime64": "timestamp64(ms)",
        "jsonb": "json",
        "uint8": "bool",
    }
}

def _load_types_yaml(path: Optional[str]) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return DEFAULT_TYPES
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        if "canonical" in y:
            return y
    except Exception:
        pass
    return DEFAULT_TYPES

_DEC_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.I)

def _canon_name(canon_or_syn: str, types_cfg: Dict[str, Any]) -> str:
    s = canon_or_syn.strip().lower()
    syn = types_cfg.get("synonyms", {})
    # нормализуем decimal без параметров
    if s == "decimal":
        s = "decimal(p,s)"
    return syn.get(s, s)

def _pg_type(canon_type: str, types_cfg: Dict[str, Any]) -> str:
    ct = _canon_name(canon_type, types_cfg)
    if m := _DEC_RE.match(ct):
        # уже decimal(p,s) с конкретными числами — вернём numeric(p,s)
        p, s = m.group(1), m.group(2)
        tmpl = types_cfg["canonical"]["decimal(p,s)"]["pg"]
        return tmpl.format(p=p, s=s)
    if ct.startswith("decimal(") and ct.endswith(")"):
        # decimal(precision,scale)
        m2 = re.match(r"^decimal\((\d+),\s*(\d+)\)$", ct, re.I)
        if m2:
            p, s = m2.group(1), m2.group(2)
            tmpl = types_cfg["canonical"]["decimal(p,s)"]["pg"]
            return tmpl.format(p=p, s=s)
    # обычный случай
    canon = types_cfg["canonical"]
    if ct in canon:
        return canon[ct]["pg"]
    # на крайний случай — text
    return canon["string"]["pg"]

def _q(ident: str) -> str:
    # идентификаторы у нас snake_case, без кавычек будет ок; оставим без кавычек
    return ident

def _limit_name(name: str, maxlen: int = 63) -> str:
    return name[:maxlen]

def _table_create_sql_pg(tbl: dict, types_cfg: Dict[str, Any], schema: str) -> str:
    full = f"{_q(schema)}.{_q(tbl['table'])}" if schema else _q(tbl["table"])
    lines: List[str] = []
    # Комментарий-описание
    if tbl.get("title") or tbl.get("description"):
        lines.append(f"-- {tbl.get('title','').strip()}")
        if tbl.get("description"):
            for ln in tbl["description"].splitlines():
                lines.append(f"-- {ln}")

    col_lines = []
    for c in tbl["columns"]:
        ctype = _pg_type(c["type"], types_cfg)
        null_sql = " NOT NULL" if (not c.get("nullable", True)) else ""
        col_lines.append(f"    {_q(c['name'])} {ctype}{null_sql}")

    # первичный ключ
    pk_cols = tbl.get("primary_key", {}).get("columns", ["id"])
    if pk_cols:
        pk_name = _limit_name(f"pk_{tbl['table']}")
        col_lines.append(f"    CONSTRAINT {pk_name} PRIMARY KEY ({', '.join(_q(c) for c in pk_cols)})")

    # уникальные ограничения
    for i, uq in enumerate(tbl.get("unique", []) or []):
        cols = uq.get("columns", [])
        if not cols:
            continue
        uq_name = _limit_name(f"uq_{tbl['table']}_{i+1}")
        col_lines.append(f"    CONSTRAINT {uq_name} UNIQUE ({', '.join(_q(c) for c in cols)})")

    # внешние ключи
    for c in tbl["columns"]:
        if c.get("role") == "fk_parent":
            ref_table = c.get("ref_table")
            ref_col = c.get("ref_column", "id")
            fk_name = _limit_name(f"fk_{tbl['table']}_{c['name']}")
            ref_full = f"{_q(schema)}.{_q(ref_table)}" if schema else _q(ref_table)
            col_lines.append(
                f"    CONSTRAINT {fk_name} FOREIGN KEY ({_q(c['name'])}) "
                f"REFERENCES {ref_full}({_q(ref_col)})"
            )

    create = [f"CREATE TABLE IF NOT EXISTS {full} (\n" + ",\n".join(col_lines) + "\n);"]

    # индексы на FK (опционально полезно)
    for c in tbl["columns"]:
        if c.get("role") == "fk_parent":
            idx = _limit_name(f"ix_{tbl['table']}_{c['name']}")
            create.append(f"CREATE INDEX IF NOT EXISTS {idx} ON {full}({_q(c['name'])});")

    return "\n".join(create)

def generate_postgres_ddl(final_spec: Dict[str, Any], schema: str = "public", types_yaml_path: str = "config/types.yaml") -> str:
    """
    Возвращает строку со всем DDL для PostgreSQL по final_spec.
    """
    types_cfg = _load_types_yaml(types_yaml_path)
    parts: List[str] = []

    if schema:
        parts.append(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")

    # порядок создания — из load_order
    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]
    tmap = {t["table"]: t for t in final_spec["tables"]}

    for tname in order:
        t = tmap[tname]
        parts.append(_table_create_sql_pg(t, types_cfg, schema))
        parts.append("")  # пустая строка-разделитель

    return "\n".join(parts).strip()
