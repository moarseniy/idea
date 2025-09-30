#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ddl_clickhouse.py

Генерирует ClickHouse DDL из final_spec (см. модуль final_profile).
Вызов из кода:
    from ddl_clickhouse import generate_clickhouse_ddl
    sql = generate_clickhouse_ddl(final_spec, database="raw")
"""

from __future__ import annotations
import os
import re
from typing import Dict, Any, Optional, List

# --- fallback types
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
    if s == "decimal":
        s = "decimal(p,s)"
    return syn.get(s, s)

def _ch_type(canon_type: str, types_cfg: Dict[str, Any], nullable: bool) -> str:
    ct = _canon_name(canon_type, types_cfg)
    ch = types_cfg["canonical"]

    if ct.startswith("decimal(") and ct.endswith(")"):
        m = _DEC_RE.match(ct)
        if m:
            p, s = m.group(1), m.group(2)
            base = ch["decimal(p,s)"]["ch"].format(p=p, s=s)
            return f"Nullable({base})" if nullable else base

    if ct in ch:
        base = ch[ct]["ch"]
        return f"Nullable({base})" if nullable else base

    base = ch["string"]["ch"]
    return f"Nullable({base})" if nullable else base

def _q(ident: str) -> str:
    # ClickHouse допускает неэкранированные snake_case
    return ident

def _table_create_sql_ch(tbl: dict, types_cfg: Dict[str, Any], database: Optional[str]) -> str:
    dbdot = f"{_q(database)}." if database else ""
    full = f"{dbdot}{_q(tbl['table'])}"
    lines: List[str] = []

    # комменты
    if tbl.get("title"):
        lines.append(f"-- {tbl['title']}")
    if tbl.get("description"):
        for ln in tbl["description"].splitlines():
            lines.append(f"-- {ln}")

    # столбцы
    col_lines = []
    for c in tbl["columns"]:
        ch_type = _ch_type(c["type"], types_cfg, nullable=c.get("nullable", True))
        # В CH нет NOT NULL/NULL — используется Nullable(T)
        # Можно добавить COMMENT для колонок, но оставим текстовым комментом
        col_lines.append(f"    {_q(c['name'])} {ch_type}")

    # ORDER BY — используем order_by, иначе (id)
    order_by = tbl.get("order_by") or ["id"]
    order_sql = ", ".join(_q(c) for c in order_by)

    # PRIMARY KEY в CH = подмножество ORDER BY, примем равным ORDER BY
    pk_sql = order_sql

    create = [
        f"CREATE TABLE IF NOT EXISTS {full} (",
        ",\n".join(col_lines),
        f") ENGINE = MergeTree",
        f"ORDER BY ({order_sql})",
        f"PRIMARY KEY ({pk_sql});"
    ]

    # FKs/UNIQUE не поддерживаются — добавим коммент-напоминание
    for c in tbl["columns"]:
        if c.get("role") == "fk_parent":
            lines.append(f"-- FK (не применяется в CH): {c['name']} -> {tbl['parent'].get('table')}({c.get('ref_column','id')})")

    for uq in tbl.get("unique", []) or []:
        cols = ", ".join(uq.get("columns", []))
        lines.append(f"-- UNIQUE (не применяется в CH): ({cols})  -- {uq.get('note','')}")

    return "\n".join(lines + create)

def generate_clickhouse_ddl(final_spec: Dict[str, Any], database: Optional[str] = None, types_yaml_path: str = "config/types.yaml") -> str:
    """
    Возвращает строку со всем DDL для ClickHouse по final_spec.
    """
    types_cfg = _load_types_yaml(types_yaml_path)
    parts: List[str] = []

    if database:
        parts.append(f"CREATE DATABASE IF NOT EXISTS {_q(database)};")
        parts.append("")

    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]
    tmap = {t["table"]: t for t in final_spec["tables"]}

    for tname in order:
        t = tmap[tname]
        parts.append(_table_create_sql_ch(t, types_cfg, database))
        parts.append("")

    return "\n".join(parts).strip()
