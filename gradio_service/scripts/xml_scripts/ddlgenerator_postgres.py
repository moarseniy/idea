#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Генератор DDL для PostgreSQL из final_spec.
Использование:
    from ddlgenerator_postgres import generate_postgres_ddl
    ddl_sql = generate_postgres_ddl(final_spec, schema="public", emit_unique=False)
"""

from __future__ import annotations
import os
import re
import json
from typing import Dict, Any, List, Optional

# --- типы ---

def _load_types_yaml(path: Optional[str] = "config/types.yaml") -> Dict[str, Any]:
    default = {
        "canonical": {
            "string": {"pg": "text"},
            "int32": {"pg": "integer"},
            "int64": {"pg": "bigint"},
            "float64": {"pg": "double precision"},
            "decimal(p,s)": {"pg": "numeric({p},{s})"},
            "bool": {"pg": "boolean"},
            "date": {"pg": "date"},
            "timestamp": {"pg": "timestamptz"},
            "timestamp64(ms)": {"pg": "timestamptz"},
            "json": {"pg": "jsonb"},
        },
        "synonyms": {}
    }
    if not path or not os.path.exists(path):
        return default
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            y = yaml.safe_load(f) or {}
        return y if "canonical" in y else default
    except Exception:
        return default

_DEC_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$", re.I)

def _pg_type_for(canon_type: str, types_cfg: Dict[str, Any]) -> str:
    canon = canon_type.strip()
    m = _DEC_RE.match(canon)
    if m:
        p, s = m.group(1), m.group(2)
        tpl = types_cfg["canonical"]["decimal(p,s)"]["pg"]
        return tpl.format(p=p, s=s)

    mapping = types_cfg["canonical"].get(canon)
    if mapping:
        return mapping["pg"]

    # поддержка возможных «синонимов»
    syn = types_cfg.get("synonyms", {}).get(canon.lower())
    if syn and syn in types_cfg["canonical"]:
        return types_cfg["canonical"][syn]["pg"]

    # запасной вариант
    return "text"

# --- утилиты ---

def _qident(*parts: str) -> str:
    """schema, table -> schema.table без кавычек (имена уже snake_case)."""
    return ".".join(parts)

def _column_line(col: Dict[str, Any], tcfg: Dict[str, Any]) -> str:
    typ = _pg_type_for(col["type"], tcfg)
    nn = "" if col.get("nullable", True) else " NOT NULL"
    return f'    {col["name"]} {typ}{nn}'

def _primary_key_clause(table: Dict[str, Any]) -> str:
    pk_cols = table.get("primary_key", {}).get("columns", []) or []
    if not pk_cols:
        return ""
    cols = ", ".join(pk_cols)
    return f"    CONSTRAINT pk_{table['table']} PRIMARY KEY ({cols})"

def _fk_clauses(table: Dict[str, Any], schema: str) -> List[str]:
    out = []
    for col in table["columns"]:
        if col.get("role") == "fk_parent":
            fkcol = col["name"]
            ref_table = col["ref_table"]
            ref_col = col.get("ref_column", "id")
            out.append(
                f"    CONSTRAINT fk_{table['table']}_{fkcol} "
                f"FOREIGN KEY ({fkcol}) REFERENCES {_qident(schema, ref_table)}({ref_col})"
            )
    return out

def _fk_indexes(table: Dict[str, Any], schema: str) -> List[str]:
    stmts = []
    for col in table["columns"]:
        if col.get("role") == "fk_parent":
            fkcol = col["name"]
            stmts.append(
                f"CREATE INDEX IF NOT EXISTS ix_{table['table']}_{fkcol} "
                f"ON {_qident(schema, table['table'])}({fkcol});"
            )
    return stmts

def _unique_clauses(table: Dict[str, Any]) -> List[str]:
    out = []
    uniques = table.get("unique", []) or []
    for i, u in enumerate(uniques, start=1):
        cols = ", ".join(u.get("columns", []))
        if not cols:
            continue
        out.append(f"    CONSTRAINT uq_{table['table']}_{i} UNIQUE ({cols})")
    return out

def _table_by_name(spec: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {t["table"]: t for t in spec["tables"]}

# --- основной генератор ---

def generate_postgres_ddl(
    final_spec: Dict[str, Any],
    schema: str = "public",
    emit_unique: bool = False,
    types_yaml_path: Optional[str] = "config/types.yaml"
) -> str:
    """
    Генерирует SQL DDL для PostgreSQL. Возвращает строку.
    - emit_unique=False -> НЕ выводить UNIQUE-ограничения (по просьбе :)
    """
    tcfg = _load_types_yaml(types_yaml_path)
    by_name = _table_by_name(final_spec)

    lines: List[str] = []
    lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # соблюдать порядок (для FK)
    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]

    for tname in order:
        table = by_name[tname]
        fq = _qident(schema, table["table"])

        col_lines = []
        for col in table["columns"]:
            col_lines.append(_column_line(col, tcfg))

        # constraints
        cons: List[str] = []
        pk = _primary_key_clause(table)
        if pk:
            cons.append(pk)

        if emit_unique:
            cons.extend(_unique_clauses(table))

        cons.extend(_fk_clauses(table, schema))

        # объединяем в CREATE TABLE
        all_lines = col_lines + ([""] if cons else []) + cons
        body = ",\n".join(all_lines)

        lines.append(f"CREATE TABLE IF NOT EXISTS {fq} (\n{body}\n);")

        # FK индексы
        lines.extend(_fk_indexes(table, schema))

    return "\n".join(lines)
