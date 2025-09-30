#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Генератор DDL для ClickHouse из final_spec.
Использование:
    from ddlgenerator_clickhouse import generate_clickhouse_ddl
    ddl_sql = generate_clickhouse_ddl(final_spec, database="raw")
"""

from __future__ import annotations
import os
import re
from typing import Dict, Any, List, Optional

def _load_types_yaml(path: Optional[str] = "config/types.yaml") -> Dict[str, Any]:
    default = {
        "canonical": {
            "string": {"ch": "String"},
            "int32": {"ch": "Int32"},
            "int64": {"ch": "Int64"},
            "float64": {"ch": "Float64"},
            "decimal(p,s)": {"ch": "Decimal({p},{s})"},
            "bool": {"ch": "Bool"},
            "date": {"ch": "Date32"},
            "timestamp": {"ch": "DateTime('UTC')"},
            "timestamp64(ms)": {"ch": "DateTime64(3, 'UTC')"},
            "json": {"ch": "String"},
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

def _ch_base_type_for(canon_type: str, types_cfg: Dict[str, Any]) -> str:
    canon = canon_type.strip()
    m = _DEC_RE.match(canon)
    if m:
        p, s = m.group(1), m.group(2)
        tpl = types_cfg["canonical"]["decimal(p,s)"]["ch"]
        return tpl.format(p=p, s=s)

    mapping = types_cfg["canonical"].get(canon)
    if mapping:
        return mapping["ch"]

    syn = types_cfg.get("synonyms", {}).get(canon.lower())
    if syn and syn in types_cfg["canonical"]:
        return types_cfg["canonical"][syn]["ch"]

    return "String"

def _ch_type_for(col: Dict[str, Any], tcfg: Dict[str, Any]) -> str:
    base = _ch_base_type_for(col["type"], tcfg)
    return f"Nullable({base})" if col.get("nullable", True) else base

def _order_by_for(table: Dict[str, Any]) -> List[str]:
    ob = table.get("order_by") or []
    if ob:
        return ob
    # запасной вариант: если есть 'id' — по нему
    names = [c["name"] for c in table["columns"]]
    if "id" in names:
        return ["id"]
    # в крайнем случае — по первому столбцу
    return [names[0]] if names else ["id"]

def generate_clickhouse_ddl(
    final_spec: Dict[str, Any],
    database: str = "raw",
    types_yaml_path: Optional[str] = "config/types.yaml",
    include_unique_comments: bool = True
) -> str:
    """
    Генерирует SQL DDL для ClickHouse. Возвращает строку.
    UNIQUE-ограничения не создаются в CH; при желании можно убрать и их комментарии (include_unique_comments=False).
    """
    tcfg = _load_types_yaml(types_yaml_path)
    by_name = {t["table"]: t for t in final_spec["tables"]}
    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]

    lines: List[str] = []
    lines.append(f"CREATE DATABASE IF NOT EXISTS {database};")

    for tname in order:
        t = by_name[tname]
        fq = f"{database}.{t['table']}"

        # комментарии (описание и «уникальные» подсказки)
        if t.get("description"):
            lines.append(f"\n-- {t.get('alias') or t['name']}")
            lines.append(f"-- {t['description']}")
        else:
            lines.append(f"\n-- {t.get('alias') or t['name']}")

        if include_unique_comments:
            for u in (t.get("unique") or []):
                cols = ", ".join(u.get("columns", []))
                note = u.get("note") or ""
                if cols:
                    lines.append(f"-- UNIQUE (не применяется в CH): ({cols})  -- {note}".rstrip())

        # список столбцов
        col_lines = []
        for col in t["columns"]:
            ch_type = _ch_type_for(col, tcfg)
            col_lines.append(f"    {col['name']} {ch_type}")

        order_by = _order_by_for(t)
        order_expr = ", ".join(order_by)
        pk_expr = order_expr  # обычно PK=ORDER BY

        body = ",\n".join(col_lines)
        lines.append(
            f"CREATE TABLE IF NOT EXISTS {fq} (\n{body}\n)"
            f" ENGINE = MergeTree\nORDER BY ({order_expr})\nPRIMARY KEY ({pk_expr});"
        )

    return "\n".join(lines)
