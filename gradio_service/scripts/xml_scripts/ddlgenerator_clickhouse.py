#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Генератор DDL для ClickHouse из final_spec.
Использование:
    from ddlgenerator_clickhouse import generate_clickhouse_ddl
    ddl_sql = generate_clickhouse_ddl(
        final_spec,
        database="analytics",
        decimal_min_precision=28,
        force_nullable=True,
        include_unique_comments=True,
    )
"""

from __future__ import annotations
import os
import re
from typing import Dict, Any, List, Optional

# ---------- типы ----------

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

def _widen_decimal(canon_type: str, min_p: int) -> str:
    """
    Возвращает канонический decimal с расширенной точностью p>=min_p (и p>=s+1).
    Если тип не decimal(...) — вернёт исходную строку.
    """
    m = _DEC_RE.match(canon_type.strip())
    if not m:
        return canon_type
    p, s = int(m.group(1)), int(m.group(2))
    p2 = max(p, s + 1, int(min_p))
    return f"decimal({p2},{s})"

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

def _ch_type_for(col: Dict[str, Any], tcfg: Dict[str, Any], force_nullable: bool) -> str:
    canon = col["type"]
    base = _ch_base_type_for(canon, tcfg)
    if force_nullable and col.get("name") != "id":
        return f"Nullable({base})"
    return base

def _order_by_for(table: Dict[str, Any]) -> List[str]:
    ob = table.get("order_by") or []
    if ob:
        return ob
    names = [c["name"] for c in table["columns"]]
    if "id" in names:
        return ["id"]
    return [names[0]] if names else ["id"]

# ---------- генератор ----------

def generate_clickhouse_ddl(
    final_spec: Dict[str, Any],
    database: str = "raw",
    types_yaml_path: Optional[str] = "config/types.yaml",
    include_unique_comments: bool = True,
    decimal_min_precision: int = 28,
    force_nullable: bool = True,
) -> str:
    """
    Возвращает строку с DDL ClickHouse.
    - decimal_min_precision: расширяет decimal(p,s) до p>=min
    - force_nullable: все пользовательские столбцы делаем Nullable(...), кроме суррогатного id
    - ключи сортировки оборачиваются в assumeNotNull(...)
    """
    tcfg = _load_types_yaml(types_yaml_path)
    by_name = {t["table"]: t for t in final_spec["tables"]}
    order = final_spec.get("load_order") or [t["table"] for t in final_spec["tables"]]

    def _canon_for(col: Dict[str, Any]) -> str:
        ctype = col["type"]
        if _DEC_RE.match(ctype):
            return _widen_decimal(ctype, decimal_min_precision)
        return ctype

    def _to_ch_type(col: Dict[str, Any]) -> str:
        # заменим col["type"] на расширенный decimal, если нужно
        c = dict(col)
        c["type"] = _canon_for(col)
        return _ch_type_for(c, tcfg, force_nullable=force_nullable)

    lines: List[str] = []
    lines.append(f"CREATE DATABASE IF NOT EXISTS {database};")

    for tname in order:
        t = by_name[tname]
        fq = f"{database}.{t['table']}"

        # комментарии
        lines.append("")  # пустая строка
        lines.append(f"-- {t.get('alias') or t['name']}")
        if t.get("description"):
            lines.append(f"-- {t['description']}")
        if include_unique_comments:
            for u in (t.get("unique") or []):
                cols = ", ".join(u.get("columns", []))
                note = u.get("note") or ""
                if cols:
                    lines.append(f"-- UNIQUE (не применяется в CH): ({cols})  -- {note}".rstrip())

        # столбцы
        col_defs: List[str] = []
        for col in t["columns"]:
            ch_type = _to_ch_type(col)
            col_defs.append(f"    {col['name']} {ch_type}")

        # ключи сортировки
        order_by_cols = _order_by_for(t)
        # wrap assumeNotNull для совместимости с Nullable
        order_exprs = [f"assumeNotNull({c})" for c in order_by_cols]
        order_sql = ", ".join(order_exprs)
        pk_sql = order_sql  # обычно PK=ORDER BY

        body = ",\n".join(col_defs)
        lines.append(
            f"CREATE TABLE IF NOT EXISTS {fq} (\n{body}\n)"
            f" ENGINE = MergeTree\nORDER BY ({order_sql})\nPRIMARY KEY ({pk_sql});"
        )

    return "\n".join(lines)
