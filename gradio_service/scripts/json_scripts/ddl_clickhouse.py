# ddl_clickhouse.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import os
import re
from typing import Any, Dict, List, Tuple

# -------- YAML loader with fallback --------
_FALLBACK_TYPES_YAML = """
canonical:
  string:        { pg: text,               ch: String,                  py: str }
  int32:         { pg: integer,            ch: Int32,                   py: int }
  int64:         { pg: bigint,             ch: Int64,                   py: int }
  float64:       { pg: double precision,   ch: Float64,                 py: float }
  decimal(p,s):  { pg: numeric({p},{s}),   ch: Decimal({p},{s}),        py: decimal.Decimal }
  bool:          { pg: boolean,            ch: Bool,                    py: bool }
  date:          { pg: date,               ch: Date32,                  py: datetime.date }
  timestamp:     { pg: timestamptz,        ch: "DateTime('UTC')",       py: datetime.datetime }
  timestamp64(ms): { pg: timestamptz,      ch: "DateTime64(3, 'UTC')",  py: datetime.datetime }
  json:          { pg: jsonb,              ch: String,                  py: typing.Any }
synonyms:
  text: string
  varchar: string
  bigint: int64
  integer: int32
  int4: int32
  int8: int64
  double: float64
  double precision: float64
  numeric: decimal(p,s)
  decimal: decimal(p,s)
  timestamptz: timestamp
  timestampz: timestamp
  datetime: timestamp
  datetime64: timestamp64(ms)
  jsonb: json
  uint8: bool
"""

def _load_types_yaml(path: str) -> Dict[str, Any]:
    try:
        import yaml  # type: ignore
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception:
        try:
            import yaml  # type: ignore
            return yaml.safe_load(_FALLBACK_TYPES_YAML)
        except Exception:
            raise RuntimeError("Failed to load YAML types mapping; please install PyYAML or provide JSON mapping")

# -------- helpers --------
def _q_ch(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

_DEC_DEFAULT = (38, 10)

def _canon_to_ch(canon: str, types_cfg: Dict[str, Any]) -> str:
    canonical = types_cfg.get("canonical", {})
    m = re.match(r"^decimal\((\d+)\s*,\s*(\d+)\)$", canon, flags=re.IGNORECASE)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        tmpl = canonical.get("decimal(p,s)", {}).get("ch", "Decimal({p},{s})")
        return tmpl.format(p=p, s=s)

    if canon.lower().startswith("decimal(") and "," in canon:
        nums = re.findall(r"\d+", canon)
        if len(nums) == 2:
            p, s = int(nums[0]), int(nums[1])
        else:
            p, s = _DEC_DEFAULT
        tmpl = canonical.get("decimal(p,s)", {}).get("ch", "Decimal({p},{s})")
        return tmpl.format(p=p, s=s)

    t = canonical.get(canon, {}).get("ch")
    if t:
        return t

    syn = types_cfg.get("synonyms", {})
    base = syn.get(canon.lower())
    if base:
        return canonical.get(base, {}).get("ch", "String")

    return "String"

def _pk_cols(depth: int) -> List[str]:
    return ["rec_id"] + [f"idx{i}" for i in range(1, depth + 1)]

# -------- main --------
def emit_ddl_ch(profile: Dict[str, Any], types_yaml_path: str = "config/types.yaml",
                database: str | None = None, engine: str = "MergeTree") -> str:
    """
    Генерирует DDL для ClickHouse (MergeTree).
    - rec_id: UInt64
    - idxN:   UInt32
    - Nullable(...) для nullable-колонок
    - ORDER BY (rec_id, idx...)  (PRIMARY KEY совпадает с ORDER BY по умолчанию)
    Параметры:
      database: если указан, добавляется префикс БД: `CREATE TABLE db.`name``
      engine:   имя движка (по умолчанию MergeTree)
    """
    types_cfg = _load_types_yaml(types_yaml_path)

    ddls: List[str] = []
    for e in profile.get("entities", []):
        tname = e.get("name")
        depth = e.get("depth", 0)

        # Полные имена: db.`table` или просто `table`
        full_table = (f"{_q_ch(database)}." if database else "") + _q_ch(tname)

        cols_sql = []
        # PK столбцы
        cols_sql.append(f"{_q_ch('rec_id')} UInt64")
        for i in range(1, depth + 1):
            cols_sql.append(f"{_q_ch(f'idx{i}')} UInt32")

        # Данные
        for c in e.get("columns", []):
            colname = c.get("name")
            ctype = c.get("type", "string")
            nullable = c.get("nullable", True)
            ch_type = _canon_to_ch(ctype, types_cfg)
            if nullable:
                ch_type = f"Nullable({ch_type})"
            cols_sql.append(f"{_q_ch(colname)} {ch_type}")

        order_by = ", ".join(_q_ch(c) for c in _pk_cols(depth))
        ddl = f"CREATE TABLE IF NOT EXISTS {full_table} (\n  " + ",\n  ".join(cols_sql) + f"\n)\nENGINE = {engine}\nORDER BY ({order_by});\n"
        ddls.append(ddl)

    return "\n".join(ddls)
