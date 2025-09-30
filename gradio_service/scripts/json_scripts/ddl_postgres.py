# ddl_postgres.py
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
        # Fallback to embedded mapping
        try:
            import yaml  # type: ignore
            return yaml.safe_load(_FALLBACK_TYPES_YAML)
        except Exception:
            # на самый крайний случай — минимальный json-парсер yaml (ибо структура простая)
            import json as _json
            # грубая конверсия: заменим одиночные кавычки вокруг типов CH
            text = _FALLBACK_TYPES_YAML.replace("'", '"')
            text = "\n".join(line for line in text.splitlines() if not line.strip().startswith("#"))
            # это не универсальный YAML->JSON, но для нашей структуры хватит в fallback'е
            raise RuntimeError("Failed to load YAML types mapping; please install PyYAML or provide JSON mapping")

# -------- helpers --------
def _q_pg(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

_DEC_DEFAULT = (38, 10)  # если p,s не заданы явно

def _canon_to_pg(canon: str, types_cfg: Dict[str, Any]) -> str:
    """
    Преобразует канонический тип в PG-тип с учётом decimal(p,s).
    Ожидается, что canon — один из ключей canonical в YAML (включая 'decimal(p,s)').
    """
    canonical = types_cfg.get("canonical", {})
    # decimal(p,s) со значениями?
    m = re.match(r"^decimal\((\d+)\s*,\s*(\d+)\)$", canon, flags=re.IGNORECASE)
    if m:
        p, s = int(m.group(1)), int(m.group(2))
        tmpl = canonical.get("decimal(p,s)", {}).get("pg", "numeric({p},{s})")
        return tmpl.format(p=p, s=s)

    if canon.lower().startswith("decimal(") and "," in canon:
        # на всякий случай другие варианты — попробуем выцепить, иначе дефолт
        nums = re.findall(r"\d+", canon)
        if len(nums) == 2:
            p, s = int(nums[0]), int(nums[1])
        else:
            p, s = _DEC_DEFAULT
        tmpl = canonical.get("decimal(p,s)", {}).get("pg", "numeric({p},{s})")
        return tmpl.format(p=p, s=s)

    # обычные типы
    t = canonical.get(canon, {}).get("pg")
    if t:
        return t

    # синонимы
    syn = types_cfg.get("synonyms", {})
    base = syn.get(canon.lower())
    if base:
        return canonical.get(base, {}).get("pg", "text")

    # fallback
    return "text"

def _pk_cols(depth: int) -> List[str]:
    return ["rec_id"] + [f"idx{i}" for i in range(1, depth + 1)]

def _entity_fk_clause(entity: Dict[str, Any], path_to_name: Dict[Tuple[str, ...], str]) -> str:
    depth = entity.get("depth", 0)
    if depth <= 0:
        return ""
    # from (rec_id, idx1..idxN-1) -> parent(rec_id, idx1..idxN-1)
    from_cols = ["rec_id"] + [f"idx{i}" for i in range(1, depth)]
    parent_path = tuple(entity.get("path", []))[:-1]
    parent_name = path_to_name.get(parent_path)
    if not parent_name:
        return ""
    fk_name = f'fk_{entity["name"]}_to_{parent_name}'
    return f',\n  CONSTRAINT {_q_pg(fk_name)} FOREIGN KEY ({", ".join(_q_pg(c) for c in from_cols)}) REFERENCES {_q_pg(parent_name)} ({", ".join(_q_pg(c) for c in from_cols)})'

# -------- main --------
def emit_ddl_pg(profile: Dict[str, Any], types_yaml_path: str = "config/types.yaml") -> str:
    """
    Генерирует DDL для PostgreSQL по итоговому профилю.
    Возвращает одну строку с набором CREATE TABLE ...;
    """
    types_cfg = _load_types_yaml(types_yaml_path)

    # индекс path->name для связей
    path_to_name = {tuple(e.get("path", [])): e.get("name") for e in profile.get("entities", [])}

    ddls: List[str] = []
    for e in profile.get("entities", []):
        tname = e.get("name")
        depth = e.get("depth", 0)
        pk_cols = _pk_cols(depth)

        # PK столбцы
        cols_sql = []
        cols_sql.append(f"{_q_pg('rec_id')} BIGINT NOT NULL")
        for i in range(1, depth + 1):
            cols_sql.append(f"{_q_pg(f'idx{i}')} INTEGER NOT NULL")

        # данные-колонки
        for c in e.get("columns", []):
            colname = c.get("name")
            ctype = c.get("type", "string")
            nullable = c.get("nullable", True)

            # поддержка decimal(p,s) по полю 'type', если нужно — можно расширить c['precision']/c['scale']
            pg_type = _canon_to_pg(ctype, types_cfg)
            null_sql = "" if nullable else " NOT NULL"
            cols_sql.append(f"{_q_pg(colname)} {pg_type}{null_sql}")

        # PK constraint
        pk_sql = f',\n  CONSTRAINT {_q_pg(tname + "__pk")} PRIMARY KEY ({", ".join(_q_pg(c) for c in pk_cols)})'

        # FK (если есть parent)
        fk_sql = _entity_fk_clause(e, path_to_name)

        ddl = f"CREATE TABLE IF NOT EXISTS {_q_pg(tname)} (\n  " + ",\n  ".join(cols_sql) + pk_sql + fk_sql + "\n);\n"
        ddls.append(ddl)

    return "\n".join(ddls)
