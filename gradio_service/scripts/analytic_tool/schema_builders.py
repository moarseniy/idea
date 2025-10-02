"""
Генераторы схем по JSON‑профилю (v2 с правками):

Что изменено по замечаниям
--------------------------
1) ClickHouse ORDER BY: по умолчанию ORDER BY tuple() (никаких id).
   - Параметр order_by: "tuple" (дефолт) | "auto" (подбор по профилю) | list[str].
   - При явном списке имён выполняется валидация — если колонки нет в профиле, бросаем ValueError.
2) Единая точность времени в CH: флаг coerce_timestamp64=True заставляет канонический
   timestamp маппиться как DateTime64(3, 'UTC') вместо DateTime('UTC').
   Канонический timestamp64(ms) — без изменений. DBML остаётся на канонических типах.
3) Сохранены прежние возможности: маппинг типов из config/types.yaml, NOT NULL в PG,
   Nullable(T) в CH, минималистичный DBML из каноники.

Функции
-------
- ddl_clickhouse_from_profile(profile, *, table=None, database=None,
      types_yaml_path="config/types.yaml", engine="MergeTree",
      order_by: str|list[str] = "tuple", partition_by: str|list[str]|None = None,
      coerce_timestamp64: bool = True, strict: bool = True) -> str
- ddl_postgres_from_profile(profile, *, table=None, schema="public",
      types_yaml_path="config/types.yaml") -> str
- dbml_from_profile(profile, *, table=None) -> str   # типы — КАНОНИЧЕСКИЕ

Аргумент profile — dict с профилем или JSON-строка от профайлера.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Iterable

try:
    import yaml  # type: ignore
except Exception as e:
    raise RuntimeError("PyYAML is required: pip install pyyaml") from e

# ---------------------------
# Загрузка профиля + типов
# ---------------------------

def _as_profile(profile: Any) -> Dict[str, Any]:
    if isinstance(profile, str):
        return json.loads(profile)
    if isinstance(profile, dict):
        return profile
    raise TypeError("profile must be dict or JSON string")

class _TypeSystem:
    def __init__(self, cfg: Dict[str, Any]):
        self.canonical: Dict[str, Dict[str, str]] = cfg.get("canonical", {})
        self.synonyms: Dict[str, str] = {k.lower(): v for k, v in cfg.get("synonyms", {}).items()}

    @classmethod
    def load(cls, path: str) -> "_TypeSystem":
        with open(path, "r", encoding="utf-8") as f:
            return cls(yaml.safe_load(f) or {})

    def _resolve_key(self, name: str) -> str:
        if name in self.canonical:
            return name
        alt = self.synonyms.get(name.lower())
        if alt and alt in self.canonical:
            return alt
        raise KeyError(f"Unknown canonical type: {name}")

    def map(self, canonical: str, backend: str) -> Tuple[str, Dict[str, int]]:
        """Вернуть (tpl, params) для backend (pg/ch) по каноническому типу из профиля.
        Поддерживает decimal(p,s) и фиксированные ключи (timestamp64(ms)).
        """
        m = re.match(r"^(?P<base>\w+)\((?P<a>[^)]*)\)$", canonical)
        params: Dict[str, int] = {}
        if m:
            base = m.group("base")
            inside = m.group("a")
            if base.lower() == "decimal":
                mm = re.match(r"^(\d+)\s*,\s*(\d+)$", inside)
                if not mm:
                    raise ValueError(f"Bad decimal args in '{canonical}'")
                params = {"p": int(mm.group(1)), "s": int(mm.group(2))}
                key = "decimal(p,s)"
            else:
                key = canonical
        else:
            key = canonical
        key = self._resolve_key(key)
        tpl = self.canonical[key][backend]
        return tpl, params

# ---------------------------
# Хелперы форматирования
# ---------------------------

def _quote_pg(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'

def _quote_ch(ident: str) -> str:
    return '`' + ident.replace('`', '``') + '`'

_IDENT_OK_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _quote_dbml(ident: str) -> str:
    if _IDENT_OK_RE.match(ident):
        return ident
    return '`' + ident.replace('`', '\\`') + '`'

# ---------------------------
# Общие хелперы
# ---------------------------

def _col_names(prof: Dict[str, Any]) -> List[str]:
    return [c["name"] for c in prof.get("columns", [])]

def _ensure_columns_exist(cols: Iterable[str], existing: Iterable[str]):
    s_exist = set(existing)
    missing = [c for c in cols if c not in s_exist]
    if missing:
        raise ValueError(f"Columns not in profile: {', '.join(missing)}")

_PREFERRED_TIME_NAMES = [
    "created", "create_time", "inserted_at", "updated_at", "update_timestamp",
    "event_time", "event_ts", "ts", "timestamp"
]

def _choose_ch_order_by(prof: Dict[str, Any]) -> List[str]:
    cols = prof.get("columns", [])
    for c in cols:
        t = (c.get("type", {}) or {}).get("canonical")
        if t in ("int32", "int64") and not c.get("nullable", True):
            return [c["name"]]
    time_candidates = [
        c for c in cols
        if (c.get("type", {}) or {}).get("canonical") in ("timestamp", "timestamp64(ms)")
        and not c.get("nullable", True)
    ]
    if time_candidates:
        def score(c):
            nm = c["name"].lower()
            for i, pat in enumerate(_PREFERRED_TIME_NAMES):
                if pat in nm:
                    return i
            return 9999
        time_candidates.sort(key=score)
        return [time_candidates[0]["name"]]
    return []

# ---------------------------
# ClickHouse DDL
# ---------------------------

def ddl_clickhouse_from_profile(
    profile: Any,
    *,
    table: Optional[str] = None,
    database: Optional[str] = None,
    types_yaml_path: str = os.path.join("config", "types.yaml"),
    engine: str = "MergeTree",
    order_by: "str|List[str]" = "tuple",
    partition_by: "str|List[str]|None" = None,
    coerce_timestamp64: bool = True,
    strict: bool = True,
) -> str:
    """
    Построить DDL для ClickHouse.
    - order_by: "tuple" | "auto" | list[str]
    - partition_by: None | str | list[str]
    - coerce_timestamp64: если True, канонический 'timestamp' маппится как DateTime64(3,'UTC')
    - strict: валидировать, что колонки в order_by/partition_by существуют
    """
    prof = _as_profile(profile)
    ts = _TypeSystem.load(types_yaml_path)

    tname = table or prof.get("entity", {}).get("name") or "table1"
    fq = _quote_ch(tname) if not database else f"{_quote_ch(database)}.{_quote_ch(tname)}"

    existing = _col_names(prof)

    cols_sql: List[str] = []
    for col in prof.get("columns", []):
        name = col["name"]
        canon = (col.get("type", {}) or {}).get("canonical") or "string"
        if coerce_timestamp64 and canon == "timestamp":
            tpl, params = ts.map("timestamp64(ms)", backend="ch")
        else:
            tpl, params = ts.map(canon, backend="ch")
        ch_type = tpl.format(**params) if params else tpl
        if col.get("nullable", False):
            ch_type = f"Nullable({ch_type})"
        cols_sql.append(f"  {_quote_ch(name)} {ch_type}")

    if isinstance(order_by, list):
        if strict:
            _ensure_columns_exist(order_by, existing)
        order_expr = "(" + ", ".join(_quote_ch(c) for c in order_by) + ")" if order_by else "tuple()"
    elif isinstance(order_by, str):
        if order_by == "auto":
            chosen = _choose_ch_order_by(prof)
            order_expr = "(" + ", ".join(_quote_ch(c) for c in chosen) + ")" if chosen else "tuple()"
        else:
            order_expr = "tuple()"
    else:
        order_expr = "tuple()"

    part_clause = None
    if partition_by is not None:
        if isinstance(partition_by, list):
            if strict:
                _ensure_columns_exist(partition_by, existing)
            part_clause = "PARTITION BY (" + ", ".join(_quote_ch(c) for c in partition_by) + ")"
        else:
            part_clause = f"PARTITION BY {partition_by}"

    ddl = [
        f"CREATE TABLE IF NOT EXISTS {fq} (",
        ",\n".join(cols_sql),
        ")",
        f"ENGINE = {engine}",
        f"ORDER BY {order_expr}",
    ]
    if part_clause:
        ddl.append(part_clause)
    ddl.append(";")
    return "\n".join(ddl)

# ---------------------------
# PostgreSQL DDL
# ---------------------------

def ddl_postgres_from_profile(
    profile: Any,
    *,
    table: Optional[str] = None,
    schema: Optional[str] = "public",
    types_yaml_path: str = os.path.join("config", "types.yaml"),
) -> str:
    prof = _as_profile(profile)
    ts = _TypeSystem.load(types_yaml_path)

    tname = table or prof.get("entity", {}).get("name") or "table1"
    if schema:
        fq = f"{_quote_pg(schema)}.{_quote_pg(tname)}"
    else:
        fq = _quote_pg(tname)

    cols_sql: List[str] = []
    for col in prof.get("columns", []):
        name = col["name"]
        canon = (col.get("type", {}) or {}).get("canonical") or "string"
        tpl, params = ts.map(canon, backend="pg")
        pg_type = tpl.format(**params) if params else tpl
        not_null = " NOT NULL" if not col.get("nullable", True) else ""
        cols_sql.append(f"  {_quote_pg(name)} {pg_type}{not_null}")

    ddl = [
        f"CREATE TABLE IF NOT EXISTS {fq} (",
        ",\n".join(cols_sql),
        ");",
    ]
    return "\n".join(ddl)

# ---------------------------
# DBML (минималистичный, канонические типы)
# ---------------------------

def dbml_from_profile(profile: Any, *, table: Optional[str] = None) -> str:
    prof = _as_profile(profile)
    tname = table or prof.get("entity", {}).get("name") or "table1"

    lines = [f"Table {_quote_dbml(tname)} {{"]
    for col in prof.get("columns", []):
        name = _quote_dbml(col["name"])
        canon = (col.get("type", {}) or {}).get("canonical") or "string"
        mods: List[str] = []
        if not col.get("nullable", True):
            mods.append("not null")
        mod_str = f" [{', '.join(mods)}]" if mods else ""
        lines.append(f"  {name} {canon}{mod_str}")
    lines.append("}")
    return "\n".join(lines)

# ---------------------------
# CLI
# ---------------------------
if __name__ == "__main__":
    import argparse, sys

    ap = argparse.ArgumentParser(description="Генераторы DDL/DBML по JSON-профилю (v2)")
    ap.add_argument("profile_path", help="Путь к JSON профилю")
    ap.add_argument("--types", dest="types_yaml_path", default=os.path.join("config", "types.yaml"))
    ap.add_argument("--table", dest="table", default=None)

    sub = ap.add_subparsers(dest="cmd", required=True)

    ch = sub.add_parser("ch", help="DDL для ClickHouse")
    ch.add_argument("--db", dest="database", default=None)
    ch.add_argument("--engine", dest="engine", default="MergeTree")
    ch.add_argument("--order-by", dest="order_by", default="tuple",
                    help='"tuple" | "auto" | comma-separated list of columns')
    ch.add_argument("--partition-by", dest="partition_by", default=None,
                    help='None | expression | comma-separated list of columns')
    ch.add_argument("--no-coerce-ts64", dest="coerce_ts64", action="store_false",
                    help="Не форсировать DateTime64(3,'UTC') для timestamp")
    ch.add_argument("--no-strict", dest="strict", action="store_false",
                    help="Не валидировать имена колонок в order/partition")

    pg = sub.add_parser("pg", help="DDL для PostgreSQL")
    pg.add_argument("--schema", dest="schema", default="public")

    dbml = sub.add_parser("dbml", help="DBML (канонические типы)")

    args = ap.parse_args()
    with open(args.profile_path, "r", encoding="utf-8") as f:
        prof = json.load(f)

    if args.cmd == "ch":
        order_by = args.order_by
        if order_by not in ("tuple", "auto"):
            order_by = [c.strip() for c in order_by.split(",") if c.strip()]
        partition_by = args.partition_by
        if partition_by and "," in partition_by:
            partition_by = [c.strip() for c in partition_by.split(",") if c.strip()]
        print(ddl_clickhouse_from_profile(
            prof, table=args.table, database=args.database,
            types_yaml_path=args.types_yaml_path,
            engine=args.engine, order_by=order_by, partition_by=partition_by,
            coerce_timestamp64=args.coerce_ts64, strict=args.strict
        ))
    elif args.cmd == "pg":
        print(ddl_postgres_from_profile(
            prof, table=args.table, schema=args.schema,
            types_yaml_path=args.types_yaml_path
        ))
    elif args.cmd == "dbml":
        print(dbml_from_profile(prof, table=args.table))
    else:
        sys.exit(2)
