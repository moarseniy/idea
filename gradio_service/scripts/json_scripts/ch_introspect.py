# ch_introspect.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import requests
import re

# ---------------- HTTP helpers ----------------

def _session(trust_env: bool = False, user: Optional[str] = None, password: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.trust_env = trust_env  # по умолчанию игнорируем системные HTTP(S)_PROXY
    if user is not None:
        s.auth = (user, password or "")
    s.headers.update({"Content-Type": "text/plain; charset=UTF-8"})
    return s

def _q(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def _query_json(http_url: str, sql: str, *, database: Optional[str] = None,
                user: Optional[str] = None, password: Optional[str] = None,
                settings: Optional[Dict[str, Any]] = None,
                trust_env: bool = False) -> Dict[str, Any]:
    if not sql.rstrip().lower().endswith("format json"):
        sql = sql.rstrip("; \n\t") + " FORMAT JSON"
    params: Dict[str, Any] = {}
    if database:
        params["database"] = database
    if settings:
        params.update({f"settings[{k}]": v for k, v in settings.items()})
    s = _session(trust_env=trust_env, user=user, password=password)
    r = s.post(http_url, params=params or None, data=(sql + ";").encode("utf-8"), timeout=60)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        raise RuntimeError(f"ClickHouse HTTP error: {r.text}") from e
    return r.json()

def ch_ping(http_url: str, *, user: Optional[str] = None, password: Optional[str] = None, trust_env: bool = False) -> None:
    s = _session(trust_env=trust_env, user=user, password=password)
    r = s.get(http_url.rstrip("/") + "/ping", timeout=10)
    r.raise_for_status()
    if not r.text.strip().startswith("Ok"):
        raise RuntimeError(f"Unexpected /ping response: {r.text!r}")

# ---------------- basic introspection ----------------

def list_tables(http_url: str, database: str, *, user: Optional[str] = None, password: Optional[str] = None,
                trust_env: bool = False) -> List[str]:
    j = _query_json(http_url, "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name",
                    database=database, user=user, password=password, trust_env=trust_env)
    return [row["name"] for row in j.get("data", [])]

# замените table_exists на это
def table_exists(http_url: str, database: str, table: str, *, user: Optional[str] = None,
                 password: Optional[str] = None, trust_env: bool = False) -> bool:
    sql = f"SELECT count() AS c FROM system.tables WHERE database = currentDatabase() AND name = {repr(table)}"
    j = _query_json(http_url, sql, database=database, user=user, password=password, trust_env=trust_env)
    c = (j.get("data") or [{}])[0].get("c", 0)
    try:
        c = int(c)  # <- важно: ClickHouse может вернуть "1" (строкой)
    except Exception:
        c = 0
    return c > 0


def describe_columns(http_url: str, database: str, table: str, *, user: Optional[str] = None,
                     password: Optional[str] = None, trust_env: bool = False) -> List[Dict[str, Any]]:
    sql = f"DESCRIBE TABLE {_q(table)}"
    j = _query_json(http_url, sql, database=database, user=user, password=password, trust_env=trust_env)
    # поля: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
    out = []
    for row in j.get("data", []):
        out.append({
            "name": row.get("name"),
            "type": row.get("type"),
            "default_type": row.get("default_type"),
            "default_expression": row.get("default_expression"),
            "comment": row.get("comment"),
        })
    return out

def table_engine_and_keys(http_url: str, database: str, table: str, *, user: Optional[str] = None,
                          password: Optional[str] = None, trust_env: bool = False) -> Dict[str, Any]:
    sql = """
    SELECT engine, sorting_key, primary_key, partition_key, sampling_key
    FROM system.tables
    WHERE database = currentDatabase() AND name = {table}
    """.replace("{table}", repr(table))
    j = _query_json(http_url, sql, database=database, user=user, password=password, trust_env=trust_env)
    row = (j.get("data") or [{}])[0]
    # нормализуем sorting_key -> список колонок
    def parse_key(expr: Optional[str]) -> List[str]:
        if not expr:
            return []
        s = expr.strip()
        # примеры: 'rec_id' или 'tuple(rec_id)' или 'tuple(rec_id, idx1)'
        s = s.replace(" ", "")
        if s.startswith("tuple(") and s.endswith(")"):
            s = s[6:-1]
        return [c for c in s.split(",") if c]
    return {
        "engine": row.get("engine"),
        "sorting_key": parse_key(row.get("sorting_key")),
        "primary_key": parse_key(row.get("primary_key")),
        "partition_key": row.get("partition_key"),
        "sampling_key": row.get("sampling_key"),
    }

def row_count(http_url: str, database: str, table: str, *, user: Optional[str] = None,
              password: Optional[str] = None, trust_env: bool = False) -> int:
    sql = f"SELECT count() AS c FROM {_q(database)}.{_q(table)}"
    j = _query_json(http_url, sql, user=user, password=password, trust_env=trust_env)
    return int((j.get("data") or [{}])[0].get("c", 0))

# ---------------- type mapping (минимальный дубликат из ddl_clickhouse) ----------------

_FALLBACK_TYPES_YAML = {
    "canonical": {
        "string":        {"ch": "String"},
        "int32":         {"ch": "Int32"},
        "int64":         {"ch": "Int64"},
        "float64":       {"ch": "Float64"},
        "decimal(p,s)":  {"ch": "Decimal({p},{s})"},
        "bool":          {"ch": "Bool"},
        "date":          {"ch": "Date32"},
        "timestamp":     {"ch": "DateTime('UTC')"},
        "timestamp64(ms)": {"ch": "DateTime64(3, 'UTC')"},
        "json":          {"ch": "String"},
    },
    "synonyms": {
        "text": "string", "varchar": "string",
        "bigint": "int64", "integer": "int32", "int4": "int32", "int8": "int64",
        "double": "float64", "double precision": "float64",
        "numeric": "decimal(p,s)", "decimal": "decimal(p,s)",
        "timestamptz": "timestamp", "timestampz": "timestamp",
        "datetime": "timestamp", "datetime64": "timestamp64(ms)",
        "jsonb": "json", "uint8": "bool"
    }
}

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
        p, s = (int(nums[0]), int(nums[1])) if len(nums) == 2 else _DEC_DEFAULT
        tmpl = canonical.get("decimal(p,s)", {}).get("ch", "Decimal({p},{s})")
        return tmpl.format(p=p, s=s)
    t = canonical.get(canon, {}).get("ch")
    if t:
        return t
    base = types_cfg.get("synonyms", {}).get(canon.lower())
    if base:
        return canonical.get(base, {}).get("ch", "String")
    return "String"

def _expected_ch_type(col: Dict[str, Any], types_cfg: Dict[str, Any]) -> str:
    # PK мы задаём жёстко:
    #   rec_id -> UInt64, idxN -> UInt32
    # для прочих — по маппингу + Nullable(...)
    name = col["name"]
    if name == "rec_id":
        return "UInt64"
    if name.startswith("idx"):
        return "UInt32"
    base = _canon_to_ch(col.get("type", "string"), types_cfg)
    return f"Nullable({base})" if col.get("nullable", True) else base

# ---------------- validation against profile ----------------

def validate_schema_against_profile_ch(http_url: str, profile: Dict[str, Any], database: str,
                                       *, types_yaml_path: Optional[str] = None,
                                       user: Optional[str] = None, password: Optional[str] = None,
                                       trust_env: bool = False, max_errors: int = 50) -> str:
    """
    Проверяет для каждой сущности профиля:
      - таблица существует в ClickHouse (в указанной БД);
      - набор колонок присутствует (PK + данные);
      - типы: rec_id=UInt64, idx*=UInt32, данные = маппинг из канонических + Nullable по nullable;
      - ORDER BY (sorting_key) == (rec_id, idx1..idxN).
    Возвращает 'SUCCESS' или компактный отчёт.
    """
    # загрузим YAML, если дан; иначе fallback
    types_cfg: Dict[str, Any]
    if types_yaml_path:
        try:
            import yaml  # type: ignore
            with open(types_yaml_path, "r", encoding="utf-8") as f:
                types_cfg = yaml.safe_load(f)
        except Exception:
            types_cfg = _FALLBACK_TYPES_YAML
    else:
        types_cfg = _FALLBACK_TYPES_YAML

    errors: List[str] = []
    def err(msg: str):
        if len(errors) < max_errors:
            errors.append(msg)

    for e in profile.get("entities", []):
        tname = e["name"]
        depth = e.get("depth", 0)

        if not table_exists(http_url, database, tname, user=user, password=password, trust_env=trust_env):
            err(f"table missing: {tname}")
            continue

        # DESCRIBE
        cols_db = describe_columns(http_url, database, tname, user=user, password=password, trust_env=trust_env)
        types_db = {c["name"]: c["type"] for c in cols_db}
        names_db = set(types_db.keys())

        # ожидания
        pk_cols = ["rec_id"] + [f"idx{i}" for i in range(1, depth + 1)]
        data_cols = [c["name"] for c in e.get("columns", [])]
        expected_names = set(pk_cols + data_cols)

        missing = expected_names - names_db
        if missing:
            err(f"{tname}: missing columns {sorted(missing)}")

        # типы
        # построим ожидаемую карта name->type
        type_expect: Dict[str, str] = {}
        for n in pk_cols:
            type_expect[n] = _expected_ch_type({"name": n}, types_cfg)
        for c in e.get("columns", []):
            type_expect[c["name"]] = _expected_ch_type(c, types_cfg)

        mism_types = []
        for n, exp_t in type_expect.items():
            got_t = types_db.get(n)
            if got_t is None:
                continue
            # нормализуем пробелы/кавычки
            g = got_t.replace(" ", "")
            e_t = exp_t.replace(" ", "")
            if g != e_t:
                mism_types.append((n, exp_t, got_t))
        if mism_types:
            pretty = "; ".join(f"{n}: expected {exp}, got {got}" for n, exp, got in mism_types[:5])
            more = f" (+{len(mism_types)-5} more)" if len(mism_types) > 5 else ""
            err(f"{tname}: type mismatch -> {pretty}{more}")

        # ORDER BY / sorting_key
        keys = table_engine_and_keys(http_url, database, tname, user=user, password=password, trust_env=trust_env)
        expected_order = pk_cols
        # нормализуем: sorting_key может содержать subset если secondary ключ пуст — мы требуем точное совпадение
        if keys.get("sorting_key") != expected_order:
            err(f"{tname}: ORDER BY mismatch, expected {expected_order}, got {keys.get('sorting_key')}")

    if not errors:
        return "SUCCESS"
    tail = "" if len(errors) <= max_errors else f" (+{len(errors)-max_errors} more)"
    return "ERROR: " + " | ".join(errors) + tail
