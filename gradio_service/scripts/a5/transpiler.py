from __future__ import annotations
from typing import Dict, Any, List
from a5.type_registry import TypeRegistry

TR = TypeRegistry()

# ---------- PG DDL ----------
def ddl_pg(schema: Dict[str, Any]) -> List[str]:
    stmts: List[str] = []
    for t in schema["tables"]:
        cols_sql = []
        for c in t["columns"]:
            col = f"{c['name']} {TR.engine_type('pg', c['type'])}"
            if not c.get("nullable", True):
                col += " NOT NULL"
            if c.get("default"):
                col += f" DEFAULT {c['default']}"
            cols_sql.append(col)

        pk = ""
        if t.get("primary_key"):
            pk_cols = ", ".join(t["primary_key"])
            pk = f", PRIMARY KEY ({pk_cols})"

        fks = ""
        if t.get("foreign_keys"):
            fk_parts = []
            for fk in t["foreign_keys"]:
                cols = ", ".join(fk["columns"])
                ref_cols = ", ".join(fk["ref_columns"])
                fk_parts.append(f"FOREIGN KEY ({cols}) REFERENCES {fk['ref_table']}({ref_cols})")
            if fk_parts:
                fks = ", " + ", ".join(fk_parts)

        stmts.append(f"CREATE TABLE IF NOT EXISTS {t['name']} (\n  " + ",\n  ".join(cols_sql) + pk + fks + "\n);")

        # Идемпотентные CHECK-constraints из ranges
        qe = t.get("quality_expectations", {}) or {}
        ranges = qe.get("ranges", {}) or {}
        for col_name, bounds in ranges.items():
            lo = bounds[0] if len(bounds) > 0 else None
            hi = bounds[1] if len(bounds) > 1 else None
            parts = []
            if lo is not None: parts.append(f"{col_name} >= {lo}")
            if hi is not None: parts.append(f"{col_name} <= {hi}")
            if parts:
                cname = f"{t['name']}_{col_name}_range"
                check_expr = " AND ".join(parts)
                stmts.append(f"""
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = '{cname}' AND conrelid = '{t['name']}'::regclass
  ) THEN
    ALTER TABLE {t['name']} ADD CONSTRAINT {cname} CHECK ({check_expr}) NOT VALID;
  END IF;
END $$;""".strip())
    return stmts

# ---------- CH DDL ----------
def ddl_ch(schema: Dict[str, Any]) -> List[str]:
    stmts: List[str] = []
    db = schema["database"]
    stmts.append(f"CREATE DATABASE IF NOT EXISTS {db};")
    for t in schema["tables"]:
        cols_sql = []
        for c in t["columns"]:
            cols_sql.append(f"{c['name']} {TR.engine_type('ch', c['type'])}")

        order_by = t.get("ordering", {}).get("by") or (t.get("primary_key") or [t["columns"][0]["name"]])
        order_sql = ", ".join(order_by)

        part_expr = (t.get("partitioning") or {}).get("by")
        part_sql = f"\nPARTITION BY {part_expr.replace('to_date','toYYYYMM')}" if part_expr else ""

        stmts.append(
            f"CREATE TABLE IF NOT EXISTS {db}.{t['name']} (\n  " + ",\n  ".join(cols_sql) +
            f"\n)\nENGINE = MergeTree\nORDER BY ({order_sql})" + part_sql + ";"
        )
    return stmts

# ---------- STAGING ----------
def staging_pg(lm: Dict[str, Any]) -> List[str]:
    cols = [f"{name} {TR.engine_type('pg', typ)}" for name, typ in lm["select_schema"].items()]
    cols += ["src_file text", "load_ts timestamptz DEFAULT now()"]
    return [f"CREATE TABLE IF NOT EXISTS {lm['staging_table']} (\n  " + ",\n  ".join(cols) + "\n);"]

def staging_ch(database: str, lm: Dict[str, Any]) -> List[str]:
    cols = []
    for name, typ in lm["select_schema"].items():
        ch_t = TR.engine_type('ch', typ)
        # staging в CH делаем максимально «всеядным»
        if not ch_t.startswith("Nullable("):
            ch_t = f"Nullable({ch_t})"
        cols.append(f"{name} {ch_t}")
    # служебные — не nullable
    cols += ["src_file String", "load_ts DateTime DEFAULT now()"]
    return [f"CREATE TABLE IF NOT EXISTS {database}.{lm['staging_table']} (\n  "
            + ",\n  ".join(cols) +
            "\n)\nENGINE=MergeTree\nORDER BY (load_ts);"]



# ---------- ROUTING ----------
def routes_pg(lm: Dict[str, Any]) -> List[str]:
    """
    Для маршрутов с upsert_key: дедуп в CTE (row_number по load_ts), затем INSERT ... ON CONFLICT DO UPDATE.
    Для остальных: обычный INSERT ... SELECT.
    """
    stmts: List[str] = []
    stg = lm["staging_table"]
    for r in lm["route"]:
        target_cols = list(r["select"].keys())
        where = r.get("when", "TRUE")

        if r.get("upsert_key"):
            keys = list(r["upsert_key"])
            key_list = ", ".join(keys)
            updates = ", ".join(f"{col}=EXCLUDED.{col}" for col in target_cols if col not in keys)
            select_with_aliases = ", ".join(f"{expr} AS {col}" for col, expr in r["select"].items())
            stmt = f"""
WITH src AS (
  SELECT {select_with_aliases}, load_ts
  FROM {stg} s
  WHERE ({where})
),
ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY {", ".join(keys)} ORDER BY load_ts DESC) AS rn
  FROM src
)
INSERT INTO {r['into']} ({", ".join(target_cols)})
SELECT {", ".join(target_cols)}
FROM ranked
WHERE rn = 1
ON CONFLICT ({key_list}) DO UPDATE SET {updates};
""".strip()
        else:
            select_exprs = ", ".join(r["select"].values())
            stmt = (
                f"INSERT INTO {r['into']} ({', '.join(target_cols)}) "
                f"SELECT {select_exprs} FROM {stg} s WHERE ({where});"
            )
        stmts.append(stmt)
    return stmts

def routes_ch(database: str, lm: Dict[str, Any]) -> List[str]:
    """
    Для маршрутов с upsert_key: используем GROUP BY по ключам и argMax(<col>, load_ts)
    (псевдо-upsert для MergeTree). Иначе — обычный INSERT SELECT.
    """
    stmts: List[str] = []
    stg = f"{database}.{lm['staging_table']}"
    for r in lm["route"]:
        target_cols = list(r["select"].keys())
        where = r.get("when", "1")

        if r.get("upsert_key"):
            keys = list(r["upsert_key"])
            terms = []
            for col in target_cols:
                expr = r["select"][col]
                if col in keys:
                    terms.append(f"{expr} AS {col}")
                else:
                    terms.append(f"argMax({expr}, load_ts) AS {col}")
            stmt = (
                f"INSERT INTO {database}.{r['into']} ({', '.join(target_cols)}) "
                f"SELECT {', '.join(terms)} FROM {stg} WHERE ({where}) "
                f"GROUP BY {', '.join(keys)};"
            )
        else:
            select_exprs = ", ".join(r["select"].values())
            stmt = (
                f"INSERT INTO {database}.{r['into']} ({', '.join(target_cols)}) "
                f"SELECT {select_exprs} FROM {stg} WHERE ({where});"
            )
        stmts.append(stmt)
    return stmts

# ---------- PUBLIC API ----------
def transpile(schema: Dict[str, Any], mapping: Dict[str, Any], engine: str) -> Dict[str, List[str]]:
    """
    Возвращает словарь списков SQL-стейтментов:
    {
      "ddl": [...],
      "staging": [...],
      "routes": [...]
    }
    """
    engine = engine.lower()
    if engine not in {"pg", "postgres", "ch", "clickhouse"}:
        raise ValueError("engine must be 'pg' or 'ch'")

    if engine in {"pg", "postgres"}:
        ddl = ddl_pg(schema)
        staging: List[str] = []
        routes: List[str] = []
        for lm in mapping["load_mappings"]:
            staging += staging_pg(lm)
            routes += routes_pg(lm)
        return {"ddl": ddl, "staging": staging, "routes": routes}

    else:
        ddl = ddl_ch(schema)
        staging = []
        routes = []
        db = schema["database"]
        for lm in mapping["load_mappings"]:
            staging += staging_ch(db, lm)
            routes += routes_ch(db, lm)
        return {"ddl": ddl, "staging": staging, "routes": routes}
