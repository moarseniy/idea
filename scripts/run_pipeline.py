import json
import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from a5.linter import lint
from a5.transpiler import transpile
from a6 import executor_pg, executor_ch

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["pg","ch"], required=True)
    parser.add_argument("--schema", default="artifacts/schema.json")
    parser.add_argument("--mapping", default="artifacts/load_mapping.json")
    args = parser.parse_args()

    schema = json.loads(Path(args.schema).read_text(encoding="utf-8"))
    mapping = json.loads(Path(args.mapping).read_text(encoding="utf-8"))

    lint(schema, mapping)
    bundle = transpile(schema, mapping, args.engine)

    lm = mapping["load_mappings"][0]
    select_schema = lm["select_schema"]
    csv_opts = lm.get("csv_options", {})
    db = schema["database"] 
    targets = sorted({ r["into"] for lm in mapping["load_mappings"] for r in lm["route"] })

    if args.engine == "pg":
        print("[PG] Applying DDL..."); executor_pg.run_sql_statements(bundle["ddl"])
        executor_pg.run_sql_statements([f"DROP TABLE IF EXISTS {lm['staging_table']};"])
        print("[PG] Creating staging..."); executor_pg.run_sql_statements(bundle["staging"])
        print(f"[PG] Loading CSV -> {lm['staging_table']} ...")
        executor_pg.copy_csv_to_staging(lm["source"], lm["staging_table"], select_schema, csv_opts)
        print("[PG] Routing to targets..."); executor_pg.run_sql_statements(bundle["routes"])
        print("[PG] Counts:", executor_pg.counts(targets))
    else:
        print("[CH] Applying DDL..."); executor_ch.run_sql_statements(bundle["ddl"])
        # важно: дропнуть старый staging, чтобы обновилась NULLability колонок
        executor_ch.run_sql_statements([f"DROP TABLE IF EXISTS {db}.{lm['staging_table']}"])
        print("[CH] Creating staging..."); executor_ch.run_sql_statements(bundle["staging"])
        print(f"[CH] Loading CSV -> {lm['staging_table']} ...")
        executor_ch.load_csv_to_staging(lm["source"], lm["staging_table"], select_schema, csv_opts)
        print("[CH] Routing to targets..."); executor_ch.run_sql_statements(bundle["routes"])
        print("[CH] Counts:", executor_ch.counts(targets))


if __name__ == "__main__":
    main()
