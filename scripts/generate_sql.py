import json
import argparse
from pathlib import Path
from a5.transpiler import transpile
from a5.linter import lint

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", choices=["pg","ch"], required=True)
    parser.add_argument("--schema", default="artifacts/schema.json")
    parser.add_argument("--mapping", default="artifacts/load_mapping.json")
    parser.add_argument("--outdir", default="build")
    args = parser.parse_args()

    schema = json.loads(Path(args.schema).read_text(encoding="utf-8"))
    mapping = json.loads(Path(args.mapping).read_text(encoding="utf-8"))

    lint(schema, mapping)
    bundle = transpile(schema, mapping, args.engine)

    outdir = Path(args.outdir) / args.engine
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "ddl.sql").write_text(";\n".join(bundle["ddl"]) + ";\n", encoding="utf-8")
    (outdir / "staging.sql").write_text(";\n".join(bundle["staging"]) + ";\n", encoding="utf-8")
    (outdir / "routes.sql").write_text(";\n".join(bundle["routes"]) + ";\n", encoding="utf-8")
    print(f"Saved SQL to {outdir}")

if __name__ == "__main__":
    main()
