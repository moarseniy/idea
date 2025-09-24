# scripts/json_to_dbml.py
import json
import re
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional

DEC_RE = re.compile(r'^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$', re.I)

# ---- канонич. тип -> DBML тип ----
def canon_to_dbml(t: str) -> str:
    tl = t.strip().lower()
    m = DEC_RE.match(tl)
    if m:
        p, s = m.groups()
        return f"decimal({p},{s})"
    # синонимы
    if tl in ("int", "int32", "integer"):
        return "int"
    if tl in ("int64", "bigint"):
        return "bigint"
    if tl in ("float", "float64", "double", "double precision"):
        return "float"
    if tl in ("bool", "boolean"):
        return "boolean"
    if tl in ("date", "date32"):
        return "date"
    if tl in ("timestamp", "timestampz", "timestamptz", "datetime"):
        return "timestamp"
    if tl.startswith("timestamp64"):
        return "timestamp"  # dbml не знает точность — добавим в note
    if tl in ("json",):
        return "json"
    # всё строковое — в varchar/text
    return "varchar"

def q(v: Any) -> str:
    if isinstance(v, str):
        # экранируем одинарные кавычки
        return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return str(v)

def table_block(t: Dict[str, Any]) -> str:
    name = t["name"]
    cols = t["columns"]
    pk = t.get("primary_key") or []
    pk = list(pk)
    fk_list = t.get("foreign_keys") or []
    ordering = (t.get("ordering") or {}).get("by")
    part = (t.get("partitioning") or {}).get("by")
    qe = t.get("quality_expectations") or {}

    lines = []
    lines.append(f"Table {name} {{")
    # колонки
    for c in cols:
        col = c["name"]
        typ = canon_to_dbml(c["type"])
        opts: List[str] = []

        # single-column PK — можно пометить прямо на колонке
        if pk and len(pk) == 1 and pk[0] == col:
            opts.append("pk")
        if not c.get("nullable", True):
            opts.append("not null")
        if c.get("default") is not None:
            opts.append(f"default: {q(c['default'])}")
        # пометка про точность timestamp64
        if c["type"].lower().startswith("timestamp64"):
            opts.append("note: 'precision: " + c['type'] + "'")

        opt_str = f" [{', '.join(opts)}]" if opts else ""
        lines.append(f"  {col} {typ}{opt_str}")

    # составной PK
    if pk and len(pk) > 1:
        lines.append("  indexes {")
        cols_list = ", ".join(pk)
        lines.append(f"    ({cols_list}) [pk]")
        lines.append("  }")

    # note с ordering/partitioning/quality
    notes = []
    if ordering:
        notes.append("ordering: " + ", ".join(ordering))
    if part:
        notes.append("partitioning: " + part)
    if qe:
        if "not_null" in qe:
            notes.append("qe.not_null: " + ", ".join(qe["not_null"]))
        if "ranges" in qe:
            rngs = []
            for k, v in qe["ranges"].items():
                lo = v[0] if len(v) > 0 else None
                hi = v[1] if len(v) > 1 else None
                rngs.append(f"{k}[{lo}..{hi}]")
            if rngs:
                notes.append("qe.ranges: " + ", ".join(rngs))
    if notes:
        note_txt = "\\n".join(notes)
        lines.append(f"  Note: '{note_txt}'")

    lines.append("}")
    return "\n".join(lines)

def fk_blocks(t: Dict[str, Any]) -> List[str]:
    name = t["name"]
    fk_list = t.get("foreign_keys") or []
    refs = []
    for fk in fk_list:
        cols = fk["columns"]
        rt = fk["ref_table"]
        rcols = fk["ref_columns"]
        # поддержим многоколон. FK
        for c, rc in zip(cols, rcols):
            refs.append(f"Ref: {name}.{c} > {rt}.{rc}")
    return refs

def staging_table_from_mapping(db: str, lm: Dict[str, Any]) -> str:
    name = lm["staging_table"]
    ss = lm["select_schema"]
    lines = [f"Table {name} {{"]

    ordered = list(ss.keys())
    for col in ordered:
        typ = canon_to_dbml(ss[col])
        # staging: все nullable по дефолту
        lines.append(f"  {col} {typ}")
    lines.append(f"  src_file varchar")
    lines.append(f"  load_ts timestamp")
    lines.append("  Note: 'staging table from mapping'")
    lines.append("}")
    return "\n".join(lines)

def build_dbml(schema_path: Path, mapping_path: Optional[Path]=None, include_staging: bool=True) -> str:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    dbml_parts: List[str] = []
    db_name = schema.get("database", "public")

    dbml_parts.append(f"// DB: {db_name}")
    dbml_parts.append("")

    # таблицы
    table_texts = [table_block(t) for t in schema["tables"]]
    dbml_parts.extend(table_texts)
    dbml_parts.append("")

    # ссылки
    ref_texts = []
    for t in schema["tables"]:
        ref_texts.extend(fk_blocks(t))
    if ref_texts:
        dbml_parts.append("// Foreign Keys")
        dbml_parts.extend(ref_texts)
        dbml_parts.append("")

    # staging из mapping (опционально)
    if include_staging and mapping_path and mapping_path.exists():
        mp = json.loads(mapping_path.read_text(encoding="utf-8"))
        dbml_parts.append("// Staging tables from load_mapping")
        for lm in mp.get("load_mappings", []):
            dbml_parts.append(staging_table_from_mapping(db_name, lm))
            dbml_parts.append("")

    return "\n".join(dbml_parts).rstrip() + "\n"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", required=True, help="path to schema_*.json")
    ap.add_argument("--mapping", help="path to load_mapping_*.json (optional)")
    ap.add_argument("--out", default="artifacts/schema.dbml", help="output .dbml path")
    ap.add_argument("--no-staging", action="store_true", help="do not include staging tables")
    args = ap.parse_args()

    schema_path = Path(args.schema)
    mapping_path = Path(args.mapping) if args.mapping else None
    include_staging = not args.no_staging

    dbml = build_dbml(schema_path, mapping_path, include_staging)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(dbml, encoding="utf-8")
    print(f"[OK] Wrote {out_path} ({len(dbml.splitlines())} lines)")

if __name__ == "__main__":
    main()
