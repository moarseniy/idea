# dbml_minimal.py

from __future__ import annotations
from typing import Dict, Any, List

def _dbml_type_from_canonical(t: str) -> str:
    t = (t or "").strip().lower()
    if t.startswith("decimal("):
        return "decimal" + t[t.find("("):]  # оставить как есть
    return {
        "string": "text",
        "bool": "boolean",
        "int32": "int",
        "int64": "bigint",
        "float64": "double",
        "date": "date",
        "timestamp": "timestamp",
        "timestamp64(ms)": "timestamp",  # в DBML достаточно timestamp
        "json": "text",                  # максимально совместимо
    }.get(t, "text")

def generate_dbml_minimal(final_spec: Dict[str, Any],
                          with_project: bool = False,
                          project_name: str = "Registry ETL") -> str:
    out: List[str] = []

    # Проект — опционально (по умолчанию отключён, чтобы не ловить придирки к синтаксису)
    if with_project:
        out.append(f'Project "{project_name}" {{}}')
        out.append("")

    # Таблицы
    for t in final_spec["tables"]:
        out.append(f"Table {t['table']} {{")
        for c in t["columns"]:
            col_name = c["name"]
            col_type = _dbml_type_from_canonical(c["type"])
            flags: List[str] = []
            if not c.get("nullable", True):
                flags.append("not null")
            if c.get("role") == "pk_surrogate":
                flags.append("pk")
            suffix = f" [{', '.join(flags)}]" if flags else ""
            out.append(f"  {col_name} {col_type}{suffix}")
        out.append("}")
        out.append("")

    # Связи FK
    for t in final_spec["tables"]:
        for c in t["columns"]:
            if c.get("role") == "fk_parent":
                out.append(f"Ref: {t['table']}.{c['name']} > {c['ref_table']}.id")
    out.append("")

    return "\n".join(out)
