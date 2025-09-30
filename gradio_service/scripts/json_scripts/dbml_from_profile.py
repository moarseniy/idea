# dbml_from_profile.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
import re

def _q_ident(name: str) -> str:
    return '"' + str(name).replace('"', '\\"') + '"'

def _q_note(text: str) -> str:
    return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"

def _canon_to_dbml_type(canon: str) -> str:
    c = (canon or "string").strip().lower()
    if c == "string": return "text"
    if c in ("int32", "int64"): return "int"
    if c == "float64": return "float"
    if c.startswith("decimal("):
        m = re.match(r"decimal\((\d+)\s*,\s*(\d+)\)", c)
        return f"decimal({m.group(1)},{m.group(2)})" if m else "decimal(38,10)"
    if c == "bool": return "boolean"
    if c == "date": return "date"
    if c in ("timestamp", "timestamp64(ms)"): return "datetime"
    if c == "json": return "json"
    return "text"

def _pk_cols_for_entity(ent: Dict[str, Any]) -> List[str]:
    if ent.get("primary_key"): return list(ent["primary_key"])
    d = int(ent.get("depth", 0) or 0)
    return ["rec_id"] + [f"idx{i}" for i in range(1, d + 1)]

def _infer_parent_ref(ent: Dict[str, Any], name_by_path: Dict[Tuple[str, ...], str]) -> Optional[Tuple[str, List[str], str, List[str]]]:
    depth = int(ent.get("depth", 0) or 0)
    if depth <= 0: return None
    path = tuple(ent.get("path", []) or [])
    parent_path = path[:-1]
    parent_name = name_by_path.get(parent_path)
    if not parent_name: return None
    from_cols = ["rec_id"] + [f"idx{i}" for i in range(1, depth)]
    to_cols   = ["rec_id"] + [f"idx{i}" for i in range(1, depth)]
    return (ent["name"], from_cols, parent_name, to_cols)

def _emit_ref_line(ft: str, fcols: List[str], tt: str, tcols: List[str]) -> str:
    if len(fcols) == 1 and len(tcols) == 1:
        return f'Ref: {_q_ident(ft)}.{_q_ident(fcols[0])} > {_q_ident(tt)}.{_q_ident(tcols[0])}'
    return (
        f'Ref: {_q_ident(ft)}.(' + ", ".join(_q_ident(c) for c in fcols) + ") > "
        f'{_q_ident(tt)}.(' + ", ".join(_q_ident(c) for c in tcols) + ")"
    )

def emit_dbml(profile: Dict[str, Any], *, project_database_type: Optional[str] = None) -> str:
    lines: List[str] = []
    if project_database_type:
        lines.append("Project {")
        lines.append(f"  database_type: '{project_database_type}'")
        lines.append("}\n")

    entities: List[Dict[str, Any]] = profile.get("entities", [])
    name_by_path = {tuple(e.get("path", []) or []): e["name"] for e in entities}
    entity_descs = profile.get("entity_descriptions", {}) or {}
    entity_names = {e["name"] for e in entities}

    # Tables
    for ent in entities:
        tname = ent["name"]
        pk_cols = _pk_cols_for_entity(ent)
        lines.append(f"Table {_q_ident(tname)} {{")
        for col in pk_cols:
            typ = "bigint" if col == "rec_id" else "int"
            lines.append(f"  {_q_ident(col)} {typ} [pk, not null]")
        for col in ent.get("columns", []):
            cname = col["name"]
            ctype = _canon_to_dbml_type(col.get("type", "string"))
            nullable = col.get("nullable", True)
            note = col.get("description")
            attrs = ["null" if nullable else "not null"]
            if note: attrs.append(f"note: {_q_note(note)}")
            lines.append(f"  {_q_ident(cname)} {ctype} [{', '.join(attrs)}]")
        tnote = entity_descs.get(str(ent.get("path", [])))
        if tnote:
            lines.append(f"  Note: {_q_note(tnote)}")
        lines.append("}\n")

    # Relations: 1) валидные из profile.relations  2) + автогенерация parent-child
    seen = set()

    # 1) Взять только те, что ссылаются на существующие таблицы
    for r in (profile.get("relations") or []):
        ft, tt = r.get("from_table"), r.get("to_table")
        if ft not in entity_names or tt not in entity_names:
            continue  # пропускаем устаревшие имена (как __root__...)
        fcols = r.get("from_columns", []) or []
        tcols = r.get("to_columns", []) or []
        key = (ft, tuple(fcols), tt, tuple(tcols))
        if key in seen: continue
        seen.add(key)
        lines.append(_emit_ref_line(ft, fcols, tt, tcols))

    # 2) Автогенерация «ребёнок → родитель» по path/depth
    for ent in entities:
        ref = _infer_parent_ref(ent, name_by_path)
        if not ref: continue
        ft, fcols, tt, tcols = ref
        key = (ft, tuple(fcols), tt, tuple(tcols))
        if key in seen: continue
        seen.add(key)
        lines.append(_emit_ref_line(ft, fcols, tt, tcols))

    return "\n".join(lines).rstrip() + "\n"

def save_dbml(profile: Dict[str, Any], out_path: str, *, project_database_type: Optional[str] = None) -> None:
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(emit_dbml(profile, project_database_type=project_database_type))
