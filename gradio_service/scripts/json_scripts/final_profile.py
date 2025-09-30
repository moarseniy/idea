# final_profile.py
# -*- coding: utf-8 -*-
"""
Собирает итоговый профиль: profile + patch(rename/describe) -> new_profile (dict).

Публичная функция:
    build_final_profile_from_files(profile_path: str, patch_path: str) -> dict
или
    build_final_profile(profile: dict, patch: dict) -> dict
"""

from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Tuple

# ---------- Компактный валидатор (как обсуждали: сводка + примеры) ----------

NAME_RE = re.compile(r"^[\w]+$", flags=re.UNICODE)

def _parse_json_array_key(key: str) -> Tuple[bool, List[str], str]:
    try:
        v = json.loads(key)
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            return True, v, ""
        return False, [], "must be JSON array of strings, e.g. [\"path\",\"to\",\"entity\"]"
    except Exception as e:
        return False, [], f"invalid JSON array: {e}"

def _parse_column_key(key: str) -> Tuple[bool, List[str], List[str], str]:
    if "::" not in key:
        return False, [], [], "must contain '::' between entity and column paths"
    ent_s, col_s = key.split("::", 1)
    ok1, ent_p, e1 = _parse_json_array_key(ent_s)
    ok2, col_p, e2 = _parse_json_array_key(col_s)
    if not ok1 or not ok2:
        msg = []
        if not ok1: msg.append(f"entity path {e1}")
        if not ok2: msg.append(f"column path {e2}")
        return False, [], [], "; ".join(msg)
    return True, ent_p, col_p, ""

def _index_entities_by_path(profile: Dict[str, Any]) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    return {tuple(e.get("path", [])): e for e in profile.get("entities", [])}

def _index_columns_by_path(entity: Dict[str, Any]) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    return {tuple(c.get("path", [])): c for c in entity.get("columns", [])}

def validate_rename_patch_compact(profile: Dict[str, Any], patch: Dict[str, Any], max_examples: int = 3) -> str:
    # собираем ошибки по категориям
    from collections import defaultdict, Counter
    counts = Counter()
    samples = defaultdict(list)

    def add(cat: str, msg: str):
        counts[cat] += 1
        if len(samples[cat]) < max_examples:
            samples[cat].append(msg)

    allowed_top = {"entity_names", "entity_descriptions", "column_names", "column_descriptions", "notes"}
    for k in patch.keys():
        if k not in allowed_top:
            add("unknown_top_level_key", f"unknown '{k}'")

    ents_by_path = _index_entities_by_path(profile)

    # entity_names
    planned_names = {tuple(e.get("path", [])): e.get("name") for e in profile.get("entities", [])}
    if "entity_names" in patch and not isinstance(patch["entity_names"], dict):
        add("entity_names_type", "entity_names must be an object")
    else:
        for raw_key, new_name in patch.get("entity_names", {}).items():
            ok, epath, msg = _parse_json_array_key(raw_key)
            if not ok:
                add("entity_key_format", f"[{raw_key}] {msg}")
                continue
            tpath = tuple(epath)
            if tpath not in ents_by_path:
                add("entity_path_missing", f"[{raw_key}] path not found")
                continue
            if not isinstance(new_name, str) or not new_name or not NAME_RE.match(new_name):
                add("entity_name_invalid", f"[{raw_key}] invalid '{new_name}'")
                continue
            old = planned_names[tpath]
            planned_names[tpath] = new_name
            vals = list(planned_names.values())
            if len(vals) != len(set(vals)):
                add("entity_name_duplicate", f"[{raw_key}] '{new_name}' duplicates another entity")
                planned_names[tpath] = old

    # column_names
    if "column_names" in patch and not isinstance(patch["column_names"], dict):
        add("column_names_type", "column_names must be an object")
    else:
        per_entity_planned = {tuple(e.get("path", [])): {c.get("name") for c in e.get("columns", [])}
                              for e in profile.get("entities", [])}
        for raw_key, new_name in patch.get("column_names", {}).items():
            ok, epath, cpath, msg = _parse_column_key(raw_key)
            if not ok:
                add("column_key_format", f"[{raw_key}] {msg}")
                continue
            te, tc = tuple(epath), tuple(cpath)
            ent = ents_by_path.get(te)
            if not ent:
                add("column_entity_missing", f"[{raw_key}] entity path not found")
                continue
            cols = _index_columns_by_path(ent)
            if tc not in cols:
                add("column_path_missing", f"[{raw_key}] column path not found")
                continue
            if not isinstance(new_name, str) or not new_name or not NAME_RE.match(new_name):
                add("column_name_invalid", f"[{raw_key}] invalid '{new_name}'")
                continue
            if new_name == "rec_id" or new_name.startswith("idx"):
                add("column_name_conflict_pk", f"[{raw_key}] '{new_name}' conflicts with PK")
                continue
            planned = per_entity_planned[te].copy()
            planned.discard(cols[tc].get("name"))
            if new_name in planned:
                add("column_name_duplicate", f"[{raw_key}] '{new_name}' duplicates in entity")

    # entity_descriptions / column_descriptions
    if "entity_descriptions" in patch and not isinstance(patch["entity_descriptions"], dict):
        add("entity_descriptions_type", "entity_descriptions must be an object")
    else:
        for raw_key, v in patch.get("entity_descriptions", {}).items():
            ok, epath, msg = _parse_json_array_key(raw_key)
            if not ok:
                add("entity_desc_key_format", f"[{raw_key}] {msg}")
                continue
            if tuple(epath) not in ents_by_path:
                add("entity_desc_path_missing", f"[{raw_key}] not found")
                continue
            if not isinstance(v, str) or not v.strip():
                add("entity_desc_invalid", f"[{raw_key}] empty description")

    if "column_descriptions" in patch and not isinstance(patch["column_descriptions"], dict):
        add("column_descriptions_type", "column_descriptions must be an object")
    else:
        for raw_key, v in patch.get("column_descriptions", {}).items():
            ok, epath, cpath, msg = _parse_column_key(raw_key)
            if not ok:
                add("column_desc_key_format", f"[{raw_key}] {msg}")
                continue
            te, tc = tuple(epath), tuple(cpath)
            ent = ents_by_path.get(te)
            if not ent:
                add("column_desc_entity_missing", f"[{raw_key}] entity not found")
                continue
            if tuple(cpath) not in _index_columns_by_path(ent):
                add("column_desc_path_missing", f"[{raw_key}] column path not found")
                continue
            if not isinstance(v, str) or not v.strip():
                add("column_desc_invalid", f"[{raw_key}] empty description")

    total = sum(counts.values())
    if total == 0:
        return "SUCCESS"

    parts = [f"ERROR: {total} problem(s). Summary: " +
             ", ".join(f"{k}={v}" for k, v in counts.most_common())]
    for cat, exs in samples.items():
        tail = f" (+{counts[cat]-len(exs)} more)" if counts[cat] > len(exs) else ""
        parts.append(f"{cat}: " + "; ".join(exs) + tail)
    parts.append('Hint: keys must be JSON array paths, e.g. entity: "[\\"orders\\"]", column: "[]::[\\"id\\"]"')
    return " | ".join(parts)

# ---------- Применение патча ----------

def apply_rename_patch(profile: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """
    Применяет патч к профилю: меняет имена сущностей/колонок и добавляет описания.
    Обновляет parent и relations по НОВЫМ именам. Ничего больше не трогает.
    """
    # работаем на копии
    import copy
    prof = copy.deepcopy(profile)

    ents_by_path = _index_entities_by_path(prof)

    # 1) подготовим карты переименований сущностей и колонок
    ent_ren: Dict[Tuple[str, ...], str] = {}
    for raw_key, new_name in patch.get("entity_names", {}).items():
        ok, epath, _ = _parse_json_array_key(raw_key)
        if ok:
            ent_ren[tuple(epath)] = new_name

    col_ren: Dict[Tuple[str, ...], Dict[Tuple[str, ...], str]] = {}
    for raw_key, new_name in patch.get("column_names", {}).items():
        ok, epath, cpath, _ = _parse_column_key(raw_key)
        if ok:
            col_ren.setdefault(tuple(epath), {})[tuple(cpath)] = new_name

    # 2) описания
    ent_desc: Dict[Tuple[str, ...], str] = {}
    for raw_key, text in patch.get("entity_descriptions", {}).items():
        ok, epath, _ = _parse_json_array_key(raw_key)
        if ok:
            ent_desc[tuple(epath)] = text

    col_desc: Dict[Tuple[str, ...], Dict[Tuple[str, ...], str]] = {}
    for raw_key, text in patch.get("column_descriptions", {}).items():
        ok, epath, cpath, _ = _parse_column_key(raw_key)
        if ok:
            col_desc.setdefault(tuple(epath), {})[tuple(cpath)] = text

    # 3) старая карта name->path (для корректного обновления relations)
    old_name_to_path = {e["name"]: tuple(e.get("path", [])) for e in prof.get("entities", [])}

    # 4) применяем к сущностям
    for e in prof.get("entities", []):
        path_t = tuple(e.get("path", []))
        # имя
        if path_t in ent_ren:
            e["name"] = ent_ren[path_t]
        # описание (добавим поле description)
        if path_t in ent_desc:
            e["description"] = ent_desc[path_t]
        # колонки
        cols = e.get("columns", [])
        by_path = {tuple(c.get("path", [])): c for c in cols}
        for cpath_t, col in by_path.items():
            if path_t in col_ren and cpath_t in col_ren[path_t]:
                col["name"] = col_ren[path_t][cpath_t]
            if path_t in col_desc and cpath_t in col_desc[path_t]:
                col["description"] = col_desc[path_t][cpath_t]

    # 5) пересобираем map path->new_name (для parent/relations)
    path_to_new_name = {tuple(e.get("path", [])): e["name"] for e in prof.get("entities", [])}

    # 6) обновляем parent по path (а не по старому имени)
    for e in prof.get("entities", []):
        depth = e.get("depth", 0)
        if depth == 0:
            e["parent"] = None
        else:
            parent_path = tuple(e.get("path", []))[:-1]
            e["parent"] = path_to_new_name.get(parent_path)

    # 7) обновляем relations, маппируя старые from/to имена к path, затем к новым именам
    new_relations = []
    for r in prof.get("relations", []):
        from_path = old_name_to_path.get(r.get("from_table"))
        to_path   = old_name_to_path.get(r.get("to_table"))
        if not from_path or not to_path:
            # если что-то не нашлось — оставим как есть (но это аномалия в исходном профиле)
            new_relations.append(r)
            continue
        new_from = path_to_new_name.get(tuple(from_path))
        new_to   = path_to_new_name.get(tuple(to_path))
        rr = dict(r)
        rr["from_table"] = new_from
        rr["to_table"] = new_to
        rr["name"] = f"fk_{new_from}_to_{new_to}"
        new_relations.append(rr)
    prof["relations"] = new_relations

    return prof

# ---------- Публичный API ----------

def build_final_profile(profile: Dict[str, Any], patch: Dict[str, Any]) -> Dict[str, Any]:
    """
    1) валидирует patch относительно profile (компактный отчёт),
    2) применяет patch, возвращает новый профиль (dict).
    Если патч некорректен — ValueError с сообщением об ошибке.
    """
    msg = validate_rename_patch_compact(profile, patch, max_examples=3)
    if msg != "SUCCESS":
        raise ValueError(msg)
    return apply_rename_patch(profile, patch)

def build_final_profile_from_files(profile_path: str, patch_path: str) -> Dict[str, Any]:
    with open(profile_path, "r", encoding="utf-8") as f:
        profile = json.load(f)
    with open(patch_path, "r", encoding="utf-8") as f:
        patch = json.load(f)
    return build_final_profile(profile, patch)
