import json
import re
from typing import Any, Dict, List, Tuple, DefaultDict
from collections import defaultdict, Counter

# Разрешаем имена в Unicode (буквы/цифры/подчёркивание), без пробелов и пунктуации
NAME_RE = re.compile(r"^[\w]+$", flags=re.UNICODE)

# ---- утилиты парсинга ключей ----

def _parse_json_array_key(key: str) -> Tuple[bool, List[str], str]:
    """Парсит ключ вида '["a","b"]' -> ["a","b"]. Возвращает (ok, path, err)."""
    try:
        v = json.loads(key)
        if isinstance(v, list) and all(isinstance(x, str) for x in v):
            return True, v, ""
        return False, [], f"must be JSON array of strings, e.g. [\"path\",\"to\",\"entity\"]"
    except Exception as e:
        return False, [], f"invalid JSON array: {e}"

def _parse_column_key(key: str) -> Tuple[bool, List[str], List[str], str]:
    """Парсит ключ вида '[..]::[..]' для колонок. Возвращает (ok, entity_path, col_path, err)."""
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

# ---- индексация профиля ----

def _index_entities_by_path(profile: Dict[str, Any]) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    return {tuple(e.get("path", [])): e for e in profile.get("entities", [])}

def _index_columns_by_path(entity: Dict[str, Any]) -> Dict[Tuple[str, ...], Dict[str, Any]]:
    return {tuple(c.get("path", [])): c for c in entity.get("columns", [])}

# ---- компактный сборщик ошибок ----

class Errs:
    def __init__(self, max_examples_per_category: int = 3):
        self.max_examples = max_examples_per_category
        self.counts = Counter()                      # category -> count
        self.samples: DefaultDict[str, List[str]] = defaultdict(list)  # category -> [examples]

    def add(self, category: str, example: str):
        self.counts[category] += 1
        if len(self.samples[category]) < self.max_examples:
            self.samples[category].append(example)

    def ok(self) -> bool:
        return sum(self.counts.values()) == 0

    def render(self) -> str:
        total = sum(self.counts.values())
        if total == 0:
            return "SUCCESS"
        parts = [f"ERROR: {total} problem(s) found"]
        # сводка категорий
        cats = ", ".join([f"{k}={v}" for k, v in self.counts.most_common()])
        parts.append(f"Summary: {cats}")
        # примеры по категориям
        for cat, examples in self.samples.items():
            parts.append(f"- {cat}:")
            for ex in examples:
                parts.append(f"  • {ex}")
            if self.counts[cat] > len(examples):
                parts.append(f"  • ... and {self.counts[cat] - len(examples)} more")
        # короткая подсказка по формату ключей
        parts.append("Hint: keys must use JSON array paths. Examples:")
        parts.append('  • entity_names:   "[]": "Root", "[\\"orders\\"]": "Orders"')
        parts.append('  • column_names:   "[]::[\\"id\\"]": "record_id", "[\\"orders\\"]::[\\"price\\"]": "amount"')
        return " | ".join(parts)

# ---- валидатор ----

def validate_rename_patch(profile: Dict[str, Any], patch: Dict[str, Any],
                          max_examples_per_category: int = 3) -> str:
    """
    Валидирует патч переименований/описаний от LLM для профиля.
    Возвращает "SUCCESS" или компактный "ERROR: ..." с сводкой по категориям и примерами.
    """
    errs = Errs(max_examples_per_category)

    allowed_top = {"entity_names", "entity_descriptions", "column_names", "column_descriptions", "notes"}
    for k in patch.keys():
        if k not in allowed_top:
            errs.add("unknown_top_level_key", f"unknown key '{k}' (allowed: {sorted(allowed_top)})")

    ents_by_path = _index_entities_by_path(profile)

    # entity_names
    ent_new_names: Dict[Tuple[str, ...], str] = {}
    planned_names = {tuple(p): e.get("name") for p, e in ents_by_path.items()}
    if isinstance(patch.get("entity_names", {}), dict):
        for raw_key, new_name in patch["entity_names"].items():
            ok, epath, msg = _parse_json_array_key(raw_key)
            if not ok:
                errs.add("entity_key_format", f"entity_names[{raw_key}]: {msg}")
                continue
            tpath = tuple(epath)
            if tpath not in ents_by_path:
                errs.add("entity_path_missing", f"entity_names[{raw_key}]: entity path not found in profile")
                continue
            if not isinstance(new_name, str) or not new_name:
                errs.add("entity_name_invalid", f"entity_names[{raw_key}]: new name must be non-empty string")
                continue
            if not NAME_RE.match(new_name):
                errs.add("entity_name_invalid", f"entity_names[{raw_key}]: invalid '{new_name}' (letters/digits/underscore only)")
                continue
            # уникальность
            old = planned_names[tpath]
            planned_names[tpath] = new_name
            # если после подстановки имя встречается более одного раза — дубликат
            vals = list(planned_names.values())
            if len(vals) != len(set(vals)):
                errs.add("entity_name_duplicate", f"'{new_name}' duplicates another entity name")
                planned_names[tpath] = old
                continue
            ent_new_names[tpath] = new_name
    else:
        if "entity_names" in patch:
            errs.add("entity_names_type", "entity_names must be an object")

    # column_names
    cols_new_names: Dict[Tuple[str, ...], Dict[Tuple[str, ...], str]] = {}
    if isinstance(patch.get("column_names", {}), dict):
        for raw_key, new_name in patch["column_names"].items():
            ok, epath, cpath, msg = _parse_column_key(raw_key)
            if not ok:
                errs.add("column_key_format", f"column_names[{raw_key}]: {msg}")
                continue
            te, tc = tuple(epath), tuple(cpath)
            if te not in ents_by_path:
                errs.add("column_entity_missing", f"column_names[{raw_key}]: entity path not found")
                continue
            ent = ents_by_path[te]
            cols_by_path = _index_columns_by_path(ent)
            if tc not in cols_by_path:
                errs.add("column_path_missing", f"column_names[{raw_key}]: column path not found in entity")
                continue
            if not isinstance(new_name, str) or not new_name:
                errs.add("column_name_invalid", f"column_names[{raw_key}]: new name must be non-empty string")
                continue
            if not NAME_RE.match(new_name):
                errs.add("column_name_invalid", f"column_names[{raw_key}]: invalid '{new_name}' (letters/digits/underscore only)")
                continue
            if new_name == "rec_id" or new_name.startswith("idx"):
                errs.add("column_name_conflict_pk", f"column_names[{raw_key}]: '{new_name}' conflicts with PK naming")
                continue
            # уникальность внутри сущности
            pending = cols_new_names.setdefault(te, {})
            planned = {c.get("name") for c in ent.get("columns", [])}
            planned.discard(cols_by_path[tc].get("name"))
            planned.update(pending.values())
            if new_name in planned:
                errs.add("column_name_duplicate", f"column_names[{raw_key}]: '{new_name}' duplicates another column in entity")
                continue
            pending[tc] = new_name
    else:
        if "column_names" in patch:
            errs.add("column_names_type", "column_names must be an object")

    # entity_descriptions
    if "entity_descriptions" in patch:
        if not isinstance(patch["entity_descriptions"], dict):
            errs.add("entity_descriptions_type", "entity_descriptions must be an object")
        else:
            for raw_key, v in patch["entity_descriptions"].items():
                ok, epath, msg = _parse_json_array_key(raw_key)
                if not ok:
                    errs.add("entity_desc_key_format", f"entity_descriptions[{raw_key}]: {msg}")
                    continue
                if tuple(epath) not in ents_by_path:
                    errs.add("entity_desc_path_missing", f"entity_descriptions[{raw_key}]: entity path not found")
                    continue
                if not isinstance(v, str) or not v.strip():
                    errs.add("entity_desc_invalid", f"entity_descriptions[{raw_key}]: description must be non-empty string")

    # column_descriptions
    if "column_descriptions" in patch:
        if not isinstance(patch["column_descriptions"], dict):
            errs.add("column_descriptions_type", "column_descriptions must be an object")
        else:
            for raw_key, v in patch["column_descriptions"].items():
                ok, epath, cpath, msg = _parse_column_key(raw_key)
                if not ok:
                    errs.add("column_desc_key_format", f"column_descriptions[{raw_key}]: {msg}")
                    continue
                te, tc = tuple(epath), tuple(cpath)
                if te not in ents_by_path:
                    errs.add("column_desc_entity_missing", f"column_descriptions[{raw_key}]: entity path not found")
                    continue
                ent = ents_by_path[te]
                cols_by_path = _index_columns_by_path(ent)
                if tc not in cols_by_path:
                    errs.add("column_desc_path_missing", f"column_descriptions[{raw_key}]: column path not found in entity")
                    continue
                if not isinstance(v, str) or not v.strip():
                    errs.add("column_desc_invalid", f"column_descriptions[{raw_key}]: description must be non-empty string")

    return errs.render()
