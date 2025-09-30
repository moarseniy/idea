#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Валидатор патча бизнес-названий/описаний к entities.json.

Проверяет:
- структуру патча (version, entities[])
- что все path в патче существуют в исходном entities.json
- что поля (fields[].path) существуют в соответствующей сущности
- допустимость alias (snake_case латиницей, <=63)
- уникальность alias сущностей (если заданы) и полей внутри сущности
- отсутствие неизвестных ключей

Возвращает строку: "SUCCESS" или многострочную с ошибками.
Также есть CLI: python entities_patch_validator.py --entities entities.json --patch patch.json
"""

import argparse
import json
import re
from typing import Dict, Any, List, Set

ALIAS_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")  # snake_case, <=63
MAX_TITLE_LEN = 255
MAX_DESC_LEN = 2000

ALLOWED_ENTITY_KEYS = {"path", "alias", "title", "description", "fields"}
ALLOWED_FIELD_KEYS  = {"path", "alias", "title", "description"}

def _load_json_bom_tolerant(path: str) -> Dict[str, Any]:
    import codecs
    with open(path, "rb") as f:
        data = f.read()
    if data.startswith(codecs.BOM_UTF8):
        text = data.decode("utf-8-sig")
    elif data[:2] == b"\xff\xfe":
        text = data.decode("utf-16-le")
    elif data[:2] == b"\xfe\xff":
        text = data.decode("utf-16-be")
    elif data[:4] == b"\xff\xfe\x00\x00":
        text = data.decode("utf-32-le")
    elif data[:4] == b"\x00\x00\xfe\xff":
        text = data.decode("utf-32-be")
    else:
        text = data.decode("utf-8")
    return json.loads(text)

def _index_entities(base_entities: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """path -> entity dict; плюс добавляем set полей для удобной проверки."""
    idx = {}
    for ent in base_entities.get("entities", []):
        path = ent.get("path")
        if not path:
            continue
        idx[path] = ent
        # Соберём множество допустимых путей полей
        field_paths = set()
        for f in ent.get("fields", []):
            p = f.get("path")
            if p:
                field_paths.add(p)
        ent["_field_paths_set"] = field_paths
    return idx

def _validate_alias(name: str) -> str:
    if not ALIAS_RE.match(name):
        return f"alias '{name}': должен быть snake_case латиницей, начинаться с буквы, длина ≤ 63"
    return ""

def validate_patch(base: Dict[str, Any], patch: Dict[str, Any]) -> str:
    errors: List[str] = []

    # Базовые проверки
    if not isinstance(patch, dict):
        return "Patch is not a JSON object"
    if patch.get("version") != 1:
        errors.append("patch.version должен быть равен 1")

    base_idx = _index_entities(base)
    if not base_idx:
        errors.append("entities.json пуст или не содержит entities[]")

    seen_entity_paths: Set[str] = set()
    seen_entity_aliases: Set[str] = set()

    ents = patch.get("entities")
    if not isinstance(ents, list) or not ents:
        errors.append("patch.entities: обязателен и должен быть непустым массивом")

    if errors:
        return "\n".join(errors)

    for i, pe in enumerate(ents):
        where = f"entities[{i}]"
        if not isinstance(pe, dict):
            errors.append(f"{where}: объект обязателен")
            continue

        # Неизвестные ключи
        unknown = set(pe.keys()) - ALLOWED_ENTITY_KEYS
        if unknown:
            errors.append(f"{where}: неизвестные поля: {sorted(unknown)}")

        path = pe.get("path")
        if not path:
            errors.append(f"{where}.path: обязателен")
            continue
        if path in seen_entity_paths:
            errors.append(f"{where}.path: дублируется '{path}'")
            continue
        seen_entity_paths.add(path)

        base_ent = base_idx.get(path)
        if not base_ent:
            errors.append(f"{where}.path: не найден в базовом entities.json: {path}")
            continue

        # alias/title/description
        alias = pe.get("alias")
        if alias is not None:
            msg = _validate_alias(alias)
            if msg:
                errors.append(f"{where}.alias: {msg}")
            if alias in seen_entity_aliases:
                errors.append(f"{where}.alias: дублируется '{alias}' среди сущностей")
            else:
                seen_entity_aliases.add(alias)

        title = pe.get("title")
        if title is not None and len(str(title)) > MAX_TITLE_LEN:
            errors.append(f"{where}.title: длина > {MAX_TITLE_LEN}")

        description = pe.get("description")
        if description is not None and len(str(description)) > MAX_DESC_LEN:
            errors.append(f"{where}.description: длина > {MAX_DESC_LEN}")

        # Поля
        pfields = pe.get("fields", [])
        if pfields is None:
            pfields = []
        if not isinstance(pfields, list):
            errors.append(f"{where}.fields: должен быть массивом")
            continue

        seen_field_paths: Set[str] = set()
        seen_field_aliases: Set[str] = set()
        allowed_field_paths = base_ent.get("_field_paths_set", set())

        for j, pf in enumerate(pfields):
            fwhere = f"{where}.fields[{j}]"
            if not isinstance(pf, dict):
                errors.append(f"{fwhere}: должен быть объектом")
                continue

            unknown_f = set(pf.keys()) - ALLOWED_FIELD_KEYS
            if unknown_f:
                errors.append(f"{fwhere}: неизвестные поля: {sorted(unknown_f)}")

            fpath = pf.get("path")
            if not fpath:
                errors.append(f"{fwhere}.path: обязателен")
                continue
            if fpath in seen_field_paths:
                errors.append(f"{fwhere}.path: дублируется '{fpath}' в рамках сущности {path}")
                continue
            seen_field_paths.add(fpath)

            if fpath not in allowed_field_paths:
                errors.append(f"{fwhere}.path: нет такого поля у сущности {path}")

            falias = pf.get("alias")
            if falias is not None:
                msg = _validate_alias(falias)
                if msg:
                    errors.append(f"{fwhere}.alias: {msg}")
                if falias in seen_field_aliases:
                    errors.append(f"{fwhere}.alias: дублируется '{falias}' в рамках сущности {path}")
                else:
                    seen_field_aliases.add(falias)

            ftitle = pf.get("title")
            if ftitle is not None and len(str(ftitle)) > MAX_TITLE_LEN:
                errors.append(f"{fwhere}.title: длина > {MAX_TITLE_LEN}")

            fdesc = pf.get("description")
            if fdesc is not None and len(str(fdesc)) > MAX_DESC_LEN:
                errors.append(f"{fwhere}.description: длина > {MAX_DESC_LEN}")

    return "SUCCESS" if not errors else "\n".join(errors)

# ==== CLI ====

def _main():
    ap = argparse.ArgumentParser(description="Validate patch JSON against entities.json.")
    ap.add_argument("--entities", required=True, help="Path to entities.json")
    ap.add_argument("--patch", required=True, help="Path to patch.json")
    args = ap.parse_args()

    base = _load_json_bom_tolerant(args.entities)
    patch = _load_json_bom_tolerant(args.patch)

    result = validate_patch(base, patch)
    print(result)
    raise SystemExit(0 if result == "SUCCESS" else 2)

if __name__ == "__main__":
    _main()
