#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
final_profile.py

Строит финальную спецификацию ETL на основе:
- entities.json (из profile2entities.py),
- rename.json (ответ LLM с бизнес-названиями),
- исходного XML (для сэмплинга значений и инференса типов).

Основная точка входа для кода:
    from final_profile import make_final_profile
    final_spec = make_final_profile(entities, rename, xml_path)

Опционально можно вызывать build_final_profile(...) c настройкой лимитов и путём к types.yaml.
"""

from __future__ import annotations
import os
import re
import json
from collections import defaultdict
from xml.etree import ElementTree as ET
from typing import Dict, List, Tuple, Any, Optional
from copy import deepcopy

# -------------------------
# Канонический набор типов
# -------------------------

DEFAULT_TYPES = {
    "canonical": {
        "string":       {"pg": "text",               "ch": "String",             "py": "str"},
        "int32":        {"pg": "integer",            "ch": "Int32",              "py": "int"},
        "int64":        {"pg": "bigint",             "ch": "Int64",              "py": "int"},
        "float64":      {"pg": "double precision",   "ch": "Float64",            "py": "float"},
        "decimal(p,s)": {"pg": "numeric({p},{s})",   "ch": "Decimal({p},{s})",   "py": "decimal.Decimal"},
        "bool":         {"pg": "boolean",            "ch": "Bool",               "py": "bool"},
        "date":         {"pg": "date",               "ch": "Date32",             "py": "datetime.date"},
        "timestamp":    {"pg": "timestamptz",        "ch": "DateTime('UTC')",    "py": "datetime.datetime"},
        "timestamp64(ms)": {"pg": "timestamptz",     "ch": "DateTime64(3, 'UTC')","py": "datetime.datetime"},
        "json":         {"pg": "jsonb",              "ch": "String",             "py": "typing.Any"},
    },
    "synonyms": {
        "text": "string",
        "varchar": "string",
        "bigint": "int64",
        "integer": "int32",
        "int4": "int32",
        "int8": "int64",
        "double": "float64",
        "double precision": "float64",
        "numeric": "decimal(p,s)",
        "decimal": "decimal(p,s)",
        "timestamptz": "timestamp",
        "timestampz": "timestamp",
        "datetime": "timestamp",
        "datetime64": "timestamp64(ms)",
        "jsonb": "json",
        "uint8": "bool",
    }
}

# В начале файла (рядом с константами)
EXAMPLES_CAP = 200


def _load_types_yaml(path: Optional[str]) -> Dict[str, Any]:
    """
    Пытается загрузить config/types.yaml. Если файл отсутствует или PyYAML не установлен — вернёт DEFAULT_TYPES.
    """
    if not path:
        return DEFAULT_TYPES
    if not os.path.exists(path):
        return DEFAULT_TYPES
    try:
        import yaml  # type: ignore
    except Exception:
        return DEFAULT_TYPES
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # простая валидация
        if "canonical" in data:
            return data
        return DEFAULT_TYPES
    except Exception:
        return DEFAULT_TYPES


# -------------------------
# Вспомогательные утилиты
# -------------------------

_SNAKE_NONWORD = re.compile(r"[^0-9a-zA-Z]+")
def _snake(s: str) -> str:
    s = _SNAKE_NONWORD.sub("_", s.strip())
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()

def _snake_path(rel_path: str) -> str:
    # 'a/b/c' -> 'a_b_c'
    return _snake(rel_path.replace("/", "_"))

def _ns_local(tag: str) -> str:
    return tag.split("}", 1)[1] if tag.startswith("{") else tag

def _split_path_no_slash(p: str) -> List[str]:
    return [seg for seg in p.split("/") if seg]

BOOL_TRUE = {"1", "true", "t", "y", "yes", "да", "истина"}
BOOL_FALSE = {"0", "false", "f", "n", "no", "нет", "ложь"}

_RE_INT = re.compile(r"^[+-]?\d+$")
_RE_DEC = re.compile(r"^[+-]?\d+[.,]\d+$")
_RE_SCI = re.compile(r"^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$")
_RE_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_RE_TS = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?"
    r"(?:Z|[+-]\d{2}:\d{2})?$"
)

def _digits_stats(num_str: str) -> Tuple[int, int, bool]:
    """
    Возвращает (int_digits, frac_digits, scientific) для строки числа (с возможным знаком),
    с учётом точки ИЛИ запятой.
    """
    s = num_str.strip()
    if _RE_SCI.match(s):
        return (0, 0, True)
    s = s.replace(",", ".")
    if "." in s:
        sign = 1 if not s.startswith("-") and not s.startswith("+") else 0
        head, tail = s.split(".", 1)
        head = head[1:] if not sign and (s[0] in "+-") else head
        head_digits = len(re.sub(r"\D", "", head))
        tail_digits = len(re.sub(r"\D", "", tail))
        return (head_digits, tail_digits, False)
    else:
        # целое
        x = s[1:] if s and s[0] in "+-" else s
        head_digits = len(re.sub(r"\D", "", x))
        return (head_digits, 0, False)

def _is_bool(s: str) -> bool:
    v = s.strip().lower()
    return v in BOOL_TRUE or v in BOOL_FALSE

def _is_int(s: str) -> bool:
    return bool(_RE_INT.match(s.strip()))

def _is_dec(s: str) -> bool:
    ss = s.strip()
    return bool(_RE_DEC.match(ss)) or bool(_RE_SCI.match(ss))

def _is_date(s: str) -> bool:
    return bool(_RE_DATE.match(s.strip()))

def _is_ts(s: str) -> bool:
    return bool(_RE_TS.match(s.strip()))

# -------------------------
# Сэмплинг значений из XML
# -------------------------

def _sample_xml(
    xml_path: str,
    merged_entities: dict,
    sample_limit_per_entity: int = 2000
) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Возвращает stats[entity_path][field_rel_path] -> {non_null, nulls, samples, _features:{...}}.
    Сэмплирует на событии 'end' целевого тега. NS-агностично (сравниваются локальные имена).
    """
    entities = merged_entities["entities"]

    # Подготовим соответствия "полный путь" -> список (ei, fi)
    target_map: Dict[Tuple[str, ...], List[Tuple[int, int]]] = {}
    row_parts: List[Tuple[str, ...]] = []

    for ei, ent in enumerate(entities):
        row = _split_path_no_slash(ent["row_xpath"])  # ['root','item']
        row_tuple = tuple(row)
        row_parts.append(row_tuple)
        for fi, fld in enumerate(ent["fields"]):
            rel = _split_path_no_slash(fld["path"])
            full = tuple(row + rel)
            target_map.setdefault(full, []).append((ei, fi))

    # Инициализация счётчиков
    stats: Dict[str, Dict[str, Dict[str, Any]]] = {}
    row_seen = [0] * len(entities)

    for ent in entities:
        stats[ent["path"]] = {}
        for fld in ent["fields"]:
            stats[ent["path"]][fld["path"]] = {
                "non_null": 0,
                "nulls": 0,
                "samples": 0,
                "examples": [], 
                "_features": {
                    "bool_all": True,
                    "int_all": True,
                    "num_all": True,   # int|dec|sci
                    "sci_seen": False,
                    "sum_examples": 0, # кол-во собранных образцов (не выводим)
                    "date_all": True,
                    "ts_all": True,
                    "max_int_digits": 0,
                    "max_frac_digits": 0,
                    "frac_seen": False,
                    "frac_ms_seen": False,  # для ts: были ли доли секунд
                }
            }

    # Основной проход
    stack: List[str] = []
    all_done = False
    for ev, el in ET.iterparse(xml_path, events=("start", "end")):
        if ev == "start":
            stack.append(_ns_local(el.tag))
            continue

        key = tuple(stack)

        # 1) закрывается целевой тег поля — обновляем статы
        if key in target_map:
            txt = (el.text or "").strip()
            for (ei, fi) in target_map[key]:
                if row_seen[ei] >= sample_limit_per_entity:
                    continue
                ent = entities[ei]
                fld = ent["fields"][fi]
                st = stats[ent["path"]][fld["path"]]
                st["samples"] += 1
                if txt:
                    st["non_null"] += 1
                    _update_features(st["_features"], txt)
                    # сохраняем пример значения
                    if len(st["examples"]) < EXAMPLES_CAP:
                        st["examples"].append(txt)
                else:
                    st["nulls"] += 1

        # 2) закрывается узел строки сущности — учитываем просмотренную строку
        for ei, rp in enumerate(row_parts):
            if key == rp:
                if row_seen[ei] < sample_limit_per_entity:
                    row_seen[ei] += 1
                break

        stack.pop()
        el.clear()

        if not all_done and all(rs >= sample_limit_per_entity for rs in row_seen):
            all_done = True
            break

    return stats


def _update_features(fe: Dict[str, Any], txt: str) -> None:
    """
    Обновляет агрегаты для инференса типов по одному значению.
    """
    v = txt.strip()
    # boolean?
    if not _is_bool(v):
        fe["bool_all"] = False

    # date?
    if not _is_date(v):
        fe["date_all"] = False

    # timestamp?
    if _is_ts(v):
        # проверим наличие долей секунд (до 3 знаков достаточно для ms)
        if "." in v:
            # дробная часть до 'Z' или конца строки
            m = re.search(r"\.(\d+)", v)
            if m and len(m.group(1)) > 0:
                fe["frac_ms_seen"] = True
    else:
        fe["ts_all"] = False

    # числовые признаки
    if _RE_SCI.match(v):
        fe["sci_seen"] = True
        # это число
        int_d, frac_d, _ = _digits_stats(v)
        fe["max_int_digits"] = max(fe["max_int_digits"], int_d)
        fe["max_frac_digits"] = max(fe["max_frac_digits"], frac_d)
    elif _RE_INT.match(v):
        int_d, frac_d, sci = _digits_stats(v)
        fe["max_int_digits"] = max(fe["max_int_digits"], int_d)
        fe["int_all"] = fe["int_all"] and True
        fe["max_frac_digits"] = max(fe["max_frac_digits"], frac_d)
        fe["frac_seen"] = fe["frac_seen"] or (frac_d > 0)
    elif _RE_DEC.match(v):
        int_d, frac_d, sci = _digits_stats(v)
        fe["int_all"] = False
        fe["frac_seen"] = True
        fe["max_int_digits"] = max(fe["max_int_digits"], int_d)
        fe["max_frac_digits"] = max(fe["max_frac_digits"], frac_d)
    else:
        fe["num_all"] = False
        fe["int_all"] = False


# -------------------------
# Переименования от LLM
# -------------------------

def _index_rename(rename_json: dict) -> Tuple[
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Dict[str, Any]]]
]:
    """
    Возвращает:
      ent_renames[path] = {"alias":..., "title":..., "description":...}
      fld_renames[path][rel_path] = {"name":..., "title":..., "description":...}
    Формат rename_json допускает варианты ключей:
      - сущность: "path" или "entity_path" или "name"
      - поле:     "path" или "source_path" или "rel_path"
    """
    ent_ren: Dict[str, Dict[str, Any]] = {}
    fld_ren: Dict[str, Dict[str, Dict[str, Any]]] = {}

    ents = rename_json.get("entities", [])
    for e in ents:
        # распознаём целевую сущность
        ep = e.get("path") or e.get("entity_path") or e.get("match", {}).get("path")
        if not ep:
            # запасной путь — иногда присылают name==последний сегмент
            nm = e.get("name") or e.get("match", {}).get("name")
            if nm:
                ep = f"/root/{nm}"  # очень грубая эвристика
        if not ep:
            continue

        ent_ren[ep] = {
            "alias": e.get("alias"),
            "title": e.get("title"),
            "description": e.get("description"),
        }

        for f in e.get("fields", []):
            fp = f.get("path") or f.get("source_path") or f.get("rel_path")
            if not fp:
                continue
            fld_ren.setdefault(ep, {})[fp] = {
                "name": f.get("name") or f.get("alias"),
                "title": f.get("title"),
                "description": f.get("description"),
            }

    return ent_ren, fld_ren


def _apply_rename(entities_json: dict, rename_json: dict) -> dict:
    """
    Применяет бизнес-названия к entities-json.
    Возвращает НОВЫЙ dict (deep copy через json-ранду-трип).
    """
    base = deepcopy(entities_json)
    ent_ren, fld_ren = _index_rename(rename_json)

    for ent in base["entities"]:
        ep = ent["path"]
        r = ent_ren.get(ep) or {}
        ent["alias"] = r.get("alias") or ent["name"]
        if r.get("title"):
            ent["title"] = r["title"]
        if r.get("description"):
            ent["description"] = r["description"]
        # переименования полей
        for fld in ent.get("fields", []):
            rel = fld["path"]
            rr = (fld_ren.get(ep) or {}).get(rel) or {}
            if rr.get("name"):
                fld["alias"] = rr["name"]
            if rr.get("title"):
                fld["title"] = rr["title"]
            if rr.get("description"):
                fld["description"] = rr["description"]

    return base


# -------------------------
# Инференс типов по фичам
# -------------------------

# --- strict type inference: any ambiguity -> string ---
import re

_INT_RE = re.compile(r"^[+-]?\d+$")
_DEC_RE = re.compile(r"^[+-]?\d+(?:[.,]\d+)?$")  # точка/запятая
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_TZ_RE = r"(?:Z|[+-]\d{2}:\d{2})"
_ISO_DT_RE = re.compile(rf"^\d{{4}}-\d{{2}}-\d{{2}}[T ]\d{{2}}:\d{{2}}:\d{{2}}{_ISO_TZ_RE}?$")
_ISO_DT_MS_RE = re.compile(rf"^\d{{4}}-\d{{2}}-\d{{2}}[T ]\d{{2}}:\d{{2}}:\d{{2}}\.\d+{_ISO_TZ_RE}?$")

def _infer_canonical_type_from_samples(values: list[str]) -> str:
    """
    Возвращает один из канонических типов:
      string, bool, date, timestamp, timestamp64(ms), int32, int64, decimal(p,s), float64
    Правило: если среди непустых сэмплов есть хоть одно значение, которое не подходит под тип —
    откатываемся к 'string'.
    """
    # нормализуем: убираем None/пустые
    vals = [(v if v is not None else "").strip() for v in values]
    vals = [v for v in vals if v != ""]
    if not vals:
        return "string"

    # bool — только 'true'/'false'
    low = {v.lower() for v in vals}
    if all(v in {"true", "false"} for v in low):
        return "bool"

    # date (строгий ISO 8601 YYYY-MM-DD)
    if all(_ISO_DATE_RE.fullmatch(v) for v in vals):
        return "date"

    # timestamp/timestamp64(ms)
    if all(_ISO_DT_MS_RE.fullmatch(v) for v in vals):
        return "timestamp64(ms)"
    if all(_ISO_DT_RE.fullmatch(v) for v in vals):
        return "timestamp"

    # целые
    if all(_INT_RE.fullmatch(v) for v in vals):
        # оценим разрядность без знака и ведущих нулей
        max_len = 0
        for v in vals:
            s = v.lstrip("+-").lstrip("0") or "0"
            max_len = max(max_len, len(s))
        return "int32" if max_len <= 9 else "int64"

    # десятичные (без научной нотации)
    if all(_DEC_RE.fullmatch(v) and ("e" not in v.lower()) for v in vals):
        int_digits, frac_digits = 0, 0
        for v in vals:
            vv = v.replace(",", ".")
            if "." in vv:
                i, f = vv.split(".", 1)
                f = f or ""
            else:
                i, f = vv, ""
            i = i.lstrip("+-")
            i = i.lstrip("0") or "0"
            int_digits = max(int_digits, len(i))
            frac_digits = max(frac_digits, len(f))
        p = max(1, int_digits + frac_digits)
        s = max(0, frac_digits)
        return f"decimal({p},{s})"

    # научная нотация — только если *все* подходят
    def _is_sci(v: str) -> bool:
        vv = v.replace(",", ".")
        try:
            return ("e" in vv.lower()) and float(vv) is not None
        except Exception:
            return False

    if all(_is_sci(v) for v in vals):
        return "float64"

    # иначе — строка
    return "string"



# -------------------------
# Построение финального профиля
# -------------------------

def _column_name_for_field(ent: dict, fld: dict, used: set) -> str:
    # приоритет: alias от LLM -> alias на сущности + колонке
    name = fld.get("alias") or _snake_path(fld["path"])
    # нормализация snake и уникальность
    name = _snake(name)
    base = name
    i = 2
    while name in used:
        name = f"{base}_{i}"
        i += 1
    used.add(name)
    return name

def _table_name_for_entity(ent: dict) -> str:
    # alias сущности -> snake -> <=63
    alias = ent.get("alias") or ent["name"]
    name = _snake(alias)[:63]
    return name or _snake(ent["name"])

def _collect_entity_map(entities: List[dict]) -> Dict[str, dict]:
    return {e["path"]: e for e in entities}

def _parent_of(ent: dict, ent_map: Dict[str, dict]) -> Optional[dict]:
    p = ent.get("parent")
    return ent_map.get(p) if p else None

def _build_table_spec_for_entity(
    ent: dict,
    ent_map: Dict[str, dict],
    stats: Dict[str, Dict[str, Dict[str, Any]]],
) -> dict:
    """
    Строит одну таблицу final-spec по сущности.
    """
    table_name = _table_name_for_entity(ent)
    used_cols = set()

    # Базовые колонки: surrogate PK
    columns = [{
        "name": "id",
        "type": "int64",
        "nullable": False,
        "role": "pk_surrogate",
        "description": "Суррогатный первичный ключ."
    }]

    # FK на родителя (если есть)
    parent = _parent_of(ent, ent_map)
    parent_fk_col = None
    if parent:
        parent_table = _table_name_for_entity(parent)
        parent_fk_col = f"{_snake(parent_table)}_id"
        columns.append({
            "name": parent_fk_col,
            "type": "int64",
            "nullable": False,
            "role": "fk_parent",
            "ref_table": parent_table,
            "ref_column": "id",
            "description": "Внешний ключ на родительскую сущность."
        })

    # Поля из XML
    colname_by_field: Dict[str, str] = {}  # rel_path -> colname
    nhints = set(ent.get("key_hints", []) or [])
    order_hints = ent.get("ordering_hints", []) or []

    for fld in ent.get("fields", []):
        colname = _column_name_for_field(ent, fld, used_cols)
        colname_by_field[fld["path"]] = colname

        # наблюдения
        st = (stats.get(ent["path"], {}) or {}).get(fld["path"], None)
        non_null = st["non_null"] if st else 0
        nulls = st["nulls"] if st else 0
        samples = st["samples"] if st else 0

        # тип
        examples = (st or {}).get("examples", [])
        if examples:
            ctype = _infer_canonical_type_from_samples(examples)
        else:
            ctype = "string"

        # nullable: если поле опционально ИЛИ наблюдались null — True
        nullable = True if (fld.get("optional") or (st and st["nulls"] > 0)) else False

        col = {
            "name": colname,
            "type": ctype,
            "nullable": nullable,
            "source_path": fld["path"],
            "is_key_hint": fld["path"] in nhints,
            "observed": {"non_null": non_null, "nulls": nulls, "samples": samples}
        }
        if "title" in fld:       col["title"] = fld["title"]
        if "description" in fld: col["description"] = fld["description"]
        columns.append(col)

    # order_by: если есть подсказки — используем их (переименованные колонки)
    order_by_cols: List[str] = []
    for oh in order_hints:
        if oh in colname_by_field:
            order_by_cols.append(colname_by_field[oh])

    # если подсказок нет — добавим синтетический seq
    is_child = parent is not None
    seq_added = False
    if is_child and not order_by_cols:
        columns.append({
            "name": "seq",
            "type": "int32",
            "nullable": False,
            "role": "sequence_within_parent",
            "description": "Порядковый номер строки в рамках родительской записи."
        })
        order_by_cols = ["seq"]
        seq_added = True

    # Кандидаты на натуральные ключи — по key_hints, но уже после переименования
    nat_keys = []
    for kh in nhints:
        if kh in colname_by_field:
            nat_keys.append(colname_by_field[kh])

    # Ограничения уникальности
    unique = []
    if nat_keys:
        unique.append({
            "columns": nat_keys,
            "note": "Наблюдаемый естественный ключ (из key_hints)."
        })
    if parent and order_by_cols:
        unique.append({
            "columns": [parent_fk_col] + order_by_cols,
            "note": "Уникальность дочерних строк в рамках родителя (FK + order_by)."
        })

    # extract mapping
    extract_fields = []
    for fld in ent.get("fields", []):
        extract_fields.append({
            "column": colname_by_field[fld["path"]],
            "rel_xpath": fld["path"]
        })

    table_spec = {
        "entity_path": ent["path"],
        "name": ent["name"],
        "alias": ent.get("alias") or ent["name"],
        "title": ent.get("title") or ent.get("alias") or ent["name"],
        "description": ent.get("description") or "",
        "table": table_name,
        "columns": columns,
        "primary_key": {"columns": ["id"], "type": "surrogate"},
        "natural_key_candidates": nat_keys,
        "unique": unique,
        "order_by": order_by_cols,
        "extract": {
            "row_xpath": ent["row_xpath"],
            "fields": extract_fields
        },
        "parent": {
            "entity_path": parent["path"] if parent else None,
            "table": _table_name_for_entity(parent) if parent else None,
            "fk_column": parent_fk_col
        },
        "children": [{
            "entity_path": ch["path"],
            "table": _snake(_table_name_for_entity(ent_map[ch["path"]])),
            "relpath_from_parent": ch["relpath_from_parent"],
            "min": int(ch.get("min", 0)),
            "max": int(ch.get("max", 0)),
        } for ch in ent.get("children", [])]
    }

    return table_spec


def _topo_load_order(tables: List[dict]) -> List[str]:
    name_to_tbl = {t["table"]: t for t in tables}
    deps = defaultdict(set)  # table -> {parents}
    for t in tables:
        p = t["parent"]["table"]
        if p:
            deps[t["table"]].add(p)
        else:
            deps.setdefault(t["table"], set())

    # Kahn
    out = []
    S = [k for k, v in deps.items() if not v]
    while S:
        n = S.pop()
        out.append(n)
        to_remove = []
        for k, v in deps.items():
            if n in v:
                v.remove(n)
                if not v:
                    to_remove.append(k)
        for r in to_remove:
            S.append(r)
    # на случай циклов — просто добавим оставшиеся
    for k in deps:
        if k not in out:
            out.append(k)
    return out


def build_final_profile(
    entities: dict,
    rename: dict,
    xml_source: str,
    types_yaml_path: Optional[str] = "config/types.yaml",
    sample_limit_per_entity: int = 2000
) -> dict:
    """
    Полная сборка финального профиля.
    """
    # 1) Типы
    _ = _load_types_yaml(types_yaml_path)  # сейчас не выводим, но может пригодиться для будущих маппингов

    if "entities" not in entities or not isinstance(entities["entities"], list):
        raise ValueError(
            "Ожидается summary-JSON из profile2entities (ключ 'entities': [...]). "
            "Похоже, вы передали исходный schema.json или объект с неподходящей структурой."
        )

    # 2) Применим переименования
    merged = _apply_rename(deepcopy(entities), rename)


    # 3) Сэмплирование для инференса типов/nullable
    stats = _sample_xml(xml_source, merged, sample_limit_per_entity=sample_limit_per_entity)

    # 4) Таблицы
    ent_map = _collect_entity_map(merged["entities"])
    tables = []
    for ent in merged["entities"]:
        tables.append(_build_table_spec_for_entity(ent, ent_map, stats))

    # 5) Порядок загрузки
    load_order = _topo_load_order(tables)

    spec = {
        "version": 1,
        "source": {"xml": xml_source},
        "typeset": "canonical",
        "tables": tables,
        "load_order": load_order,
        "notes": {
            "types_origin": "Types inferred from XML samples and mapped to canonical names.",
            "decimal_policy": "decimal(p,s) chosen by max observed integer/fraction digits; scientific notation → float64.",
            "timestamp_policy": "ISO-8601 with fractional seconds → timestamp64(ms); without → timestamp.",
            "nullable_policy": "nullable=True if field was optional in schema or any NULL/empty observed.",
            "pk_fk": "Each table has surrogate PK id (int64); children have FK to parent id.",
            "order_policy": "order_by uses observed ordering_hints; if absent, synthetic seq within parent is added."
        }
    }
    return spec


# Удобная обёртка для вызова из кода
def make_final_profile(entities_json: dict, rename_json: dict, xml_path: str) -> dict:
    return build_final_profile(
        entities=entities_json,
        rename=rename_json,
        xml_source=xml_path,
        types_yaml_path="config/types.yaml",
        sample_limit_per_entity=2000
    )


# Необязательный CLI — на случай запуска из консоли
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Build final ETL profile from entities.json, rename.json and XML.")
    ap.add_argument("--entities", required=True, help="Path to entities.json")
    ap.add_argument("--rename", required=True, help="Path to rename.json (LLM patch)")
    ap.add_argument("--xml", required=True, help="Path to XML")
    ap.add_argument("-o", "--out", help="Output JSON path (default: stdout)")
    ap.add_argument("--limit", type=int, default=2000, help="Row sample limit per entity (default: 2000)")
    ap.add_argument("--types", default="config/types.yaml", help="Path to types.yaml (default: config/types.yaml)")
    args = ap.parse_args()

    with open(args.entities, "r", encoding="utf-8") as f:
        entities_json = json.load(f)
    with open(args.rename, "r", encoding="utf-8") as f:
        rename_json = json.load(f)

    spec = build_final_profile(
        entities=entities_json,
        rename=rename_json,
        xml_source=args.xml,
        types_yaml_path=args.types,
        sample_limit_per_entity=args.limit
    )

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(spec, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(spec, ensure_ascii=False, indent=2))
