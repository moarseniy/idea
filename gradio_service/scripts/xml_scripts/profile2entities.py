#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
profile2entities: свёртка граф-профиля XML в ETL-дружественные сущности.

Использование из кода:
    from profile2entities import profile_to_entities

    entities = profile_to_entities(profile_dict, table_name_style="short")  # dict -> dict

CLI:
    python profile2entities.py schema.json -o entities.json
"""

from __future__ import annotations
import argparse
import json
import re
from collections import deque, defaultdict
from typing import Dict, Any, Tuple, List, Set

# Подсказки для возможных "естественных" ключей и порядков
ID_HINTS = [
    "cad_number", "record_number", "section_number",
    "ord_nmb", "number_pp", "num_geopoint",
    "id", "guid", "uuid", "code"
]
ORDER_HINT_RE = re.compile(r"(ord(?:er)?|nmb|number_pp|num_geopoint|seq|index)$", re.I)

def _snake(parts: List[str]) -> str:
    return "_".join(p.replace("-", "_") for p in parts if p)

def _split_path(p: str) -> List[str]:
    return [seg for seg in p.split("/") if seg]

def _index_edges(edges: List[Dict[str, Any]]):
    by_from = defaultdict(list)
    by_pair = {}
    for e in edges:
        by_from[e["from"]].append(e)
        by_pair[(e["from"], e["to"])] = e
    return by_from, by_pair

def _is_optional(edge: Dict[str, Any]) -> bool:
    return edge["parents_with_child_observed"] < edge["parent_instances_observed"]

def _collect_entities(schema: Dict[str, Any]):
    """Сущность = любой узел-список (is_list=true) + дети корня (/root/*)."""
    entities: Set[str] = set()
    list_parents = {}
    for e in schema["edges"]:
        if e.get("is_list"):
            entities.add(e["to"])
            list_parents[e["to"]] = e
    # гарантируем /root/* как стартовые сущности (например, /root/item)
    for e in schema["edges"]:
        if e["from"].count("/") == 1:  # from == /root
            entities.add(e["to"])
    return entities, list_parents

def _nearest_entity_ancestor(path: str, entity_set: Set[str]) -> str | None:
    parts = _split_path(path)
    for i in range(len(parts) - 1, 0, -1):
        cand = "/" + "/".join(parts[:i])
        if cand in entity_set:
            return cand
    return None

def _build_child_map(entity_path: str, edges_by_from, entity_set, edges_by_pair):
    """Ищем ближайшие дочерние сущности, проходя через 1:1-обёртки до первого списка."""
    out = {}
    q = deque()
    q.append((entity_path, []))
    visited = set([entity_path])

    def path_has_optional(path_segments):
        if not path_segments:
            return False
        cur = entity_path
        opt = False
        for seg in path_segments:
            nxt = cur + "/" + seg
            edge = edges_by_pair.get((cur, nxt))
            if edge is not None and (not edge["is_list"]) and _is_optional(edge):
                opt = True
            cur = nxt
        return opt

    while q:
        cur, segs = q.popleft()
        for e in edges_by_from.get(cur, []):
            new_segs = segs + [e["label"]]
            if e["is_list"]:
                child_path = e["to"]
                out[child_path] = (
                    "/".join(new_segs),
                    e,
                    path_has_optional(segs)  # были ли опциональные 1:1-обёртки по пути
                )
            else:
                nxt = e["to"]
                if nxt not in visited:
                    visited.add(nxt)
                    q.append((nxt, new_segs))
    return out

def _collect_fields(entity_path: str, elements: Dict[str, Any], edges_by_from, edges_by_pair):
    """Листовые значения, достижимые ТОЛЬКО через 1:1 (списки пропускаем, но ветку не обрубаем)."""
    fields = []

    def dfs(cur, rel_segments, opt_accum):
        # лист с текстом и без дочерних элементов?
        el = elements.get(cur)
        if el and el.get("has_text") and not el.get("has_children"):
            if rel_segments:
                fields.append(("/".join(rel_segments), opt_accum))
            return
        # идём только по 1:1, список — пропускаем
        for e in edges_by_from.get(cur, []):
            if e["is_list"]:
                continue
            nxt = e["to"]
            opt2 = opt_accum or _is_optional(e)
            dfs(nxt, rel_segments + [e["label"]], opt2)

    dfs(entity_path, [], False)

    # уникализируем
    seen = set()
    out = []
    for p, o in fields:
        if p not in seen:
            seen.add(p)
            out.append({"path": p, "optional": bool(o)})
    return out

def _choose_key_hints(fields: List[Dict[str, Any]]) -> List[str]:
    """До 4-х лучших кандидатов по словарю ID_HINTS, ближе к корню — лучше."""
    def score(f):
        base = f["path"].split("/")[-1]
        try:
            pri = ID_HINTS.index(base)
        except ValueError:
            pri = 999
        depth = len(f["path"].split("/"))
        return (pri, depth, len(f["path"]))
    cands = sorted(fields, key=score)
    top = [f["path"] for f in cands if f["path"].split("/")[-1] in ID_HINTS][:4]
    return top

def _choose_order_hints(fields: List[Dict[str, Any]]) -> List[str]:
    return [f["path"] for f in fields if ORDER_HINT_RE.search(f["path"].split("/")[-1])]

def _guess_table_name(entity_path: str, root_entity: str | None, style: str = "short") -> str:
    """
    Генерация имени таблицы.
      - style="short": <root_entity_name>_<last_segment> (например: item_ordinate)
      - style="full":  все сегменты, усечённые до 63 символов (snake_case)
    """
    parts = _split_path(entity_path)
    if style == "short":
        if root_entity and entity_path.startswith(root_entity):
            root_parts = _split_path(root_entity)
            tail = [parts[len(root_parts)-1], parts[-1]]  # например: ["item","ordinate"]
        else:
            tail = parts[-2:]  # запасной вариант
        name = "_".join(tail).replace("-", "_")
        return name[:63]
    else:
        # полный, но обрезанный
        name = _snake(parts)
        return name[:63]

def profile_to_entities(schema: Dict[str, Any], table_name_style: str = "short") -> Dict[str, Any]:
    """
    Свернуть graph-профиль (как из xml2graph) в список сущностей для ETL.

    :param schema: dict с ключами elements/edges/...
    :param table_name_style: "short" (по умолчанию) или "full"
    :return: dict {"version":1, "entities":[...]}
    """
    elements = schema["elements"]
    edges = schema["edges"]
    edges_by_from, edges_by_pair = _index_edges(edges)
    entity_set, list_parents = _collect_entities(schema)

    # корневая сущность = первый список-потомок от /root (например, /root/item)
    root_entity = None
    for e in edges:
        if e["from"].count("/") == 1 and e["is_list"]:
            root_entity = e["to"]
            break
    if root_entity is None:
        # запасной вариант: возьмём первого потомка /root
        for e in edges:
            if e["from"].count("/") == 1:
                root_entity = e["to"]
                break

    entities_out = []

    for ent in sorted(entity_set):
        parent_entity = _nearest_entity_ancestor(ent, entity_set)
        fields = _collect_fields(ent, elements, edges_by_from, edges_by_pair)
        key_hints = _choose_key_hints(fields)
        order_hints = _choose_order_hints(fields)
        # дети-сущности
        child_map = _build_child_map(ent, edges_by_from, entity_set, edges_by_pair)
        children = []
        for ch_path, (relp, list_edge, wrappers_opt) in sorted(child_map.items()):
            min_eff = list_edge["min_per_parent_observed"]
            if wrappers_opt:
                min_eff = 0
            children.append({
                "entity": ch_path.split("/")[-1],
                "path": ch_path,
                "table": _guess_table_name(ch_path, root_entity, table_name_style),
                "relpath_from_parent": relp,
                "min": int(min_eff),
                "max": int(list_edge["max_per_parent_observed"])
            })

        entities_out.append({
            "name": ent.split("/")[-1],
            "path": ent,
            "table": _guess_table_name(ent, root_entity, table_name_style),
            "parent": parent_entity,
            "row_xpath": ent,
            "key_hints": key_hints,
            "fields": fields,
            "children": children,
            "ordering_hints": order_hints
        })

    return {"version": 1, "entities": entities_out}

# --------- вспомогательные функции для CLI ---------

def _load_json_bom_tolerant(path: str) -> Dict[str, Any]:
    """Читает JSON (UTF-8/UTF-8-SIG/UTF-16/UTF-32). Удобно для Windows-редиректов."""
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

def _main():
    ap = argparse.ArgumentParser(description="Convert XML graph profile (schema.json) to ETL-friendly entity summary.")
    ap.add_argument("schema_json", help="Path to schema.json produced by xml2graph.py")
    ap.add_argument("-o", "--out", help="Output entities.json (default: stdout)")
    ap.add_argument("--table-name-style", choices=["short", "full"], default="short",
                    help="Имя таблицы: short=<root>_<last> (по умолч), full=весь путь (усечённый)")
    args = ap.parse_args()

    schema = _load_json_bom_tolerant(args.schema_json)
    summary = profile_to_entities(schema, table_name_style=args.table_name_style)

    txt = json.dumps(summary, ensure_ascii=False, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(txt)
    else:
        print(txt)

if __name__ == "__main__":
    _main()
