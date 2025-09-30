#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
xml2graph: построение наблюдаемого графа структуры XML (узлы/рёбра/кратности).

Использование из кода:
    from xml2graph import build_graph_from_address

    profile = build_graph_from_address("path/to/file.xml")      # путь к файлу
    profile = build_graph_from_address("https://.../file.xml")  # URL (http/https/file)
    profile = build_graph_from_address(xml_string)              # сам XML как строка

    # profile — это dict (JSON-совместимый)

CLI:
    python xml2graph.py path_or_url.xml -o schema.json
"""

from __future__ import annotations
import argparse
import io
import json
import sys
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
import xml.etree.ElementTree as ET
from typing import Dict, Any, IO, Tuple, Optional

def _strip_ns(tag: str) -> Tuple[str, Optional[str]]:
    """Вернуть (local_name, ns_uri_or_None) из '{uri}local' или 'local'."""
    if tag.startswith("{"):
        uri, local = tag[1:].split("}", 1)
        return local, uri
    return tag, None

def _path_join(parent_path: str, name: str) -> str:
    if parent_path == "":
        return f"/{name}"
    return f"{parent_path}/{name}"

def _open_source(address_or_xml: str | Path | IO[bytes] | IO[str]) -> IO:
    """
    Универсально открывает источник для ET.iterparse:
      - файловый путь (str/Path)
      - URL (http/https/file)
      - XML-строка (начинается с '<')
      - уже открытый file-like (text/binary)
    Возвращает file-like объект (text или binary).
    """
    # Уже file-like? просто вернуть
    if hasattr(address_or_xml, "read"):
        return address_or_xml  # type: ignore[return-value]

    # pathlib.Path
    if isinstance(address_or_xml, Path):
        return open(address_or_xml, "rb")

    # str
    if isinstance(address_or_xml, str):
        s = address_or_xml.strip()

        # XML как строка
        if s.startswith("<"):
            return io.StringIO(address_or_xml)

        # Путь к локальному файлу?
        p = Path(address_or_xml)
        if p.exists():
            return open(p, "rb")

        # URL?
        parsed = urllib.parse.urlparse(address_or_xml)
        if parsed.scheme in {"http", "https", "file"}:
            resp = urllib.request.urlopen(address_or_xml)  # nosec: stdlib, доверяем пользовательскому окружению
            data = resp.read()
            return io.BytesIO(data)

    raise ValueError("Не удалось распознать источник XML: передайте путь/URL/XML-строку/файлоподобный объект")

def build_graph_from_address(address_or_xml: str | Path | IO[bytes] | IO[str]) -> Dict[str, Any]:
    """
    Построить JSON-профиль графа по XML-источнику.
    Возвращает dict с ключами: version, namespaces, roots, elements, edges.
    """
    # --- Aggregates we build ---
    elements: Dict[str, Dict[str, Any]] = {}  # path -> {name, ns_uri, attributes:set, has_text:bool, has_children:bool}
    # Edge key: (parent_path, child_name, child_ns_uri)
    edge_stats: Dict[Tuple[str, str, Optional[str]], Dict[str, Any]] = {}
    parent_instances: Counter[str] = Counter()
    # Namespaces seen in the doc: uri -> last prefix
    ns_map: Dict[str, str] = {}

    # Стек кадров текущего пути
    stack: list[Dict[str, Any]] = []

    # Готовим источник для iterparse
    src = _open_source(address_or_xml)

    # iterparse с событиями, включая пространства имён
    events = ("start", "end", "start-ns")
    context = ET.iterparse(src, events=events)

    for event, obj in context:
        if event == "start-ns":
            prefix, uri = obj
            ns_map[uri] = prefix or ""
            continue

        elem = obj
        if event == "start":
            local, ns = _strip_ns(elem.tag)
            parent_path = stack[-1]["path"] if stack else ""
            cur_path = _path_join(parent_path, local)

            frame = {
                "path": cur_path,
                "name": local,
                "ns": ns,
                "child_counts": Counter(),
                "has_children": False,
            }
            stack.append(frame)

            # обновим счётчики у родителя
            if len(stack) > 1:
                parent = stack[-2]
                parent["has_children"] = True
                parent["child_counts"][(local, ns)] += 1

            # регистрация узла
            node = elements.get(cur_path)
            if node is None:
                node = {
                    "name": local,
                    "ns_uri": ns,
                    "attributes": set(),
                    "has_text": False,
                    "has_children": False,
                }
                elements[cur_path] = node

            # атрибуты по именам (без типов)
            if elem.attrib:
                for akey in elem.attrib.keys():
                    alocal, _ans = _strip_ns(akey)
                    node["attributes"].add(alocal)

        elif event == "end":
            # закрываем элемент, финализируем кадр
            local, ns = _strip_ns(elem.tag)
            frame = stack.pop()
            cur_path = frame["path"]

            # текст непустой?
            has_text = bool(elem.text and elem.text.strip())

            # флаги узла
            node = elements[cur_path]
            node["has_text"] = node["has_text"] or has_text
            node["has_children"] = node["has_children"] or frame["has_children"]

            # учёт экземпляров родителей
            parent_instances[cur_path] += 1

            # обновление статистики рёбер из child_counts
            if frame["child_counts"]:
                for (cname, cns), cnt in frame["child_counts"].items():
                    ekey = (cur_path, cname, cns)
                    est = edge_stats.get(ekey)
                    if est is None:
                        est = {
                            "min": cnt,
                            "max": cnt,
                            "parents_with_child": 1,
                        }
                        edge_stats[ekey] = est
                    else:
                        if cnt < est["min"]:
                            est["min"] = cnt
                        if cnt > est["max"]:
                            est["max"] = cnt
                        est["parents_with_child"] += 1

            # освобождение памяти
            elem.clear()

    # корневые пути (глубина 1)
    roots = [p for p in elements.keys() if p.count("/") == 1]
    roots.sort()

    # множества атрибутов → отсортированные списки
    for p, nd in elements.items():
        nd["attributes"] = sorted(nd["attributes"])

    # материализация рёбер
    edges = []
    for (ppath, cname, cns), est in edge_stats.items():
        total_parents = parent_instances[ppath]
        edge = {
            "from": ppath,
            "to": _path_join(ppath, cname),
            "label": cname,
            "child_ns_uri": cns,
            "min_per_parent_observed": est["min"] if est["parents_with_child"] == total_parents else 0,
            "max_per_parent_observed": est["max"],
            "parent_instances_observed": total_parents,
            "parents_with_child_observed": est["parents_with_child"],
            "is_list": est["max"] > 1
        }
        edges.append(edge)

    # пространства имён
    namespaces = [{"uri": uri, "prefix": pref} for uri, pref in ns_map.items()]
    namespaces.sort(key=lambda x: (x["prefix"], x["uri"]))

    out = {
        "version": 1,
        "namespaces": namespaces,
        "roots": roots,
        "elements": elements,
        "edges": edges
    }
    return out

# -------- CLI-обёртка (необязательно) --------

def _main():
    ap = argparse.ArgumentParser(
        description="Build graph-like JSON structure from XML (elements, attributes, edges, observed multiplicities)."
    )
    ap.add_argument("xml", help="Path/URL/XML-string (if starts with '<')")
    ap.add_argument("-o", "--out", help="Path to write JSON (default: stdout)")
    args = ap.parse_args()

    result = build_graph_from_address(args.xml)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    else:
        json.dump(result, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")

if __name__ == "__main__":
    _main()
