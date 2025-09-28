#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
convert_optimal.py

Оптимальная (strict) версия: требует ijson и pandas+openpyxl.
Работает потоково для больших JSON (ijson) и для XML (iterparse).
"""

import argparse
import csv
import json
import xml.etree.ElementTree as ET
from collections import defaultdict
from itertools import product
import os
import sys

# Требуемые внешние зависимости (строгая версия)
import ijson
import pandas as pd  # для .xlsx
# openpyxl используется pandas автоматически при экспорте в xlsx (install openpyxl)

#
# --- Общие вспомогательные функции ---
#
def strip_ns(tag: str) -> str:
    return tag.split('}', 1)[-1] if '}' in tag else tag

def text_or_none(elem) -> str:
    if elem is None:
        return None
    if elem.text:
        t = elem.text.strip()
        return t if t else None
    return None

def dict_lists_to_rows(dlists: dict, explode: bool=False, join_sep: str=" | "):
    """
    dlists: mapping path -> list of values
    Возвращает список dict'ов (строк). Если explode==False: каждая key -> join(vals).
    Если explode==True: декартов продукт по спискам.
    """
    keys = list(dlists.keys())
    if not explode:
        row = {}
        for k in keys:
            vals = dlists.get(k, [])
            if not vals:
                row[k] = ""
            elif len(vals) == 1:
                row[k] = vals[0]
            else:
                row[k] = join_sep.join(map(str, vals))
        return [row]
    else:
        lists = [dlists.get(k, []) if dlists.get(k, []) else [""] for k in keys]
        out = []
        for combo in product(*lists):
            out.append({k: (v if v is not None else "") for k, v in zip(keys, combo)})
        return out

#
# --- XML flatten & streaming ---
#
def iter_flatten_xml(elem, path="", out=None):
    """
    Рекурсивный расплющиватель XML-элемента в mapping path -> list(values).
    Атрибуты: path@attr
    Текст у "листьев" — записывается как path.
    """
    if out is None:
        out = defaultdict(list)
    # атрибуты
    for aname, aval in elem.attrib.items():
        key = f"{path}@{aname}" if path else f"{strip_ns(elem.tag)}@{aname}"
        out[key].append(aval)
    children = list(elem)
    if not children:
        t = text_or_none(elem)
        if t is not None:
            key = path if path else strip_ns(elem.tag)
            out[key].append(t)
        return out
    for child in children:
        child_tag = strip_ns(child.tag)
        child_path = f"{path}/{child_tag}" if path else child_tag
        iter_flatten_xml(child, child_path, out)
    return out

def xml_collect_headers(xml_path, record_tag=None):
    headers = set()
    context = ET.iterparse(xml_path, events=("start", "end"))
    stack = []
    for event, elem in context:
        if event == "start":
            stack.append(elem.tag)
        else:
            if record_tag:
                if strip_ns(elem.tag) == record_tag:
                    d = iter_flatten_xml(elem, "")
                    headers.update(d.keys())
                    elem.clear()
            else:
                # считаем прямыми детьми корня (глубина 2)
                if len(stack) == 2:
                    d = iter_flatten_xml(elem, "")
                    headers.update(d.keys())
                    elem.clear()
            stack.pop()
    return sorted(headers)

def xml_rows_generator(xml_path, record_tag=None, explode=False, join_sep=" | "):
    context = ET.iterparse(xml_path, events=("start", "end"))
    stack = []
    for event, elem in context:
        if event == "start":
            stack.append(elem.tag)
        else:
            if record_tag:
                if strip_ns(elem.tag) == record_tag:
                    d = iter_flatten_xml(elem, "")
                    for r in dict_lists_to_rows(d, explode=explode, join_sep=join_sep):
                        yield r
                    elem.clear()
            else:
                if len(stack) == 2:
                    d = iter_flatten_xml(elem, "")
                    for r in dict_lists_to_rows(d, explode=explode, join_sep=join_sep):
                        yield r
                    elem.clear()
            stack.pop()

#
# --- JSON flatten & streaming (ijson) ---
#
def flatten_json_to_lists(obj, prefix=""):
    """
    Возвращает mapping path -> list(values). Для списков:
    - список простых значений -> просто список значений (join later)
    - список объектов -> special marker "__list_of_objs__::<prefix>" -> JSON-serialized flattened dicts
    - смешанные/вложенные списки -> сериализуем элементы в JSON строки
    """
    out = defaultdict(list)
    if obj is None:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}/{k}" if prefix else k
            if isinstance(v, dict):
                sub = flatten_json_to_lists(v, p)
                for kk, vv in sub.items():
                    out[kk].extend(vv)
            elif isinstance(v, list):
                if all(not isinstance(x, dict) and not isinstance(x, list) for x in v):
                    out[p].extend([str(x) for x in v if x is not None])
                elif all(isinstance(x, dict) for x in v):
                    # список объектов: сохраним как список flattened-dicts (json-строки)
                    list_flat = []
                    for elem in v:
                        elem_flat = flatten_json_to_lists(elem, "")
                        # приводим к простому dict: ключ->list(str)
                        simple = {kk: [str(x) for x in vv] for kk, vv in elem_flat.items()}
                        list_flat.append(simple)
                    out[f"__list_of_objs__::{p}"].extend([json.dumps(x, ensure_ascii=False) for x in list_flat])
                else:
                    out[p].extend([json.dumps(x, ensure_ascii=False) for x in v])
            else:
                out[p].append("" if v is None else str(v))
        return out
    else:
        key = prefix if prefix else "value"
        out[key].append(str(obj))
        return out

def json_record_to_rows(record, explode=False, join_sep=" | "):
    """
    Преобразует один JSON-объект (словарь) в список строк (dict).
    Поддерживает explode по спискам объектов (если explode==True).
    """
    flat = flatten_json_to_lists(record, "")
    base = {}
    lists_of_objs = {}
    for k, v in flat.items():
        if k.startswith("__list_of_objs__::"):
            p = k.split("::", 1)[1]
            # v содержит JSON-строки flattened-dicts
            parsed = []
            for s in v:
                try:
                    parsed.append(json.loads(s))
                except Exception:
                    parsed.append({})
            lists_of_objs[p] = parsed
        else:
            base[k] = v[:]  # list of strings
    # если нет списков объектов или explode==False -> одна строка
    if not lists_of_objs or not explode:
        row = {}
        for k, vals in base.items():
            row[k] = join_sep.join(vals) if vals else ""
        # сериализуем списки-объектов в одну ячейку
        for p, lst in lists_of_objs.items():
            row[p] = json.dumps(lst, ensure_ascii=False)
        return [row]
    # explode == True: декартов продукт по спискам-объектов
    list_keys = list(lists_of_objs.keys())
    list_values = [lists_of_objs[k] for k in list_keys]
    out_rows = []
    for combo in product(*list_values):
        merged = {}
        # базовые поля
        for k, vals in base.items():
            merged[k] = join_sep.join(vals) if vals else ""
        # вставляем данные из combo
        for p, elem_flat in zip(list_keys, combo):
            # elem_flat: flattened dict where values are lists
            for subk, subv in elem_flat.items():
                fullk = f"{p}/{subk}" if subk else p
                if isinstance(subv, list):
                    merged[fullk] = join_sep.join([str(x) for x in subv])
                else:
                    merged[fullk] = str(subv)
        out_rows.append(merged)
    return out_rows

def json_stream_records(path, record_path=None, jsonlines=False):
    """
    Генератор записей JSON:
     - если jsonlines=True: читаем построчно (каждая строка — JSON)
     - иначе используем ijson:
         * если record_path is None -> top-level array -> ijson.items(f, 'item')
         * если record_path задан как 'a.b' -> ijson.items(f, 'a.b.item')
    """
    if jsonlines:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue
        return

    with open(path, "rb") as f:
        if not record_path:
            prefix = 'item'  # top-level array
        else:
            # transform dot-notation to ijson prefix like 'a.b.item'
            prefix = record_path.strip().strip('.') + '.item'
        for obj in ijson.items(f, prefix):
            yield obj

def json_collect_headers(path, record_path=None, explode=False, jsonlines=False):
    headers = set()
    # 1-й проход — полностью просканировать стрим и собрать все ключи
    for rec in json_stream_records(path, record_path=record_path, jsonlines=jsonlines):
        for r in json_record_to_rows(rec, explode=explode):
            headers.update(r.keys())
    return sorted(headers)

def json_rows_generator(path, record_path=None, explode=False, jsonlines=False):
    # просто делаем второй проход и yield'им готовые строки (dict)
    for rec in json_stream_records(path, record_path=record_path, jsonlines=jsonlines):
        for r in json_record_to_rows(rec, explode=explode):
            yield r

#
# --- Вывод в CSV / XLSX ---
#
def write_csv_out(path_out, headers, rows_iter):
    with open(path_out, "w", newline="", encoding="utf-8-sig") as fout:
        writer = csv.DictWriter(fout, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        for r in rows_iter:
            row = {h: r.get(h, "") for h in headers}
            writer.writerow(row)

def write_xlsx_out(path_out, headers, rows_iter):
    # Сбор в список (для больших данных — может потребоваться много памяти)
    rows = []
    for r in rows_iter:
        rows.append({h: r.get(h, "") for h in headers})
    df = pd.DataFrame(rows, columns=headers)
    df.to_excel(path_out, index=False)

#
# --- Команды CLI ---
#
def cmd_xml2csv(args):
    print("Collecting headers from XML (streaming)...")
    headers = xml_collect_headers(args.input, record_tag=args.record)
    print(f"Detected {len(headers)} columns.")
    rows_iter = xml_rows_generator(args.input, record_tag=args.record, explode=args.explode, join_sep=args.sep)
    if args.output.lower().endswith(".xlsx"):
        write_xlsx_out(args.output, headers, rows_iter)
    else:
        write_csv_out(args.output, headers, rows_iter)
    print("Saved:", args.output)

def cmd_json2csv(args):
    if args.jsonlines:
        print("JSON Lines mode enabled.")
    print("Collecting headers from JSON (streaming via ijson)...")
    headers = json_collect_headers(args.input, record_path=args.record, explode=args.explode, jsonlines=args.jsonlines)
    print(f"Detected {len(headers)} columns.")
    rows_iter = json_rows_generator(args.input, record_path=args.record, explode=args.explode, jsonlines=args.jsonlines)
    if args.output.lower().endswith(".xlsx"):
        write_xlsx_out(args.output, headers, rows_iter)
    else:
        write_csv_out(args.output, headers, rows_iter)
    print("Saved:", args.output)

def main():
    parser = argparse.ArgumentParser(description="convert_optimal.py — JSON/XML -> CSV/XLSX (requires ijson and pandas)")
    sub = parser.add_subparsers(dest="command", required=True)

    pjson = sub.add_parser("json2csv", help="Convert JSON -> CSV/XLSX using ijson streaming")
    pjson.add_argument("input", help="input JSON file")
    pjson.add_argument("output", help="output CSV or XLSX file")
    pjson.add_argument("--record", help="dot-path to array of records inside JSON (e.g. data.items). If omitted, expects top-level array.", default=None)
    pjson.add_argument("--sep", help="join separator for repeated values (default: ' | ')", default=" | ")
    pjson.add_argument("--explode", action="store_true", help="explode lists of objects into multiple rows (cartesian product)")
    pjson.add_argument("--jsonlines", action="store_true", help="treat input as JSON Lines (one JSON object per line)")
    pjson.set_defaults(func=cmd_json2csv)

    pxml = sub.add_parser("xml2csv", help="Convert XML -> CSV/XLSX using iterparse streaming")
    pxml.add_argument("input", help="input XML file")
    pxml.add_argument("output", help="output CSV or XLSX file")
    pxml.add_argument("--record", help="explicit tag name for a record element (e.g. item). If omitted, direct children of root are records.", default=None)
    pxml.add_argument("--sep", help="join separator for repeated values (default: ' | ')", default=" | ")
    pxml.add_argument("--explode", action="store_true", help="explode repeated elements into multiple rows (cartesian product)")
    pxml.set_defaults(func=cmd_xml2csv)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
