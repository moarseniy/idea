# row_iterator.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import Any, Dict, Iterable, Iterator, List, Tuple

def get_table_columns(profile: Dict[str, Any]) -> Dict[str, List[str]]:
    """
    Возвращает порядок колонок для каждой таблицы:
    ['rec_id', 'idx1'.., '<data columns ...>'].
    Данные-колонки отсортированы по name (стабильная детерминация).
    """
    cols_by_table: Dict[str, List[str]] = {}
    for e in sorted(profile.get("entities", []), key=lambda x: x.get("depth", 0)):
        tname = e["name"]
        depth = e.get("depth", 0)
        cols = ["rec_id"] + [f"idx{i}" for i in range(1, depth + 1)]
        data_cols = sorted((c["name"] for c in e.get("columns", [])))
        cols_by_table[tname] = cols + data_cols
    return cols_by_table

def iter_rows(profile: Dict[str, Any], records: Iterable[Dict[str, Any]]) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Главный итератор строк.
    На вход: итоговый профиль и поток JSON-записей (dict).
    На выход: кортежи (table_name, row_dict) для всех сущностей, всех записей.
    row_dict уже содержит PK: rec_id, idx1..idxN, и все data-колонки по 'name'.
    """
    entities = sorted(profile.get("entities", []), key=lambda e: e.get("depth", 0))

    for rec_id, obj in enumerate(records, start=1):
        for ent in entities:
            table = ent["name"]
            path = ent.get("path", [])
            depth = ent.get("depth", 0)
            columns = ent.get("columns", [])

            # Идём к "носителю" данных по пути сущности: [] -> сам obj; ["a","b"] -> obj["a"]["b"]...
            # На каждом шаге: dict -> ключ; list -> развернуть (индекс в idxN).
            for carrier, idx_chain in _iter_carriers_for_entity(obj, path):
                # materialize row
                row: Dict[str, Any] = {}
                # PK
                row["rec_id"] = rec_id
                for i, idx in enumerate(idx_chain, start=1):
                    row[f"idx{i}"] = idx
                # данные
                for col in columns:
                    col_name = col["name"]     # уже после патча
                    col_path = col.get("path", [])  # путь относительно carrier
                    row[col_name] = _extract_value(carrier, col_path)
                yield (table, row)

def _iter_carriers_for_entity(root_obj: Any, entity_path: List[str]) -> Iterator[Tuple[Any, Tuple[int, ...]]]:
    """
    Возвращает итератор (carrier, idx_chain) для сущности.
    carrier — объект (или скаляр) на уровне сущности, из которого извлекаются её колонки.
    idx_chain — кортеж индексов массивов, встреченных по пути (начиная с 1).
    Логика:
      - dict -> спускаемся по ключу
      - list -> разворачиваем в элементы и дополняем idx_chain
    Последний шаг должен прийти на массив (для depth>0) или остаться на объекте (для depth=0).
    Если по пути что-то отсутствует/несовместимо — строк нет.
    """
    carriers: List[Tuple[Any, Tuple[int, ...]]] = [(root_obj, ())]
    for seg in entity_path:
        next_carriers: List[Tuple[Any, Tuple[int, ...]]] = []
        for cur_obj, idxc in carriers:
            val = None
            if isinstance(cur_obj, dict):
                val = cur_obj.get(seg)
            else:
                # если неожиданно не dict — путь не совпал
                continue

            if isinstance(val, list):
                for i, el in enumerate(val, start=1):
                    next_carriers.append((el, idxc + (i,)))
            elif isinstance(val, dict):
                next_carriers.append((val, idxc))
            else:
                # отсутствует/скаляр — путь не ведёт к нужной сущности
                continue
        carriers = next_carriers
        if not carriers:
            break
    # Если путь пустой (depth=0) — вернём исходный объект
    if len(entity_path) == 0:
        yield (root_obj, ())
    else:
        # Вернуть только те, что реально соответствуют сущности (обычно элементы массива на конце пути)
        for c in carriers:
            yield c

def _extract_value(carrier: Any, path: List[str]) -> Any:
    """
    Извлекает значение по path относительно carrier.
    Особые случаи:
      - path == []  -> вернуть carrier целиком (для сущностей-скаляров с колонкой 'value')
      - если на любом шаге ключа нет или тип не dict -> None
    """
    if not path:
        # если колонка хочет сам элемент (например, 'value' в профиле может иметь path=[])
        return carrier
    cur: Any = carrier
    for seg in path:
        if isinstance(cur, dict):
            cur = cur.get(seg, None)
        else:
            return None
    return cur
