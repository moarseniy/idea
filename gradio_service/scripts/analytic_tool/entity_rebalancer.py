# entity_rebalancer.py  — v2
from __future__ import annotations
from typing import Dict, List, Any, Tuple, Set, Optional
import math
import copy
import json


def _unique_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _get_total_rows(total_rows_param: Optional[int], cardinalities: Dict[str, Any]) -> int:
    """
    Возвращает total_rows, предпочитая явный параметр, иначе берёт из cardinalities['raws'] или ['rows'].
    """
    if isinstance(total_rows_param, int) and total_rows_param > 0:
        return total_rows_param
    for k in ("raws", "rows"):
        if k in cardinalities and isinstance(cardinalities[k], int) and cardinalities[k] > 0:
            return cardinalities[k]
    raise ValueError("Не удалось определить общее число строк: передайте total_rows или заполните 'raws'/'rows'.")


def _is_high_card(value: Optional[int], threshold: int) -> bool:
    if value is None:
        return False
    return value >= threshold


def _fmt_card(value: Optional[int]) -> str:
    return "—" if value is None else str(value)


def _entity_high_cards(cols: List[str], col_cards: Dict[str, int], threshold: int) -> List[Tuple[str, Optional[int]]]:
    """Возвращает список (col, card) только для колонок с высокой кардинальностью."""
    out = []
    for c in cols or []:
        v = col_cards.get(c)
        if _is_high_card(v, threshold):
            out.append((c, v))
    return out


def reorganize_entities(
    model: Dict[str, Any],
    cardinalities: Dict[str, Any],
    total_rows: Optional[int] = None,
    threshold_ratio: float = 0.20,
) -> Dict[str, Any]:
    """
    Главная функция переразбивки с подробными логами.

    Алгоритм:
      1) Ищем сущности, содержащие столбцы с высокой кардинальностью (>= ceil(threshold_ratio * total_rows)).
      2) Объединяем все такие сущности в одну большую (имя = main_entity.name).
      3) Берём из "под вопросом" колонки с высокой кардинальностью, переносим в большую сущность.
      4) Обеспечиваем уникальность колонок между сущностями (приоритет у большой).
      5) Печатаем сводки на каждом шаге, финальный JSON — перед возвратом.

    Возвращает:
      {
        "entities": [{"name": ..., "keys": [...], "columns": [...]}, ...],
        "under_question_columns": [ ... ]
      }
    """
    print("\n=== ШАГ 0. Подготовка и копирование входных данных ===")
    model = copy.deepcopy(model)
    cardinalities = copy.deepcopy(cardinalities)
    col_cards: Dict[str, int] = cardinalities.get("column_cardinalities", {}) or {}

    total = _get_total_rows(total_rows, cardinalities)
    threshold = math.ceil(threshold_ratio * total)
    print(f"Всего строк в исходном файле: {total}")
    print(f"Порог большой кардинальности: >= {threshold} (доля {threshold_ratio:.0%})")

    main = model.get("main_entity", {}) or {}
    main_name = main.get("name", "MAIN FACT")  # имя большой сущности = имя факта
    grain_text = main.get("grain", "")
    main_keys: List[str] = list(main.get("keys") or [])
    main_cols: List[str] = list(main.get("columns") or [])
    print(f"Главная сущность/факт: name='{main_name}', grain='{grain_text}'")
    print(f"Ключи факта: {main_keys}")
    print(f"Колонки факта: {main_cols}")

    entities: List[Dict[str, Any]] = list(model.get("entities") or [])
    under_question: List[Dict[str, Any]] = list(model.get("under_question_columns") or [])

    # Итоговый контроль покрытия
    original_entity_columns = []
    for e in entities:
        original_entity_columns.extend(e.get("columns") or [])
    original_all_columns = set(main_cols) | set(original_entity_columns) | {x.get("column") for x in under_question if x.get("column")}
    print(f"\nВсего уникальных колонок (fact + entities + under_question): {len(original_all_columns)}")

    # --- Сводка по входу (включая факт как сущность) ---
    print("\n=== ШАГ 1. Сводка по входным сущностям и кардинальностям ===")
    # факт
    hc_main = _entity_high_cards(main_cols, col_cards, threshold)
    print(f" - [ФАКТ] '{main_name}': {len(main_cols)} кол.; высоких: {len(hc_main)} -> " +
          (", ".join([f"{c}={_fmt_card(v)}" for c,v in hc_main]) if hc_main else "—"))
    # прочие сущности
    for e in entities:
        name = e.get("name", "?")
        cols = e.get("columns") or []
        hc = _entity_high_cards(cols, col_cards, threshold)
        print(f" - Сущность '{name}': {len(cols)} кол.; высоких: {len(hc)} -> " +
              (", ".join([f"{c}={_fmt_card(v)}" for c,v in hc]) if hc else "—"))

    # Под вопросом — до изменений
    print("\n[Под вопросом — исходно]")
    if under_question:
        for item in under_question:
            col = item.get("column")
            print(f" * {col}: кардинальность={_fmt_card(col_cards.get(col))}")
    else:
        print(" (пусто)")

    # --- Поиск сущностей, которые пойдут в объединение ---
    print("\n=== ШАГ 2. Поиск сущностей с колонками высокой кардинальности (кроме факта) ===")
    high_card_entities: List[Tuple[str, List[Tuple[str, int]]]] = []
    for e in entities:
        name = e.get("name", "?")
        cols = e.get("columns") or []
        hc = _entity_high_cards(cols, col_cards, threshold)
        if hc:
            high_card_entities.append((name, hc))
            print(f" * '{name}': высокие колонки -> " + ", ".join([f"{c}={v}" for c, v in hc]))
        else:
            print(f"   '{name}': высоких колонок нет.")

    merge_entity_names = {name for (name, _) in high_card_entities}
    if not merge_entity_names:
        print("\n(!) Ни одна из дочерних сущностей не имеет высоких колонок. Большая сущность всё равно будет фактом.")
    else:
        print(f"\nСущности для слияния в '{main_name}': {sorted(merge_entity_names)}")

    # --- Формируем большую сущность (имя = имя факта) ---
    print(f"\n=== ШАГ 3. Формирование большой сущности '{main_name}' и перенос колонок ===")
    big_entity_name = main_name
    big_columns = list(main_cols)         # стартуем с колонок факта
    big_keys = list(main_keys)            # и ключей факта

    for e in entities:
        if e.get("name") in merge_entity_names:
            # переносим колонки и ключи сущности
            big_columns.extend(e.get("columns") or [])
            big_keys.extend(e.get("keys") or [])

    # Уникализация и гарантия, что ключи присутствуют среди колонок
    big_keys = _unique_preserve_order(big_keys)
    for k in big_keys:
        if k and k not in big_columns:
            big_columns.append(k)
    big_columns = _unique_preserve_order(big_columns)

    print(f"Итог по '{big_entity_name}' (до учёта 'Под вопросом'):")
    print(f" - Ключи: {big_keys}")
    print(f" - Колонки: {big_columns}")

    # --- Перенос из "под вопросом" высоких колонок ---
    print("\n=== ШАГ 4. Анализ 'Под вопросом' и перенос высоких колонок в большую сущность ===")
    print("[Под вопросом — перед переносом]")
    if under_question:
        for item in under_question:
            col = item.get("column")
            print(f" * {col}: кардинальность={_fmt_card(col_cards.get(col))}")
    else:
        print(" (пусто)")

    remaining_under_question: List[Dict[str, Any]] = []
    moved_from_uq = []
    for item in under_question:
        col = item.get("column")
        card = col_cards.get(col)
        if _is_high_card(card, threshold):
            if col in big_columns:
                print(f" * {col} (={card}) уже есть в '{big_entity_name}', перенос не требуется.")
            else:
                big_columns.append(col)
                print(f" * {col} (={card}) перенесена из 'Под вопросом' в '{big_entity_name}'.")
            moved_from_uq.append(col)
        else:
            remaining_under_question.append(item)

    if not moved_from_uq:
        print("   Нет колонок 'Под вопросом' с высокой кардинальностью для переноса.")

    # --- Сборка финального списка сущностей ---
    print("\n=== ШАГ 5. Сборка и нормализация финального списка сущностей ===")
    # исключаем слитые сущности из списка
    remaining_entities: List[Dict[str, Any]] = []
    for e in entities:
        if e.get("name") not in merge_entity_names:
            remaining_entities.append(copy.deepcopy(e))

    # создаём большую сущность (факт)
    big_entity_obj = {
        "name": big_entity_name,
        "keys": big_keys,
        "columns": _unique_preserve_order(big_columns),
    }

    print(f"Большая сущность '{big_entity_name}' собрана: {len(big_entity_obj['columns'])} колонок.")

    # Удаляем пересечения колонок: приоритет у большой сущности
    print("\nУдаляем пересечения колонок между сущностями (приоритет у большой сущности).")
    big_set = set(big_entity_obj["columns"])
    for e in remaining_entities:
        cols = e.get("columns") or []
        filtered = [c for c in cols if c not in big_set]
        removed = [c for c in cols if c in big_set]
        e["columns"] = _unique_preserve_order(filtered)
        if removed:
            print(f" - Из '{e.get('name')}' убраны дубли, уже находящиеся в '{big_entity_name}': {removed}")

    # Устраняем пересечения между оставшимися сущностями
    print("\nПроверка/устранение пересечений между оставшимися сущностями.")
    seen_cols = set(big_entity_obj["columns"])
    for e in remaining_entities:
        cols = e.get("columns") or []
        new_cols = []
        dup_removed = []
        for c in cols:
            if c in seen_cols:
                dup_removed.append(c)
            else:
                new_cols.append(c)
                seen_cols.add(c)
        e["columns"] = new_cols
        if dup_removed:
            print(f" - Дубли удалены из '{e.get('name')}': {dup_removed}")

    final_entities = [big_entity_obj] + remaining_entities

    # --- Валидация покрытия и уникальности ---
    print("\n=== ШАГ 6. Финальная валидация покрытия и уникальности ===")
    final_cols_union = set()
    for e in final_entities:
        final_cols_union |= set(e.get("columns") or [])
    final_cols_union |= {x.get("column") for x in remaining_under_question if x.get("column")}

    missing_initial = original_all_columns - final_cols_union
    new_extras = final_cols_union - original_all_columns

    print(f"Итоговое число уникальных колонок (entities + under_question): {len(final_cols_union)}")
    if missing_initial:
        print(f"(!) ПОТЕРЯНЫ колонки относительно исходного набора: {sorted(missing_initial)}")
    else:
        print("Все исходные колонки присутствуют в финальном разбиении.")

    if new_extras:
        print(f"(!) ВНИМАНИЕ: появились неожиданные колонки: {sorted(new_extras)}")
    else:
        print("Неожиданных дополнительных колонок не обнаружено.")

    # --- Сводка по финальным сущностям ---
    print("\n=== ШАГ 7. Сводка по финальным сущностям ===")
    for e in final_entities:
        name = e["name"]
        cols = e.get("columns", [])
        hc = _entity_high_cards(cols, col_cards, threshold)
        print(f" - '{name}': {len(cols)} кол.; высоких: {len(hc)} -> " +
              (", ".join([f"{c}={_fmt_card(v)}" for c,v in hc]) if hc else "—"))

    print("\n[Под вопросом — финально]")
    if remaining_under_question:
        for item in remaining_under_question:
            col = item.get("column")
            print(f" * {col}: кардинальность={_fmt_card(col_cards.get(col))}")
    else:
        print(" (пусто)")

    result = {
        "entities": [{"name": e["name"], "keys": e.get("keys", []), "columns": e.get("columns", [])} for e in final_entities],
        "under_question_columns": remaining_under_question,
    }

    print("\n=== ШАГ 8. Финальный JSON ===")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    print("\n=== ГОТОВО. Возвращаем результат. ===")
    return result


# --- Пример минимального запуска (для локальной проверки). ---
if __name__ == "__main__":
    example_model = {
        "main_entity": {
            "name": "Ticket Fact",
            "grain": "Одна строка — это один билет на мероприятие",
            "keys": ["ticket_number"],
            "description": "Факт билета",
            "columns": [
                "ticket_number", "ticket_id", "order_number", "ticket_status",
                "ticket_price", "is_active", "is_entrance", "is_entrance_mdate",
                "count_visitor", "valid_to", "update_timestamp", "created"
            ]
        },
        "entities": [
            {
                "name": "Клиент",
                "description": "Данные о клиенте",
                "keys": ["client_phone"],
                "columns": ["client_name", "name", "surname", "client_phone", "birthday_date", "museum_inn"],
                "confidence": "0.9"
            },
            {
                "name": "Событие",
                "description": "Данные о событии",
                "keys": ["event_id"],
                "columns": ["event_id", "event_name", "event_kind_name", "start_datetime"],
                "confidence": "0.9"
            },
            {
                "name": "Площадка",
                "description": "Площадка мероприятия",
                "keys": ["spot_id"],
                "columns": ["spot_id", "spot_name"],
                "confidence": "0.9"
            },
            {
                "name": "Музей",
                "description": "Музей",
                "keys": ["museum_name"],
                "columns": ["museum_name"],
                "confidence": "0.9"
            },
            {
                "name": "Категория посетителя",
                "description": "Категория посетителя",
                "keys": ["visitor_category"],
                "columns": ["visitor_category"],
                "confidence": "0.9"
            },
            {
                "name": "Статусы заказа",
                "description": "Статусы заказа",
                "keys": [],
                "columns": ["order_status"],
                "confidence": "0.7"
            }
        ],
        "relationships": [],
        "under_question_columns": [
            {
                "column": "museum_inn",
                "description": "Под вопросом",
                "suspected_entities": ["Клиент", "Музей"],
                "suggested_action": "Уточнить",
                "confidence": "0.6"
            }
        ],
        "coverage": {"total_columns_in_csv": "27", "assigned_to_entities": "26", "under_question": "1"},
        "notes": ""
    }

    example_cards = {
        "raws": 478563,
        "column_cardinalities": {
            "ticket_number": 478563, "ticket_id": 478563, "order_number": 211245, "ticket_status": 6,
            "ticket_price": 13245, "is_active": 2, "is_entrance": 2, "is_entrance_mdate": 203412,
            "count_visitor": 3, "valid_to": 4500, "update_timestamp": 478563, "created": 478563,
            "client_name": 478432, "name": 470000, "surname": 468000, "client_phone": 475000,
            "birthday_date": 190000, "museum_inn": 478432,
            "event_id": 15000, "event_name": 12000, "event_kind_name": 50, "start_datetime": 20000,
            "spot_id": 1200, "spot_name": 1200,
            "museum_name": 45,
            "visitor_category": 15,
            "order_status": 7
        }
    }

    reorganize_entities(example_model, example_cards)
