# -*- coding: utf-8 -*-
"""
grain_module.py
Модуль для выявления "зерна" исходной таблицы по списку бизнес-сущностей
и форматирования итоговой строки-отчёта.

Правила:
- "Зерно" — сущность с наибольшим числом колонок (поле 'columns').
- В отчёте перечисляются колонки, которые "обязательно включает" зерно.
  По умолчанию — это ключевые колонки сущности (поле 'keys').
"""

from typing import Any, Dict, List, Optional


class GrainError(ValueError):
    """Исключение для ошибок в данных о сущностях."""


def _validate_entities(entities: Any) -> None:
    if not isinstance(entities, list) or not entities:
        raise GrainError("Ожидается непустой список 'entities'.")
    for i, e in enumerate(entities):
        if not isinstance(e, dict):
            raise GrainError(f"Entity #{i} должен быть dict.")
        if "name" not in e or "columns" not in e:
            raise GrainError(f"Entity #{i} должен содержать 'name' и 'columns'.")
        if not isinstance(e.get("columns"), list):
            raise GrainError(f"Entity '{e.get('name', i)}': 'columns' должен быть списком.")
        if "keys" in e and not isinstance(e["keys"], list):
            raise GrainError(f"Entity '{e.get('name', i)}': 'keys' должен быть списком.")


def choose_grain_entity(entities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Возвращает сущность-«зерно» — ту, у которой больше всего колонок.
    При равенстве количества колонок побеждает сущность с большим числом ключей;
    если снова равенство — берётся первая по порядку.

    :param entities: список сущностей со свойствами 'name', 'columns', опц. 'keys'
    :return: словарь сущности
    """
    _validate_entities(entities)

    def key_fn(e: Dict[str, Any]):
        cols = len(e.get("columns", []))
        keys = len(e.get("keys", []))
        # Сортировка по убыванию: больше колонок, затем больше ключей
        return (cols, keys)

    # max с кортежом-ключом даст нужный приоритет
    return max(entities, key=key_fn)


def grain_required_columns(grain: Dict[str, Any], source: str = "keys") -> List[str]:
    """
    Возвращает список колонок, которые «обязательно включает» зерно.
    По умолчанию — это ключевые колонки ('keys'). Можно выбрать 'columns'.

    :param grain: сущность-«зерно»
    :param source: 'keys' (по умолчанию) или 'columns'
    :return: список имён колонок
    """
    if source not in {"keys", "columns"}:
        raise GrainError("Параметр source должен быть 'keys' или 'columns'.")
    cols = grain.get(source) or []
    if not isinstance(cols, list):
        raise GrainError(f"У сущности '{grain.get('name', '?')}' поле '{source}' должно быть списком.")
    return cols


def format_grain_report(
    data: Dict[str, Any],
    *,
    list_source: str = "keys",
    include_entity_name: bool = False
) -> str:
    """
    Высокоуровневая функция: принимает полный JSON, выбирает зерно и
    формирует красивую строку-отчёт.

    :param data: входной JSON со списком сущностей в поле 'entities'
    :param list_source: откуда брать колонки для перечисления — 'keys' или 'columns'
    :param include_entity_name: добавлять ли имя зерна в текст отчёта
    :return: строка-отчёт
    """
    entities = data.get("entities")
    grain = choose_grain_entity(entities)
    cols = grain_required_columns(grain, source=list_source)

    header_base = (
        "Анализ исходной таблицы, основанный на выявлении бизнес-сущностей "
        "и учитывающий кардинальности, показал, что зерно таблицы "
    )
    if include_entity_name:
        header = f'{header_base}— «{grain.get("name", "Неизвестная сущность")}» — должно в себя обязательно включать следующие колонки:'
    else:
        header = f"{header_base}должно в себя обязательно включать следующие колонки:"

    # Каждую колонку — с новой строки, без маркеров, как в примере
    body = "\n".join(cols) if cols else "— (ключевые колонки не указаны)"
    return f"{header}\n{body}"


def analyze_and_format(data: Dict[str, Any]) -> str:
    """
    Упрощённый фасад: формирует отчёт по умолчанию
    (перечисляет ключевые колонки зерна, без указания имени сущности).
    """
    return format_grain_report(data, list_source="keys", include_entity_name=False)


# ----- Пример использования -----
if __name__ == "__main__":
    sample = {
        'entities': [
            {
                'name': 'Посещение мероприятия',
                'keys': ['ticket_number', 'ticket_id', 'museum_inn', 'client_name', 'order_number'],
                'columns': [
                    'ticket_number','ticket_id','created','order_status','ticket_status','ticket_price',
                    'visitor_category','is_active','valid_to','count_visitor','is_entrance','is_entrance_mdate',
                    'update_timestamp','museum_name','museum_inn','client_name','name','surname','client_phone',
                    'birthday_date','order_number'
                ],
            },
            {'name': 'Мероприятие', 'keys': ['event_id'], 'columns': ['event_id','event_name','event_kind_name','start_datetime']},
            {'name': 'Площадка', 'keys': ['spot_id'], 'columns': ['spot_id','spot_name']},
        ],
        'under_question_columns': [
            {'column': 'name', 'description': '...', 'suspected_entities': ['Клиент'], 'suggested_action': '...', 'confidence': '0.7'},
            {'column': 'surname', 'description': '...', 'suspected_entities': ['Клиент'], 'suggested_action': '...', 'confidence': '0.7'},
        ],
    }

    # По умолчанию — выведет ключевые колонки зерна
    print(analyze_and_format(sample))

    # Если хотите перечислить все колонки зерна и указать имя сущности:
    # print(format_grain_report(sample, list_source="columns", include_entity_name=True))
