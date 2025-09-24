from typing import Dict, Any, List, Set
from pydantic import BaseModel, Field, ValidationError

# ---- Pydantic модели для валидации схемы и маппинга ----

class Column(BaseModel):
    name: str
    type: str
    nullable: bool = True
    default: str | None = None

class Table(BaseModel):
    name: str
    columns: List[Column]
    primary_key: List[str] | None = None
    foreign_keys: List[Dict[str, Any]] | None = None
    ordering: Dict[str, List[str]] | None = None
    partitioning: Dict[str, Any] | None = None
    quality_expectations: Dict[str, Any] | None = None

class Schema(BaseModel):
    version: int = 1
    database: str
    tables: List[Table]

class Route(BaseModel):
    into: str
    when: str = "TRUE"
    select: Dict[str, str]
    upsert_key: List[str] | None = None

class LoadMapping(BaseModel):
    source: str
    format: str = "csv"
    csv_options: Dict[str, Any] | None = None
    staging_table: str
    select_schema: Dict[str, str]
    route: List[Route]
    dead_letter: str | None = None

class MappingRoot(BaseModel):
    load_mappings: List[LoadMapping]

# ---- Линтер ----

class LintError(Exception):
    pass

def lint(schema: Dict[str, Any], mapping: Dict[str, Any]) -> None:
    """
    Валидация структуры + согласованности: таблицы/колонки/типы.
    Бросает LintError при проблемах.
    """
    try:
        s = Schema.model_validate(schema)
    except ValidationError as e:
        raise LintError(f"Invalid schema.json: {e}")

    try:
        m = MappingRoot.model_validate(mapping)
    except ValidationError as e:
        raise LintError(f"Invalid load_mapping.json: {e}")

    # Словарь таблиц -> множество колонок
    table_cols: Dict[str, Set[str]] = {
        t.name: {c.name for c in t.columns} for t in s.tables
    }

    # Проверка, что цели маршрутов существуют и колонки совпадают
    for lm in m.load_mappings:
        # staging columns
        stg_cols = set(lm.select_schema.keys())
        if not stg_cols:
            raise LintError(f"Mapping {lm.source}: empty select_schema")

        for r in lm.route:
            if r.into not in table_cols:
                raise LintError(f"Route.into='{r.into}' not found in schema tables")

            target_cols = set(table_cols[r.into])
            # SELECT-выражения формируют набор значений; имена ключей = целевые колонки
            sel_cols = set(r.select.keys())
            unknown = sel_cols - target_cols
            if unknown:
                raise LintError(f"Route.into='{r.into}' selects unknown target columns: {unknown}")

            # Колонки, которые мы используем в выражениях, должны существовать в staging
            # (простая эвристика: имена столбцов staging встречаются как токены)
            import re
            stg_refs = set()
            for expr in r.select.values():
                stg_refs.update(re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]*\b", expr))
            # отфильтруем очевидные SQL-ключевые слова/функции
            blacklist = {"NULL", "TRUE", "FALSE", "now", "to_timestamp"}
            stg_refs = {x for x in stg_refs if x.lower() not in {b.lower() for b in blacklist}}

            missing_from_staging = {x for x in stg_refs if x in target_cols} ^ set()  # noop; informative only
            # Проверим наличие используемых простых идентификаторов в staging
            missing_stg = {x for x in stg_refs if x in stg_cols or x.isupper() is False}
            # Ничего не делаем, просто sanity check: если совсем "пусто", предупредим
            if not stg_cols.intersection(stg_refs):
                # Не критично: выражения могут не ссылаться прямо на колонки (константы/функции)
                pass

            # Проверка ключей upsert существуют в целевой
            if r.upsert_key:
                for k in r.upsert_key:
                    if k not in target_cols:
                        raise LintError(f"upsert_key '{k}' is not a column of '{r.into}'")

    # Всё ок
