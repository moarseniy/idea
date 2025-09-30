# -*- coding: utf-8 -*-
"""
Генератор профиля схемы для произвольного JSON-массива (без БД).

Вход: JSON-файл, где root = массив записей (обычно объектов).
Выход: JSON-профиль с сущностями (TABLE-PER-PATH), колонками и связями,
с указанием канонических типов столбцов.

CLI:
  python json_profile_generator.py input.json --out profile.json --root ip_record

Профиль пригоден для дальнейшей генерации DBML/DDL и роутинга
в ClickHouse/PostgreSQL по карте канонических типов.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Set, Union

# Универсальная заглушка имени корня (больше не передаём извне)
ROOT_NAME = "__root__"

# ---------------- Имя/путь ----------------

def norm_ident(s: str) -> str:
    s = str(s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^\w]", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"

def join_path(parts: Sequence[str]) -> str:
    return "__".join(norm_ident(p) for p in parts)

# ---------------- Типы (канонические) ----------------

CANONICAL_TYPES = {
    "string": {"pg": "text", "ch": "String", "py": "str"},
    "int32": {"pg": "integer", "ch": "Int32", "py": "int"},
    "int64": {"pg": "bigint", "ch": "Int64", "py": "int"},
    "float64": {"pg": "double precision", "ch": "Float64", "py": "float"},
    "decimal(p,s)": {"pg": "numeric({p},{s})", "ch": "Decimal({p},{s})", "py": "decimal.Decimal"},
    "bool": {"pg": "boolean", "ch": "Bool", "py": "bool"},
    "date": {"pg": "date", "ch": "Date32", "py": "datetime.date"},
    "timestamp": {"pg": "timestamptz", "ch": "DateTime('UTC')", "py": "datetime.datetime"},
    "timestamp64(ms)": {"pg": "timestamptz", "ch": "DateTime64(3, 'UTC')", "py": "datetime.datetime"},
    "json": {"pg": "jsonb", "ch": "String", "py": "typing.Any"},
}

SYNONYMS = {
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

INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1

ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DT_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})[ T]"  # дата
    r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})"  # время
    r"(?P<frac>\.\d{1,9})?"  # доли секунды
    r"(?P<tz>Z|[+-]\d{2}:?\d{2})?$"  # зона
)

# ---------------- Модель профиля ----------------

@dataclass
class ColumnProfile:
    name: str
    path: Tuple[str, ...]
    types_seen: Set[str] = field(default_factory=set)
    canonical: str = "string"
    nullable: bool = True
    examples: List[Any] = field(default_factory=list)
    count: int = 0

    def register(self, value: Any) -> None:
        self.count += 1
        if value is None:
            self.nullable = True
            self.types_seen.add("null")
            return
        t = infer_canonical_type(value)
        self.types_seen.add(t)
        # Сохраним пару примеров
        if len(self.examples) < 3:
            self.examples.append(value)

    def finalize(self) -> None:
        self.canonical = decide_type(self.types_seen)
        # если поле ни разу не встречено (теоретически) — string, nullable
        if self.count == 0:
            self.canonical = "string"
            self.nullable = True

@dataclass
class TableSpec:
    name: str
    full_path: Tuple[str, ...]
    depth: int
    parent: Optional[str]
    columns: Dict[str, ColumnProfile] = field(default_factory=dict)

    def pk_cols(self) -> List[str]:
        return ["rec_id"] + [f"idx{i}" for i in range(1, self.depth + 1)]

@dataclass
class SchemaProfile:
    root_name: str
    tables: Dict[str, TableSpec] = field(default_factory=dict)  # name -> spec
    by_path: Dict[Tuple[str, ...], str] = field(default_factory=dict)  # full_path -> name

    def table_name_for(self, full_path: Tuple[str, ...]) -> str:
        return self.root_name if not full_path else f"{self.root_name}__{join_path(full_path)}"

    def ensure_table(self, full_path: Tuple[str, ...], depth: int, parent: Optional[str]) -> TableSpec:
        name = self.table_name_for(full_path)
        if name not in self.tables:
            spec = TableSpec(name=name, full_path=full_path, depth=depth, parent=parent)
            self.tables[name] = spec
            self.by_path[full_path] = name
        return self.tables[name]

# ---------------- Инференс типов ----------------

def infer_canonical_type(v: Any) -> str:
    # Порядок важен: bool — подмножество int в Python
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, int) and not isinstance(v, bool):
        return "int32" if INT32_MIN <= v <= INT32_MAX else "int64"
    if isinstance(v, float):
        return "float64"
    if isinstance(v, str):
        # дата/время по ISO
        if ISO_DATE_RE.match(v):
            return "date"
        m = ISO_DT_RE.match(v)
        if m:
            frac = m.group("frac")
            if frac and len(frac) >= 4:  # >= миллисекунд — считаем повышенную точность
                return "timestamp64(ms)"
            return "timestamp"
        return "string"
    # JSON-скаляры только выше; остальные типы трактуем как json (редко, но на всякий)
    return "json"


def decide_type(types_seen: Set[str]) -> str:
    # Убираем "null" — влияет только на nullable
    ts = {t for t in types_seen if t != "null"}
    if not ts:
        return "string"
    if len(ts) == 1:
        return next(iter(ts))
    # Пробуем согласовать очевидные пары
    if ts == {"int32", "int64"}:
        return "int64"
    if ts == {"int32", "float64"} or ts == {"int64", "float64"} or ts == {"int32", "int64", "float64"}:
        return "float64"
    if ts == {"timestamp", "timestamp64(ms)"}:
        return "timestamp64(ms)"
    # Остальные смешанные случаи считаем неоднозначными → string
    return "string"

# ---------------- Обход JSON и построение профиля ----------------

def is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (str, int, float, bool))


def build_profile(data: List[Any], root_name: str = ROOT_NAME) -> SchemaProfile:
    schema = SchemaProfile(root_name=root_name)
    root = schema.ensure_table((), depth=0, parent=None)

    for rec_id, obj in enumerate(data, start=1):
        if not isinstance(obj, dict):
            # Для не-объектов в корне создаём столбец value
            col = root.columns.get("value")
            if not col:
                col = ColumnProfile(name="value", path=())
                root.columns["value"] = col
            col.register(obj)
            continue
        walk_object(schema, current_path=(), base_table=root, idx_chain=(), prefix=(), obj=obj)

    # finalize columns
    for t in schema.tables.values():
        for c in t.columns.values():
            c.finalize()

    return schema


def walk_object(schema: SchemaProfile, current_path: Tuple[str, ...], base_table: TableSpec,
                idx_chain: Tuple[int, ...], prefix: Tuple[str, ...], obj: Dict[str, Any]) -> None:
    # 1) зарегистрировать типы скалярных ключей (разворачивание объектов в колонки)
    for k, v in obj.items():
        fqn = join_path(prefix + (k,))
        if is_scalar(v):
            col = base_table.columns.get(fqn)
            if not col:
                col = ColumnProfile(name=fqn, path=prefix + (k,))
                base_table.columns[fqn] = col
            col.register(v)
        elif isinstance(v, dict):
            if v:
                walk_object(schema, current_path=current_path + (k,), base_table=base_table,
                            idx_chain=idx_chain, prefix=prefix + (k,), obj=v)
        elif isinstance(v, list):
            child = schema.ensure_table(current_path + (k,), depth=base_table.depth + 1, parent=base_table.name)
            if not v:
                # пустой массив — тип value остаётся неизвестным → позже станет string
                # но создадим колонку value, чтобы было куда маппить при генерации DDL
                child.columns.setdefault("value", ColumnProfile(name="value", path=()))
                continue
            all_scalar = True
            for i, elem in enumerate(v, start=1):
                if is_scalar(elem):
                    col = child.columns.get("value")
                    if not col:
                        col = ColumnProfile(name="value", path=())
                        child.columns["value"] = col
                    col.register(elem)
                elif isinstance(elem, dict):
                    all_scalar = False
                    # зарегистрировать скаляры элемента
                    for ek, ev in elem.items():
                        efqn = join_path((ek,))
                        if is_scalar(ev):
                            col = child.columns.get(efqn)
                            if not col:
                                col = ColumnProfile(name=efqn, path=(ek,))
                                child.columns[efqn] = col
                            col.register(ev)
                        elif isinstance(ev, dict):
                            # развернуть объект в текущую child-таблицу
                            flatten_into_table(schema, child, prefix=(ek,), obj=ev)
                        elif isinstance(ev, list):
                            # внучий массив → отдельная таблица глубже
                            load_deep_array(schema, parent_table=child, parent_path=current_path + (k,), key=ek, arr=ev)
                else:
                    # экзотика → трактуем как json-скаляр в value
                    col = child.columns.get("value")
                    if not col:
                        col = ColumnProfile(name="value", path=())
                        child.columns["value"] = col
                    col.register(elem)
            if all_scalar:
                # убедимся, что есть колонка value
                child.columns.setdefault("value", ColumnProfile(name="value", path=()))


def flatten_into_table(schema: SchemaProfile, table: TableSpec, prefix: Tuple[str, ...], obj: Dict[str, Any]) -> None:
    for k, v in obj.items():
        fqn = join_path(prefix + (k,))
        if is_scalar(v):
            col = table.columns.get(fqn)
            if not col:
                col = ColumnProfile(name=fqn, path=prefix + (k,))
                table.columns[fqn] = col
            col.register(v)
        elif isinstance(v, dict):
            if v:
                flatten_into_table(schema, table, prefix + (k,), v)
        elif isinstance(v, list):
            # массив глубже: создадим таблицу ниже относительно table.full_path
            load_deep_array(schema, parent_table=table, parent_path=table.full_path + prefix, key=k, arr=v)


def load_deep_array(schema: SchemaProfile, parent_table: TableSpec, parent_path: Tuple[str, ...], key: str, arr: List[Any]) -> None:
    child = schema.ensure_table(parent_path + (key,), depth=parent_table.depth + 1, parent=parent_table.name)
    if not arr:
        child.columns.setdefault("value", ColumnProfile(name="value", path=()))
        return
    for elem in arr:
        if is_scalar(elem):
            col = child.columns.get("value")
            if not col:
                col = ColumnProfile(name="value", path=())
                child.columns["value"] = col
            col.register(elem)
        elif isinstance(elem, dict):
            for ek, ev in elem.items():
                efqn = join_path((ek,))
                if is_scalar(ev):
                    col = child.columns.get(efqn)
                    if not col:
                        col = ColumnProfile(name=efqn, path=(ek,))
                        child.columns[efqn] = col
                    col.register(ev)
                elif isinstance(ev, dict):
                    flatten_into_table(schema, child, prefix=(ek,), obj=ev)
                elif isinstance(ev, list):
                    load_deep_array(schema, parent_table=child, parent_path=parent_path + (key,), key=ek, arr=ev)
        else:
            col = child.columns.get("value")
            if not col:
                col = ColumnProfile(name="value", path=())
                child.columns["value"] = col
            col.register(elem)

# ---------------- Экспорт профиля ----------------

def schema_to_profile_json(schema: SchemaProfile) -> Dict[str, Any]:
    entities: List[Dict[str, Any]] = []
    relations: List[Dict[str, Any]] = []

    # entities
    for t in schema.tables.values():
        cols_out = []
        for c in sorted(t.columns.values(), key=lambda x: x.name):
            cols_out.append({
                "name": c.name,
                "path": list(c.path),
                "type": c.canonical,
                "nullable": c.nullable,
                "examples": c.examples,
                "types_seen": sorted([x for x in c.types_seen if x != "null"]) or ["string"],
            })
        entities.append({
            "name": t.name,
            "path": list(t.full_path),
            "depth": t.depth,
            "parent": t.parent,
            "primary_key": t.pk_cols(),
            "columns": cols_out,
        })

    # relations: дочерняя таблица -> родитель по составному ключу
    for t in schema.tables.values():
        if t.parent:
            parent = schema.tables[t.parent]
            from_cols = ["rec_id"] + [f"idx{i}" for i in range(1, t.depth)]
            to_cols = ["rec_id"] + [f"idx{i}" for i in range(1, t.depth)]
            relations.append({
                "name": f"fk_{t.name}_to_{parent.name}",
                "from_table": t.name,
                "to_table": parent.name,
                "from_columns": from_cols,
                "to_columns": to_cols,
                "cardinality": "many-to-one",
            })

    profile = {
        "version": "1.0",
        "root": schema.root_name,
        "entities": entities,
        "relations": relations,
        "type_system": {
            "canonical": CANONICAL_TYPES,
            "synonyms": SYNONYMS,
            "fallback": "string",
        },
        "notes": [
            "root = массив записей; для каждого пути до массива создаётся сущность (TABLE-PER-PATH)",
            "скалярные поля объектов разворачиваются в колонки с префиксом key__sub__leaf",
            "nullable=true если встречались null или отсутствие поля",
            "при смешанных типах берётся безопасный тип (int→int64; int+float→float64; иное→string)",
        ],
    }
    return profile

# ---------------- Публичный API ----------------

def generate_profile(json_path: Union[str, Path]) -> Dict[str, Any]:
    """Считывает JSON (root=list) и возвращает JSON-профиль как Python-объект (dict)."""
    p = Path(json_path)
    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Ожидается JSON-массив (root = список записей)")
    schema = build_profile(data, root_name=ROOT_NAME)
    return schema_to_profile_json(schema)

# ---------------- CLI ----------------

def main():
    ap = argparse.ArgumentParser(description="Генератор JSON-профиля для TABLE-PER-PATH без БД")
    ap.add_argument("json_file", help="Путь к входному JSON (массив записей)")
    ap.add_argument("--out", default="profile.json", help="Куда сохранить профиль JSON")
    args = ap.parse_args()

    data = json.loads(Path(args.json_file).read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("Ожидается JSON-массив (root = список записей)")

    schema = build_profile(data, root_name=ROOT_NAME)
    profile = schema_to_profile_json(schema)
    Path(args.out).write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Профиль записан: {args.out}")

if __name__ == "__main__":
    main()
