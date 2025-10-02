"""
CSV → JSON профиль (ускоренная версия на pandas)

Ключевые отличия от предыдущей реализации:
- Используется pandas с построчной обработкой chunk'ами (по умолчанию 200k строк)
- Векторные операции для длин/nullable/булевых/дат/чисел
- Парсинг выполняется только если *все* ненулевые значения колонки соответствуют кандидату на тип
- Автоопределение разделителя через csv.Sniffer; чтение через pandas.read_csv
- Если что-то неоднозначно — тип остаётся "string"
- Канонические типы и маппинги читаются из `config/types.yaml`

Пример использования:
    from csv_profiler_module import profile_csv_to_json
    js = profile_csv_to_json('data/sample.csv', entity_name='__FILL_ME__')
    print(js)

Зависимости: pandas, PyYAML
    pip install pandas pyyaml

Примечание по производительности:
- Чтение происходит как строки (dtype=str), чтобы строго контролировать распознавание типов.
- Обработка JSON/decimal выполняется только для колонок-кандидатов; иначе сразу "string".
"""
from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple
import datetime as dt

import pandas as pd

try:
    import yaml  # type: ignore
except Exception as e:  # pragma: no cover
    raise RuntimeError("PyYAML is required: pip install pyyaml") from e

# ---------------------------
# Конфиг типов
# ---------------------------

@dataclass
class CanonicalType:
    name: str  # e.g. "int32", "decimal(p,s)"
    pg: str
    ch: str
    py: str

@dataclass
class TypeSystem:
    canonical: Dict[str, CanonicalType] = field(default_factory=dict)
    synonyms: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str) -> "TypeSystem":
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        canonical = {}
        for k, v in cfg.get("canonical", {}).items():
            canonical[k] = CanonicalType(
                name=k,
                pg=v.get("pg", k),
                ch=v.get("ch", k),
                py=v.get("py", k),
            )
        synonyms = {k.lower(): v for k, v in cfg.get("synonyms", {}).items()}
        return cls(canonical=canonical, synonyms=synonyms)

    def resolve(self, name: str) -> CanonicalType:
        key = name
        if key not in self.canonical:
            key2 = self.synonyms.get(name.lower())
            if key2 and key2 in self.canonical:
                key = key2
            else:
                raise KeyError(f"Unknown canonical type: {name}")
        return self.canonical[key]

# ---------------------------
# Утилиты распознавания
# ---------------------------

_NULL_TOKENS = {"", "null", "none", "nan", "n/a", "na", "\\n", "\\N"}
_BOOL_TRUE = {"true", "t", "1", "yes", "y", "да"}
_BOOL_FALSE = {"false", "f", "0", "no", "n", "нет"}
_INT32_MIN, _INT32_MAX = -2_147_483_648, 2_147_483_647

NUMERIC_PATTERN = re.compile(r"^[+-]?(?:\d+(?:[.,]\d+)?|\d{1,3}(?:[ ,]\d{3})+(?:[.,]\d+)?)$")
DATE_ONLY_RE = re.compile(r"^(?:\d{4}-\d{2}-\d{2}|\d{2}[./-]\d{2}[./-]\d{4}|\d{4}/\d{2}/\d{2})$")
JSON_LIKE_RE = re.compile(r"^\s*[\[{].*[\]}]\s*$")


def _is_null_series(s: pd.Series) -> pd.Series:
    return s.str.strip().str.lower().isin(_NULL_TOKENS)


def _normalize_number_token(tok: str) -> Optional[str]:
    if tok is None:
        return None
    s = tok.strip()
    if not s:
        return None
    # уже полностью числовое?
    if NUMERIC_PATTERN.match(s) is None:
        return None
    # Убрать пробелы (разделители тысяч)
    s = s.replace(" ", "")
    # Если есть и запятая, и точка — удалим запятые как разделители тысяч
    if "," in s and "." in s:
        s = s.replace(",", "")
    else:
        # Если только запятая — считаем её десятичной
        if "," in s and "." not in s:
            if s.count(",") == 1:
                s = s.replace(",", ".")
            else:
                return None
        # если только точки — уже ок
    return s


def _try_int(s: str) -> Optional[int]:
    if re.match(r"^[+-]?\d+$", s):
        try:
            return int(s)
        except Exception:
            return None
    return None

# ---------------------------
# Агрегатор по колонке (chunk-aware)
# ---------------------------

@dataclass
class ColumnAgg:
    name: str
    distinct_cap: int = 100_000

    # counters
    non_nulls: int = 0
    nulls: int = 0

    # samples & distinct
    examples: List[str] = field(default_factory=list)
    distinct: set = field(default_factory=set)
    distinct_capped: bool = False

    # lengths
    max_len: int = 0
    min_len: Optional[int] = None

    # type candidates
    possible_json: bool = True
    possible_dt: bool = True
    possible_date_only: bool = True
    possible_bool: bool = True
    possible_number: bool = True

    # time flags
    any_microseconds: bool = False

    # numeric accumulators
    seen_exponent: bool = False
    frac_set: set = field(default_factory=set)
    min_int: Optional[int] = None
    max_int: Optional[int] = None
    min_dec: Optional[Decimal] = None
    max_dec: Optional[Decimal] = None

    def update_chunk(self, s: pd.Series):
        # На входе Series строк
        s = s.astype(str)
        is_null = _is_null_series(s)
        s_nn = s[~is_null]
        self.nulls += int(is_null.sum())
        self.non_nulls += int((~is_null).sum())
        if self.non_nulls == 0:
            return

        # примеры
        if len(self.examples) < 3:
            for v in s_nn.unique().tolist():
                v = v.strip()
                if v and v not in self.examples:
                    self.examples.append(v)
                    if len(self.examples) >= 3:
                        break

        # длины
        lens = s_nn.str.len()
        if not lens.empty:
            self.max_len = max(self.max_len, int(lens.max()))
            mn = int(lens.min())
            self.min_len = mn if self.min_len is None else min(self.min_len, mn)

        # distinct (ограниченно)
        if not self.distinct_capped and self.distinct_cap > 0:
            # быстрые уникальные в чанке
            for v in s_nn.unique().tolist():
                self.distinct.add(v)
                if len(self.distinct) > self.distinct_cap:
                    self.distinct_capped = True
                    self.distinct = set()
                    break

        # JSON кандидат
        if self.possible_json:
            mask_like = s_nn.str.match(JSON_LIKE_RE)
            if bool((~mask_like).any()):
                self.possible_json = False
            else:
                # проверим парсинг; выходим при первой ошибке
                for v in s_nn.tolist():
                    try:
                        obj = json.loads(v)
                        if not isinstance(obj, (dict, list)):
                            self.possible_json = False
                            break
                    except Exception:
                        self.possible_json = False
                        break

        # Datetime кандидат
        if self.possible_dt:
            dts = pd.to_datetime(s_nn, errors='coerce', utc=True, infer_datetime_format=True)
            if bool(dts.isna().any()):
                self.possible_dt = False
                self.possible_date_only = False
            else:
                if bool((dts.dt.microsecond > 0).any()):
                    self.any_microseconds = True
                # дата-только (все строки соответствуют шаблонам даты без времени)
                if self.possible_date_only:
                    if not bool(s_nn.str.match(DATE_ONLY_RE).all()):
                        self.possible_date_only = False

        # Bool кандидат
        if self.possible_bool:
            low = s_nn.str.strip().str.lower()
            mask_bool = low.isin(_BOOL_TRUE | _BOOL_FALSE)
            if bool((~mask_bool).any()):
                self.possible_bool = False

        # Number кандидат
        if self.possible_number:
            mask_num = s_nn.str.strip().str.match(NUMERIC_PATTERN)
            if bool((~mask_num).any()):
                self.possible_number = False
            else:
                # нормализуем и соберём статистику
                vals = [ _normalize_number_token(v) for v in s_nn.tolist() ]
                # все обязаны нормализоваться
                if any(v is None for v in vals):
                    self.possible_number = False
                else:
                    for nv in vals:
                        # exponent?
                        if 'e' in nv.lower():
                            self.seen_exponent = True
                        # integer?
                        iv = _try_int(nv)
                        if iv is not None:
                            self.min_int = iv if self.min_int is None else min(self.min_int, iv)
                            self.max_int = iv if self.max_int is None else max(self.max_int, iv)
                            # также обновим decimal метрики
                            dec = Decimal(iv)
                            self.min_dec = dec if self.min_dec is None else min(self.min_dec, dec)
                            self.max_dec = dec if self.max_dec is None else max(self.max_dec, dec)
                            self.frac_set.add(0)
                        else:
                            try:
                                dec = Decimal(nv)
                            except InvalidOperation:
                                self.possible_number = False
                                break
                            # scale
                            tup = dec.as_tuple()
                            scale = -tup.exponent if tup.exponent < 0 else 0
                            self.frac_set.add(scale)
                            self.min_dec = dec if self.min_dec is None else min(self.min_dec, dec)
                            self.max_dec = dec if self.max_dec is None else max(self.max_dec, dec)

    # ----------
    # Решение по типу
    # ----------
    def decide_type(self) -> str:
        if self.non_nulls == 0:
            return "string"
        # строгий JSON
        if self.possible_json:
            return "json"
        # даты
        if self.possible_dt:
            if self.possible_date_only:
                return "date"
            return "timestamp64(ms)" if self.any_microseconds else "timestamp"
        # bool
        if self.possible_bool:
            return "bool"
        # числа
        if self.possible_number:
            # все целые?
            if self.frac_set and self.frac_set == {0}:
                if self.min_int is None or self.max_int is None:
                    return "int64"  # запасной вариант
                if self.min_int >= _INT32_MIN and self.max_int <= _INT32_MAX:
                    return "int32"
                return "int64"
            # десятичные без экспоненты и с обязательной дробной частью
            if 0 not in self.frac_set and not self.seen_exponent:
                p, s = self._decimal_ps()
                if p is not None and s is not None:
                    return f"decimal({p},{s})"
                return "float64"
            return "float64"
        return "string"

    def _decimal_ps(self) -> Tuple[Optional[int], Optional[int]]:
        if self.min_dec is None or self.max_dec is None:
            return None, None
        max_scale = max(self.frac_set) if self.frac_set else 0
        max_abs = max(abs(self.min_dec), abs(self.max_dec))
        s = format(max_abs, 'f')
        int_part = s.split('.')[0].lstrip('-').lstrip('+')
        int_digits = len(int_part.lstrip('0')) or 1
        precision = int_digits + max_scale
        return precision, max_scale

    def to_profile_obj(self, ts: "TypeSystem") -> Dict[str, Any]:
        canon = self.decide_type()
        args: Dict[str, Any] = {}
        base = canon
        m = re.match(r"^(\w+)\((\d+),(\d+)\)$", canon)
        if m:
            base = f"{m.group(1)}(p,s)"
            args = {"p": int(m.group(2)), "s": int(m.group(3))}
        try:
            ct = ts.resolve(base)
        except KeyError:
            ct = CanonicalType(base, base, base, base)

        def _fmt(tmpl: str) -> str:
            return tmpl.format(**args) if args else tmpl

        obj = {
            "name": self.name,
            "type": {
                "canonical": canon,
                "pg": _fmt(ct.pg),
                "ch": _fmt(ct.ch),
                "py": _fmt(ct.py),
            },
            "nullable": self.nulls > 0,
            "stats": {
                "non_nulls": self.non_nulls,
                "nulls": self.nulls,
                "max_length": self.max_len,
                "min_length": 0 if self.min_len is None else self.min_len,
                "distinct_count": None if self.distinct_capped else len(self.distinct),
                "distinct_count_capped": self.distinct_capped,
                "examples": self.examples,
            },
        }
        # числовые метаданные
        if self.possible_number:
            if self.min_int is not None and self.max_int is not None:
                obj["stats"].update({"min_int": self.min_int, "max_int": self.max_int})
            if self.min_dec is not None and self.max_dec is not None:
                obj["stats"].update({
                    "min_decimal": format(self.min_dec, 'f'),
                    "max_decimal": format(self.max_dec, 'f'),
                })
            if self.frac_set:
                obj["stats"]["observed_scales"] = sorted(self.frac_set)
        if self.possible_dt:
            obj["stats"]["time_precision_ms"] = bool(self.any_microseconds)
        return obj

# ---------------------------
# Публичные функции
# ---------------------------

def profile_csv(
    path: str,
    *,
    entity_name: str = "__FILL_ME__",
    types_yaml_path: str = os.path.join("config", "types.yaml"),
    encoding: str = "utf-8-sig",
    has_header: Optional[bool] = None,
    sample_size: int = 64 * 1024,
    chunk_rows: int = 200_000,
) -> Dict[str, Any]:
    """
    Построить JSON-профиль по CSV (быстро, pandas + чанки).

    :param path: путь к CSV
    :param entity_name: название сущности
    :param types_yaml_path: путь к config/types.yaml
    :param encoding: кодировка файла
    :param has_header: явно указать наличие заголовка (иначе авто)
    :param sample_size: объём сэмпла для csv.Sniffer
    :param chunk_rows: размер чанка при чтении
    :return: словарь с профилем
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    ts = TypeSystem.load(types_yaml_path)

    # Определим разделитель и наличие заголовка
    with open(path, "r", encoding=encoding, newline="") as f:
        sample = f.read(sample_size)
        f.seek(0)
        try:
            dialect: csv.Dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.get_dialect("excel")
        if has_header is None:
            try:
                has_header = csv.Sniffer().has_header(sample)
            except Exception:
                has_header = True

    # Вычитываем заголовки (или сгенерим)
    # Берём первую строку отдельно для определения числа колонок
    first_df = pd.read_csv(
        path,
        sep=getattr(dialect, 'delimiter', ','),
        header=0 if has_header else None,
        nrows=1,
        dtype=str,
        encoding=encoding,
        na_filter=False,
        keep_default_na=False,
        engine='c',
        on_bad_lines='skip',
    )
    if first_df.empty:
        return {
            "entity": {
                "name": entity_name,
                "source_path": path,
                "delimiter": getattr(dialect, 'delimiter', ','),
                "rows": 0,
            },
            "columns": [],
        }

    if has_header:
        headers = [c if c else f"col_{i+1}" for i, c in enumerate(first_df.columns.tolist())]
    else:
        headers = [f"col_{i+1}" for i in range(first_df.shape[1])]

    # Создадим агрегаторы
    aggs = {h: ColumnAgg(name=h) for h in headers}

    # Основной проход по чанкам
    total_rows = 0
    reader = pd.read_csv(
        path,
        sep=getattr(dialect, 'delimiter', ','),
        header=0 if has_header else None,
        names=headers if not has_header else None,
        dtype=str,
        encoding=encoding,
        na_filter=False,
        keep_default_na=False,
        engine='c',
        chunksize=chunk_rows,
        on_bad_lines='skip',
    )

    for chunk in reader:
        total_rows += len(chunk)
        for h in headers:
            aggs[h].update_chunk(chunk[h])

    # Сборка результата
    columns_out = [aggs[h].to_profile_obj(ts) for h in headers]
    profile = {
        "entity": {
            "name": entity_name,
            "source_path": path,
            "delimiter": getattr(dialect, 'delimiter', ','),
            "rows": total_rows,
        },
        "columns": columns_out,
    }
    return profile


def profile_csv_to_json(
    path: str,
    *,
    entity_name: str = "__FILL_ME__",
    types_yaml_path: str = os.path.join("config", "types.yaml"),
    encoding: str = "utf-8-sig",
    has_header: Optional[bool] = None,
    sample_size: int = 64 * 1024,
    chunk_rows: int = 200_000,
    ensure_ascii: bool = False,
    indent: int = 2,
) -> str:
    prof = profile_csv(
        path,
        entity_name=entity_name,
        types_yaml_path=types_yaml_path,
        encoding=encoding,
        has_header=has_header,
        sample_size=sample_size,
        chunk_rows=chunk_rows,
    )
    return json.dumps(prof, ensure_ascii=ensure_ascii, indent=indent)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="CSV → JSON профиль (pandas)")
    ap.add_argument("csv_path", help="Путь к CSV")
    ap.add_argument("--entity", dest="entity_name", default="__FILL_ME__")
    ap.add_argument("--types", dest="types_yaml_path", default=os.path.join("config", "types.yaml"))
    ap.add_argument("--encoding", default="utf-8-sig")
    ap.add_argument("--no-header", dest="has_header", action="store_false", help="Явно указать отсутствие заголовка")
    ap.add_argument("--header", dest="has_header", action="store_true", help="Явно указать наличие заголовка")
    ap.add_argument("--sample", dest="sample_size", type=int, default=64 * 1024)
    ap.add_argument("--chunk", dest="chunk_rows", type=int, default=200_000)
    args = ap.parse_args()

    js = profile_csv_to_json(
        args.csv_path,
        entity_name=args.entity_name,
        types_yaml_path=args.types_yaml_path,
        encoding=args.encoding,
        has_header=args.has_header,
        sample_size=args.sample_size,
        chunk_rows=args.chunk_rows,
    )
    print(js)
