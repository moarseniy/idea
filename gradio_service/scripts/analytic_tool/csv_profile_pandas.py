# csv_profile_pandas.py
from __future__ import annotations
import csv
import io
import os
import json
import math
import re
from decimal import Decimal, InvalidOperation
from typing import Dict, List, Tuple, Optional, Iterable, Any

import pandas as pd
try:
    import yaml  # PyYAML
except Exception as _e:
    yaml = None


# -------------------- Детект кодировки и разделителя (без внешних либ) --------------------

_CANDIDATE_DELIMS = [",", ";", "\t", "|", "^"]
_CANDIDATE_ENCODINGS = ["utf-8-sig", "utf-8", "cp1251", "latin1"]


def _read_sample_bytes(path: str, nbytes: int = 131072) -> bytes:
    with open(path, "rb") as f:
        return f.read(nbytes)


def _decode_with_fallback(b: bytes, encodings = None) -> Tuple[str, str]:
    encodings = encodings or _CANDIDATE_ENCODINGS
    for enc in encodings:
        try:
            return b.decode(enc), enc
        except Exception:
            continue
    return b.decode(encodings[0], errors="replace"), encodings[0]


def _sniff_delimiter(sample_text: str, delimiters = None) -> Optional[str]:
    delimiters = delimiters or _CANDIDATE_DELIMS
    try:
        dialect = csv.Sniffer().sniff(sample_text, delimiters="".join(delimiters))
        return dialect.delimiter
    except Exception:
        return None


def _heuristic_delimiter(sample_text: str, delimiters = None) -> Optional[str]:
    delimiters = delimiters or _CANDIDATE_DELIMS
    lines = [ln for ln in sample_text.splitlines() if ln.strip()][:50]
    if not lines:
        return None
    best, best_score = None, (-1.0, -1.0)
    for d in delimiters:
        counts = [ln.count(d) for ln in lines]
        if not any(counts):
            continue
        avg = sum(counts) / len(counts)
        zero_share = sum(1 for c in counts if c == 0) / len(counts)
        score = (avg, -zero_share)
        if score > best_score:
            best_score = score
            best = d
    return best


def detect_encoding_and_delimiter(
    path: str,
    *,
    sample_size: int = 131072,
    explicit_delimiter: Optional[str] = None,
    explicit_encoding: Optional[str] = None,
    verbose: bool = True,
) -> Tuple[str, str]:
    # encoding
    if explicit_encoding:
        encoding = explicit_encoding
        try:
            sample_text = _read_sample_bytes(path, sample_size).decode(encoding)
        except Exception:
            if verbose:
                print(f"[detect] Явная кодировка '{encoding}' не подошла, автоопределяю…")
            sample_text, encoding = _decode_with_fallback(_read_sample_bytes(path, sample_size))
    else:
        sample_text, encoding = _decode_with_fallback(_read_sample_bytes(path, sample_size))

    # delimiter
    if explicit_delimiter:
        delimiter = explicit_delimiter
    else:
        delimiter = _sniff_delimiter(sample_text) or _heuristic_delimiter(sample_text) or ","

    if verbose:
        print(f"[detect] Кодировка: {encoding}")
        printable = delimiter.replace("\t", "\\t")  # покажем TAB как \t
        print(f"[detect] Разделитель: {printable}")
    return encoding, delimiter


def _looks_like_header(first_row: List[str]) -> bool:
    if not first_row:
        return False
    # имена не должны повторяться
    if len(set(first_row)) < len(first_row):
        return False
    non_numeric = 0
    for cell in first_row:
        cell = (cell or "").strip()
        if not cell:
            continue
        try:
            float(cell.replace(",", "."))
        except ValueError:
            non_numeric += 1
    return non_numeric >= max(1, len(first_row)//2)


def _make_unique_headers(headers: List[str]) -> List[str]:
    seen = {}
    out = []
    for h in headers:
        base = (h or "").strip() or "column"
        if base not in seen:
            seen[base] = 1
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}__{seen[base]}")
    return out


# -------------------- Загрузка канонических типов из YAML --------------------

_DEFAULT_CANONICAL = {
    "lowcard_string": {},
    "string": {},
    "int32": {},
    "int64": {},
    "float64": {},
    "decimal(p,s)": {},
    "bool": {},
    "date": {},
    "timestamp": {},
    "timestamp64(ms)": {},
    "json": {},
}
_DEFAULT_SYNONYMS = {
    "text": "string", "varchar": "string",
    "bigint": "int64", "integer": "int32", "int4": "int32", "int8": "int64",
    "double": "float64", "double precision": "float64",
    "numeric": "decimal(p,s)", "decimal": "decimal(p,s)",
    "timestamptz": "timestamp", "timestampz": "timestamp",
    "datetime": "timestamp", "datetime64": "timestamp64(ms)",
    "jsonb": "json",
    "uint8": "bool",
}


def load_types_yaml(path: str = "configs/types.yaml", verbose: bool = True) -> Tuple[Dict[str, Any], Dict[str, str]]:
    if yaml is None:
        if verbose:
            print("[types] PyYAML не установлен; использую встроенный набор канонических типов.")
        return _DEFAULT_CANONICAL, _DEFAULT_SYNONYMS
    if not os.path.isfile(path):
        if verbose:
            print(f"[types] YAML не найден по пути {path}; использую встроенный набор канонических типов.")
        return _DEFAULT_CANONICAL, _DEFAULT_SYNONYMS
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    canonical = cfg.get("canonical", {}) or {}
    synonyms = cfg.get("synonyms", {}) or {}
    # sanity: гарантируем ключи из дефолтов
    for k in _DEFAULT_CANONICAL:
        canonical.setdefault(k, {})
    for k, v in _DEFAULT_SYNONYMS.items():
        synonyms.setdefault(k, v)
    if verbose:
        print(f"[types] Загружены канонические типы из {path}.")
    return canonical, synonyms


# -------------------- Инференс типов по данным --------------------

_BOOL_TOKENS_TRUE = {"true", "t", "1", "yes", "y", "да", "истина"}
_BOOL_TOKENS_FALSE = {"false", "f", "0", "no", "n", "нет", "ложь"}
_JSON_LIKE_RE = re.compile(r'^\s*[\{\[]')  # начинается с { или [
_INT_RE = re.compile(r'^[+-]?[0-9]+$')
_FLOAT_RE = re.compile(r'^[+-]?(?:\d+\.?\d*|\d*\.\d+)(?:[eE][+-]?\d+)?$')
_NUM_WITH_COMMA_RE = re.compile(r'^[+-]?(?:\d{1,3}(?:[\s_]\d{3})+|\d+)(?:[,]\d+)?$')  # 1 234,56

_INT32_MIN, _INT32_MAX = -2147483648, 2147483647
_INT64_MIN, _INT64_MAX = -9223372036854775808, 9223372036854775807


def _is_bool_token(v: str) -> Optional[bool]:
    s = v.strip().lower()
    if s in _BOOL_TOKENS_TRUE:
        return True
    if s in _BOOL_TOKENS_FALSE:
        return False
    return None


def _try_parse_int(s: str) -> Optional[int]:
    if not _INT_RE.match(s):
        return None
    try:
        return int(s)
    except Exception:
        return None


def _normalize_num_for_decimal(s: str) -> Optional[str]:
    s = s.strip()
    if not s:
        return None
    # если используется запятая как десятичный разделитель (и нет точки) — заменим на точку
    if "," in s and "." not in s:
        s = s.replace(" ", "").replace("_", "").replace(",", ".")
    else:
        s = s.replace(" ", "").replace("_", "")
    return s


def _try_parse_decimal(s: str) -> Optional[Tuple[int, int]]:
    """
    Возвращает (precision, scale) если удалось распарсить Decimal, иначе None.
    """
    s_norm = _normalize_num_for_decimal(s)
    if s_norm is None:
        return None
    try:
        d = Decimal(s_norm)
    except InvalidOperation:
        return None
    tup = d.as_tuple()
    if d.as_tuple().exponent <= 0:
        scale = -tup.exponent
    else:
        scale = 0
    digits = len(tup.digits)
    precision = digits if scale == 0 else digits
    return precision, scale


def _try_parse_float(s: str) -> bool:
    s_norm = _normalize_num_for_decimal(s)
    if s_norm is None:
        return False
    # если научная нотация — это точно float64
    if "e" in s_norm.lower():
        try:
            float(s_norm)
            return True
        except Exception:
            return False
    # обычный float
    try:
        float(s_norm)
        return True
    except Exception:
        return False


def _try_parse_datetime(series: pd.Series) -> Tuple[bool, Optional[bool], Optional[bool]]:
    """
    Пытаемся распарсить все непустые значения колонки как дату/время.
    Возвращает:
      (удачно_ли_вообще, is_pure_date (True/False/None), has_ms_precision (True/False/None))
    """
    try:
        parsed = pd.to_datetime(series, errors="coerce", infer_datetime_format=True, utc=True)
    except Exception:
        return False, None, None
    ok = parsed.notna()
    if ok.mean() < 0.95:  # если много нераспарсенных — считаем неоднозначным
        return False, None, None
    # чистая дата: все времена == 00:00:00 и исходные строки не содержат явного времени у большинства
    times = parsed.dt.time
    is_midnight = (parsed.dt.hour == 0) & (parsed.dt.minute == 0) & (parsed.dt.second == 0) & (parsed.dt.microsecond == 0)
    is_pure_date = is_midnight.mean() > 0.99
    # точность до миллисекунд или лучше?
    has_ms = (parsed.dt.microsecond > 0).mean() > 0.01
    return True, is_pure_date, has_ms


def infer_canonical_type_for_series(
    s: pd.Series,
    *,
    total_rows: int,
    canonical_names: Iterable[str],
    lowcard_ratio: float = 0.10,
    lowcard_max: int = 5000,
) -> str:
    """
    Возвращает строку с каноническим типом (ключ из YAML canonical).
    Если тип определить нельзя однозначно — возвращает 'string'.
    """
    # работаем только с непустыми строковыми значениями
    vals = s.dropna()
    vals = vals[vals.astype(str).str.strip() != ""].astype(str)
    if len(vals) == 0:
        return "string"

    # 1) JSON
    if "json" in canonical_names:
        if vals.str.match(_JSON_LIKE_RE).mean() > 0.9:
            # пробуем распарсить подвыборку
            sample = vals.sample(min(200, len(vals)), random_state=0)
            ok = 0
            for v in sample:
                v = v.strip()
                if not v:
                    continue
                try:
                    import json as _json
                    x = _json.loads(v)
                    if isinstance(x, (dict, list)):
                        ok += 1
                except Exception:
                    pass
            if ok / max(1, len(sample)) > 0.9:
                return "json"

    # 2) BOOL
    if "bool" in canonical_names:
        sample = vals.sample(min(1000, len(vals)), random_state=0)
        checks = [_is_bool_token(v) is not None for v in sample]
        if sum(checks) == len(sample):
            return "bool"

    # 3) INT / DECIMAL / FLOAT
    ints_ok = True
    int32_ok = True
    int64_ok = True
    decimal_ok = True
    max_p, max_s = 0, 0
    float_ok = True
    saw_exponent = False

    sample = vals.sample(min(5000, len(vals)), random_state=0)  # быстрая оценка
    for v in sample:
        v = v.strip()
        # INT
        iv = _try_parse_int(v)
        if iv is None:
            ints_ok = False
            # DECIMAL / FLOAT
            ds = _try_parse_decimal(v)
            if ds is None:
                float_ok = float_ok and _try_parse_float(v)
                if "e" in v.lower():
                    saw_exponent = True
            else:
                p, s_ = ds
                max_p = max(max_p, p)
                max_s = max(max_s, s_)
        else:
            # число — проверим диапазоны
            if not (_INT32_MIN <= iv <= _INT32_MAX):
                int32_ok = False
            if not (_INT64_MIN <= iv <= _INT64_MAX):
                int64_ok = False

    if ints_ok:
        # решаем между int32 и int64
        if int32_ok:
            return "int32"
        if int64_ok:
            return "int64"
        # вне диапазона int64 — спасаемся в decimal
        if "decimal(p,s)" in canonical_names:
            return f"decimal({max_p},{max_s})" if max_s > 0 else "decimal(38,0)"

    if decimal_ok and max_p > 0:
        # Если видели экспоненту — лучше float64
        if saw_exponent:
            return "float64" if "float64" in canonical_names else "string"
        return f"decimal({max_p},{max_s})"

    if float_ok:
        return "float64"

    # 4) DATE / TIMESTAMP
    if "date" in canonical_names or "timestamp" in canonical_names:
        ok, is_date, has_ms = _try_parse_datetime(vals)
        if ok:
            if is_date and "date" in canonical_names:
                return "date"
            if has_ms and "timestamp64(ms)" in canonical_names:
                return "timestamp64(ms)"
            return "timestamp"

    # 5) STRING / LOWCARD_STRING
    uniq = vals.nunique(dropna=True)
    ratio = uniq / max(1, total_rows)
    if "lowcard_string" in canonical_names and ratio <= lowcard_ratio and uniq <= lowcard_max:
        return "lowcard_string"
    return "string"


# -------------------- Основная функция профилирования --------------------

def compute_csv_profile(
    path: str,
    *,
    delimiter: Optional[str] = None,
    encoding: Optional[str] = None,
    sample_size: int = 131072,
    chunksize: int = 200_000,
    type_sample_rows: int = 50_000,
    lowcard_ratio: float = 0.10,
    lowcard_max: int = 5000,
    types_yaml_path: str = "configs/types.yaml",
    verbose: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Возвращает два словаря:
      1) card_json = {"raws": <int>, "column_cardinalities": {...}}
      2) types_json = {"column_types": { "<col>": "<canonical>" , ... }}

    - Читает CSV чанками (dtype=str, без NA-конверсии), поэтому экономно по памяти.
    - Разделитель и кодировка автоопределяются, если не заданы явно.
    - Типы выводятся в канонических ключах из YAML (иначе — дефолтный набор).
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Файл не найден: {path}")

    canonical, synonyms = load_types_yaml(types_yaml_path, verbose=verbose)
    canonical_names = set(canonical.keys())

    enc, dlm = detect_encoding_and_delimiter(
        path,
        sample_size=sample_size,
        explicit_delimiter=delimiter,
        explicit_encoding=encoding,
        verbose=verbose,
    )

    # Определяем наличие заголовка на основе первой строки
    with open(path, "r", encoding=enc, newline="") as f:
        reader = csv.reader(f, delimiter=dlm)
        try:
            first_row = next(reader)
        except StopIteration:
            if verbose:
                print("[read] Пустой файл.")
            return {"raws": 0, "column_cardinalities": {}}, {"column_types": {}}
    has_header = _looks_like_header(first_row)
    if has_header:
        header = 0
        names = None
        if verbose:
            print(f"[header] Обнаружен заголовок: {first_row}")
    else:
        header = None
        names = [f"col_{i+1}" for i in range(len(first_row))]
        if verbose:
            print(f"[header] Заголовок не обнаружен. Сгенерированы имена: {names}")

    # ---- Первый проход: считаем кардинальности и количество строк (чанки) ----
    raws = 0
    uniques: Dict[str, set] = {}
    col_order: List[str] = []

    reader = pd.read_csv(
        path,
        sep=dlm,
        encoding=enc,
        header=header,
        names=names,
        dtype=str,
        na_filter=False,  # не превращаем пустые в NaN
        chunksize=chunksize,
        engine="python",  # устойчивее к разным разделителям/кавычкам
        on_bad_lines="skip",
    )

    for chunk in reader:
        if not col_order:
            col_order = list(chunk.columns)
            # уникализируем, если вдруг дубли в хедерах
            col_order = _make_unique_headers(col_order)
            if list(chunk.columns) != col_order:
                chunk.columns = col_order
        raws += len(chunk)
        for c in col_order:
            if c not in uniques:
                uniques[c] = set()
            # прибавляем уникальные в чанке
            uniques[c].update(chunk[c].unique().tolist())

    cards = {c: len(uniques.get(c, set())) for c in col_order}
    if verbose:
        print(f"[done] Строк (без заголовка): {raws}")
        for c in col_order:
            print(f"  - {c}: {cards[c]} уникальных")

    card_json = {"raws": raws, "column_cardinalities": cards}

    # ---- Второй проход (облегчённый): инференс типов по подвыборке ----
    # чтобы не читать весь файл второй раз, возьмём n первых строк (type_sample_rows)
    sample_rows = min(type_sample_rows, max(10_000, min(raws, type_sample_rows)))
    types: Dict[str, str] = {}

    # Если файл очень большой, читаем небольшой префикс; иначе можно целиком
    df_sample = pd.read_csv(
        path,
        sep=dlm,
        encoding=enc,
        header=header,
        names=names,
        dtype=str,
        na_filter=False,
        nrows=sample_rows,
        engine="python",
        on_bad_lines="skip",
    )
    if not list(df_sample.columns) == col_order:
        # синхронизация имён
        df_sample.columns = col_order

    for c in col_order:
        t = infer_canonical_type_for_series(
            df_sample[c],
            total_rows=raws if raws > 0 else len(df_sample),
            canonical_names=canonical_names,
            lowcard_ratio=lowcard_ratio,
            lowcard_max=lowcard_max,
        )
        types[c] = t
        if verbose:
            print(f"[type] {c}: {t}")

    types_json = {"column_types": types}
    return card_json, types_json


# -------------------- Утилиты вывода и CLI --------------------

def to_json(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


def main(argv: Optional[List[str]] = None) -> int:
    import argparse
    p = argparse.ArgumentParser(description="Профилирование CSV: кардинальности и канонические типы (pandas).")
    p.add_argument("csv_path", help="Путь к CSV")
    p.add_argument("--delimiter", help="Явный разделитель", default=None)
    p.add_argument("--encoding", help="Явная кодировка", default=None)
    p.add_argument("--types-yaml", default="configs/types.yaml", help="Путь к YAML с каноническими типами")
    p.add_argument("--chunksize", type=int, default=200_000)
    p.add_argument("--type-sample-rows", type=int, default=50_000)
    p.add_argument("--lowcard-ratio", type=float, default=0.10)
    p.add_argument("--lowcard-max", type=int, default=5000)
    p.add_argument("--quiet", action="store_true")
    p.add_argument("-o", "--out-prefix", help="Префикс файлов для сохранения JSON (создаст *_card.json и *_types.json)")

    args = p.parse_args(argv)

    card_json, types_json = compute_csv_profile(
        args.csv_path,
        delimiter=args.delimiter,
        encoding=args.encoding,
        chunksize=args.chunksize,
        type_sample_rows=args.type_sample_rows,
        lowcard_ratio=args.lowcard_ratio,
        lowcard_max=args.lowcard_max,
        types_yaml_path=args.types_yaml,
        verbose=not args.quiet,
    )

    if args.out_prefix:
        card_path = f"{args.out_prefix}_card.json"
        types_path = f"{args.out_prefix}_types.json"
        with open(card_path, "w", encoding="utf-8") as f:
            f.write(to_json(card_json))
        with open(types_path, "w", encoding="utf-8") as f:
            f.write(to_json(types_json))
        if not args.quiet:
            print(f"[save] {card_path}")
            print(f"[save] {types_path}")
    else:
        print("=== CARDINALITIES ===")
        print(to_json(card_json))
        print("\n=== TYPES ===")
        print(to_json(types_json))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
