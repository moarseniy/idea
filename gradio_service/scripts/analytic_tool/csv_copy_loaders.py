"""
Мини-лоадеры CSV → БД (только копирование в уже созданные таблицы).

Функции:
- copy_into_pg(profile, csv_path, conn, *, table=None, schema="public", has_header=True,
               encoding="utf-8-sig", batch_rows=50_000, delimiter_override=None) -> int
- copy_into_clickhouse(profile, csv_path, client, *, table=None, has_header=True,
                       encoding="utf-8-sig", batch_rows=50_000, delimiter_override=None) -> int

Где:
- profile: dict или JSON-строка, полученная из профайлера (содержит порядок колонок и canonical-типы)
- csv_path: путь к CSV
- conn: psycopg.Connection (psycopg 3)
- client: clickhouse_connect.driver.client.Client
- table: имя таблицы; по умолчанию берётся из profile["entity"]["name"]
- schema (PG): схема; по умолчанию "public"
- функции возвращают число загруженных строк (без заголовка)
"""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

# типы подключений оставляем Any, чтобы не требовать обязательных импортов в модуле
try:
    import psycopg2  # noqa: F401
except Exception:
    psycopg = None  # type: ignore

try:
    import clickhouse_connect  # noqa: F401
except Exception:
    clickhouse_connect = None  # type: ignore


# ---------- общие утилиты нормализации ----------

_NULL_TOKENS = {"", "null", "none", "nan", "n/a", "na", "\\n", "\\N"}
_TRUE_TOKENS = {"true", "t", "1", "yes", "y", "да"}
_FALSE_TOKENS = {"false", "f", "0", "no", "n", "нет"}

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:[.,]\d+)?|\d{1,3}(?:[ ,]\d{3})+(?:[.,]\d+)?)$")
_DT_Z_RE = re.compile(r"Z$", re.I)

_DATE_ONLY_FORMATS = [
    "%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d",
]
_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f",
    "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M:%S.%f",
]

def _is_null(s: str) -> bool:
    return s.strip().lower() in _NULL_TOKENS

def _to_bool(s: str) -> Optional[bool]:
    low = s.strip().lower()
    if low in _TRUE_TOKENS: return True
    if low in _FALSE_TOKENS: return False
    return None

def _normalize_number(s: str) -> Optional[str]:
    t = s.strip()
    if not t or _NUMERIC_RE.match(t) is None:
        return None
    t = t.replace(" ", "")
    if "," in t and "." in t:
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        if t.count(",") == 1:
            t = t.replace(",", ".")
        else:
            return None
    return t

def _parse_date(s: str) -> Optional[date]:
    t = s.strip()
    if not t: return None
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", t):
            return date.fromisoformat(t)
    except Exception:
        pass
    for fmt in _DATE_ONLY_FORMATS:
        try:
            return datetime.strptime(t, fmt).date()
        except Exception:
            continue
    return None

def _parse_datetime_utc(s: str) -> Optional[datetime]:
    t = s.strip()
    if not t: return None
    tt = _DT_Z_RE.sub("+00:00", t)
    try:
        if ("T" in tt) or (" " in tt):
            dt = datetime.fromisoformat(tt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in _DATETIME_FORMATS:
        try:
            dt = datetime.strptime(t, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


# ---------- профиль: колонки и типы ----------

def _as_profile(profile: Any) -> Dict[str, Any]:
    if isinstance(profile, str):
        return json.loads(profile)
    if isinstance(profile, dict):
        return profile
    raise TypeError("profile must be dict or JSON string")

def _column_names(profile: Dict[str, Any]) -> List[str]:
    return [c["name"] for c in profile.get("columns", [])]

def _canonical_types(profile: Dict[str, Any]) -> List[str]:
    return [(c.get("type") or {}).get("canonical", "string") for c in profile.get("columns", [])]

def _delimiter_from_profile(profile: Dict[str, Any], default: str = ",") -> str:
    return (profile.get("entity") or {}).get("delimiter", default) or default


# ---------- PostgreSQL ----------

def copy_into_pg(
    profile: Any,
    csv_path: str,
    conn: Any,  # psycopg.Connection
    *,
    table: Optional[str] = None,
    schema: str = "public",
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
) -> int:
    """
    Копирует CSV в существующую таблицу PostgreSQL через COPY FROM STDIN.
    Таблица должна быть создана заранее (DDL).
    Возвращает число загруженных строк (без заголовка).
    """
    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)
    tname = table or (prof.get("entity") or {}).get("name") or "table1"

    # полное имя таблицы
    fq = f'"{schema}"."{tname}"' if schema else f'"{tname}"'
    quoted_delim = delimiter.replace("'", "''")

    total = 0
    with conn.cursor() as cur:
        copy_sql = (
            f"COPY {fq} FROM STDIN WITH (FORMAT csv, HEADER {str(has_header).lower()}, DELIMITER '{quoted_delim}')"
        )
        with cur.copy(copy_sql) as cp:
            buf = io.StringIO()
            writer = csv.writer(buf, delimiter=delimiter, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

            with open(csv_path, "r", encoding=encoding, newline="") as f:
                rdr = csv.reader(f, delimiter=delimiter)
                first = True
                batch = 0
                for row in rdr:
                    if first and has_header:
                        first = False
                        continue
                    first = False

                    if len(row) < len(cols):
                        row = row + [""] * (len(cols) - len(row))
                    elif len(row) > len(cols):
                        row = row[: len(cols)]

                    norm: List[str] = []
                    for val, ctype in zip(row, ctypes):
                        if _is_null(val):
                            norm.append("")
                            continue
                        if ctype == "bool":
                            b = _to_bool(val)
                            norm.append("true" if b is True else ("false" if b is False else ""))
                            continue
                        if ctype in ("int32", "int64", "float64") or ctype.startswith("decimal("):
                            nv = _normalize_number(val)
                            norm.append(nv if nv is not None else "")
                            continue
                        # date/timestamp/json/string — оставляем как есть (PG COPY сам разберёт)
                        norm.append(val)

                    writer.writerow(norm)
                    batch += 1
                    total += 1
                    if batch >= batch_rows:
                        cp.write(buf.getvalue())
                        buf.seek(0); buf.truncate(0)
                        batch = 0
                if batch > 0:
                    cp.write(buf.getvalue())
    return total


# ---------- ClickHouse ----------

def copy_into_clickhouse(
    profile: Any,
    csv_path: str,
    client: Any,  # clickhouse_connect.driver.client.Client
    *,
    table: Optional[str] = None,
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
) -> int:
    """
    Копирует CSV в существующую таблицу ClickHouse батчами (client.insert).
    Таблица должна быть создана заранее (DDL). Если client создан с database=..., можно
    передавать только имя таблицы (без БД).
    Возвращает число загруженных строк (без заголовка).
    """
    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)
    tname = table or (prof.get("entity") or {}).get("name") or "table1"

    total = 0
    rows_batch: List[Tuple[Any, ...]] = []

    def _cast_cell(val: str, ctype: str):
        if _is_null(val):
            return None
        if ctype == "bool":
            b = _to_bool(val); return None if b is None else b
        if ctype in ("int32", "int64"):
            nv = _normalize_number(val); return None if nv is None else int(float(nv))
        if ctype == "float64":
            nv = _normalize_number(val); return None if nv is None else float(nv)
        if ctype.startswith("decimal("):
            nv = _normalize_number(val); return None if nv is None else Decimal(nv)
        if ctype == "date":
            return _parse_date(val)
        if ctype in ("timestamp", "timestamp64(ms)"):
            return _parse_datetime_utc(val)
        return val  # json/string

    with open(csv_path, "r", encoding=encoding, newline="") as f:
        rdr = csv.reader(f, delimiter=delimiter)
        first = True
        for row in rdr:
            if first and has_header:
                first = False
                continue
            first = False

            if len(row) < len(cols):
                row = row + [""] * (len(cols) - len(row))
            elif len(row) > len(cols):
                row = row[: len(cols)]

            casted = tuple(_cast_cell(v, t) for v, t in zip(row, ctypes))
            rows_batch.append(casted)
            total += 1
            if len(rows_batch) >= batch_rows:
                client.insert(tname, rows_batch, column_names=cols)
                rows_batch.clear()
        if rows_batch:
            client.insert(tname, rows_batch, column_names=cols)
            rows_batch.clear()

    return total
