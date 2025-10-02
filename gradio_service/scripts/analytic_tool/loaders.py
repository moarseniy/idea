"""
Загрузчики CSV → БД (PostgreSQL и ClickHouse)

Цели:
- «Точно загрузилось»: нормализуем значения согласно профилю:
  * NULL-токены → NULL
  * bool понимает: true/false, 1/0, yes/no, да/нет
  * числа: убираем разделители тысяч, запятая → точка
  * date/timestamp парсим (TZ → UTC)
- Работа потоками/батчами, без pandas
- Принимают: profile (dict/JSON), DDL (str), путь к CSV
- Возвращают: счётчик загруженных строк данных (без заголовка)

Зависимости:
    pip install psycopg[binary] clickhouse-connect

Примеры вызова (на хосте):
    from loaders import load_to_postgres, load_to_clickhouse
    rows_pg = load_to_postgres(profile, sql_pg, "data/my.csv",
                               dsn="postgresql://postgres:postgres@localhost:5432/analytics",
                               schema="public", table="my_table")
    rows_ch = load_to_clickhouse(profile, sql_ch, "data/my.csv",
                                 host="localhost", port=8123, username="default", password="",
                                 database="raw")
"""
from __future__ import annotations

import csv
import io
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Ленивая загрузка клиентов, чтобы модуль можно было использовать частично
try:
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:
    import clickhouse_connect
except Exception:  # pragma: no cover
    clickhouse_connect = None  # type: ignore

# ---------------------------
# Общие утилиты нормализации
# ---------------------------

_NULL_TOKENS = {"", "null", "none", "nan", "n/a", "na", "\\n", "\\N"}
_TRUE_TOKENS = {"true", "t", "1", "yes", "y", "да"}
_FALSE_TOKENS = {"false", "f", "0", "no", "n", "нет"}

_NUMERIC_RE = re.compile(
    r"^[+-]?(?:\d+(?:[.,]\d+)?|\d{1,3}(?:[ ,]\d{3})+(?:[.,]\d+)?)$"
)
_DT_Z_RE = re.compile(r"Z$", re.I)

_DATE_ONLY_FORMATS = [
    "%Y-%m-%d",
    "%d.%m.%Y",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
]
_DATETIME_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%d.%m.%Y %H:%M:%S",
    "%d.%m.%Y %H:%M:%S.%f",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S.%f",
]


def _is_null_token(s: str) -> bool:
    return s.strip().lower() in _NULL_TOKENS


def _to_bool(s: str) -> Optional[bool]:
    low = s.strip().lower()
    if low in _TRUE_TOKENS:
        return True
    if low in _FALSE_TOKENS:
        return False
    return None


def _normalize_number(s: str) -> Optional[str]:
    t = s.strip()
    if not t or _NUMERIC_RE.match(t) is None:
        return None
    # уберём пробелы (тысячи)
    t = t.replace(" ", "")
    if "," in t and "." in t:
        # запятые были разделителями тысяч
        t = t.replace(",", "")
    elif "," in t and "." not in t:
        if t.count(",") == 1:
            t = t.replace(",", ".")
        else:
            return None
    return t


def _parse_date(s: str) -> Optional[date]:
    t = s.strip()
    if not t:
        return None
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
    if not t:
        return None
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


# ---------------------------
# Профиль → порядок/типы колонок
# ---------------------------

def _as_profile(profile: Any) -> Dict[str, Any]:
    if isinstance(profile, str):
        return json.loads(profile)
    if isinstance(profile, dict):
        return profile
    raise TypeError("profile must be dict or JSON string")


def _columns(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(profile.get("columns", []))


def _column_names(profile: Dict[str, Any]) -> List[str]:
    return [c["name"] for c in _columns(profile)]


def _canonical_types(profile: Dict[str, Any]) -> List[str]:
    return [(c.get("type") or {}).get("canonical", "string") for c in _columns(profile)]


def _delimiter_from_profile(profile: Dict[str, Any], default: str = ",") -> str:
    return (profile.get("entity") or {}).get("delimiter", default) or default


# ---------------------------
# 1) PostgreSQL loader
# ---------------------------

def load_to_postgres(
    profile: Any,
    ddl_sql: str,
    csv_path: str,
    *,
    dsn: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    table: Optional[str] = None,     # если None — возьмём из профиля
    schema: str = "public",
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
) -> int:
    """
    Создаёт таблицу (DDL) и грузит CSV в PostgreSQL через COPY FROM STDIN.
    Нормализует значения под канонические типы профиля.
    Возвращает число загруженных строк (без хедера).
    """
    if psycopg is None:
        raise RuntimeError("psycopg не установлен. Установите: pip install psycopg[binary]")

    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)
    tname = table or (prof.get("entity") or {}).get("name") or "table1"

    # Соединение
    if dsn:
        conn = psycopg.connect(dsn)
    else:
        conn = psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password)

    total = 0
    with conn:
        with conn.cursor() as cur:
            # Схема и таблица
            if schema:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                fq = f'"{schema}"."{tname}"'
            else:
                fq = f'"{tname}"'
            cur.execute(ddl_sql)

            # COPY
            copy_sql = (
                f"COPY {fq} FROM STDIN WITH (FORMAT csv, HEADER {str(has_header).lower()}, DELIMITER '{delimiter}')"
            )
            with cur.copy(copy_sql) as cp:
                buf = io.StringIO()
                writer = csv.writer(
                    buf,
                    delimiter=delimiter,
                    lineterminator="\n",
                    quoting=csv.QUOTE_MINIMAL,
                )

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

                        # нормализация по каноническим типам
                        norm: List[str] = []
                        for val, ctype in zip(row, ctypes):
                            if _is_null_token(val):
                                norm.append("")  # пустое поле -> NULL для COPY CSV
                                continue
                            if ctype == "bool":
                                b = _to_bool(val)
                                norm.append("true" if b is True else ("false" if b is False else ""))
                                continue
                            if ctype in ("int32", "int64"):
                                nv = _normalize_number(val)
                                norm.append(nv if nv is not None else "")
                                continue
                            if ctype == "float64" or ctype.startswith("decimal("):
                                nv = _normalize_number(val)
                                norm.append(nv if nv is not None else "")
                                continue
                            # date/timestamp/json/string — оставляем как есть
                            norm.append(val)

                        writer.writerow(norm)
                        batch += 1
                        total += 1
                        if batch >= batch_rows:
                            cp.write(buf.getvalue())
                            buf.seek(0)
                            buf.truncate(0)
                            batch = 0
                    if batch > 0:
                        cp.write(buf.getvalue())
    return total


# ---------------------------
# 2) ClickHouse loader
# ---------------------------

def load_to_clickhouse(
    profile: Any,
    ddl_sql: str,
    csv_path: str,
    *,
    host: str = "localhost",
    port: Optional[int] = 8123,
    username: str = "default",
    password: str = "",
    database: Optional[str] = None,
    secure: bool = False,
    settings: Optional[Dict[str, Any]] = None,
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
) -> int:
    """
    Создаёт таблицу (DDL) и грузит CSV в ClickHouse батчевыми вставками через clickhouse-connect.
    - Базу данных создаём ЗАРАНЕЕ, до выполнения DDL.
    - Если DDL без квалификации БД, а параметр `database` задан, DDL выполняется в этой БД.
    Возвращает число загруженных строк (без хедера).
    """
    if clickhouse_connect is None:
        raise RuntimeError(
            "clickhouse-connect не установлен. Установите: pip install clickhouse-connect"
        )

    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)

    # Базовый клиент БЕЗ database для создания БД заранее
    base_client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        secure=secure,
        settings=settings or {},
    )

    # Вытащим БД/таблицу из DDL
    m = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(`?([^.`]+)`?\.)?`?([A-Za-z0-9_]+)`?",
        ddl_sql,
        re.I,
    )
    if not m:
        raise ValueError("Не удалось определить имя таблицы из DDL для ClickHouse")
    db_in_ddl = m.group(2)  # может быть None
    table_in_ddl = m.group(3)

    # Целевая БД для создания/выполнения
    target_db = database or db_in_ddl
    if target_db:
        base_client.command(f"CREATE DATABASE IF NOT EXISTS `{target_db}`")

    # Клиент для выполнения DDL/insert:
    # - если target_db задана, подключимся в неё
    # - иначе остаёмся в default
    client = (
        clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=target_db,
            secure=secure,
            settings=settings or {},
        )
        if target_db
        else base_client
    )

    # Выполняем DDL (он может быть как квалифицированным, так и нет)
    client.command(ddl_sql)

    # Полное имя таблицы для insert
    if db_in_ddl:
        full_table = f"`{db_in_ddl}`.`{table_in_ddl}`"
    elif target_db:
        full_table = f"`{target_db}`.`{table_in_ddl}`"
    else:
        full_table = f"`{table_in_ddl}`"

    total = 0
    rows_batch: List[Tuple[Any, ...]] = []

    def _cast_cell(val: str, ctype: str) -> Any:
        if _is_null_token(val):
            return None
        if ctype == "bool":
            b = _to_bool(val)
            return None if b is None else b
        if ctype in ("int32", "int64"):
            nv = _normalize_number(val)
            return None if nv is None else int(float(nv))  # на случай вида "1.0"
        if ctype == "float64":
            nv = _normalize_number(val)
            return None if nv is None else float(nv)
        if ctype.startswith("decimal("):
            nv = _normalize_number(val)
            return None if nv is None else Decimal(nv)
        if ctype == "date":
            return _parse_date(val)
        if ctype in ("timestamp", "timestamp64(ms)"):
            return _parse_datetime_utc(val)
        # json/string — строкой
        return val

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
                client.insert(full_table, rows_batch, column_names=cols)
                rows_batch.clear()
        if rows_batch:
            client.insert(full_table, rows_batch, column_names=cols)
            rows_batch.clear()

    return total
