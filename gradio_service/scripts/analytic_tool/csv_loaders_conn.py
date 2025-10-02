"""
loaders_conn.py — загрузка CSV в PostgreSQL и ClickHouse с уже созданными соединениями

Идея:
- Соединения создаёшь сам (psycopg.connect(...) / clickhouse_connect.get_client(...))
- В функции передаёшь: connection/client, профиль, DDL, путь к CSV
- Мы нормализуем значения по каноническим типам профиля и заливаем батчами

Зависимости:
    pip install psycopg[binary] clickhouse-connect

Примеры:
    import json, psycopg, clickhouse_connect
    from loaders_conn import load_to_postgres_conn, load_to_clickhouse_client

    # Профиль и DDL’ы (как сгенерили ранее)
    profile = json.loads(profile_json)

    # --- PG подключение ---
    pg_conn = psycopg.connect("postgresql://postgres:postgres@localhost:5432/analytics")
    rows_pg = load_to_postgres_conn(
        pg_conn, profile, ddl_pg, "data/my.csv",
        schema="public", table="my_table", has_header=True,
        commit=True,  # закоммитим после загрузки
    )

    # --- CH подключения ---
    # Важно: если БД из DDL (например raw) ещё не создана —
    # создайте "админ" клиент без database или с существующей БД:
    ch_admin = clickhouse_connect.get_client(host="localhost", port=8123, username="default", password="")
    rows_ch = load_to_clickhouse_client(
        client=clickhouse_connect.get_client(host="localhost", port=8123, username="default", password=""),
        profile=profile,
        ddl_sql=ddl_ch,                 # содержит CREATE TABLE IF NOT EXISTS `raw`.`my_table`
        csv_path="data/my.csv",
        admin_client=ch_admin,          # потребуется чтобы создать БД до CREATE TABLE
        ensure_database=True,
        has_header=True,
    )
"""
from __future__ import annotations

import csv
import io
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Типы для подсказок (не обязательны для исполнения)
try:  # psycopg3
    import psycopg
    from psycopg import Connection as PGConnection
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore
    PGConnection = Any  # type: ignore

try:
    import clickhouse_connect
    from clickhouse_connect.driver.client import Client as CHClient
except Exception:  # pragma: no cover
    clickhouse_connect = None  # type: ignore
    CHClient = Any  # type: ignore

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
        # запятые — разделители тысяч
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
# 1) PostgreSQL loader (с готовым conn)
# ---------------------------

def load_to_postgres_conn(
    conn: PGConnection,
    profile: Any,
    ddl_sql: str,
    csv_path: str,
    *,
    schema: str = "public",
    table: Optional[str] = None,     # если None — возьмём из профиля
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
    commit: bool = True,             # закоммитить после успешной загрузки
) -> int:
    """
    Загрузить CSV в PostgreSQL, используя уже открытое соединение psycopg3.

    - Таблица создаётся по переданному DDL (idempotent).
    - Значения нормализуются по каноническим типам профиля.
    - Возвращает число загруженных строк (без хедера).

    Примечание: мы не закрываем соединение. Если commit=False — транзакцию коммитит вызывающий код.
    """
    if psycopg is None:
        raise RuntimeError("psycopg не установлен. Установите: pip install psycopg[binary]")

    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)
    tname = table or (prof.get("entity") or {}).get("name") or "table1"

    total = 0
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
                        if ctype in ("int32", "int64", "float64") or ctype.startswith("decimal("):
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

    if commit:
        conn.commit()
    return total


# ---------------------------
# 2) ClickHouse loader (с готовым client)
# ---------------------------

def load_to_clickhouse_client(
    client: CHClient,
    profile: Any,
    ddl_sql: str,
    csv_path: str,
    *,
    admin_client: Optional[CHClient] = None,  # если нужно создать БД до DDL
    ensure_database: bool = True,             # создать БД из DDL (если указана) до CREATE TABLE
    has_header: bool = True,
    encoding: str = "utf-8-sig",
    batch_rows: int = 50_000,
    delimiter_override: Optional[str] = None,
) -> int:
    """
    Загрузить CSV в ClickHouse, используя уже созданный clickhouse_connect Client.

    - Если ensure_database=True, пытаемся создать БД из DDL до CREATE TABLE.
      ВАЖНО: это сработает, только если есть клиент, способный обратиться к серверу без ошибки
             из-за несуществующей БД. Если ваш `client` привязан к несуществующей БД,
             создайте отдельный `admin_client` без database и передайте его сюда.
    - DDL выполняется как есть (с/без квалификации БД).
    - Вставка батчами через client.insert(...).
    - Возвращает число загруженных строк (без хедера).
    """
    if clickhouse_connect is None:
        raise RuntimeError(
            "clickhouse-connect не установлен. Установите: pip install clickhouse-connect"
        )

    prof = _as_profile(profile)
    cols = _column_names(prof)
    ctypes = _canonical_types(prof)
    delimiter = delimiter_override or _delimiter_from_profile(prof)

    # Извлечём БД/таблицу из DDL
    m = re.search(
        r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(`?([^.`]+)`?\.)?`?([A-Za-z0-9_]+)`?",
        ddl_sql,
        re.I,
    )
    if not m:
        raise ValueError("Не удалось определить имя таблицы из DDL для ClickHouse")
    db_in_ddl = m.group(2)  # может быть None
    table_in_ddl = m.group(3)

    # Создадим БД до выполнения DDL (если нужно)
    if ensure_database and db_in_ddl:
        (admin_client or client).command(f"CREATE DATABASE IF NOT EXISTS `{db_in_ddl}`")

    # Выполняем DDL
    client.command(ddl_sql)

    # Полное имя таблицы для insert
    full_table = f"`{db_in_ddl}`.`{table_in_ddl}`" if db_in_ddl else f"`{table_in_ddl}`"

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
