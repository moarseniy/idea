# load_clickhouse.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import json
from typing import Any, Dict, Iterable, List, Tuple, Optional
import requests
from datetime import datetime, timezone

from row_iterator import iter_rows, get_table_columns

# ---------- небольшие хелперы ----------

def _session(trust_env: bool = False, user: Optional[str] = None, password: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.trust_env = trust_env  # не наследуем HTTP(S)_PROXY; полезно на Windows/корп. окружениях
    if user is not None:
        s.auth = (user, password or "")
    s.headers.update({"Content-Type": "text/plain; charset=UTF-8"})
    return s

def _fmt_dt64_utc(dt: datetime, ms: int = 3) -> str:
    """Формат для ClickHouse DateTime64(ms) 'YYYY-MM-DD HH:MM:SS.mmm' в UTC."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    # округлим/обрежем до нужных миллисекунд
    frac = f"{int(dt.microsecond/10**(6-ms)):0{ms}d}"
    return dt.strftime("%Y-%m-%d %H:%M:%S") + ("." + frac if ms > 0 else "")

def _cast_value_for_ch(val: Any, canonical_type: str) -> Any:
    """
    Консервативный каст под ClickHouse:
      - int32/int64 -> int
      - float64     -> float
      - bool        -> bool
      - date        -> 'YYYY-MM-DD' (если строка ISO — оставим; если datetime — возьмём .date())
      - timestamp   -> 'YYYY-MM-DD HH:MM:SS' (UTC)
      - timestamp64(ms) -> 'YYYY-MM-DD HH:MM:SS.mmm' (UTC)
      - string/json -> как есть (json -> строка уже приходит из итератора)
    Если парсинг не удался — возвращаем исходное значение (пусть ClickHouse сам ругнётся).
    """
    if val is None:
        return None
    t = canonical_type.lower()

    try:
        if t in ("int32", "int64"):
            return int(val)
        if t == "float64":
            return float(val)
        if t == "bool":
            # ClickHouse Bool — это UInt8 внутри, но JSONEachRow принимает true/false
            return bool(val)
        if t == "date":
            # ожидаем строку 'YYYY-MM-DD' либо datetime/date
            if isinstance(val, str):
                # простая проверка на уже валидный формат
                if len(val) == 10 and val[4] == "-" and val[7] == "-":
                    return val
                # попробуем fromisoformat()
                return datetime.fromisoformat(val).date().isoformat()
            if isinstance(val, datetime):
                return val.date().isoformat()
            # другие типы — оставим как есть
            return val
        if t in ("timestamp", "timestamp64(ms)"):
            # поддержим ISO 8601 с T и с таймзоной
            if isinstance(val, str):
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))  # Z -> +00:00
            elif isinstance(val, datetime):
                dt = val
            else:
                return val
            if t == "timestamp":
                # секундами без миллисекунд тоже норм для CH
                return _fmt_dt64_utc(dt, ms=0)
            else:
                return _fmt_dt64_utc(dt, ms=3)
    except Exception:
        # не смогли привести — вернём как есть
        return val

    # string/json/default
    return val

# ---------- основной загрузчик ----------

def insert_into_ch(http_url: str,
                   profile: Dict[str, Any],
                   records: Iterable[Dict[str, Any]],
                   database: Optional[str] = None,
                   batch_size: int = 100_000,
                   settings: Optional[Dict[str, Any]] = None,
                   user: Optional[str] = None,
                   password: Optional[str] = None,
                   cast: bool = True,
                   trust_env: bool = False) -> None:
    """
    Вставка батчами в ClickHouse через HTTP, форматом JSONEachRow.
    Изменения относительно простой версии:
      - SQL и данные отправляются в ОДНОМ body (INSERT ... FORMAT JSONEachRow\\n{...}\\n{...})
      - по умолчанию приводим данные к ожидаемым типам CH (особенно timestamp*).
      - показываем текст ошибки CH при HTTPError.
    """
    cols_by_table = get_table_columns(profile)
    buffers: Dict[str, List[Dict[str, Any]]] = {t: [] for t in cols_by_table}
    sess = _session(trust_env=trust_env, user=user, password=password)

    def flush_table(table: str):
        rows = buffers[table]
        if not rows:
            return
        # Полное имя таблицы
        q_table = f"`{table.replace('`','``')}`"
        full = (q_table if not database else f"`{database.replace('`','``')}`.{q_table}")

        # Соберём SQL + данные в одно body
        sql_head = f"INSERT INTO {full} FORMAT JSONEachRow\n"
        data_lines = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n"
        body = (sql_head + data_lines).encode("utf-8")

        params = {}
        if settings:
            params.update({f"settings[{k}]": v for k, v in settings.items()})
        # database можно не указывать, т.к. полное имя уже содержит db.table

        r = sess.post(http_url, params=params or None, data=body, timeout=300)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # выведем полезную ошибку от CH
            raise RuntimeError(f"CH INSERT failed for table {table}: {r.text}") from e
        rows.clear()

    # проход по данным
    for table, row in iter_rows(profile, records):
        cols = cols_by_table[table]
        # отфильтруем и приведём значения
        payload: Dict[str, Any] = {}
        # найдём типы колонок для каста
        ent = next(e for e in profile["entities"] if e["name"] == table)
        types_by_name = {c["name"]: c.get("type", "string") for c in ent.get("columns", [])}
        for c in cols:
            v = row.get(c, None)
            if cast and c not in ("rec_id",) and not c.startswith("idx"):
                v = _cast_value_for_ch(v, types_by_name.get(c, "string"))
            payload[c] = v
        buffers[table].append(payload)

        if sum(len(v) for v in buffers.values()) >= batch_size:
            for t in list(buffers.keys()):
                flush_table(t)

    # финальный слив
    for t in list(buffers.keys()):
        flush_table(t)
