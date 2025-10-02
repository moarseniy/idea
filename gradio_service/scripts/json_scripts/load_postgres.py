# load_postgres.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import io, csv
from typing import Any, Dict, Iterable, List, Tuple
import psycopg2  # pip install psycopg2-binary

from scripts.json_scripts.row_iterator import iter_rows, get_table_columns  # из модуля, что мы сделали ранее

def copy_into_pg(conn, profile: Dict[str, Any], records: Iterable[Dict[str, Any]],
                 schema: str = "public", batch_size: int = 50_000) -> None:
    """
    Загружает данные в PostgreSQL, используя COPY FROM STDIN (CSV \t).
    Таблицы должны быть созданы заранее (emit_ddl_pg -> выполнить).
    """
    cols_by_table = get_table_columns(profile)
    buffers: Dict[str, List[Tuple[Any, ...]]] = {t: [] for t in cols_by_table}

    def flush_table(cur, table: str):
        cols = cols_by_table[table]
        rows = buffers[table]
        if not rows:
            return
        buf = io.StringIO()
        w = csv.writer(buf, delimiter='\t', lineterminator='\n',
                       quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        for r in rows:
            # None -> пустое поле => COPY CSV прочитает как NULL (если поле не в кавычках)
            w.writerow(['' if v is None else v for v in r])
        buf.seek(0)
        col_list = ", ".join(f'"{c}"' for c in cols)
        cur.copy_expert(
            f'COPY "{schema}"."{table}" ({col_list}) FROM STDIN '
            f"WITH (FORMAT csv, DELIMITER E'\\t', QUOTE '\"', ESCAPE '\\')",
            buf
        )
        rows.clear()

    with conn.cursor() as cur:
        # немного ускоряем вставку
        cur.execute("SET LOCAL synchronous_commit = OFF")
        cur.execute("SET LOCAL maintenance_work_mem = '512MB'")
        cur.execute("SET LOCAL work_mem = '256MB'")

        n = 0
        for table, row in iter_rows(profile, records):
            cols = cols_by_table[table]
            # порядок значений по колонкам таблицы
            vals = tuple(row.get(c) for c in cols)
            buffers[table].append(vals)
            n += 1
            if n % batch_size == 0:
                for t in buffers:
                    flush_table(cur, t)
                conn.commit()

        # финальный слив
        for t in buffers:
            flush_table(cur, t)
        conn.commit()
