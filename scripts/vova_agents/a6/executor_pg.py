from typing import List, Dict
import psycopg2
from psycopg2 import sql
from pathlib import Path
import csv
from io import StringIO
from a5.type_registry import TypeRegistry

TR = TypeRegistry()
PG_DSN = "dbname=analytics user=postgres password=postgres host=localhost port=5432"

def run_sql_statements(statements: List[str], autocommit: bool = True):
    with psycopg2.connect(PG_DSN) as conn:
        conn.autocommit = autocommit
        with conn.cursor() as cur:
            for s in statements:
                if s.strip():
                    cur.execute(s)

def copy_csv_to_staging(csv_path: str, staging_table: str, select_schema: dict, csv_options: dict | None = None):
    csv_opts = csv_options or {}
    delim = csv_opts.get("delimiter", ",")
    quote = csv_opts.get("quotechar", '"')
    enc = csv_opts.get("encoding", "utf-8")

    ordered_cols = list(select_schema.keys())

    from io import StringIO
    buf = StringIO()
    w = csv.writer(buf)  # временный CSV для COPY с запятой — ок
    # читаем исходник с нужным разделителем/кавычкой/кодировкой
    with open(csv_path, "r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f, delimiter=delim, quotechar=quote)
        for row in reader:
            w.writerow([row.get(col, "") for col in ordered_cols])
    buf.seek(0)

    columns_sql = ",".join(ordered_cols)
    copy_sql = f"COPY {staging_table}({columns_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)"
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, buf)

def counts(tables: List[str]) -> Dict[str, int]:
    def ident(name: str):
        parts = name.split(".")
        return sql.SQL(".").join([sql.Identifier(p) for p in parts])
    res: Dict[str, int] = {}
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(sql.SQL("SELECT count(*) FROM {}").format(ident(t)))
                res[t] = cur.fetchone()[0]
    return res
