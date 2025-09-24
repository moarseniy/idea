from typing import List, Dict
import clickhouse_connect
import csv
from datetime import datetime
from a5.type_registry import TypeRegistry

TR = TypeRegistry()
CH_HOST = "localhost"; CH_PORT = 8123
CH_USER = "default"; CH_PASSWORD = ""; CH_DB = "analytics"

def client():
    return clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)

def run_sql_statements(statements: List[str]):
    c = client()
    for s in statements:
        if s.strip():
            c.command(s)

def load_csv_to_staging(csv_path: str, staging_table: str, select_schema: dict, csv_options: dict | None = None):
    csv_opts = csv_options or {}
    delim = csv_opts.get("delimiter", ",")
    quote = csv_opts.get("quotechar", '"')
    enc = csv_opts.get("encoding", "utf-8")

    ordered_cols = list(select_schema.keys())
    types = [select_schema[c] for c in ordered_cols]
    rows = []

    with open(csv_path, "r", newline="", encoding=enc) as f:
        r = csv.DictReader(f, delimiter=delim, quotechar=quote)
        for raw in r:
            tup = tuple(TR.parse_value(t, raw.get(col, "")) for col, t in zip(ordered_cols, types))
            tup = tup + (csv_path, datetime.utcnow())
            rows.append(tup)

    c = client()
    c.insert(f"{CH_DB}.{staging_table}", rows, column_names=ordered_cols + ["src_file", "load_ts"])


def counts(tables: List[str]) -> Dict[str, int]:
    c = client()
    res: Dict[str, int] = {}
    for t in tables:
        full = t if "." in t else f"{CH_DB}.{t}"
        res[t] = c.query(f"SELECT count() FROM {full}").result_rows[0][0]
    return res
