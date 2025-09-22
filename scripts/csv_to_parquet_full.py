#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_to_parquet_full_fixed.py — надёжная конвертация CSV -> Parquet целиком (чанками) с фиксированной схемой.
"""
import os
import argparse
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

def human_bytes(n: int) -> str:
    for u in ["B","KB","MB","GB","TB"]:
        if n < 1024 or u=="TB":
            return f"{n:.2f} {u}"
        n /= 1024

# ЕДИНАЯ схема столбцов для твоего датасета
ARROW_SCHEMA = pa.schema([
    ("created",            pa.timestamp("ms", tz="UTC")),
    ("order_status",       pa.string()),
    ("ticket_status",      pa.string()),
    ("ticket_price",       pa.float64()),  # можно сделать decimal128(12,2), если захочешь
    ("visitor_category",   pa.string()),
    ("event_id",           pa.int64()),
    ("is_active",          pa.bool_()),
    ("valid_to",           pa.date32()),
    ("count_visitor",      pa.int64()),
    ("is_entrance",        pa.bool_()),
    ("is_entrance_mdate",  pa.timestamp("ms", tz="UTC")),
    ("event_name",         pa.string()),
    ("event_kind_name",    pa.string()),
    ("spot_id",            pa.int64()),
    ("spot_name",          pa.string()),
    ("museum_name",        pa.string()),
    ("start_datetime",     pa.timestamp("ms", tz="UTC")),
    ("ticket_id",          pa.int64()),
    ("update_timestamp",   pa.timestamp("ms", tz="UTC")),
    ("client_name",        pa.string()),
    ("name",               pa.string()),
    ("surname",            pa.string()),
    ("client_phone",       pa.string()),
    ("museum_inn",         pa.string()),   # обязательно строкой: возможны ведущие нули/разная длина
    ("birthday_date",      pa.date32()),
    ("order_number",       pa.string()),
    ("ticket_number",      pa.string()),   # uuid храним как строку
])

DATE_COLS     = {"valid_to", "birthday_date"}
TS_COLS       = {"created", "is_entrance_mdate", "start_datetime", "update_timestamp"}
BOOL_COLS     = {"is_active", "is_entrance"}
INT_COLS      = {"event_id", "spot_id", "ticket_id", "count_visitor"}
FLOAT_COLS    = {"ticket_price"}

def normalize_chunk(df: pd.DataFrame) -> pd.DataFrame:
    # убедимся, что все ожидаемые колонки присутствуют
    for name in ARROW_SCHEMA.names:
        if name not in df.columns:
            df[name] = pd.NA

    # всё в строку (безопасно), потом приводим по группам
    for c in df.columns:
        if c not in df:
            continue
    # строки: подрежем пробелы
    str_cols = set(ARROW_SCHEMA.names) - (DATE_COLS | TS_COLS | BOOL_COLS | INT_COLS | FLOAT_COLS)
    for c in str_cols:
        df[c] = df[c].astype("string").str.strip()

    # числовые
    for c in INT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")  # допускаем NA

    for c in FLOAT_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # bool: принимаем True/False/1/0/yes/no
    def to_bool_series(s: pd.Series) -> pd.Series:
        if pd.api.types.is_bool_dtype(s):
            return s
        s = s.astype("string").str.lower().str.strip()
        true_set  = {"true","1","t","y","yes"}
        false_set = {"false","0","f","n","no"}
        res = pd.Series(pd.NA, index=s.index, dtype="boolean")
        res = np.where(s.isin(true_set), True, np.where(s.isin(false_set), False, pd.NA))
        return pd.Series(res, dtype="boolean")

    for c in BOOL_COLS:
        df[c] = to_bool_series(df[c])

    # даты и таймстемпы
    for c in DATE_COLS:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True).dt.date  # date32

    for c in TS_COLS:
        df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)  # timestamp(ms, UTC) при конвертации

    # порядок колонок как в схеме
    df = df[list(ARROW_SCHEMA.names)]
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path")
    ap.add_argument("--sep", default=";")
    ap.add_argument("--out", default=None)
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--compression", default="zstd", choices=["zstd","snappy","gzip","brotli","none"])
    ap.add_argument("--row_group_size", type=int, default=1_000_000)
    args = ap.parse_args()

    csv_path = args.csv_path
    out_path = args.out or os.path.join(
        os.path.dirname(csv_path),
        os.path.splitext(os.path.basename(csv_path))[0] + ".parquet"
    )
    comp = None if args.compression == "none" else args.compression

    writer = None
    total = 0

    for chunk in pd.read_csv(
        csv_path,
        sep=args.sep,
        chunksize=args.chunksize,
        low_memory=False,
        dtype=str,            # читаем как строки, чтобы потом детерминированно приводить типы
        keep_default_na=False # пустые строки не превращать в NaN автоматически
    ):
        # пустые строки -> NA
        chunk = chunk.replace({"": pd.NA})

        df = normalize_chunk(chunk)

        # конвертация в Arrow строго по схеме
        table = pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False, safe=False)

        if writer is None:
            writer = pq.ParquetWriter(
                out_path, ARROW_SCHEMA,
                compression=comp, use_dictionary=True, write_statistics=True
            )

        writer.write_table(table, row_group_size=args.row_group_size)
        total += len(df)

    if writer is not None:
        writer.close()

    csv_size = os.path.getsize(csv_path)
    pq_size  = os.path.getsize(out_path)

    print(f"CSV:     {csv_path} -> {human_bytes(csv_size)}")
    print(f"Parquet: {out_path} -> {human_bytes(pq_size)}")
    if pq_size > 0:
        print(f"Коэффициент (CSV/Parquet): x{csv_size / pq_size:.2f}")
    print(f"Строк записано: {total:,}".replace(",", " "))

if __name__ == "__main__":
    main()
