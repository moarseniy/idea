#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
profile_csv.py — быстрый профиль CSV для выбора БД и схемы.
Выводит человекочитаемое резюме в консоль и кладёт JSON-отчёт рядом с файлом.
"""

import argparse, os, json, math, re, csv, sys, hashlib, io
from collections import Counter
from datetime import datetime
import numpy as np
import pandas as pd

# ---------- утилиты ----------
LIKELY_ID_PAT = re.compile(r"(?:^|_)(id|uuid|guid|hash|key)(?:$|_)", re.I)
LIKELY_TIME_PAT = re.compile(r"(?:date|dt|time|timestamp|ts|created|updated|event_time)", re.I)

def human_int(n):
    return n # f"{n:,}".replace(",", " ")

def human_bytes(n):
    units = ["B","KB","MB","GB","TB"]
    i = 0
    f = float(n)
    while f >= 1024 and i < len(units)-1:
        f /= 1024.0; i += 1
    v = f if f >= 10 or i == 0 else round(f, 1)
    return f"{v} {units[i]}"

def sniff_delimiter(path, sample_size=200_000):
    with open(path, "rb") as fb:
        raw = fb.read(sample_size)
    try:
        dialect = csv.Sniffer().sniff(raw.decode("utf-8", errors="ignore"))
        return dialect.delimiter
    except Exception:
        return ","  # дефолт

def count_lines(path):
    # Быстрое подсчёт строк по \n
    cnt = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            cnt += chunk.count(b"\n")
    return cnt

def try_parse_datetime(s: pd.Series):
    # Пытаемся привести к datetime с UTC
    dt = pd.to_datetime(s, errors="coerce", utc=True, infer_datetime_format=True)
    valid_ratio = 1.0 - dt.isna().mean()
    return dt, valid_ratio

def string_len_stats(s: pd.Series):
    lens = s.dropna().astype(str).str.len()
    if lens.empty:
        return {"avg_len": None, "p95_len": None, "max_len": None}
    return {
        "avg_len": float(lens.mean()),
        "p95_len": float(lens.quantile(0.95)),
        "max_len": int(lens.max()),
    }

def monotonic_non_decreasing_ratio(dt: pd.Series):
    if dt.dropna().empty:
        return None
    # считаем долю неубывающих шагов
    diffs = dt.dropna().diff().dropna()
    if diffs.empty:
        return None
    return float((diffs >= pd.Timedelta(0)).mean())

def top_values(s: pd.Series, n=5):
    vc = s.value_counts(dropna=True).head(n)
    out = []
    for v, c in vc.items():
        try:
            v = v.item() if hasattr(v, "item") else v
        except Exception:
            pass
        out.append({"value": None if pd.isna(v) else str(v)[:200], "count": int(c)})
    return out

def numeric_profile(s: pd.Series):
    s_num = pd.to_numeric(s, errors="coerce")
    if s_num.dropna().empty:
        return None
    return {
        "min": float(s_num.min()),
        "max": float(s_num.max()),
        "mean": float(s_num.mean()),
        "std": float(s_num.std(ddof=0)) if s_num.shape[0] > 1 else 0.0,
    }

def guess_dtype(name, s: pd.Series):
    # сначала доверимся pandas
    dt = str(s.dtype)
    if dt.startswith(("int", "float", "bool")):
        base = "numeric" if not dt.startswith("bool") else "bool"
    else:
        # пробуем датавремя
        parsed, frac = try_parse_datetime(s)
        if frac >= 0.85:
            base = "datetime"
        else:
            # пробуем числа в object
            s_num = pd.to_numeric(s, errors="coerce")
            if (1 - s_num.isna().mean()) >= 0.95:
                base = "numeric"
            else:
                base = "string"
    return base

def composite_nunique(df: pd.DataFrame, cols):
    key = df[cols[0]].astype("string")
    for c in cols[1:]:
        key = key + "|" + df[c].astype("string")
    return int(key.nunique(dropna=False))

def pick_candidate_time_column(col_summaries):
    # приоритет по имени, затем по доле монотоничности
    dt_cols = [c for c in col_summaries if c["dtype"] == "datetime"]
    if not dt_cols:
        return None
    named = [c for c in dt_cols if LIKELY_TIME_PAT.search(c["name"])]
    cand_pool = named if named else dt_cols
    cand_pool.sort(key=lambda x: (x.get("monotonic_ratio") or 0.0,
                                  x.get("non_null_ratio") or 0.0),
                   reverse=True)
    return cand_pool[0]


def analyze_source_data(path, sep, header, enc, nrows):

    if not os.path.exists(path):
        print(f"Файл не найден: {path}", file=sys.stderr); sys.exit(2)

    print(f"=== ФАЙЛ ===")
    size = os.path.getsize(path)
    print(f"Путь: {path}")
    print(f"Размер: {human_bytes(size)}")
    try:
        total_lines = count_lines(path)
        print(f"Строк (включая заголовок): {human_int(total_lines)}")
    except Exception as e:
        total_lines = None
        print(f"Не удалось посчитать строки быстро: {e}")

    print(f"\n=== ЧИТАЕМ SAMPLE: {human_int(nrows)} строк ===")
    df = pd.read_csv(path, nrows=nrows, sep=sep, header=header, encoding=enc, low_memory=False)
    if header is None:
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]

    n = len(df)
    print(f"Фактически прочитано строк: {human_int(n)}, колонок: {df.shape[1]}")

    # профили по колонкам
    col_summaries = []
    for col in df.columns:
        s = df[col]
        non_null_ratio = float(1.0 - s.isna().mean())
        nunique = int(s.nunique(dropna=False))
        dtype_guess = guess_dtype(col, s)

        summary = {
            "name": col,
            "dtype": dtype_guess,
            "sample_nunique": nunique,
            "sample_unique_ratio": float(nunique / max(1, n)),
            "non_null_ratio": non_null_ratio,
            "top_values": top_values(s, n=5),
        }

        if dtype_guess == "numeric":
            summary["numeric"] = numeric_profile(s)
        elif dtype_guess == "datetime":
            dt, frac = try_parse_datetime(s)
            summary["datetime"] = {
                "valid_ratio": frac,
                "min": (dt.min().isoformat() if frac > 0 and not pd.isna(dt.min()) else None),
                "max": (dt.max().isoformat() if frac > 0 and not pd.isna(dt.max()) else None),
            }
            summary["monotonic_ratio"] = monotonic_non_decreasing_ratio(dt)
        elif dtype_guess == "string":
            summary["string"] = string_len_stats(s)

        summary["looks_like_id"] = bool(LIKELY_ID_PAT.search(col))
        summary["looks_like_time"] = bool(LIKELY_TIME_PAT.search(col))
        # эвристика вложенных JSON
        if dtype_guess == "string":
            s_nonnull = s.dropna().astype(str)
            if not s_nonnull.empty:
                starts_json = (s_nonnull.str.startswith("{") | s_nonnull.str.startswith("[")).mean()
                summary["json_like_ratio"] = float(starts_json)
        col_summaries.append(summary)

    # кандидаты на PK: одиночные
    pk_single = [c["name"] for c in col_summaries
                 if c["sample_unique_ratio"] >= 0.999 and c["non_null_ratio"] >= 0.999]

    # кандидаты на PK: пары (только среди топ-10 по уникальности)
    col_summaries_sorted = sorted(col_summaries, key=lambda c: c["sample_unique_ratio"], reverse=True)[:10]
    pk_pairs = []
    try:
        for i in range(len(col_summaries_sorted)):
            for j in range(i+1, len(col_summaries_sorted)):
                a = col_summaries_sorted[i]["name"]
                b = col_summaries_sorted[j]["name"]
                nunq = composite_nunique(df[[a,b]], [a,b])
                if nunq >= n * 0.999:
                    pk_pairs.append({"columns": [a,b], "sample_unique_ratio": float(nunq / n)})
                if len(pk_pairs) >= 5: break
            if len(pk_pairs) >= 5: break
    except Exception:
        pass

    # кандидат на колонку партиционирования/сортировки по времени
    time_cand = pick_candidate_time_column(col_summaries)

    # признаки для выбора СУБД: размер строки и кардинальности
    approx_avg_row_size = size / max(1, (total_lines - 1)) if total_lines and total_lines > 1 else None
    high_card_cols = [c["name"] for c in col_summaries if c["sample_unique_ratio"] > 0.2]
    low_card_cols = [c["name"] for c in col_summaries if 0 < c["sample_unique_ratio"] <= 0.02]

    summary = {
        "file": {
            "path": os.path.abspath(path),
            "size_bytes": size,
            "size_human": human_bytes(size),
            "total_lines_including_header": total_lines,
            "delimiter": sep,
            "header": header is not None,
            "approx_avg_row_size_bytes": approx_avg_row_size
        },
        "sample": {
            "rows_read": n,
            "columns": list(df.columns)
        },
        "columns": col_summaries,
        "candidates": {
            "primary_key_single": pk_single,
            "primary_key_pairs": pk_pairs,
            "time_partition_candidate": time_cand["name"] if time_cand else None
        },
        "cardinality": {
            "high_cardinality_columns": high_card_cols[:10],
            "low_cardinality_columns": low_card_cols[:10]
        }
    }

    # печать краткого резюме
    print("\n=== РЕЗЮМЕ (коротко) ===")
    print(f"- Размер файла: {summary['file']['size_human']}")
    if total_lines:
        print(f"- Строк (оценка): {human_int(total_lines-1 if summary['file']['header'] else total_lines)}")
    print(f"- Кандидаты на PK (одиночные): {pk_single[:5]}")
    print(f"- Кандидаты на PK (пары): {[p['columns'] for p in pk_pairs]}")
    print(f"- Кандидат на колонку времени/партицию: {summary['candidates']['time_partition_candidate']}")
    print(f"- Высокая кардинальность: {summary['cardinality']['high_cardinality_columns']}")
    print(f"- Низкая кардинальность: {summary['cardinality']['low_cardinality_columns']}")

    # пишем JSON
    base = os.path.splitext(os.path.basename(path))[0]
    out_json = os.path.join(os.path.dirname(path), f"profile_{base}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"\nJSON-отчёт сохранён: {out_json}")

    return summary

# ---------- основная логика ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("csv_path", help="Путь к CSV")
    ap.add_argument("--rows", type=int, default=500_000, help="Сколько строк читать (sample)")
    ap.add_argument("--sep", default=None, help="Разделитель (по умолчанию автоопределение)")
    ap.add_argument("--encoding", default=None, help="Кодировка (по умолчанию utf-8)")
    ap.add_argument("--no_header", action="store_true", help="Если в файле нет заголовка")
    args = ap.parse_args()

    path = args.csv_path

    sep = args.sep or sniff_delimiter(path)
    print(f"Разделитель: {repr(sep)}")

    header = 0 if not args.no_header else None
    enc = args.encoding or "utf-8"

    # читаем sample
    nrows = args.rows


    summary = analyze_source_data(path, sep, header, enc, nrows)

if __name__ == "__main__":
    main()
