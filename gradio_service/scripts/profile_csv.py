#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse, json, os, time, re
from datetime import datetime, timezone
import polars as pl

# ---------- utils ----------

TZ_RE_STR = r"(Z|[+\-]\d{2}:\d{2})$"

def fast_count_lines(path: str, chunk_size: int = 64 * 1024) -> int:
    n = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk_size)
            if not b: break
            n += b.count(b"\n")
    return n

def detect_tz_presence(sample_utf8: pl.Series) -> str | None:
    if sample_utf8.null_count() == sample_utf8.len():
        return None
    s = sample_utf8.drop_nulls().head(5000).cast(pl.Utf8, strict=False)
    if s.len() == 0:
        return None
    hits = s.str.contains(TZ_RE_STR).sum()
    if hits == 0:
        return "none"
    if hits == s.len():
        return "offset_in_values"
    return "mixed"

def quantiles(series: pl.Series, probs=(0.5, 0.95)):
    if series.len() == 0:
        return {}
    try:
        q = series.quantile(list(probs), interpolation="nearest")
        return {f"p{int(p*100)}": (float(q[i]) if q[i] is not None else None) for i, p in enumerate(probs)}
    except Exception:
        return {}

def to_jsonable(x):
    if isinstance(x, (datetime,)):
        return x.isoformat()
    return x

# ---------- type inference helpers ----------

BOOL_TRUE = {"1","true","t","y","yes"}
BOOL_FALSE = {"0","false","f","n","no"}

def score_bool(col: pl.Series) -> float:
    s = col.drop_nulls().cast(pl.Utf8, strict=False).str.to_lowercase()
    if s.len() == 0:
        return 0.0
    m = s.is_in(list(BOOL_TRUE | BOOL_FALSE)).sum()
    return m / s.len()

def score_int(col: pl.Series) -> float:
    try:
        casted = col.cast(pl.Int64, strict=False)
        return (casted.len() - casted.null_count()) / casted.len()
    except Exception:
        return 0.0

def score_float(col: pl.Series) -> float:
    try:
        casted = col.cast(pl.Float64, strict=False)
        return (casted.len() - casted.null_count()) / casted.len()
    except Exception:
        return 0.0

def score_date(col: pl.Series) -> tuple[float, str]:
    try:
        parsed = col.str.strptime(pl.Date, strict=False, exact=False)
        frac = (parsed.len() - parsed.null_count()) / parsed.len()
        min_v = parsed.min()
        engine = None
        if getattr(min_v, "year", None) is not None:
            engine = "Date32" if min_v.year < 1970 else "Date"
        return frac, (engine or "Date")
    except Exception:
        return 0.0, "Date"

def score_dt_ms(col: pl.Series) -> float:
    try:
        parsed = col.str.strptime(pl.Datetime(time_unit="ms"), strict=False, exact=False)
        return (parsed.len() - parsed.null_count()) / parsed.len()
    except Exception:
        return 0.0

# ---------- compact view ----------

def build_compact(columns_out: dict, amb_thr: float):
    columns_compact = {}
    ambiguous_cols = 0
    mostly_null_cols = 0
    low_card_cols = 0
    engine_override_cols = 0

    for name, prof in columns_out.items():
        cand = prof.get("candidates", [])
        ambig = [c for c in cand if c["score"] >= amb_thr]
        if len(ambig) >= 2:
            ambiguous_cols += 1
        if prof.get("null_frac", 0.0) > 0.9:
            mostly_null_cols += 1
        d = prof.get("distinct")
        if isinstance(d, int) and d <= 20:
            low_card_cols += 1
        if prof.get("engine_overrides"):
            engine_override_cols += 1

        columns_compact[name] = {
            "chosen_type": prof.get("chosen_type"),
            "succ": prof.get("parse_success_frac"),
            "null": prof.get("null_frac"),
            "distinct": d,
        }
        if len(ambig) >= 2:
            columns_compact[name]["ambig"] = ambig

    profile_summary = {
        "num_columns": len(columns_out),
        "ambiguous_columns": ambiguous_cols,
        "mostly_null_cols": mostly_null_cols,
        "low_card_cols": low_card_cols,
        "engine_overrides_present": engine_override_cols
    }
    return columns_compact, profile_summary

# ---------- profiler ----------

def profile_csv(
    path: str,
    delimiter: str | None,
    quotechar: str = '"',
    encoding: str = "utf-8",
    header: bool = True,
    sample_rows: int = 200_000,
    seed: int = 42,
    debug: str = "off",         # "off" | "summary" | "full"
    debug_out: str | None = None,
    amb_threshold: float = 0.15,
    compact_only: bool = False
):
    t_start = time.perf_counter()
    dbg = {"stages": {}, "file": {}, "columns": {}, "keys": {}, "routing": {}}

    def log(stage, msg):
        if debug != "off":
            now = time.perf_counter() - t_start
            print(f"[A2 {now:8.3f}s][{stage}] {msg}")

    file_size = os.path.getsize(path)
    detected = {
        "delimiter": delimiter or ",",
        "quotechar": quotechar,
        "encoding": encoding,
        "header": bool(header)
    }
    try:
        total_rows = fast_count_lines(path) - (1 if header else 0)
        if total_rows < 0: total_rows = None
        detected["row_count"] = total_rows
    except Exception:
        pass

    dbg["file"] = {"path": path, "size_bytes": file_size, "detected": detected}
    log("detect", f"format=csv delimiter='{detected['delimiter']}' quote='{detected['quotechar']}' "
                  f"encoding={encoding} header={header} rows≈{detected.get('row_count')}")

    # read sample
    t0 = time.perf_counter()
    read_opts = dict(
        separator=detected["delimiter"],
        quote_char=detected["quotechar"],
        has_header=detected["header"],
        null_values=["", "NULL", "N/A", " "],
        infer_schema_length=2000,
        ignore_errors=True
    )
    lf = pl.scan_csv(path, **read_opts)
    if sample_rows:
        lf = lf.limit(sample_rows)
    df = lf.collect()
    dbg["stages"]["read_ms"] = int((time.perf_counter() - t0) * 1000)
    log("read", f"sample_rows={df.height} columns={len(df.columns)}")

    n = df.height
    columns_out = {}

    # dtypes
    dtype_map = {c: str(dt) for c, dt in zip(df.columns, df.dtypes)}
    if debug != "off":
        log("schema", f"dtypes={dtype_map}")

    # per-column profile
    t1 = time.perf_counter()
    for col in df.columns:
        s = df[col]
        sample_size = n
        nulls = s.null_count()
        null_frac = (nulls / sample_size) if sample_size else 0.0

        try:
            distinct = int(s.n_unique())
        except Exception:
            distinct = None

        try:
            vc = s.drop_nulls().value_counts()
            cnt_col = "counts" if "counts" in vc.columns else ("count" if "count" in vc.columns else vc.columns[-1])
            val_col = "values" if "values" in vc.columns else vc.columns[0]
            vc = vc.sort(cnt_col, descending=True).head(10)
            top_k = [[to_jsonable(vc[val_col][i]), int(vc[cnt_col][i])] for i in range(len(vc))]
        except Exception:
            top_k = []

        if s.dtype == pl.Utf8:
            lens = s.drop_nulls().str.len_chars()
        else:
            lens = s.drop_nulls().cast(pl.Utf8, strict=False).str.len_chars()
        length = {
            "min": int(lens.min()) if lens.len() else None,
            "max": int(lens.max()) if lens.len() else None
        } | quantiles(lens, (0.5, 0.95))

        numeric = {"min": None, "max": None, "mean": None, "stddev": None}
        try:
            s_float = s.cast(pl.Float64, strict=False)
            if s_float.len() - s_float.null_count() > 0:
                numeric["min"] = float(s_float.min())
                numeric["max"] = float(s_float.max())
                numeric["mean"] = float(s_float.mean())
                numeric["stddev"] = float(s_float.std())
        except Exception:
            pass

        dt_stats = {"min": None, "max": None, "timezone_presence": None}
        if s.dtype == pl.Utf8:
            tzp = detect_tz_presence(s)
            dt_stats["timezone_presence"] = tzp
        else:
            dt_stats["timezone_presence"] = None

        cand = []
        cand.append(("bool", score_bool(s)))
        cand.append(("int64", score_int(s)))
        cand.append(("float64", score_float(s)))
        frac_date, date_engine = score_date(s)
        cand.append(("date", frac_date))
        cand.append(("timestamp64(ms)", score_dt_ms(s)))
        s_head = s.drop_nulls().cast(pl.Utf8, strict=False).head(1000)
        json_frac = 0.0
        if s_head.len():
            starts = s_head.str.strip_chars().str.starts_with("{") | s_head.str.strip_chars().str.starts_with("[")
            json_frac = starts.sum() / s_head.len()
        cand.append(("json", json_frac * 0.9))
        cand.append(("string", 0.5 + 0.5 * (1.0 if s.dtype == pl.Utf8 else 0.0)))

        total = sum(x for _, x in cand) or 1.0
        cand_norm = [{"type": t, "score": round(x / total, 4)} for t, x in cand]
        chosen = max(cand_norm, key=lambda z: z["score"])["type"]

        parse_success_frac = None
        parse_issues = []
        engine_overrides = {}
        try:
            if chosen == "bool":
                parse_success_frac = score_bool(s)
            elif chosen == "int64":
                parse_success_frac = score_int(s)
            elif chosen == "float64":
                parse_success_frac = score_float(s)
            elif chosen == "date":
                parse_success_frac = frac_date
                if date_engine == "Date32":
                    engine_overrides["ch"] = "Date32"
            elif chosen == "timestamp64(ms)":
                parse_success_frac = score_dt_ms(s)
            elif chosen == "json":
                parse_success_frac = json_frac
            else:
                parse_success_frac = 1.0 if s.dtype == pl.Utf8 else 0.8
        except Exception:
            parse_success_frac = 0.0
            parse_issues.append("parse_error")

        anomalies = []
        if distinct == 1 and null_frac < 0.99:
            anomalies.append("constant")
        if null_frac > 0.9:
            anomalies.append("mostly_null")
        if s.dtype == pl.Utf8:
            trimmed_diff = (s.drop_nulls().str.strip_chars() != s.drop_nulls()).sum()
            if trimmed_diff > 0:
                anomalies.append("leading_trailing_ws")

        pii = {"kinds": [], "risk_score": 0.0}
        if s.dtype == pl.Utf8:
            digits = s.drop_nulls().cast(pl.Utf8, strict=False).str.replace_all(r"\D", "")
            lens = digits.str.len_chars()
            ph_hits = ((lens >= 11) & (lens <= 15)).sum()
            ph_rate = ph_hits / max(1, s.len() - s.null_count())
            if ph_rate > 0.2:
                pii["kinds"].append("phone")
                pii["risk_score"] += 0.5
            token_hits = s.drop_nulls().str.contains(r"^[\p{L}\s\-']+$").sum()
            token_rate = token_hits / max(1, s.len() - s.null_count())
            if token_rate > 0.2:
                pii["kinds"].append("name_like")
                pii["risk_score"] += 0.3

        uniqueness_est = None if distinct is None or sample_size == 0 else distinct / sample_size
        key_signals = {
            "uniqueness_est": uniqueness_est,
            "is_monotonic": bool(s.is_sorted()) if s.null_count() == 0 else False,
            "candidate_key_alone": bool(uniqueness_est and uniqueness_est > 0.98)
        }

        if s.dtype == pl.Utf8:
            try:
                dt_ms = s.str.strptime(pl.Datetime(time_unit="ms"), strict=False, exact=False)
                if dt_ms.len() - dt_ms.null_count() > 0:
                    dt_stats["min"] = dt_ms.min().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                    dt_stats["max"] = dt_ms.max().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except Exception:
                pass

        columns_out[col] = {
            "sample_size": sample_size,
            "null_frac": round(float(null_frac), 6),
            "distinct": distinct,
            "top_k": top_k,
            "length": {k: (int(v) if isinstance(v, float) and k in ("min","max") else v) for k, v in length.items()},
            "numeric": numeric,
            "datetime": dt_stats,
            "candidates": cand_norm,
            "chosen_type": chosen,
            "parse_success_frac": round(float(parse_success_frac or 0.0), 6),
            "parse_issues": parse_issues,
            "engine_overrides": engine_overrides,
            "anomalies": anomalies,
            "suggested_preprocess": {},
            "pii": pii,
            "key_signals": key_signals
        }

        if debug in ("summary","full"):
            msg = (f"{col}: chosen={chosen} succ={columns_out[col]['parse_success_frac']:.3f} "
                   f"null={columns_out[col]['null_frac']:.3f} distinct={distinct} "
                   f"tz={dt_stats['timezone_presence']} anomalies={anomalies} "
                   f"engine={engine_overrides or '-'}")
            log("col", msg)
        if debug == "full":
            dbg["columns"][col] = {
                "candidates": cand_norm,
                "top_k": top_k,
                "length": columns_out[col]["length"],
                "numeric": numeric,
                "datetime": dt_stats,
                "pii": pii,
                "engine_overrides": engine_overrides
            }

        # simple preprocess suggestions
        sugg = {}
        if s.dtype == pl.Utf8:
            sugg["trim"] = True
            sugg["null_if"] = ["", " ", "NULL", "N/A"]
            if "phone" in pii["kinds"]:
                sugg["regex_replace"] = [{"pattern": "\\D+", "repl": ""}]
        columns_out[col]["suggested_preprocess"] = sugg

    dbg["stages"]["columns_ms"] = int((time.perf_counter() - t1) * 1000)

    # candidate keys
    t2 = time.perf_counter()
    candidate_keys = []
    high_uni_cols = [c for c, prof in columns_out.items() if (prof["key_signals"]["uniqueness_est"] or 0) > 0.8]
    for c in high_uni_cols:
        u = columns_out[c]["key_signals"]["uniqueness_est"]
        candidate_keys.append({"columns": [c], "uniqueness_est": round(float(u), 6), "conflicts": int(n - (columns_out[c]["distinct"] or 0))})

    max_pairs = 50
    tried = 0
    for i in range(len(high_uni_cols)):
        for j in range(i+1, len(high_uni_cols)):
            if tried >= max_pairs: break
            c1, c2 = high_uni_cols[i], high_uni_cols[j]
            try:
                d = df.select(pl.struct([pl.col(c1), pl.col(c2)]).alias("k")).select(pl.col("k").n_unique()).item()
                u = d / n if n else 0.0
                candidate_keys.append({"columns": [c1, c2], "uniqueness_est": round(float(u), 6), "conflicts": int(n - d)})
                tried += 1
            except Exception:
                continue
    candidate_keys_sorted = sorted(candidate_keys, key=lambda k: k["uniqueness_est"], reverse=True)[:15]
    dbg["keys"]["top"] = candidate_keys_sorted
    dbg["stages"]["keys_ms"] = int((time.perf_counter() - t2) * 1000)
    if debug in ("summary","full"):
        log("keys", f"top={candidate_keys_sorted[:5]}")

    # routing signals
    t3 = time.perf_counter()
    routing_signals = {}
    for c, prof in columns_out.items():
        d = prof["distinct"]
        if d is not None and d <= 20:
            try:
                vc = df[c].drop_nulls().value_counts()
                cnt_col = "counts" if "counts" in vc.columns else ("count" if "count" in vc.columns else vc.columns[-1])
                val_col = "values" if "values" in vc.columns else vc.columns[0]
                vc = vc.sort(cnt_col, descending=True).head(10)
                topk = [[to_jsonable(vc[val_col][i]), int(vc[cnt_col][i])] for i in range(len(vc))]
            except Exception:
                topk = []
            routing_signals[c] = {"present": True, "distinct": int(d), "top_k": topk}
    dbg["routing"]["candidates"] = routing_signals
    dbg["stages"]["routing_ms"] = int((time.perf_counter() - t3) * 1000)
    if debug in ("summary","full"):
        log("route", f"low-card columns={list(routing_signals.keys())[:10]}")

    # engine hints
    ch_needs_date32 = [c for c, prof in columns_out.items()
                       if prof["chosen_type"] == "date" and prof.get("engine_overrides", {}).get("ch") == "Date32"]
    lowcard = [c for c, prof in columns_out.items()
               if (prof["distinct"] or 999999) <= 256 and (columns_out[c]["null_frac"] < 0.95)]
    engine_hints = {"pg": {}, "ch": {"needs_date32": ch_needs_date32, "low_cardinality": lowcard}}

    # compact + summary
    columns_compact, profile_summary = build_compact(columns_out, amb_thr=amb_threshold)

    # quality flags
    quality_flags = []
    if len(ch_needs_date32) > 0:
        quality_flags.append("ch:date32_recommended")
    if any(prof["anomalies"] for prof in columns_out.values()):
        quality_flags.append("anomalies_detected")
    if profile_summary["ambiguous_columns"] > 0:
        quality_flags.append("ambiguous_types_present")

    # assemble output
    out = {
        "input_id": os.path.basename(path),
        "profile_time": datetime.now(timezone.utc).isoformat(),
        "tool_version": "a2-profiler/0.3",
        "file_meta": {
            "path": path,
            "format": "csv",
            "size_bytes": file_size,
            "detected": detected
        },
        "sample": {
            "sample_rows": n,
            "sample_fraction": None,
            "seed": None
        },
        "candidate_keys": candidate_keys_sorted,
        "routing_signals": routing_signals,
        "engine_hints": engine_hints,
        "quality_flags": quality_flags,
        "columns_compact": columns_compact,
        "profile_summary": profile_summary
    }

    if not compact_only:
        out["columns"] = columns_out
        out["bad_rows_count"] = 0
        out["bad_rows_sample"] = []

    dbg["stages"]["total_ms"] = int((time.perf_counter() - t_start) * 1000)
    if debug_out:
        try:
            with open(debug_out, "w", encoding="utf-8") as f:
                json.dump(dbg, f, ensure_ascii=False, indent=2)
            if debug != "off":
                log("debug", f"wrote debug to {debug_out}")
        except Exception as e:
            if debug != "off":
                log("debug", f"failed to write debug: {e}")

    return out

def main():
    ap = argparse.ArgumentParser(description="A2 CSV profiler (fast, Polars) with debug.")
    ap.add_argument("--amb-threshold", type=float, default=0.15, help="Threshold to consider a candidate type as significant")
    ap.add_argument("--compact-only", action="store_true", help="Output only compact view (omit heavy per-column details)")
    ap.add_argument("--input", required=True, help="Path to CSV")
    ap.add_argument("--delimiter", default=None, help="CSV delimiter (default: auto or ,)")
    ap.add_argument("--quotechar", default='"', help="CSV quote char")
    ap.add_argument("--encoding", default="utf-8", help="CSV encoding (Polars supports utf-8/utf-8-lossy natively)")
    ap.add_argument("--no-header", action="store_true", help="CSV has no header")
    ap.add_argument("--sample-rows", type=int, default=200_000, help="Rows to sample from top")
    ap.add_argument("--output", required=True, help="Path to write column_profile.json")
    ap.add_argument("--debug", choices=["off","summary","full"], default="off", help="Print intermediate steps")
    ap.add_argument("--debug-out", help="Path to write debug JSON (stages, per-column candidates, keys, routing)")
    args = ap.parse_args()

    header = not args.no_header
    out = profile_csv(
        path=args.input,
        delimiter=args.delimiter,
        quotechar=args.quotechar,
        encoding=args.encoding,
        header=header,
        sample_rows=args.sample_rows,
        debug=args.debug,
        debug_out=args.debug_out,
        amb_threshold=args.amb_threshold,
        compact_only=args.compact_only
    )
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
