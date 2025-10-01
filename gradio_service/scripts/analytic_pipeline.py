import json

from scripts.analytic_tool.json_cleaner import parse_json_from_url_or_obj

from scripts.analytic_tool.utils import head_csv, format_cardinalities
from scripts.analytic_tool.csv_profile_pandas import compute_csv_profile, to_json
from scripts.analytic_tool.entity_rebalancer import reorganize_entities
from scripts.analytic_tool.grain_module import analyze_and_format, format_grain_report
from scripts.analytic_tool.colcomp import estimate_parquet_ratio

def run_compute_profile(path):
    card_json, types_json = compute_csv_profile(
        path,
        types_yaml_path="scripts/analytic_tool/configs/types.yaml",  # ваш файл с каноническими типами
        chunksize=200_000,                     # размер чанка
        type_sample_rows=50_000,               # сколько строк брать для типизации
        lowcard_ratio=0.10, lowcard_max=5000,  # пороги для lowcard_string
        verbose=True
    )

    print("CARD:\n", json.dumps(card_json, ensure_ascii=False, indent=2))
    print("TYPES:\n", json.dumps(types_json, ensure_ascii=False, indent=2))

    return card_json, types_json


def run_estimate_parquet_ratio(path):
    csv_size, results = estimate_parquet_ratio(
        path,
        codecs=("zstd", "gzip"),
        sample_rows=200_000,        # либо None для полного прохода
        row_group_size=100_000,
        use_dictionary=True,
        compression_level=8,
    )

    best_codec, best_est = max(results.items(), key=lambda kv: kv[1].ratio_csv_over_parquet)

    best_ratio = best_est.ratio_csv_over_parquet
    best_size  = best_est.parquet_bytes

    report = (
        "Отчёт о сжатии в колоночном формате:\n"
        f"Удалось получить коэффициент сжатия {best_ratio:.0f}x "
        f"(кодек: {best_codec}, размер Parquet ≈ {best_size:,} bytes)"
    )
    return report


def run_build_analytic_prompt(path):
    preview = head_csv(path, 10)  

    card_json, types_json = run_compute_profile(path)

    parquet_report = run_estimate_parquet_ratio(path)
    
    cardinality_text = format_cardinalities(card_json)

    return preview, cardinality_text, card_json, types_json, parquet_report

def clean_json(json_str):
    return parse_json_from_url_or_obj(json_str)[0]

def run_build_final_prompt(json_answer, card_json):

    model, raw = parse_json_from_url_or_obj(json_answer)
    result = reorganize_entities(model, card_json, total_rows=None, threshold_ratio=0.20)

    txt_report = format_grain_report(result, list_source="columns", include_entity_name=False) 

    return txt_report