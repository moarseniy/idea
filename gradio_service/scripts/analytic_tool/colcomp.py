"""
colcomp.py — оценка коэффициента сжатия CSV при хранении в колоночном формате (Parquet),
c авто-определением разделителя, заголовка и (опционально) кодировки.

Зависимости:
    pip install pyarrow

Основные функции:
- estimate_parquet_ratio(...): считает размеры для набора кодеков, Parquet пишется в память.
- parquet_size_from_table(...): возвращает размер Parquet из pa.Table (без диска).
- quick_sample_bytes(...): оценивает объём первых N строк CSV.
"""

from __future__ import annotations
import os
import io
import csv as pycsv
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


# ---- Авто-детект формата CSV -------------------------------------------------

_CANDIDATE_DELIMS = [",", ";", "\t", "|", ":"]

def _try_detect_encoding(sample: bytes, candidates=("utf-8-sig", "utf-8", "cp1251", "latin-1")) -> str:
    """
    Очень лёгкая эвристика: пробуем несколько популярных кодировок.
    Возвращаем первую, которой удаётся декодировать без ошибок (или 'latin-1' в конце).
    """
    for enc in candidates:
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def _heuristic_delimiter(text: str, candidates=_CANDIDATE_DELIMS) -> Optional[str]:
    """
    Простая статистика по первым строкам: выбираем разделитель с наибольшей
    медианой количества вхождений среди непустых строк.
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    lines = lines[:200]  # достаточно
    if not lines:
        return None

    scores = {}
    for d in candidates:
        counts = [ln.count(d) for ln in lines]
        counts = [c for c in counts if c > 0]
        if not counts:
            continue
        counts.sort()
        median = counts[len(counts)//2]
        # штраф за высокую дисперсию
        spread = (counts[-1] - counts[0]) if len(counts) > 1 else 0
        scores[d] = (median, -spread)

    if not scores:
        return None
    # max по (median, -spread)
    return max(scores.items(), key=lambda kv: kv[1])[0]


def detect_csv_format(
    csv_path: str,
    *,
    encoding: Optional[str] = None,
    sample_bytes: int = 1 << 16,  # 64 KiB
    candidate_delimiters: Iterable[str] = _CANDIDATE_DELIMS,
) -> Tuple[str, bool, str]:
    """
    Возвращает (delimiter, has_header, encoding).

    - delimiter — один символ (напр. ',', ';', '\\t', '|', ':')
    - has_header — True, если первая строка выглядит как заголовок
    - encoding — выбранная (или заданная) кодировка
    """
    with open(csv_path, "rb") as f:
        sample = f.read(sample_bytes)

    enc = encoding or _try_detect_encoding(sample)
    text = sample.decode(enc, errors="replace")

    # 1) пробуем стандартный csv.Sniffer
    try:
        sniffer = pycsv.Sniffer()
        dialect = sniffer.sniff(text, delimiters="".join(candidate_delimiters))
        delimiter = dialect.delimiter
        has_header = sniffer.has_header(text)
        if delimiter not in candidate_delimiters:
            # иногда Sniffer отдаёт странное; валидируем и откатываемся
            delimiter = None
    except Exception:
        delimiter = None
        has_header = True  # безопасный дефолт

    # 2) если не получилось — эвристика
    if not delimiter:
        delimiter = _heuristic_delimiter(text, candidate_delimiters) or ","

    return delimiter, has_header, enc


# ---- Модель результата -------------------------------------------------------

@dataclass
class ParquetEstimate:
    codec: str
    parquet_bytes: int
    ratio_csv_over_parquet: float
    details: Dict[str, object]


# ---- Чтение CSV в Arrow ------------------------------------------------------

def _read_csv_to_table(
    csv_path: str,
    sample_rows: Optional[int] = None,
    delimiter: Optional[str] = None,            # None => авто-детект
    encoding: Optional[str] = None,             # None => авто-детект
    convert_options: Optional[pacsv.ConvertOptions] = None,
    parse_options: Optional[pacsv.ParseOptions] = None,
    read_options: Optional[pacsv.ReadOptions] = None,
) -> Tuple[pa.Table, bool]:
    """
    Возвращает (таблица, has_header_detected).
    """
    # авто-детект формата
    delim, has_header, enc = detect_csv_format(csv_path, encoding=encoding)
    if delimiter:
        delim = delimiter  # явное значение важнее

    if convert_options is None:
        convert_options = pacsv.ConvertOptions()  # авто-типизация
    if parse_options is None:
        parse_options = pacsv.ParseOptions(delimiter=delim, quote_char='"')
    else:
        # уважаем внешний parse_options, но если там delimiter не задан — ставим наш
        if getattr(parse_options, "delimiter", None) in (None, ""):
            parse_options = pacsv.ParseOptions(
                delimiter=delim,
                quote_char=parse_options.quote_char,
                double_quote=parse_options.double_quote,
                escape_char=parse_options.escape_char,
                newlines_in_values=parse_options.newlines_in_values,
                invalid_row_handler=parse_options.invalid_row_handler,
            )

    if read_options is None:
        # Если заголовка нет — просим Arrow сгенерировать имена столбцов
        read_options = pacsv.ReadOptions(
            use_threads=True,
            block_size=1 << 22,
            encoding=enc,
            autogenerate_column_names=not has_header,
        )
    else:
        # применим выбранную кодировку
        read_options = pacsv.ReadOptions(
            use_threads=read_options.use_threads,
            block_size=read_options.block_size,
            encoding=enc,
            skip_rows=read_options.skip_rows,
            autogenerate_column_names=(getattr(read_options, "autogenerate_column_names", False) or not has_header),
        )

    table = pacsv.read_csv(
        csv_path,
        convert_options=convert_options,
        parse_options=parse_options,
        read_options=read_options,
    )
    if sample_rows is not None and 0 < sample_rows < table.num_rows:
        table = table.slice(0, sample_rows)

    return table, has_header


# ---- Вычисление размера Parquet ---------------------------------------------

def parquet_size_from_table(
    table: pa.Table,
    codec: str = "zstd",
    compression_level: Optional[int] = None,
    row_group_size: Optional[int] = None,
    use_dictionary: bool | Iterable[str] = True,
    write_statistics: bool = True,
) -> int:
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression=codec,
        compression_level=compression_level,
        use_dictionary=use_dictionary,
        write_statistics=write_statistics,
        row_group_size=row_group_size,
    )
    buf = sink.getvalue()
    return int(getattr(buf, "nbytes", getattr(buf, "size", len(buf))))



def quick_sample_bytes(csv_path: str, data_rows: int, has_header: bool = True, chunk: int = 1 << 20) -> int:
    """
    Грубо меряет байтовый объём первых N строк (по `\n`), плюс заголовок (если есть).
    """
    target_newlines = data_rows + (1 if has_header else 0)
    seen = 0
    total_bytes = 0
    with open(csv_path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            total_bytes += len(b)
            seen += b.count(b"\n")
            if seen >= target_newlines:
                return total_bytes
    return total_bytes


# ---- Публичная функция оценки ------------------------------------------------

def estimate_parquet_ratio(
    csv_path: str,
    codecs: Iterable[str] = ("zstd", "snappy", "gzip", "brotli"),
    compression_level: Optional[int] = None,
    row_group_size: Optional[int] = None,
    use_dictionary: bool | Iterable[str] = True,
    sample_rows: Optional[int] = None,
    delimiter: Optional[str] = None,   # None => авто
    encoding: Optional[str] = None,    # None => авто
) -> Tuple[int, Dict[str, ParquetEstimate]]:
    """
    Оценивает коэффициент сжатия CSV -> Parquet для набора кодеков.
    Если указан sample_rows, кодирует первые N строк и экстраполирует.

    Возвращает:
        (csv_size_bytes, {codec: ParquetEstimate, ...})
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(csv_path)

    csv_size = os.path.getsize(csv_path)

    # Читаем CSV → Arrow; узнаем наличие заголовка
    table, has_header = _read_csv_to_table(
        csv_path,
        sample_rows=sample_rows,
        delimiter=delimiter,
        encoding=encoding,
    )

    # Экстраполяция по сэмплу (если задан)
    sample_csv_bytes = None
    scale = 1.0
    if sample_rows is not None and sample_rows > 0:
        sample_csv_bytes = quick_sample_bytes(csv_path, sample_rows, has_header=has_header)
        scale = csv_size / max(sample_csv_bytes, 1)

    results: Dict[str, ParquetEstimate] = {}
    for codec in codecs:
        pbytes_sample = parquet_size_from_table(
            table,
            codec=codec,
            compression_level=compression_level,
            row_group_size=row_group_size,
            use_dictionary=use_dictionary,
            write_statistics=True,
        )
        pbytes_total = int(round(pbytes_sample * scale))
        ratio = csv_size / max(pbytes_total, 1)

        results[codec] = ParquetEstimate(
            codec=codec,
            parquet_bytes=pbytes_total,
            ratio_csv_over_parquet=ratio,
            details={
                "sample_rows": sample_rows,
                "sample_parquet_bytes": pbytes_sample,
                "sample_csv_bytes": sample_csv_bytes,
                "row_group_size": row_group_size,
                "use_dictionary": use_dictionary,
                "compression_level": compression_level,
            },
        )

    return csv_size, results


# ---- CLI ---------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json

    ap = argparse.ArgumentParser(description="Оценка сжатия CSV в Parquet (в памяти) с авто-детектом разделителя.")
    ap.add_argument("csv", help="Путь к CSV файлу")
    ap.add_argument("--codecs", default="zstd,snappy,gzip,brotli", help="Список кодеков через запятую")
    ap.add_argument("--sample-rows", type=int, default=None, help="Взять только первые N строк для быстрой оценки")
    ap.add_argument("--row-group-size", type=int, default=None, help="Размер row group (строк)")
    ap.add_argument("--no-dict", action="store_true", help="Отключить словарное кодирование")
    ap.add_argument("--compression-level", type=int, default=None, help="Уровень сжатия (если поддерживается)")
    ap.add_argument("--delimiter", default=None, help="Явный разделитель (по умолчанию авто-детект). Примеры: ',', ';', '\\t'")
    ap.add_argument("--encoding", default=None, help="Явная кодировка (по умолчанию авто-детект популярных)")

    args = ap.parse_args()

    csv_size, res = estimate_parquet_ratio(
        args.csv,
        codecs=[c.strip() for c in args.codecs.split(",") if c.strip()],
        sample_rows=args.sample_rows,
        row_group_size=args.row_group_size,
        use_dictionary=(False if args.no_dict else True),
        compression_level=args.compression_level,
        delimiter=args.delimiter,
        encoding=args.encoding,
    )

    out = {
        "csv_bytes": csv_size,
        "results": {
            k: {
                "parquet_bytes": v.parquet_bytes,
                "ratio_csv_over_parquet": v.ratio_csv_over_parquet,
                "details": v.details,
            }
            for k, v in res.items()
        },
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))
