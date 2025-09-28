from __future__ import annotations

import csv
import io
from typing import Dict, Any, Iterable, Tuple


def head_csv(path: str, n: int) -> str:
    """
    Прочитать CSV-файл и вернуть текст с заголовком и первыми n строками данных.

    - path: путь к CSV-файлу.
    - n: количество строк данных (без учёта строки заголовков), которые нужно вернуть.

    Возвращает:
        Строка, содержащая:
          - первую строку с названиями колонок,
          - затем первые n строк данных,
        записанные снова в CSV-формате (с учётом разделителя/кавычек, определённых Sniffer).

    Примечания:
      * Если в файле меньше n строк данных — вернёт все имеющиеся.
      * Диалект CSV определяется автоматически (delimiter, quotechar и т.д.).
    """
    if n < 0:
        n = 0

    # Прочитаем небольшой пролог для сниффера (но всё равно потом вернёмся к началу)
    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(64_000)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample)
        except csv.Error:
            # По умолчанию — стандартный "excel" диалект (запятая как разделитель)
            dialect = csv.get_dialect("excel")

        # Определим, есть ли заголовок (если не определить — считаем, что есть)
        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = True

        reader = csv.reader(f, dialect)
        rows: Iterable[list[str]] = reader

        output = io.StringIO(newline="")
        writer = csv.writer(output, dialect)

        # Если есть заголовок — запишем его; иначе — если нет, то будем считать первую строку данными,
        # но всё равно вернём её как "первую строку" результата.
        header_written = False
        first_row: list[str] | None = None

        try:
            first_row = next(rows)
        except StopIteration:
            # Пустой файл
            return ""

        if has_header:
            writer.writerow(first_row)
            header_written = True
        else:
            # Нет заголовка — всё равно включаем "первую строку" как «первую» строку результата
            # (пользователь просил, чтобы «колонки тоже есть в первой строке», но если их нет,
            # просто возвращаем всё, что есть).
            writer.writerow(first_row)

        # Сколько строк данных ещё писать
        remaining = n
        if has_header:
            # мы уже записали только заголовок, данные ещё впереди
            pass
        else:
            # первую строку уже записали как «данные»
            remaining -= 1

        if remaining > 0:
            for i, row in enumerate(rows):
                writer.writerow(row)
                remaining -= 1
                if remaining == 0:
                    break

        return output.getvalue()


def format_cardinalities(stats: Dict[str, Any]) -> str:
    """
    Преобразовать JSON-подобный словарь со статистикой кардинальностей в форматированный текст.

    Ожидается структура:
        {
          "raws": <int>,  # необязательно, если нет — строка с количеством не выводится
          "column_cardinalities": { <column_name>: <int>, ... }
        }

    Возвращает строку вида:

        Количество строк во всем файле csv: <raws>

        Кардинальности:

        <col_A> - <count_A>
        <col_B> - <count_B>
        ...

    Столбцы сортируются по убыванию <count>. При равенстве — по имени столбца (лексикографически).
    Никаких имен/ключей не хардкодится.
    """
    lines = []

    raws = stats.get("raws", None)
    if isinstance(raws, int):
        lines.append(f"Количество строк во всем файле csv: {raws}")
        lines.append("")  # пустая строка

    lines.append("Кардинальности:")
    lines.append("")

    card = stats.get("column_cardinalities", {})
    if not isinstance(card, dict):
        # Если структура неожиданная — мягко обработаем
        card = {}

    # Сортировка: по убыванию значения, при равенстве — по имени ключа
    def sort_key(item: Tuple[str, Any]):
        k, v = item
        try:
            v_int = int(v)
        except (TypeError, ValueError):
            v_int = -1  # некорректные значения уедут в конец
        return (-v_int, k)

    for col, cnt in sorted(card.items(), key=sort_key):
        # Гарантируем, что значение окажется в виде числа/строки
        try:
            cnt_str = str(int(cnt))
        except (TypeError, ValueError):
            cnt_str = str(cnt)
        lines.append(f"{col} - {cnt_str}")

    return "\n".join(lines)
