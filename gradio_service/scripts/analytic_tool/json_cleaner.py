# json_cleaner.py
from __future__ import annotations

import json
import re
from typing import Any, Tuple, Optional
from urllib.request import urlopen, Request

__all__ = [
    "clean_json_string",
    "parse_json",
    "parse_json_from_url_or_obj",
    "JsonCleanerError",
]

class JsonCleanerError(Exception):
    """Ошибки очистки/разбора JSON."""


def _fetch_text(url: str, timeout: int = 15, user_agent: Optional[str] = None) -> str:
    """Загружает текст по URL с базовой защитой от плохих заголовков/кодировок."""
    ua = user_agent or "json-cleaner/1.0 (+https://example)"
    req = Request(url, headers={"User-Agent": ua, "Accept": "application/json, */*;q=0.8"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    # Пытаемся корректно декодировать
    try:
        # utf-8-sig сразу уберёт возможный BOM
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        # запасной вариант
        return raw.decode("utf-8", errors="replace")


def _strip_to_balanced_json(s: str) -> str:
    """
    Обрезает всё до первой '{' (если её нет — до первой '[') и после соответствующей
    закрывающей скобки, учитывая строки и экранирование.
    """
    if not s:
        raise JsonCleanerError("Пустая строка — нечего разбирать.")

    # Ищем предпочтительно объект {…}, иначе массив […]
    start_obj = s.find("{")
    start_arr = s.find("[")

    if start_obj == -1 and start_arr == -1:
        raise JsonCleanerError("Не найдены открывающие скобки '{' или '['.")

    if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
        start = start_obj
        open_ch, close_ch = "{", "}"
    else:
        start = start_arr
        open_ch, close_ch = "[", "]"

    # Проходим и считаем баланс скобок, игнорируя содержимое строк
    depth = 0
    in_string = False
    string_quote = ""
    escape = False
    end = None

    for i in range(start, len(s)):
        ch = s[i]
        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == string_quote:
                in_string = False
        else:
            if ch in ("'", '"'):
                in_string = True
                string_quote = ch
            elif ch == open_ch:
                depth += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    end = i
                    break

    if end is None:
        # Если парность сломана, попробуем обрезать по последней подходящей закрывающей
        end = s.rfind(close_ch)
        if end == -1 or end < start:
            raise JsonCleanerError("Не удалось найти закрывающую скобку для JSON.")

    return s[start : end + 1]


def _remove_bom_and_controls(s: str) -> str:
    # Убираем BOM в начале и запрещённые управляющие, кроме \t \r \n
    s = s.lstrip("\ufeff")
    return "".join(ch for ch in s if (ch >= " " or ch in "\t\r\n"))


def _remove_comments(s: str) -> str:
    """Удаляет // и /* ... */ вне строк."""
    out = []
    i, n = 0, len(s)
    in_str = False
    quote = ""
    esc = False
    while i < n:
        ch = s[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            i += 1
        else:
            if ch in ('"', "'"):
                in_str = True
                quote = ch
                out.append(ch)
                i += 1
            elif ch == "/" and i + 1 < n and s[i + 1] == "/":
                # однострочный
                i += 2
                while i < n and s[i] not in "\r\n":
                    i += 1
            elif ch == "/" and i + 1 < n and s[i + 1] == "*":
                # многострочный
                i += 2
                while i + 1 < n and not (s[i] == "*" and s[i + 1] == "/"):
                    i += 1
                i = min(i + 2, n)
            else:
                out.append(ch)
                i += 1
    return "".join(out)


def _remove_trailing_commas(s: str) -> str:
    """Удаляет запятые, за которыми идут только пробелы и затем '}' или ']' (вне строк)."""
    out = []
    i, n = 0, len(s)
    in_str = False
    quote = ""
    esc = False

    while i < n:
        ch = s[i]
        if in_str:
            out.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            i += 1
        else:
            if ch in ('"', "'"):
                in_str = True
                quote = ch
                out.append(ch)
                i += 1
            elif ch == ",":
                j = i + 1
                # пропускаем пробелы/переводы
                while j < n and s[j] in " \t\r\n":
                    j += 1
                if j < n and s[j] in "}]":
                    # пропускаем запятую (не добавляем)
                    i += 1
                    continue
                else:
                    out.append(ch)
                    i += 1
            else:
                out.append(ch)
                i += 1
    return "".join(out)


def clean_json_string(text: str) -> str:
    """
    Полная очистка строки с JSON:
      - обрезка мусора снаружи до/после JSON;
      - удаление BOM/управляющих;
      - удаление комментариев;
      - удаление «висячих» запятых.
    Возвращает ОЧИЩЕННУЮ строку JSON.
    """
    if not isinstance(text, str):
        raise JsonCleanerError("Ожидалась строка для очистки.")

    s = _remove_bom_and_controls(text)
    s = _strip_to_balanced_json(s)
    s = _remove_comments(s)
    s = _remove_trailing_commas(s)
    # Нормализуем переносы строк
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()
    return s


def _try_json5_parse(cleaned: str) -> Any:
    """Пробует распарсить через json5, если установлен. Иначе бросает ImportError."""
    import importlib
    json5 = importlib.import_module("json5")  # может вызвать ImportError
    return json5.loads(cleaned)


def parse_json(text: str) -> Any:
    """
    Пытается распарсить «грязный» JSON-текст.
    Сначала чистит, затем json.loads(strict=False).
    Если не получилось — пробует json5 (если установлен).
    """
    cleaned = clean_json_string(text)
    try:
        return json.loads(cleaned, strict=False)
    except json.JSONDecodeError:
        try:
            return _try_json5_parse(cleaned)
        except Exception as e:
            # Покажем небольшой фрагмент для диагностики
            snippet = cleaned[:200].replace("\n", " ")
            raise JsonCleanerError(f"Не удалось распарсить JSON. Фрагмент: {snippet!r}. Детали: {e}") from e


def parse_json_from_url_or_obj(src: Any, *, timeout: int = 15) -> Tuple[Any, Optional[str]]:
    """
    Универсальная точка входа.
    Принимает:
      - URL (str, начинающийся с http:// или https://) — загрузит и распарсит;
      - строку с «грязным» JSON — очистит и распарсит;
      - уже распарсенный объект (dict/list) — вернёт как есть.

    Возвращает кортеж: (python-объект, исходная_строка_если_была_загружена_или_None)
    """
    # Уже объект — просто вернуть
    if isinstance(src, (dict, list)):
        return src, None

    # Байт-строка
    if isinstance(src, (bytes, bytearray)):
        try:
            src = src.decode("utf-8-sig")
        except UnicodeDecodeError:
            src = src.decode("utf-8", errors="replace")

    if isinstance(src, str):
        stripped = src.lstrip()
        is_url = stripped.startswith("http://") or stripped.startswith("https://")
        if is_url:
            raw = _fetch_text(stripped, timeout=timeout)
            return parse_json(raw), raw
        else:
            return parse_json(src), None

    raise JsonCleanerError("Источник должен быть URL, строкой, байтами или уже JSON-объектом.")


# Пример использования
if __name__ == "__main__":
    # Грязная строка
    dirty = """
        !!!noise!!!
        // комментарий
        { "a": 1, "b": [2,3,], /* tail comma */ "c": {"d": 4,}, } trailing
    """
    data, _ = parse_json_from_url_or_obj(dirty)
    print(data)  # {'a': 1, 'b': [2, 3], 'c': {'d': 4}}
