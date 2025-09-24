from __future__ import annotations
import re, json
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Any, Dict
from pathlib import Path
import yaml

_DEC_RE = re.compile(r'^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$', re.I)

class TypeRegistry:
    def __init__(self, path: str = "config/types.yaml"):
        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        self.canonical = cfg["canonical"]
        self.synonyms = {k.lower(): v for k, v in cfg.get("synonyms", {}).items()}

    def _canon(self, t: str) -> tuple[str, dict]:
        t_norm = t.strip().lower()
        # decimal(p,s) с параметрами
        m = _DEC_RE.match(t_norm)
        if m:
            p, s = m.groups()
            return "decimal(p,s)", {"p": int(p), "s": int(s)}
        # синонимы
        base = self.synonyms.get(t_norm, t_norm)
        if base == "decimal(p,s)":
            # если в synonyms пришло 'numeric' без (p,s) — дефолт
            return "decimal(p,s)", {"p": 18, "s": 6}
        return base, {}

    def engine_type(self, engine: str, t: str) -> str:
        base, params = self._canon(t)
        spec = self.canonical.get(base)
        if not spec:
            # незнакомое — пусть пройдёт как есть
            return t
        templ = spec["pg" if engine in {"pg","postgres"} else "ch"]
        if "{p}" in templ or "{s}" in templ:
            return templ.format(**params)
        return templ

    def py_kind(self, t: str) -> str:
        base, _ = self._canon(t)
        spec = self.canonical.get(base)
        return (spec or {}).get("py", "str")

    # Универсальный парсер строкового значения CSV в Python-тип
    def parse_value(self, t: str, raw: str) -> Any:
        if raw is None:
            return None
        raw = raw.strip()
        if raw == "" or raw.upper() == "NULL":
            return None
        base, params = self._canon(t)
        kind = self.py_kind(base)

        try:
            if base.startswith("decimal"):
                return Decimal(raw)
            if kind == "int":
                return int(raw)
            if kind == "float":
                return float(raw)
            if kind == "bool":
                return raw.lower() in {"1","t","true","y","yes"}
            if base in {"timestamp", "timestamp64(ms)"}:
                s = raw
                # числовой epoch?
                if re.fullmatch(r"-?\d+(\.\d+)?", s):
                    return datetime.fromtimestamp(float(s), tz=timezone.utc)
                # ISO-8601 -> добавить UTC-офсет если его нет
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                elif not re.search(r"[+-]\d\d:\d\d$", s):
                    s = s + "+00:00"
                s = s.replace("T", " ")
                return datetime.fromisoformat(s)  # tz-aware (UTC)
            if base == "date":
                return date.fromisoformat(raw)
            if base == "json":
                # возвращаем строку для CH 24.3, а для PG — парсить не обязательно
                return raw
            # string по умолчанию
            return raw
        except Exception:
            # если парсинг не удался — отдадим как строку
            return raw
