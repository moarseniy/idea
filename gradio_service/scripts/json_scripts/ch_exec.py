# ch_exec.py
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import requests
from typing import Optional, Dict

def _session(trust_env: bool = False, user: Optional[str] = None, password: Optional[str] = None) -> requests.Session:
    s = requests.Session()
    s.trust_env = trust_env  # по умолчанию НЕ использовать системные прокси
    if user is not None:
        s.auth = (user, password or "")
    s.headers.update({"Content-Type": "text/plain; charset=UTF-8"})
    return s

def ch_ping(http_url: str, *, user: Optional[str] = None, password: Optional[str] = None, trust_env: bool = False) -> None:
    s = _session(trust_env=trust_env, user=user, password=password)
    r = s.get(http_url.rstrip("/") + "/ping", timeout=10)
    r.raise_for_status()
    txt = r.text.strip()
    # ClickHouse может вернуть 'Ok', 'Ok.' и/или с переводом строки
    if not (txt == "Ok" or txt.startswith("Ok")):
        # допускаем любые хвосты, но если ответ не начинается с 'Ok' — считаем ошибкой
        raise RuntimeError(f"Unexpected CH ping response: {r.text!r}")


def ch_ensure_database(http_url: str, database: str, *, user: Optional[str] = None,
                       password: Optional[str] = None, settings: Optional[Dict[str, str]] = None,
                       trust_env: bool = False) -> None:
    s = _session(trust_env=trust_env, user=user, password=password)
    params = {"database": "default"}
    if settings:
        params.update({f"settings[{k}]": v for k, v in settings.items()})
    r = s.post(http_url, params=params, data=f"CREATE DATABASE IF NOT EXISTS `{database}`;".encode("utf-8"), timeout=30)
    r.raise_for_status()

def ch_exec_many(http_url: str, ddl: str, *, database: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None,
                 settings: Optional[Dict[str, str]] = None, trust_env: bool = False) -> None:
    """
    Выполняет несколько SQL-стейтментов в ClickHouse (HTTP).
    - Делит по ';' и шлёт каждый отдельным POST (SQL в body).
    - При необходимости создаёт БД.
    - По умолчанию НЕ использует системные HTTP(S)_PROXY (trust_env=False).
    """
    ch_ping(http_url, user=user, password=password, trust_env=trust_env)

    if database:
        ch_ensure_database(http_url, database, user=user, password=password, settings=settings, trust_env=trust_env)

    parts = [p.strip() for p in re.split(r";\s*(?:\n|$)", ddl) if p.strip()]
    s = _session(trust_env=trust_env, user=user, password=password)
    for sql in parts:
        params = {}
        if database:
            params["database"] = database
        if settings:
            params.update({f"settings[{k}]": v for k, v in settings.items()})
        r = s.post(http_url, params=params or None, data=(sql + ";").encode("utf-8"), timeout=120)
        r.raise_for_status()
