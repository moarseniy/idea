# pg_reset.py
from typing import Any, Dict
from ddl_postgres import emit_ddl_pg
from load_postgres import copy_into_pg

def drop_pg_tables_for_profile(conn, profile: Dict[str, Any], schema: str = "public") -> None:
    """Дроп таблиц профиля: дети -> родитель. ВАЖНО: IF EXISTS (без NOT)."""
    names = [e["name"] for e in sorted(profile["entities"], key=lambda x: x.get("depth", 0), reverse=True)]
    with conn.cursor() as cur:
        cur.execute("SET client_min_messages = WARNING")
        for t in names:
            cur.execute(f'DROP TABLE IF EXISTS "{schema}"."{t}" CASCADE;')
    conn.commit()

def truncate_pg_tables_for_profile(conn, profile: Dict[str, Any], schema: str = "public") -> None:
    """Быстро очистить таблицы без дропа (если хочешь сохранить схему)."""
    names = [e["name"] for e in sorted(profile["entities"], key=lambda x: x.get("depth", 0), reverse=True)]
    if not names:
        return
    with conn.cursor() as cur:
        cur.execute("SET client_min_messages = WARNING")
        tbls = ", ".join(f'"{schema}"."{t}"' for t in names)
        cur.execute(f"TRUNCATE TABLE {tbls} CASCADE;")  # RESTART IDENTITY не обязателен — у нас нет sequences
    conn.commit()

def recreate_and_load_pg(conn, profile: Dict[str, Any], records,
                         schema: str = "public",
                         types_yaml_path: str = "config/types.yaml",
                         batch_size: int = 50_000) -> None:
    """Создать DDL (в нужной схеме через search_path) и залить COPY."""
    ddl = emit_ddl_pg(profile, types_yaml_path=types_yaml_path)
    with conn.cursor() as cur:
        cur.execute("SET client_min_messages = WARNING")
        cur.execute(f'SET search_path TO "{schema}"')
        cur.execute(ddl)
    conn.commit()
    copy_into_pg(conn, profile, records, schema=schema, batch_size=batch_size)
