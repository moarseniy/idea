#!/usr/bin/env python3
import os, sys, json, glob

import psycopg2
from psycopg2 import sql

from clickhouse_driver import Client
import clickhouse_connect

import xmltodict
import pandas as pd
from typing import Iterator, Dict, Any, List
from urllib.parse import urlparse, urlunparse

### POSTGRES ###
# JSON #
from scripts.json_scripts.pg_introspect import (
    table_exists, list_tables, describe_columns,
    primary_key, foreign_keys, row_count,
    validate_schema_against_profile
)

from scripts.json_scripts.pg_reset import drop_pg_tables_for_profile, recreate_and_load_pg
from scripts.json_scripts.ddl_postgres import emit_ddl_pg
from scripts.json_scripts.load_postgres import copy_into_pg

# CSV #
from scripts.analytic_tool.csv_copy_loaders_psycopg2 import csv_copy_into_pg
# from scripts.analytic_tool.csv_loaders_conn import load_to_postgres_conn
# from scripts.analytic_tool.loaders import load_to_postgres

# XML #
from scripts.xml_scripts.xml_etl_postgres2 import xml_copy_into_pg 
# from scripts.xml_scripts.etl_postgres import load_xml_to_postgres


### CLICKHOUSE ###
# JSON #
from scripts.json_scripts.ch_introspect import (
    ch_ping, list_tables, table_exists, describe_columns,
    table_engine_and_keys, row_count, validate_schema_against_profile_ch
)

from scripts.json_scripts.ch_reset import drop_ch_tables_for_profile, recreate_and_load_ch

from scripts.json_scripts.ch_exec import ch_exec_many
from scripts.json_scripts.ddl_clickhouse import emit_ddl_ch
from scripts.json_scripts.load_clickhouse import insert_into_ch

# CSV #
from scripts.analytic_tool.csv_copy_loaders_psycopg2 import csv_copy_into_clickhouse
# from scripts.analytic_tool.csv_loaders_conn import load_to_clickhouse_client
# from scripts.analytic_tool.loaders import load_to_clickhouse

# XML #
from scripts.xml_scripts.xml_etl_clickhouse2 import xml_copy_into_clickhouse
# from scripts.xml_scripts.etl_clickhouse import load_xml_to_clickhouse

PG_ADMIN_URI = "postgresql://myuser:mypass@db:5432/postgres"

CH_ADMIN_URI = "http://127.0.0.1:8123"
CH_ADMIN_PORT = "8123"

############# POSTGRES #############

def drop_database_pg(dbname):
    """
    Удаляет базу данных dbname.
    Подключение идёт к служебной базе (postgres).
    """
    parsed = urlparse(PG_ADMIN_URI)
    admin_db = parsed.path[1:] if parsed.path else "postgres"
    admin_conn_uri = PG_ADMIN_URI.replace(f"/{parsed.path[1:]}", f"/{admin_db}")

    with psycopg2.connect(admin_conn_uri) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Завершаем активные подключения к удаляемой базе
            cur.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid();
            """, (dbname,))

            # Проверяем, существует ли база
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cur.fetchone():
                cur.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(dbname)))
                print(f"База {dbname} удалена")
            else:
                print(f"База {dbname} не существует")


def drop_db_pg(PG_URI, final_prof):
    conn = psycopg2.connect(PG_URI)
    drop_pg_tables_for_profile(conn, final_prof, schema="public")
    conn.close()


def recreate_db_pg(PG_URI, final_prof, records):
    conn = psycopg2.connect(PG_URI)
    recreate_and_load_pg(conn, final_prof, records, schema="public")
    conn.close()


def check_db_pg(PG_URI):
    conn = psycopg2.connect(PG_URI)

    stat_info = ""
    stat_info += str(list_tables(conn, "public")) + '\n'
    stat_info += str(table_exists(conn, "public", "IndividualEntrepreneur")) + '\n'          # True/False
    stat_info += str(primary_key(conn, "public", "IndividualEntrepreneur")) + '\n'          # ['rec_id']
    stat_info += str(describe_columns(conn, "public", "IndividualEntrepreneur")[:5]) + '\n'  # первые 5 колонок
    # stat_info += str(foreign_keys(conn, "public", "EntrepreneurOKVEDOptional")) + '\n'       # FK -> parent
    stat_info += str(row_count(conn, "public", "IndividualEntrepreneur")) + '\n'             # количество строк
    # stat_info += str(row_count(conn, "public", "EntrepreneurOKVEDOptional")) + '\n'

    # stat_info += str(validate_schema_against_profile(conn, final_prof, schema="public"))  + '\n'

    conn.close()

    return stat_info


def ensure_database_exists(admin_uri, dbname):
    """
    Проверяет, существует ли база dbname; если нет — создаёт её.
    """
    parsed = urlparse(admin_uri)
    admin_db = parsed.path[1:] if parsed.path else "postgres"
    admin_conn_uri = admin_uri.replace(f"/{parsed.path[1:]}", f"/{admin_db}")

    conn = psycopg2.connect(admin_conn_uri)
    conn.autocommit = True  # важно делать сразу после подключения
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
    if not cur.fetchone():
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print(f"База {dbname} создана")
    else:
        print(f"База {dbname} уже существует")
    cur.close()
    conn.close()


def run_etl_pg(PG_URI, dbname, ddl, profile, data_path):
    
    # dbname = urlparse(PG_URI).path[1:]

    ensure_database_exists(PG_ADMIN_URI, dbname)

    conn = psycopg2.connect(PG_URI)
    conn.autocommit = True  # чтобы не падало из-за транзакций
    cur = conn.cursor()
    cur.execute("SET client_min_messages = WARNING")
    cur.execute(ddl)
    conn.commit()

    # conn = psycopg2.connect(PG_URI)
    # conn.autocommit = True # чтобы не падало из-за транзакций
    # with conn.cursor() as cur:
    #     cur.execute("SET client_min_messages = WARNING")
    #     # psycopg2 нормально выполняет несколько CREATE в одном execute,
    #     # но если вдруг драйвер ругнётся — можно разбить по ';'
    #     cur.execute(ddl)
    # conn.commit()

    if data_path.endswith('.csv') or data_path.endswith('.CSV'):
        rows_pg = csv_copy_into_pg(profile, data_path, conn, 
                        schema="public", table=profile["entity"]["name"], has_header=True)
        print("PG loaded rows:", rows_pg)
    elif data_path.endswith('.json') or data_path.endswith('.JSON'):

        with open(data_path) as f:
            records = json.load(f)
        json_copy_into_pg(conn, profile, records, schema="public", batch_size=50_000)
    
    elif data_path.endswith('.xml') or data_path.endswith('.XML'):

        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            cur.execute(stmt + ";")
        conn.commit()

        for t in final_spec["tables"]:
            cur.execute(f"TRUNCATE TABLE {dbname}.{t['table']} CASCADE;")
        conn.commit()

        xml_copy_into_pg(profile, data_path, conn, schema="public", batch_size=5000)

    conn.commit()
    conn.close()


############# CLICKHOUSE #############

def _make_client_from_uri(uri: str, database: str = None) -> Client:
    """
    Создаёт clickhouse_driver.Client из URI вида:
      - clickhouse://host:9000
      - host:9000
      - host (порт по умолчанию 9000)
    Если uri начинается с http:// или https:// — функция всё равно попытается извлечь host/port (для native client).
    """
    parsed = urlparse(uri if "://" in uri else f"//{uri}", scheme="clickhouse")
    host = parsed.hostname or "clickhouse"
    port = parsed.port or 9000
    # username/password можно добавить при необходимости: parsed.username, parsed.password
    return Client(host=host, port=port, database=database or "default",
                  user=(parsed.username or None), password=(parsed.password or None))


def drop_db_ch(dbname: str) -> None:
    """
    Удаляет базу в ClickHouse (DROP DATABASE IF EXISTS dbname).
    """
    client = _make_client_from_uri(CH_ADMIN_URI, database=dbname)
    # В ClickHouse DROP DATABASE нельзя выполнять в транзакции — тут это простая команда
    sql = f"DROP DATABASE IF EXISTS {dbname}"
    client.execute(sql)
    client.disconnect()
    print(f"База ClickHouse `{dbname}` удалена (если существовала)")


def check_db_ch(clickhouse_uri: str, dbname: str) -> List[Dict[str, Any]]:

    # 0) проверка соединения
    ch_ping(CH_ADMIN_URI)

    # 1) список таблиц
    stat_info += str(list_tables(CH_ADMIN_URI, dbname)) + '\n'  

    # 2) существует ли таблица
    stat_info += str(table_exists(CH_ADMIN_URI, dbname, "IndividualEntrepreneur")) + '\n'  

    # 3) описание колонок
    stat_info += str(describe_columns(CH_ADMIN_URI, dbname, "IndividualEntrepreneur")[:5]) + '\n'  

    # 4) ключи / движок
    stat_info += str(table_engine_and_keys(CH_ADMIN_URI, dbname, "EntrepreneurOKVEDOptional")) + '\n'  

    # 5) количество строк
    stat_info += str(row_count(CH_ADMIN_URI, "analytics", "IndividualEntrepreneur")) + '\n'  
    stat_info += str(row_count(CH_ADMIN_URI, "analytics", "EntrepreneurOKVEDOptional")) + '\n'  

    # 6) валидация против итогового профиля
    # print(validate_schema_against_profile_ch(http, final_prof, db, types_yaml_path="config/types.yaml"))
    
    return stat_info


def run_etl_ch(dbname, ddl, profile, data_path):
        
    dbname = "raw"
    admin_client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="")
    admin_client.command(f"CREATE DATABASE IF NOT EXISTS `{dbname}`")

    client = clickhouse_connect.get_client(host="clickhouse", port=8123, username="default", password="", database=dbname)
    client.command(ddl)

    # admin_client = _make_client_from_uri(CH_ADMIN_URI, database="default")

    # admin_client.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    # admin_client.disconnect()

    # client = _make_client_from_uri(CH_ADMIN_URI, database=dbname)
    # client.execute(ddl)

    # если вызываешь из контейнера Airflow — меняй хост:
    # ch_exec_many("http://clickhouse:8123", ddl_ch, database="analytics")

    # подключаемся к нужной базе и вызываем copy_func для загрузки данных
    # client = _make_client_from_uri(CH_ADMIN_URI, database=dbname)
    # c хоста:
    # ch_exec_many(CH_URI, ddl_ch, database=dbname)


    profile = profile if isinstance(profile, dict) else json.loads(profile)

    print("(ETL):", profile)

    if data_path.endswith('.csv') or data_path.endswith('.CSV'):
        rows_ch = csv_copy_into_clickhouse(profile, data_path, client, table=profile["entity"]["name"], has_header=True)
        print("CH loaded rows:", rows_ch)

    elif data_path.endswith('.json') or data_path.endswith('.JSON'):
        with open(data_path) as f:
            records = json.load(f)

        insert_into_ch(
            "http://clickhouse:8123", #"http://127.0.0.1:8123",
            profile,
            records,
            database=dbname,     # таблицы уже созданы в analytics
            batch_size=100_000,
            cast=True,                # важный момент из-за timestamp64
            trust_env=False           # игнорируем системные HTTP(S)_PROXY
        )
    
    elif data_path.endswith('.xml') or data_path.endswith('.XML'):

        import requests

        ch = requests.Session()
        ch.trust_env = False                   # игнорировать системные прокси
        ch.proxies = {"http": None, "https": None}  # ЯВНО без прокси
        ch.base_url = "http://clickhouse:8123"
        ch.params_default = {"user": "default"}     # при необходимости: "password": "..."

        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            r = ch.post(ch.base_url, params=ch.params_default, data=(stmt + ";").encode("utf-8"))
            r.raise_for_status()

        for t in profile["tables"]:
            r = ch.post(ch.base_url, params=ch.params_default, data=(f"TRUNCATE TABLE {dbname}.{t['table']};").encode("utf-8"))
            r.raise_for_status()

        xml_copy_into_clickhouse(profile, data_path, ch, database=dbname, batch_size=5000)


    client.disconnect()


# ---------- main ----------
def main():
    print("main")

if __name__ == "__main__":
    main()
