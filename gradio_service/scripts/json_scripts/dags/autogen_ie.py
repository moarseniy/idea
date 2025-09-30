# dags/autogen_ie.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# твои модули (лежат в dags/ или plugins/)
from ddl_postgres import emit_ddl_pg
from ddl_clickhouse import emit_ddl_ch
from load_postgres import copy_into_pg
from load_clickhouse import insert_into_ch
from ch_exec import ch_exec_many

CFG = {
  "final_profile_path": "/opt/airflow/data/final_profile.json",
  "data_path": "/opt/airflow/data/ie.json",
  "types_yaml": "/opt/airflow/config/types.yaml",
  "pg": {"enabled": True, "dsn": "postgresql://postgres:postgres@pg:5432/analytics", "schema": "public"},
  "ch": {"enabled": True, "http_url": "http://clickhouse:8123", "database": "analytics"},
  "schedule": "0 * * * *",
}

default_args = {"retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG("ie_pipeline", start_date=datetime(2024,1,1), schedule=CFG["schedule"], catchup=False, default_args=default_args):
    def pg_ddl(**_):
        import json, psycopg2, pathlib
        prof = json.loads(pathlib.Path(CFG["final_profile_path"]).read_text(encoding="utf-8"))
        ddl = emit_ddl_pg(prof, types_yaml_path=CFG["types_yaml"])
        conn = psycopg2.connect(CFG["pg"]["dsn"])
        with conn.cursor() as cur:
            cur.execute('SET client_min_messages = WARNING')
            cur.execute(f'SET search_path TO "{CFG["pg"]["schema"]}"')
            cur.execute(ddl)
        conn.commit(); conn.close()

    def pg_load(**_):
        import json, psycopg2, pathlib
        prof = json.loads(pathlib.Path(CFG["final_profile_path"]).read_text(encoding="utf-8"))
        records = json.loads(pathlib.Path(CFG["data_path"]).read_text(encoding="utf-8"))
        conn = psycopg2.connect(CFG["pg"]["dsn"])
        copy_into_pg(conn, prof, records, schema=CFG["pg"]["schema"], batch_size=100_000)
        conn.close()

    def ch_ddl_and_load(**_):
        import json, pathlib
        prof = json.loads(pathlib.Path(CFG["final_profile_path"]).read_text(encoding="utf-8"))
        ddl = emit_ddl_ch(prof, types_yaml_path=CFG["types_yaml"], database=CFG["ch"]["database"])
        ch_exec_many(CFG["ch"]["http_url"], ddl, database=CFG["ch"]["database"])
        records = json.loads(pathlib.Path(CFG["data_path"]).read_text(encoding="utf-8"))
        insert_into_ch(CFG["ch"]["http_url"], prof, records, database=CFG["ch"]["database"], batch_size=200_000)

    if CFG["pg"]["enabled"]:
        t_pg_ddl  = PythonOperator(task_id="pg_ddl",  python_callable=pg_ddl)
        t_pg_load = PythonOperator(task_id="pg_load", python_callable=pg_load)
        t_pg_ddl >> t_pg_load

    if CFG["ch"]["enabled"]:
        t_ch = PythonOperator(task_id="ch_ddl_and_load", python_callable=ch_ddl_and_load)

    # при желании — зависимости между PG и CH
