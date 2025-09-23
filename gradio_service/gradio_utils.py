
import pandas as pd
import json
import tempfile
import os
import datetime
from urllib.parse import urlparse, unquote

# ---------------------- Утилиты / Заглушки ----------------------

def parse_connection_string(conn_str):
    if not conn_str:
        return None
    try:
        parsed = urlparse(conn_str)
    except Exception:
        return None
    scheme = parsed.scheme.lower()
    res = {'scheme': scheme}
    if '@' in parsed.netloc:
        userinfo, hostinfo = parsed.netloc.split('@', 1)
        if ':' in userinfo:
            user, password = userinfo.split(':', 1)
            res['user'] = unquote(user)
            res['password'] = unquote(password)
        else:
            res['user'] = unquote(userinfo)
            res['password'] = None
    else:
        hostinfo = parsed.netloc
    if ':' in hostinfo:
        host, port = hostinfo.split(':', 1)
        res['host'] = host
        res['port'] = port
    else:
        res['host'] = hostinfo
        res['port'] = None
    path = parsed.path.lstrip('/')
    if scheme.startswith('postgres'):
        res['database'] = path or None
    elif scheme.startswith('clickhouse'):
        res['database'] = path or None
    elif scheme.startswith('kafka'):
        res['topic'] = path or None
    else:
        res['path'] = path or None
    return res

# аналитика данных
def analyze_source_stub(source_choice, upload_file):
    conn_info = parse_connection_string(upload_file) if upload_file else None
    if upload_file is not None:
        try:
            fname = upload_file.name
            if fname.lower().endswith('.csv') or fname.lower().endswith('.json'):
                df = pd.read_csv(upload_file.name, nrows=100)
                schema = [{'column': c, 'type': str(t)} for c,t in zip(df.columns, df.dtypes)]
                preview = df.head(5).to_dict(orient='records')
                return {'schema': schema, 'preview': preview, 'conn_info': conn_info}
        except Exception:
            pass
    if conn_info:
        if conn_info['scheme'].startswith('postgres'):
            schema = [{'column':'id','type':'int64'},{'column':'created_at','type':'datetime64[ns]'},{'column':'payload','type':'object'}]
        elif conn_info['scheme'].startswith('clickhouse'):
            schema = [{'column':'event_time','type':'datetime64[ns]'},{'column':'user_id','type':'int64'},{'column':'value','type':'float64'}]
        else:
            schema = [{'column':'ts','type':'datetime64[ns]'},{'column':'key','type':'object'},{'column':'value','type':'object'}]
        return {'schema': schema, 'preview': [{'note':'inferred from connection string'}], 'conn_info': conn_info}
    schema = [{'column':'event_time','type':'datetime64[ns]'},{'column':'user_id','type':'int64'},{'column':'amount','type':'float64'}]
    return {'schema': schema, 'preview': [{'event_time':'2025-09-20','user_id':1,'amount':10.5}], 'conn_info': None}


def recommend_storage(schema):
    cols = [c['column'].lower() for c in schema]
    if any('time' in c or 'date' in c for c in cols):
        return {'recommendation':'ClickHouse','rationale':'Временные/аналитические данные — рекомендую ClickHouse с партицированием по дате.'}
    if any('id' in c for c in cols):
        return {'recommendation':'PostgreSQL','rationale':'Оперативные данные с идентификаторами — PostgreSQL.'}
    return {'recommendation':'HDFS/Object Storage','rationale':'Сырые файлы — HDFS или объектное хранилище.'}


def generate_ddl(schema, table_name, target_db):
    type_map = {'int64':'BIGINT','float64':'DOUBLE','object':'VARCHAR','datetime64[ns]':'TIMESTAMP'}
    col_lines = []
    for c in schema:
        mapped = 'VARCHAR'
        for k in type_map:
            if k in c['type']:
                mapped = type_map[k]
                break
        col_lines.append(f"    {c['column']} {mapped}")
    ddl = f"-- DDL for {target_db}\n"
    ddl += f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
    ddl += ",".join(col_lines) + ");\n"
    ddl += "-- Recommend: partition by date if present; add indexes for frequent filters."
    return ddl


def generate_airflow_dag_from_pipeline(pipeline_name, schedule, pipeline_steps, target_desc):
    steps_comments = " ".join([f"# {i+1}. {s['type']} {s.get('params','')}" for i,s in enumerate(pipeline_steps)])
    dag = (
        "from airflow import DAG\n"
        "from airflow.operators.python_operator import PythonOperator\n"
        "from datetime import datetime\n"
        f"{steps_comments}\n"
        "def etl_task():\n"
        f"    print(\"ETL pipeline stub to {target_desc}\")\n"
        f"with DAG(dag_id='{pipeline_name}', start_date=datetime(2025,1,1), schedule_interval='{schedule}', catchup=False) as dag:\n"
        "    task = PythonOperator(task_id='run_etl', python_callable=etl_task)0\n"
    )
    return dag


def save_text_to_tmp(text, name='generated.txt'):
    tmp = tempfile.gettempdir()
    path = os.path.join(tmp, name)
    with open(path,'w',encoding='utf-8') as f:
        f.write(text)
    return path

if __name__ == '__main__':
    print("main")
