"""
Минимальный Gradio MVP интерфейс для цифрового инженера данных (динамический UI, заглушки).
Файл: gradio_etl_assistant_mvp.py
Запуск: python gradio_etl_assistant_mvp.py
Открыть: http://localhost:7860

Особенности:
- Минимальный начальный экран: Create / Connect + явная кнопка Create/Connect (объединяет создание и анализ).
- Убрана лишняя кнопка Parse/Test Connection — вместо неё единая кнопка Create/Connect & Analyze появляется там, где нужно.
- Есть редактор пайплайна (add/remove/move) для подготовки шагов перед генерацией DAG.
- Все интеграции — заглушки; замените их реальной логикой при интеграции.
"""

import gradio as gr
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


def analyze_source_stub(source_choice, upload_file, conn_str, sample_size=100):
    conn_info = parse_connection_string(conn_str) if conn_str else None
    if upload_file is not None:
        try:
            fname = upload_file.name
            if fname.lower().endswith('.csv') or fname.lower().endswith('.json'):
                df = pd.read_csv(upload_file.name, nrows=sample_size)
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
    ddl = f"-- DDL for {target_db}"
    ddl += f"CREATE TABLE IF NOT EXISTS {table_name} ("
    ddl += ",".join(col_lines) + ");"
    ddl += "-- Recommend: partition by date if present; add indexes for frequent filters"
    return ddl


def generate_airflow_dag_from_pipeline(pipeline_name, schedule, pipeline_steps, target_desc):
    steps_comments = " ".join([f"# {i+1}. {s['type']} {s.get('params','')}" for i,s in enumerate(pipeline_steps)])
    dag = (
        "from airflow import DAG"
        "from airflow.operators.python_operator import PythonOperator"
        "from datetime import datetime"
        f"{steps_comments}"
        "def etl_task():"
        f"    print(\"ETL pipeline stub to {target_desc}\")"
        f"with DAG(dag_id='{pipeline_name}', start_date=datetime(2025,1,1), schedule_interval='{schedule}', catchup=False) as dag:"
        "    task = PythonOperator(task_id='run_etl', python_callable=etl_task)0"
    )
    return dag


def save_text_to_tmp(text, name='generated.txt'):
    tmp = tempfile.gettempdir()
    path = os.path.join(tmp, name)
    with open(path,'w',encoding='utf-8') as f:
        f.write(text)
    return path

# ---------------------- Интерфейс Gradio (динамический, с редактором шагов) ----------------------

with gr.Blocks(title='MVP: Цифровой инженер данных') as demo:
    gr.Markdown('''
    # ETL Assistant — редактор пайплайна
    Минимальный экран: Create / Connect + кнопка Create/Connect & Analyze. Затем — выбор действия и редактор пайплайна (add/remove/move).
    ''')

    with gr.Row():
        with gr.Column(scale=1):
            start_choice = gr.Radio(choices=['Создать новое хранилище','Подключиться к существующему хранилищу'], value=None, label='Что вы хотите сделать?')

            new_source_choice = gr.Dropdown(choices=['Загрузить файл (CSV/JSON/XML)','На основе существующего хранилища (указать ссылку)'], value='Upload file (CSV/JSON/XML)', label='Create: источник', visible=False)
            upload_file_new = gr.File(label='Загрузить файл (CSV/JSON/XML)', visible=False)
            new_base_conn = gr.Textbox(label='Ссылка на существующее хранилище (для создания на его основе)', placeholder='Например: postgres://user:pass@host:5432/db', visible=False)

            existing_conn = gr.Textbox(label='Connection string (URI) для подключения к существующему хранилищу', placeholder='postgres://user:pass@host:5432/db', visible=False)

            action_choice = gr.Radio(choices=['DDL & Recommendations','Generate Airflow DAG','Add new data to DB'], value=None, label='Далее — что вы хотите сделать?', visible=False)

            # единая кнопка создания/подключения + анализа
            create_connect_btn = gr.Button('Create/Connect & Analyze', visible=False)
            info_box = gr.Textbox(label='Info / Recommendations', lines=6, interactive=False, visible=False)

            ddl_preview = gr.Code(label='DDL preview', language='sql', visible=False)
            dag_preview = gr.Code(label='Airflow DAG (preview)', language='python', visible=False)

            # pipeline editor components (drag-like UX via move buttons)
            pipeline_steps_state = gr.State(value=[])
            pipeline_steps_html = gr.HTML('<div style="color:#666">Pipeline editor hidden</div>', visible=False)
            add_step_type = gr.Dropdown(choices=['Source','Filter','Aggregate','Join','Sink'], value='Source', label='Step type', visible=False)
            add_step_btn = gr.Button('Add step', visible=False)
            remove_index = gr.Number(label='Index to remove (1-based)', value=1, visible=False)
            remove_btn = gr.Button('Remove step', visible=False)
            move_from = gr.Number(label='Move from index (1-based)', value=1, visible=False)
            move_to = gr.Number(label='Move to index (1-based)', value=1, visible=False)
            move_btn = gr.Button('Move step', visible=False)
            gen_dag_btn = gr.Button('Generate DAG from pipeline', visible=False)

            # DAG identity controls (needed for generation)
            dag_name = gr.Textbox(label='Pipeline / DAG id', value='example_pipeline', visible=False)
            dag_schedule = gr.Textbox(label='Schedule (cron or @hourly)', value='@hourly', visible=False)

            # add-data controls
            add_data_file = gr.File(label='Upload file to append (CSV/JSON/XML)', visible=False)
            add_data_target = gr.Textbox(label='Target table (existing)', value='my_table', visible=False)
            add_data_btn = gr.Button('Append data (stub)', visible=False)
            append_logs = gr.Textbox(label='Append logs', lines=6, interactive=False, visible=False)

    # --- UI dynamics: show create_connect button after start choice ---
    def on_start(choice):
        if choice == 'Create new storage':
            # show source selector, file upload and create_connect button
            return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False), gr.update(visible=False)
        elif choice == 'Connect to existing storage':
            # show connection string and create_connect button
            return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True), gr.update(visible=False)
        else:
            return [gr.update(visible=False)]*4

    # outputs: new_source_choice, upload_file_new, existing_conn, action_choice
    start_choice.change(fn=on_start, inputs=[start_choice], outputs=[new_source_choice, upload_file_new, existing_conn, action_choice])

    def on_new_source_change(sel):
        if sel == 'Upload file (CSV/JSON/XML)':
            return gr.update(visible=True), gr.update(visible=False), gr.update(visible=True)
        return gr.update(visible=False), gr.update(visible=True), gr.update(visible=True)

    # outputs: upload_file_new, new_base_conn, create_connect_btn
    new_source_choice.change(fn=on_new_source_change, inputs=[new_source_choice], outputs=[upload_file_new, new_base_conn, create_connect_btn])

    # If user directly uploads a file (without clicking start_choice), show Create/Connect button
    def on_file_upload(file):
        return gr.update(visible=bool(file))

    upload_file_new.change(fn=on_file_upload, inputs=[upload_file_new], outputs=[create_connect_btn])

    # If user pastes a connection string, show Create/Connect button
    def on_existing_conn_change(conn):
        return gr.update(visible=bool(conn and str(conn).strip()))

    existing_conn.change(fn=on_existing_conn_change, inputs=[existing_conn], outputs=[create_connect_btn])

    # --- pipeline editor helpers ---
    def render_pipeline_html(steps):
        if not steps:
            return '<div style="color:#666">Pipeline is empty. Add steps to build the flow.</div>'
        html = ['<ol>']
        for i,s in enumerate(steps):
            html.append(f"<li><b>{s['type']}</b>: {s.get('params','')} (index={i+1})</li>")
        html.append('</ol>')
        return ' '.join(html)

    def add_step(step_type, steps):
        steps = steps or []
        steps.append({'type': step_type, 'params': ''})
        return steps, render_pipeline_html(steps)

    def remove_step(index, steps):
        steps = steps or []
        try:
            idx = int(index)-1
            if 0 <= idx < len(steps):
                steps.pop(idx)
        except Exception:
            pass
        return steps, render_pipeline_html(steps)

    def move_step(frm, to, steps):
        steps = steps or []
        try:
            f = int(frm)-1
            t = int(to)-1
            if 0 <= f < len(steps) and 0 <= t < len(steps) and f != t:
                item = steps.pop(f)
                steps.insert(t, item)
        except Exception:
            pass
        return steps, render_pipeline_html(steps)

    add_step_btn.click(fn=add_step, inputs=[add_step_type, pipeline_steps_state], outputs=[pipeline_steps_state, pipeline_steps_html])
    remove_btn.click(fn=remove_step, inputs=[remove_index, pipeline_steps_state], outputs=[pipeline_steps_state, pipeline_steps_html])
    move_btn.click(fn=move_step, inputs=[move_from, move_to, pipeline_steps_state], outputs=[pipeline_steps_state, pipeline_steps_html])

    # --- Create/Connect & Analyze action: shows action_choice and editor previews ---
    def on_create_connect(start_choice_val, new_source_sel, upload_new_file, new_base_conn_val, existing_conn_val):
        conn_str = None
        upload = None
        source_desc = 'unknown'
        if start_choice_val == 'Create new storage':
            if new_source_sel == 'Upload file (CSV/JSON/XML)':
                upload = upload_new_file
                source_desc = getattr(upload,'name','uploaded_file')
            else:
                conn_str = new_base_conn_val
                source_desc = conn_str
        else:
            conn_str = existing_conn_val
            source_desc = conn_str
        result = analyze_source_stub(new_source_sel, upload, conn_str)
        rec = recommend_storage(result['schema'])
        info_lines = [f"Recommendation: {rec['recommendation']}", f"Rationale: {rec['rationale']}"]
        if result.get('conn_info'):
            info_lines.append('Parsed connection: ' + json.dumps(result['conn_info'], ensure_ascii=False))
        info_text = ' '.join(info_lines)
        ddl = generate_ddl(result['schema'], table_name='my_table', target_db=rec['recommendation'])
        dag = generate_airflow_dag_from_pipeline('example_pipeline', '@hourly', [], rec['recommendation'])
        # show action choices and pipeline editor after successful create/connect
        return info_text, ddl, dag, gr.update(visible=True), gr.update(visible=True), gr.update(visible=True)

    # wire create/connect button
    create_connect_btn.click(fn=on_create_connect, inputs=[start_choice, new_source_choice, upload_file_new, new_base_conn, existing_conn], outputs=[info_box, ddl_preview, dag_preview, action_choice, pipeline_steps_html, create_connect_btn])

    # --- Action change: show appropriate UI blocks; editor controls shown when DAG chosen ---
    def on_action_change(act):
        # default hide all
        ddl_vis = gr.update(visible=False)
        editor_html_vis = gr.update(visible=False)
        add_step_vis = gr.update(visible=False)
        add_btn_vis = gr.update(visible=False)
        remove_idx_vis = gr.update(visible=False)
        remove_btn_vis = gr.update(visible=False)
        move_from_vis = gr.update(visible=False)
        move_to_vis = gr.update(visible=False)
        move_btn_vis = gr.update(visible=False)
        gen_btn_vis = gr.update(visible=False)
        add_data_file_vis = gr.update(visible=False)
        add_data_target_vis = gr.update(visible=False)
        add_data_btn_vis = gr.update(visible=False)
        dag_preview_vis = gr.update(visible=False)
        dag_name_vis = gr.update(visible=False)
        dag_schedule_vis = gr.update(visible=False)

        if act == 'DDL & Recommendations':
            ddl_vis = gr.update(visible=True)
        elif act == 'Generate Airflow DAG':
            editor_html_vis = gr.update(visible=True)
            add_step_vis = gr.update(visible=True)
            add_btn_vis = gr.update(visible=True)
            remove_idx_vis = gr.update(visible=True)
            remove_btn_vis = gr.update(visible=True)
            move_from_vis = gr.update(visible=True)
            move_to_vis = gr.update(visible=True)
            move_btn_vis = gr.update(visible=True)
            gen_btn_vis = gr.update(visible=True)
            dag_preview_vis = gr.update(visible=True)
            dag_name_vis = gr.update(visible=True)
            dag_schedule_vis = gr.update(visible=True)
        elif act == 'Add new data to DB':
            add_data_file_vis = gr.update(visible=True)
            add_data_target_vis = gr.update(visible=True)
            add_data_btn_vis = gr.update(visible=True)
            append_logs_vis = gr.update(visible=True)

        return (ddl_vis, editor_html_vis, add_step_vis, add_btn_vis, remove_idx_vis, remove_btn_vis,
                move_from_vis, move_to_vis, move_btn_vis, gen_btn_vis, add_data_file_vis, add_data_target_vis,
                add_data_btn_vis, dag_preview_vis, dag_name_vis, dag_schedule_vis)

    # outputs order must match returned tuple
    action_choice.change(fn=on_action_change, inputs=[action_choice], outputs=[ddl_preview, pipeline_steps_html, add_step_type, add_step_btn, remove_index, remove_btn, move_from, move_to, move_btn, gen_dag_btn, add_data_file, add_data_target, add_data_btn, dag_preview, dag_name, dag_schedule])

    def on_generate_pipeline(dag_id, schedule, steps, target_desc):
        dag_text = generate_airflow_dag_from_pipeline(dag_id, schedule, steps or [], target_desc)
        save_text_to_tmp(dag_text, name=f"{dag_id}_pipeline_dag.py")
        return dag_text

    gen_dag_btn.click(fn=on_generate_pipeline, inputs=[dag_name, dag_schedule, pipeline_steps_state, info_box], outputs=[dag_preview])

    def on_append(upload_file, target_table):
        ts = datetime.datetime.utcnow().isoformat() + 'Z'
        msg = f"[{ts}] Appended file {getattr(upload_file,'name',None)} to {target_table} (stub)"
        return msg

    add_data_btn.click(fn=on_append, inputs=[add_data_file, add_data_target], outputs=[append_logs])

if __name__ == '__main__':
    demo.launch()
