import gradio as gr
import json
import os
import datetime

from gradio_utils import *

from modules import DataProvider, UserData, PipeLine, Summarizer, TextSplitter

from modules import customLogger
_LOGGER = customLogger.getLogger(__name__)

# Основной интерфейс приложения
class AppInterface:
    def __init__(self):
        self.text_splitter = TextSplitter()
        self.pipeline = PipeLine()
        self.data_provider = DataProvider()
        self.summarizer = Summarizer()

    @log_execution_time
    def extract_text(self, file_path):
        txt = self.pipeline.process(file_path)
        return txt

    @log_execution_time
    def split_text(self, txt):
        txt_chunks = self.text_splitter.split(txt)
        return txt_chunks

    @log_execution_time
    def summarize_text(self, txt_chunks):
        txt_summaries = self.summarizer.generate_summaries(txt_chunks, summarize=False)
        return txt_summaries

    def get_summary(self, username, filename):
        summary = self.data_provider.load_summary(username, filename)
        return summary

    @log_execution_time
    def process_llm_agent_request(self, llm_agent_response, user_data, is_logged_in):
        response_json = llm_agent_response.json()
        result = []
        img_result = None
        if "deRequirements" in response_json and response_json["deRequirements"]:
            result.append(response_json["deRequirements"])
        elif "darchRequirements" in response_json and response_json["darchRequirements"]:
            result.append(response_json["darchRequirements"])
        
        markdown_content = "\n\n".join(result)

        return markdown_content, img_result


def run_web_interface(app):
    with gr.Blocks(title='MVP: Цифровой инженер данных') as demo:

        gr.Markdown(''' # ETL Assistant ''')

        llm_agent_request = gr.State({
            "deRequirements": False,
            "darchRequirements": False,
            "needFix": False,
            "history": {},
            "task": "",
            "requestDateTime": datetime.datetime.utcnow().isoformat() + 'Z'
        })

        with gr.Row():
            with gr.Column(scale=1):
                start_choice = gr.Radio(choices=['Создать новое хранилище','Подключиться к существующему хранилищу'], value=None, label='Что вы хотите сделать?')

                new_source_choice = gr.Dropdown(choices=['Загрузить файл (CSV/JSON/XML)','На основе существующего хранилища (указать ссылку)'], value='Загрузить файл (CSV/JSON/XML)', label='Create: источник', visible=False)
                upload_file_new = gr.File(label='Загрузить файл (CSV/JSON/XML)', visible=False)
                new_base_conn = gr.Textbox(label='Ссылка на существующее хранилище (для создания на его основе)', placeholder='Например: postgres://user:pass@host:5432/db', visible=False)

                existing_conn = gr.Textbox(label='Connection string для подключения к существующему хранилищу', placeholder='postgres://user:pass@host:5432/db', visible=False)
                
                analytic_btn = gr.Button('Загрузить данные, сделать аналитику', visible=False)

                create_connect_btn = gr.Button('Создать/Подключиться', visible=False)

                action_choice = gr.Radio(choices=['Generate Airflow DAG','Add new data to DB'], value=None, label='Далее — что вы хотите сделать?', visible=False)

                # DAG
                gen_dag_btn = gr.Button('Generate DAG from pipeline', visible=False)
                dag_name = gr.Textbox(label='Pipeline / DAG id', value='example_pipeline', visible=False)
                dag_schedule = gr.Textbox(label='Schedule (cron or @hourly)', value='@hourly', visible=False)

                # new data add
                add_data_file = gr.File(label='Upload file to append (CSV/JSON/XML)', visible=False)
                add_data_btn = gr.Button('Append data (stub)', visible=False)
                append_logs = gr.Textbox(label='Append logs', lines=6, interactive=False, visible=False)
            
            with gr.Column(scale=1):
                log_display = gr.Textbox(value="", label="Логгирование", lines=7, max_lines=7, interactive=False, show_copy_button=True)
                info_box = gr.Textbox(value="", label='Info / Recommendations', lines=7, max_lines=7, interactive=False, visible=True)

                # DDL
                ddl_preview = gr.Code(label='DDL preview', language='sql', visible=False)
                # DAG
                dag_preview = gr.Code(label='Airflow DAG (preview)', language='python', visible=False)

        def on_start(choice):
            if choice == 'Создать новое хранилище':
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False), gr.update(visible=False)
            elif choice == 'Подключиться к существующему хранилищу':
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True), gr.update(visible=False)
            else:
                return [gr.update(visible=False)]*4

        start_choice.change(fn=on_start, inputs=[start_choice], outputs=[new_source_choice, upload_file_new, existing_conn, action_choice])

        def on_new_source_change(sel):
            if sel == 'Загрузить файл (CSV/JSON/XML)':
                return gr.update(visible=True), gr.update(visible=False), gr.update(visible=True)
            return gr.update(visible=False), gr.update(visible=True), gr.update(visible=True)

        new_source_choice.change(fn=on_new_source_change, inputs=[new_source_choice], outputs=[upload_file_new, new_base_conn, analytic_btn])

        def on_file_upload(file):
            return gr.update(visible=bool(file))

        upload_file_new.change(fn=on_file_upload, inputs=[upload_file_new], outputs=[analytic_btn])

        def on_existing_conn_change(conn):
            return gr.update(visible=bool(conn and str(conn).strip()))

        existing_conn.change(fn=on_existing_conn_change, inputs=[existing_conn], outputs=[analytic_btn])

        def on_analytic(start_choice_val, new_source_sel, upload_new_file, new_base_conn_val, existing_conn_val, log_text, info_text):
            conn_str = None
            upload = None
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить (CSV/JSON/XML)':
                    upload = upload_new_file
                    source_desc = getattr(upload,'name','uploaded_file')
                else:
                    conn_str = new_base_conn_val
                    source_desc = conn_str
            else:
                conn_str = existing_conn_val
                source_desc = conn_str

            log_text += 'Данные успешно загружены.\n'

            result = analyze_source_stub(new_source_sel, upload, conn_str)
            log_text += 'Аналитика данных завершена.\n'

            rec = recommend_storage(result['schema'])
            info_lines = [f"Рекоммендация: {rec['recommendation']}", f"Обоснование: {rec['rationale']}"]
            if result.get('conn_info'):
                info_lines.append('Parsed connection: ' + json.dumps(result['conn_info'], ensure_ascii=False))
            info_text += '\n'.join(info_lines)

            ddl = generate_ddl(result['schema'], table_name='my_table', target_db=rec['recommendation'])
            dag = generate_airflow_dag_from_pipeline('example_pipeline', '@hourly', [], rec['recommendation'])
            log_text += 'DDL сгенерирован.\nDAG сгенерирован.\n'

            return gr.update(visible=True), log_text, info_text, gr.update(value=ddl, visible=True), gr.update(value=dag, visible=True), gr.update(visible=True)

        analytic_btn.click(fn=on_analytic, inputs=[start_choice, new_source_choice, upload_file_new, new_base_conn, existing_conn, log_display, info_box], 
                                            outputs=[create_connect_btn, log_display, info_box, ddl_preview, dag_preview, action_choice])

        def on_create_connect(start_choice_val, new_source_sel, upload_new_file, new_base_conn_val, existing_conn_val, log_text, info_text):
            conn_str = None
            upload = None
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить (CSV/JSON/XML)':
                    upload = upload_new_file
                    source_desc = getattr(upload,'name','uploaded_file')
                else:
                    conn_str = new_base_conn_val
                    source_desc = conn_str
                log_text += "Новое хранилище успешно создано.\n"
            else:
                conn_str = existing_conn_val
                source_desc = conn_str
                log_text += "Подключение к хранилищу успешно выполнено.\n"

            return log_text

        create_connect_btn.click(fn=on_create_connect, inputs=[start_choice, new_source_choice, upload_file_new, new_base_conn, existing_conn, log_display, info_box],
                                                        outputs=[log_display])

        def on_action_change(act):
            gen_btn_vis = gr.update(visible=False)
            add_data_file_vis = gr.update(visible=False)
            add_data_btn_vis = gr.update(visible=False)
            dag_name_vis = gr.update(visible=False)
            dag_schedule_vis = gr.update(visible=False)

            if act == 'Generate Airflow DAG':
                gen_btn_vis = gr.update(visible=True)
                dag_name_vis = gr.update(visible=True)
                dag_schedule_vis = gr.update(visible=True)
            elif act == 'Add new data to DB':
                add_data_file_vis = gr.update(visible=True)
                add_data_btn_vis = gr.update(visible=True)
                append_logs_vis = gr.update(visible=True)

            return (gen_btn_vis, add_data_file_vis,
                    add_data_btn_vis, dag_name_vis, dag_schedule_vis)

        action_choice.change(fn=on_action_change, inputs=[action_choice], outputs=[gen_dag_btn, add_data_file, add_data_btn, dag_name, dag_schedule])

        def on_generate_pipeline(dag_id, schedule, target_desc):
            dag_text = generate_airflow_dag_from_pipeline(dag_id, schedule, steps or [], target_desc)
            save_text_to_tmp(dag_text, name=f"{dag_id}_pipeline_dag.py")
            return dag_text

        gen_dag_btn.click(fn=on_generate_pipeline, inputs=[dag_name, dag_schedule, info_box], outputs=[dag_preview])

        def on_append(upload_file):
            target_table = "table_name"
            ts = datetime.datetime.utcnow().isoformat() + 'Z'
            msg = f"[{ts}] Appended file {getattr(upload_file,'name',None)} to {target_table} (stub)"
            return msg

        add_data_btn.click(fn=on_append, inputs=[add_data_file], outputs=[append_logs])

if __name__ == '__main__':
    # try:
    app_instance = AppInterface()
    demo = run_web_interface(app_instance)
    demo.launch(server_name="0.0.0.0", server_port=7862)
    # except:
        # reboot_system()
