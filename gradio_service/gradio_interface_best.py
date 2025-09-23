import gradio as gr
import json
import os
import datetime

from gradio_utils import *

from modules import DataProvider, UserData, PipeLine, Summarizer, TextSplitter

from modules import customLogger
_LOGGER = customLogger.getLogger(__name__)

llm_host = "http://agent_app:7861/llm_agents"

# Декоратор для логирования времени выполнения функций
def log_execution_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        _LOGGER.info(f"Время выполнения {func.__name__}: {elapsed_time:.2f} секунд.")
        return result
    return wrapper

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

                new_source_choice = gr.Dropdown(choices=['Загрузить файлы (CSV/JSON/XML)','На основе существующего хранилища (указать ссылку)'], value='Загрузить файлы (CSV/JSON/XML)', label='Источник:', visible=False)
                upload_file_new = gr.Textbox(label='Путь до директории с CSV/JSON/XML файлами', visible=False)
                new_base_conn = gr.Textbox(label='Ссылка на существующее хранилище (для создания на его основе)', placeholder='Например: postgres://user:pass@host:5432/db', visible=False)

                existing_conn = gr.Textbox(label='Connection string для подключения к существующему хранилищу', placeholder='postgres://user:pass@host:5432/db', visible=False)
                
                analytic_btn = gr.Button('Загрузить данные, сделать аналитику', visible=False)
                create_connect_btn = gr.Button('Создать/Подключиться', visible=False)

            with gr.Column(scale=1):
                log_display = gr.Textbox(value="", label="Логгирование", lines=7, max_lines=7, interactive=False, show_copy_button=True)
                info_box = gr.Textbox(value="", label='Info / Recommendations', lines=7, max_lines=7, interactive=False, visible=True)

                # DDL
                ddl_preview = gr.Code(label='DDL script (preview)', language='sql', visible=False)
                # DAG
                dag_preview = gr.Code(label='Airflow DAG (preview)', language='python', visible=False)

        def on_start(choice):
            if choice == 'Создать новое хранилище':
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False)
            elif choice == 'Подключиться к существующему хранилищу':
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)
            else:
                return [gr.update(visible=False)]*3

        start_choice.change(fn=on_start, 
                            inputs=[start_choice], 
                            outputs=[new_source_choice, upload_file_new, existing_conn])

        def on_new_source_change(sel):
            if sel == 'Загрузить файлы (CSV/JSON/XML)':
                return gr.update(visible=True), gr.update(visible=False), gr.update(visible=True)
            return gr.update(visible=False), gr.update(visible=True), gr.update(visible=True)

        new_source_choice.change(fn=on_new_source_change, 
                                    inputs=[new_source_choice], 
                                    outputs=[upload_file_new, new_base_conn, analytic_btn])

        def upload_files(dir_path):
            return gr.update(visible=True)

        upload_file_new.change(fn=upload_files, 
                                inputs=[upload_file_new], 
                                outputs=[analytic_btn])

        def on_existing_conn_change(conn):
            return gr.update(visible=bool(conn and str(conn).strip()))

        existing_conn.change(fn=on_existing_conn_change, 
                                inputs=[existing_conn], 
                                outputs=[analytic_btn])

        def on_analytic(start_choice_val, new_source_sel, upload_new_file, new_base_conn_val, existing_conn_val, log_text, info_text):
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                    source_desc = upload_new_file
                else:
                    source_desc = new_base_conn_val
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
            log_text += 'DDL сгенерирован.\n'

            dag = generate_airflow_dag_from_pipeline('example_pipeline', '@hourly', [], rec['recommendation'])
            log_text += 'DAG сгенерирован.\n'

            return gr.update(visible=True), log_text, info_text, gr.update(value=ddl, visible=True), gr.update(value=dag, visible=True)

        analytic_btn.click(fn=on_analytic, 
                            inputs=[start_choice, new_source_choice, upload_file_new, new_base_conn, existing_conn, log_display, info_box], 
                            outputs=[create_connect_btn, log_display, info_box, ddl_preview, dag_preview])

        def on_create_connect(start_choice_val, new_source_sel, upload_new_file, new_base_conn_val, existing_conn_val, log_text, info_text):
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                    source_desc = upload_new_file
                else:
                    source_desc = new_base_conn_val

                log_text += "Новое хранилище успешно создано.\n"
            else:
                conn_str = existing_conn_val
                source_desc = conn_str
                
                log_text += "Подключение к хранилищу успешно выполнено.\n"

            return log_text

        create_connect_btn.click(fn=on_create_connect, 
                                    inputs=[start_choice, new_source_choice, upload_file_new, new_base_conn, existing_conn, log_display, info_box],
                                    outputs=[log_display])

    return demo

if __name__ == '__main__':
    _LOGGER.info("Starting app...")
    # try:
    app_instance = AppInterface()
    demo = run_web_interface(app_instance)
    demo.launch(server_name="0.0.0.0", server_port=7863)
    # except:
        # reboot_system()
