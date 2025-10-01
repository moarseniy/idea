import gradio as gr
import json, os, time, re, ast
import datetime
import requests

from gradio_utils import *

# from scripts.profile_csv import profile_csv
from scripts.analytic_pipeline import clean_json, run_build_analytic_prompt, run_build_final_prompt
from scripts.json_analytic_pipeline import run_compute_json_profile, run_final_json_profile, json_get_postgres_ddl, json_get_clickhouse_ddl, json_get_dbml
from scripts.xml_analytic_pipeline import run_compute_xml_profile, run_final_xml_profile, xml_get_postgres_ddl, xml_get_clickhouse_ddl, xml_get_dbml


from modules import customLogger

from file_utils import *

_LOGGER = customLogger.getLogger(__name__)

llm_host = "http://agent_app:7861/llm_agents"

def reboot_system():
    import subprocess
    subprocess.check_call('reboot')

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


def run_web_interface():
    with gr.Blocks(title='MVP: Цифровой инженер данных') as demo:

        gr.Markdown(''' # ETL Assistant ''')

        llm_agent_request = gr.State({
            "daRequirements": False,
            "daJsonRequirements": False,
            "daXmlRequirements": False,
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
                new_source_choice = gr.Dropdown(choices=['Загрузить файлы (CSV/JSON/XML)','Указать директорию','На основе существующего хранилища (указать ссылку)'], value='Загрузить файлы (CSV/JSON/XML)', label='Источник:', visible=False)
                
                upload_file_new = gr.File(label='Перенесите файлы мышкой или выберите в открывшемся окне.', file_count='multiple', visible=False)
                upload_file_new2 = gr.Textbox(label='Путь до директории с CSV/JSON/XML файлами', placeholder='Например: C:\\Users\\1\\data\\', visible=False)
                new_base_conn = gr.Textbox(label='Ссылка на существующее хранилище (для создания на его основе)', placeholder='Например: postgres://user:pass@host:5432/db', visible=False)

                existing_conn = gr.Textbox(label='Connection string для подключения к существующему хранилищу', placeholder='postgres://user:pass@host:5432/db', visible=False)
                
                with gr.Row():
                    with gr.Column():
                        chatbot_ui = gr.Chatbot(label="Чат-бот", type="messages", visible=False)

                        with gr.Row():
                            user_input = gr.Textbox(scale=30, label="", placeholder="Введите текст...", lines=1, visible=False)
                            submit_button = gr.Button(scale=1, value="➤", elem_id="submit_button", visible=False)

                analytic_btn = gr.Button('Загрузить данные, сделать аналитику', visible=False)
                create_connect_btn = gr.Button('Создать', visible=False)

            with gr.Column(scale=1):
                log_display = gr.Textbox(value="", label="Логгирование", lines=5, max_lines=5, interactive=False, show_copy_button=True)
                md_download_button = gr.DownloadButton("Скачать отчет", visible=False)
                info_box = gr.Markdown(value="", label='Отчет/Рекомендации', visible=True)

                # DDL
                ddl_preview = gr.Code(label='DDL script (preview)', language='sql', visible=False)
                # DAG
                dag_preview = gr.Code(label='Airflow DAG (preview)', language='python', visible=False)


        @start_choice.change(inputs=[start_choice], 
                            outputs=[new_source_choice, upload_file_new, upload_file_new2, existing_conn])
        def on_start(choice):
            if choice == 'Создать новое хранилище':
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False), gr.update(visible=False)
            elif choice == 'Подключиться к существующему хранилищу':
                return gr.update(visible=False), gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)
            else:
                return [gr.update(visible=False)]*4

        
        @new_source_choice.change(inputs=[new_source_choice], 
                                 outputs=[upload_file_new, upload_file_new2, new_base_conn, analytic_btn])
        def on_new_source_change(sel):
            if sel == 'Загрузить файлы (CSV/JSON/XML)':
                return gr.update(visible=True), gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)
            if sel == 'Указать директорию':
                return gr.update(visible=False), gr.update(visible=True), gr.update(visible=False), gr.update(visible=True)
            return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True), gr.update(visible=True)


        @upload_file_new.change(inputs=[upload_file_new], 
                                outputs=[analytic_btn])
        def upload_files(dir_path):
            return gr.update(visible=True)


        @upload_file_new2.change(inputs=[upload_file_new2], 
                                outputs=[analytic_btn])
        def upload_files2(files):
            return gr.update(visible=True)


        @existing_conn.change(inputs=[existing_conn], 
                              outputs=[analytic_btn])
        def on_existing_conn_change(conn):
            return gr.update(visible=bool(conn and str(conn).strip()))


        @analytic_btn.click(inputs=[start_choice, new_source_choice, upload_file_new, upload_file_new2, new_base_conn, existing_conn, log_display, info_box, llm_agent_request, chatbot_ui], 
                            outputs=[create_connect_btn, log_display, info_box, ddl_preview, dag_preview, llm_agent_request, chatbot_ui, user_input, submit_button, md_download_button])
        def on_analytic(start_choice_val, new_source_sel, upload_new_file, upload_new_file2, new_base_conn_val, existing_conn_val, log_text, info_text, llm_agent_request, chat_history):
            
            final_profile_json = None

            if "needFix" in llm_agent_request and llm_agent_request["needFix"]:
                info_text = ""
            else:
                source_desc = 'unknown'

                if start_choice_val == 'Создать новое хранилище':
                    if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                        source_desc = upload_new_file[0] # TODO: read list of paths
                    elif new_source_sel == 'Указать директорию':
                        source_desc = upload_new_file2[0] # TODO: read list of paths
                    else:
                        source_desc = new_base_conn_val
                else:
                    source_desc = existing_conn_val

                log_text += 'Данные успешно загружены.\n'

                # _LOGGER.info(f"PATH: {source_desc}")
                log_text += f'Обрабатываем файл: {source_desc}\n'
                # result = profile_csv(source_desc, ";")

                if source_desc.endswith('.json') or source_desc.endswith('.JSON'):
                    profile_json = run_compute_json_profile(source_desc)

                    if not isinstance(profile_json, dict):
                        profile_json = ast.literal_eval(profile_json)


                    print("ПРОФИЛЬ JSON", profile_json)

                    llm_agent_request["task"] = f"{profile_json}"
                    llm_agent_request["daJsonRequirements"] = True

                    log_text += 'Отправляем запрос к LLM агенту...\n'
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(llm_host, data=json.dumps(llm_agent_request), verify=False, headers=headers) #timeout=120)
                    print(f"FIRST RESPONSE:\n{response}")
                    # clean_response = clean_json(response)#.replace('json','').replace('````','')
                    response_json = response.json()

                    print("FINAL:", response_json, type(response_json["daJsonRequirements"]))
                    # answer = clean_json(str(response_json["daRequirements"]))
                    log_text += 'Получен ответ от LLM агентов...\n'

                    final_json = response_json["daJsonRequirements"].replace("Отчет:", "")
                    if "`" in final_json and "json" in final_json:
                        final_json = extract_json_data(final_json)
                    
                    print("final_json: ", final_json)
                    
                    if not isinstance(final_json, dict):
                        final_json = ast.literal_eval(final_json)

                    final_profile_json = run_final_json_profile(profile_json, final_json)

                    if "daJsonRequirements" in response_json and response_json["daJsonRequirements"]:
                        # info_text += response_json["daJsonRequirements"] + '\n'
                        llm_agent_request["history"]["daJsonRequirements"] = response_json["daJsonRequirements"]

                    log_text += 'Аналитика данных завершена.\n'
                    
                    # print(f"RESULT: {result}")

                    # TODO: !!!! SAVE THIS PROMPT TO prev_context FOR NEXT corrector REQUESTS
                    llm_agent_request["daJsonRequirements"] = False
                    # TODO: ADD PREVIEW
                    llm_agent_request["task"] = f"{final_profile_json}\n"

                elif source_desc.endswith('.xml') or source_desc.endswith('.XML'): 
                    profile_json = run_compute_xml_profile(source_desc)

                    if not isinstance(profile_json, dict):
                        profile_json = ast.literal_eval(profile_json)

                    print("ПРОФИЛЬ XML", profile_json)

                    llm_agent_request["task"] = f"{profile_json}"
                    llm_agent_request["daXmlRequirements"] = True

                    log_text += 'Отправляем запрос к LLM агенту...\n'
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(llm_host, data=json.dumps(llm_agent_request), verify=False, headers=headers) #timeout=120)
                    print(f"FIRST RESPONSE:\n{response}")
                    # clean_response = clean_json(response)#.replace('json','').replace('````','')
                    response_json = response.json()

                    print("FINAL:", response_json, type(response_json["daXmlRequirements"]))
                    # answer = clean_json(str(response_json["daRequirements"]))
                    log_text += 'Получен ответ от LLM агентов...\n'

                    final_json = response_json["daXmlRequirements"].replace("Отчет:", "")
                    if "`" in final_json and "json" in final_json:
                        final_json = extract_json_data(final_json)
                    
                    print("final_json: ", final_json)
                    
                    if not isinstance(final_json, dict):
                        final_json = ast.literal_eval(final_json)

                    final_profile_json = run_final_xml_profile(profile_json, final_json, source_desc)

                    if "daXmlRequirements" in response_json and response_json["daXmlRequirements"]:
                        # info_text += response_json["daXmlRequirements"] + '\n'
                        llm_agent_request["history"]["daXmlRequirements"] = response_json["daXmlRequirements"]

                    log_text += 'Аналитика данных завершена.\n'
                    
                    # print(f"RESULT: {result}")

                    # TODO: !!!! SAVE THIS PROMPT TO prev_context FOR NEXT corrector REQUESTS
                    llm_agent_request["daXmlRequirements"] = False
                    # TODO: ADD PREVIEW
                    llm_agent_request["task"] = f"{final_profile_json}\n"


                elif source_desc.endswith('.csv') or source_desc.endswith('.CSV'):
                    preview, cardinality_text, card_json, types_json, parquet_report = run_build_analytic_prompt(source_desc)

                    llm_agent_request["daRequirements"] = True
                    llm_agent_request["task"] = f"{preview}\n{cardinality_text}"

                    log_text += 'Отправляем запрос к LLM агенту...\n'
                    headers = {"Content-Type": "application/json"}
                    response = requests.post(llm_host, data=json.dumps(llm_agent_request), verify=False, headers=headers) #timeout=120)
                    print(f"FIRST RESPONSE:\n{response}")
                    # clean_response = clean_json(response)#.replace('json','').replace('````','')
                    response_json = response.json()

                    print("FINAL:", response_json, type(response_json["daRequirements"]), card_json)
                    # answer = clean_json(str(response_json["daRequirements"]))
                    log_text += 'Получен ответ от LLM агентов...\n'

                    entity_report = run_build_final_prompt(response_json["daRequirements"], 
                                                            card_json)

                    if "daRequirements" in response_json and response_json["daRequirements"]:
                        # info_text += response_json["daRequirements"] + '\n'
                        llm_agent_request["history"]["daRequirements"] = response_json["daRequirements"]

                    log_text += 'Аналитика данных завершена.\n'
                    
                    # print(f"RESULT: {result}")

                    # TODO: !!!! SAVE THIS PROMPT TO prev_context FOR NEXT corrector REQUESTS
                    llm_agent_request["daRequirements"] = False
                    llm_agent_request["task"] = f"{preview}'\n'{entity_report}'\n'{cardinality_text}'\n'{parquet_report}'\n'{str(types_json)}'\n'"


            llm_agent_request["darchRequirements"] = True
            print(f"SECOND REQUEST:\n{json.dumps(llm_agent_request)}")
            log_text += 'Отправляем запрос к LLM агенту...\n'

            headers = {"Content-Type": "application/json"}
            response = requests.post(llm_host, data=json.dumps(llm_agent_request), verify=False, headers=headers) #timeout=120)
            print(f"SECOND RESPONSE:\n{response}")
            response_json = response.json()
            log_text += 'Получен ответ от LLM агентов...\n'

            # save result
            if "darchRequirements" in response_json and response_json["darchRequirements"]:
                info_text += response_json["darchRequirements"] + '\n'
                llm_agent_request["history"]["darchRequirements"] = response_json["darchRequirements"]

            llm_agent_request["task"] = ""
            llm_agent_request["darchRequirements"] = False

            # TODO: ???
            if "needFix" in llm_agent_request and llm_agent_request["needFix"]:
                if "message" in response_json and response_json["message"]:
                    msg = response_json["message"]
                    chat_history.append({"role": "assistant", "content": msg})
            else:
                # TODO: ???
                llm_agent_request["needFix"] = True

            # TODO: Это полная жопа, либо упростить, либо завернуть в try-catch
            sql_script = ""#extract_sql_data(response_json['darchRequirements'])
            # print("(extract_sql_data)", sql_script)

            # db_type = extract_db_type(response_json['darchRequirements']:50)
            # print("DB_TYPE: " + db_type)
            if source_desc.endswith('.csv') or source_desc.endswith('.CSV'):
                if sql_script:
                    # clean_sql_script = clean_clickhouse_ddl(sql_script)
                    tables = parse_create_tables(sql_script)
                    clean_sql_script = to_dbml_with_refs(tables)
                    print("(clean_clickhouse_ddl)", clean_sql_script)
                    clean_sql_script_path = save_dbml_file(clean_sql_script)
                    # clean_sql_script_path = save_sql_file(clean_sql_script)
                    print("(save_dbml_file)", clean_sql_script_path)
                    # dbml_path = convert_sql_to_dbml(clean_sql_script_path)
                    # print(dbml_path)
                    dbml_svg_path = convert_dbml_to_svg(clean_sql_script_path)
                    print(dbml_svg_path)

                    markdown_content = (
                        f"# Результат в виде DBML схемы\n"
                        f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                    )

                    info_text += markdown_content

            elif source_desc.endswith('.json') or source_desc.endswith('.JSON'):
                ddl_ch = json_get_clickhouse_ddl(final_profile_json) 
                ddl_pg = json_get_postgres_ddl(final_profile_json)
                dbml = json_get_dbml(final_profile_json)

                dbml_path = save_dbml_file(dbml)
                dbml_svg_path = convert_dbml_to_svg(dbml_path)

                markdown_content = (
                        f"# Результат в виде DBML схемы\n"
                        f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                    )

                info_text += markdown_content

            elif source_desc.endswith('.xml') or source_desc.endswith('.XML'):
                ddl_ch = xml_get_clickhouse_ddl(final_profile_json) 
                ddl_pg = xml_get_postgres_ddl(final_profile_json)
                dbml = xml_get_dbml(final_profile_json)

                dbml_path = save_dbml_file(dbml)
                dbml_svg_path = convert_dbml_to_svg(dbml_path)

                markdown_content = (
                        f"# Результат в виде DBML схемы\n"
                        f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                    )

                info_text += markdown_content

            md_file_path = save_markdown_file(info_text)

            # TODO: CALL LLM
            # ddl = generate_ddl(result['schema'], table_name='my_table', target_db=rec['recommendation'])
            log_text += 'DDL сгенерирован.\n'
            ddl, dag = "", ""
            # # TODO: CALL LLM
            # dag = generate_airflow_dag_from_pipeline('example_pipeline', '@hourly', [], rec['recommendation'])
            # log_text += 'DAG сгенерирован.\n'

            return gr.update(visible=True), log_text, info_text, gr.update(value=ddl, visible=True), gr.update(value=dag, visible=True), gr.update(value=llm_agent_request), gr.update(value=chat_history, visible=True), gr.update(visible=True), gr.update(visible=True), gr.update(visible=True, value=md_file_path)


        @submit_button.click(inputs=[user_input, chatbot_ui, llm_agent_request], outputs=[chatbot_ui, user_input, llm_agent_request])
        @user_input.submit(inputs=[user_input, chatbot_ui, llm_agent_request], outputs=[chatbot_ui, user_input, llm_agent_request])
        def submit_and_get_response(user_text, chat_history, llm_agent_request):
            llm_agent_request["task"] = user_text
            print(llm_agent_request)
            response_text = "Информация записана!"
            chat_history.append({"role": "user", "content": user_text})
            chat_history.append({"role": "assistant", "content": response_text})
            _LOGGER.info(f"Запрос пользователя: {user_text}")
            return chat_history, "", gr.update(value=llm_agent_request)
        

        @create_connect_btn.click(inputs=[start_choice, new_source_choice, upload_file_new, upload_file_new2, new_base_conn, existing_conn, log_display, info_box],
                                 outputs=[log_display])
        def on_create_connect(start_choice_val, new_source_sel, upload_new_file, upload_new_file2, new_base_conn_val, existing_conn_val, log_text, info_text):
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                    source_desc = '\n'.join(upload_new_file)
                elif new_source_sel == 'Указать директорию':
                    source_desc = '\n'.join(upload_new_file2)
                else:
                    # на основе существующего
                    source_desc = new_base_conn_val

                # TODO: create bd
                log_text += source_desc + '\n'
                log_text += "Новое хранилище успешно создано.\n"
            else:
                source_desc = existing_conn_val
                
                # TODO: connect to bd
                log_text += "Подключение к хранилищу успешно выполнено.\n"

            return log_text


    return demo

if __name__ == '__main__':
    _LOGGER.info("Starting app...")
    # try:
    demo = run_web_interface()
    demo.launch(server_name="0.0.0.0", server_port=7862)
    # except:
        # reboot_system()
