import gradio as gr
import json, os, time, re, ast
import datetime
import requests

from gradio_utils import *

from scripts.analytic_pipeline import csv_profile2json, clean_json, run_build_analytic_prompt, run_build_final_prompt, csv_get_postgres_ddl, csv_get_clickhouse_ddl, csv_get_dbml
from scripts.json_analytic_pipeline import run_compute_json_profile, run_final_json_profile, json_get_postgres_ddl, json_get_clickhouse_ddl, json_get_dbml
from scripts.xml_analytic_pipeline import run_compute_xml_profile, run_final_xml_profile, xml_get_postgres_ddl, xml_get_clickhouse_ddl, xml_get_dbml

from scripts.run_etl import run_etl_pg, run_etl_ch, drop_db_pg, drop_database_pg, check_db_pg

from modules import customLogger

from file_utils import *

_LOGGER = customLogger.getLogger(__name__)

llm_host = "http://agent_app:7861/llm_agents"

# FOR SERVER
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

        gr.Markdown(''' # IDEA! - Intelligent Data Engineering Assistant (Команда JaJaBinx) ''')

        data_path = gr.State("")
        new_db_type = gr.State("")
        ddl_script = gr.State("")
        bd_list = gr.State([])
        final_profile = gr.State(dict())

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
                with gr.Row():
                    gr.Image(
                        value="logo.jpg",       
                        show_download_button=False,
                        show_label=False,
                        type="filepath" # можно "numpy" или "pil"
                    )
                    with gr.Column():
                        start_choice = gr.Radio(choices=['Создать новое хранилище'], value=None, label='Что вы хотите сделать?')
                        new_source_choice = gr.Dropdown(choices=['Загрузить файлы (CSV/JSON/XML)'], value='Загрузить файлы (CSV/JSON/XML)', label='Источник:', visible=False)
                    
                upload_file_new = gr.File(label='Перенесите файлы мышкой или выберите в открывшемся окне.', file_count='multiple', visible=False)

                existing_conn = gr.Textbox(label='Connection string для подключения к существующему хранилищу', placeholder='postgres://user:pass@host:5432/db', visible=False)
                
                # with gr.Row():
                #     new_db_address = gr.Textbox(label='Адрес', placeholder='localhost', visible=False)
                #     new_db_port = gr.Textbox(label='Порт', placeholder='5432', visible=False)
                
                with gr.Row():
                    new_db_user = gr.Textbox(label='Имя пользователя', placeholder='arseniy', visible=False)
                    new_db_password = gr.Textbox(label='Пароль', placeholder='12345', visible=False)
                    new_db_name = gr.Textbox(label='Имя хранилища', placeholder='analytics', visible=False)

                with gr.Row():
                    with gr.Column():
                        chatbot_ui = gr.Chatbot(label="Чат-бот", type="messages", visible=False)

                        with gr.Row():
                            user_input = gr.Textbox(scale=30, label="", placeholder="Введите текст...", lines=1, visible=False)
                            submit_button = gr.Button(scale=1, value="➤", elem_id="submit_button", visible=False)

                analytic_btn = gr.Button('Загрузить данные, сделать аналитику', visible=False)
                create_connect_btn = gr.Button('Создать хранилище (загрузить данные)', visible=False)

            with gr.Column(scale=1):
                with gr.Row():
                    listbox = gr.Dropdown(label="Список доступных БД", interactive=True)
                
                with gr.Row():
                    connect_btn = gr.Button("Подключиться", interactive=True, visible=False)
                    drop_btn = gr.Button("Удалить", interactive=True, visible=False)

                log_display = gr.Textbox(value="", label="Логгирование", lines=5, max_lines=5, interactive=False, show_copy_button=True)
                md_download_button = gr.DownloadButton("Скачать отчет", visible=False)
                info_box = gr.Markdown(value="", label='Отчет/Рекомендации', visible=True)


        @demo.load(inputs=[bd_list], outputs=[bd_list])
        def load_bd_list(bd_list):
            path = "bd_list.txt"
            if os.path.exists(path):
                with open("bd_list.txt", "r") as f:
                    lines = [line.strip() for line in f]

            return gr.update(value=bd_list)

        @bd_list.change(inputs=[bd_list], outputs=[])
        def save_bd_list(bd_list):
            with open("bd_list.txt", "w") as f:
                for line in bd_list:
                    f.write(line + "\n")

        @demo.load(inputs=bd_list, outputs=listbox)
        def update_bd_list(bd_list): 
            return gr.update(choices=bd_list)

        @listbox.change(inputs=[], outputs=[connect_btn, drop_btn])
        def select_item():
            return gr.update(visible=True), gr.update(visible=True)
        
        @connect_btn.click(inputs=[listbox, log_display], 
                            outputs=[log_display])
        def connect_to_bd(bd_name, info_text):
            
            info = check_db_pg(bd_name)#, final_profile_json)
            info_text += "Подключено: " + bd_name + '\n'
            info_text += info

            return info_text

        @drop_btn.click(inputs=[listbox, bd_list, final_profile], 
                        outputs=[log_display, listbox])
        def delete_bd(bd_name, bd_list, final_profile_json):
            # TODO: change to variable
            drop_database_pg(bd_name) 
            # drop_db_pg(bd_name, final_profile_json)
            bd_list.remove(bd_name)
            info_text = "Удалено: " + bd_name + '\n'
            return info_text, gr.update(choices=bd_list, value=None)

        @start_choice.change(inputs=[start_choice], 
                            outputs=[new_source_choice, upload_file_new, existing_conn])
        def on_start(choice):
            if choice == 'Создать новое хранилище':
                return gr.update(visible=True), gr.update(visible=True), gr.update(visible=False)
            # elif choice == 'Подключиться к существующему хранилищу':
            #     return gr.update(visible=False), gr.update(visible=False), gr.update(visible=False), gr.update(visible=True)
            else:
                return [gr.update(visible=False)]*3

        
        @new_source_choice.change(inputs=[new_source_choice], 
                                 outputs=[upload_file_new, analytic_btn])
        def on_new_source_change(sel):
            if sel == 'Загрузить файлы (CSV/JSON/XML)':
                return gr.update(visible=True), gr.update(visible=False)
            # if sel == 'Указать директорию':
            #     return gr.update(visible=False), gr.update(visible=True), gr.update(visible=False), gr.update(visible=False)
            # return gr.update(visible=False), gr.update(visible=False), gr.update(visible=True), gr.update(visible=False)


        @upload_file_new.change(inputs=[upload_file_new],
                                outputs=[new_db_user, new_db_password, new_db_name])
        def upload_files(files):
            return [gr.update(visible=True)]*3


        @existing_conn.change(inputs=[existing_conn], 
                              outputs=[analytic_btn])
        def on_existing_conn_change(conn):
            return gr.update(visible=bool(conn and str(conn).strip()))

        @new_db_user.change(inputs=[new_db_user, new_db_password, new_db_name], 
                                outputs=[analytic_btn])
        def on_new_db_user_change(user, password, db_name):
            value = bool(user and password and db_name and str(user).strip())
            return gr.update(visible=value)

        @new_db_password.change(inputs=[new_db_user, new_db_password, new_db_name], 
                                outputs=[analytic_btn])
        def on_new_db_password_change(user, password, db_name):
            value = bool(user and password and db_name and str(password).strip())
            return gr.update(visible=value)

        @new_db_name.change(inputs=[new_db_user, new_db_password, new_db_name], 
                                outputs=[analytic_btn])
        def on_new_db_name_change(user, password, db_name):
            value = bool(user and password and db_name and str(db_name).strip())
            return gr.update(visible=value)


        @analytic_btn.click(inputs=[new_db_name, final_profile, start_choice, new_source_choice, upload_file_new, existing_conn, log_display, info_box, llm_agent_request, data_path, chatbot_ui], 
                            outputs=[final_profile, ddl_script, new_db_type, create_connect_btn, log_display, info_box, llm_agent_request, data_path, chatbot_ui, user_input, submit_button, md_download_button])
        def on_analytic(db_address, final_profile_dict, start_choice_val, new_source_sel, upload_new_file, existing_conn_val, log_text, info_text, llm_agent_request, data_path, chat_history):
            # TODO: !
            # if not new_db_val:
            #     return gr.Info(f"Отсутствует информация про хранилище!")

            final_profile_json = final_profile_dict
            # TODO: replace somewhere to global level
            source_desc = ""
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                    source_desc = upload_new_file[0]

            if "needFix" in llm_agent_request and llm_agent_request["needFix"]:
                info_text = ""
            else:
                if not source_desc:
                    if start_choice_val == 'Создать новое хранилище':
                        if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                            source_desc = upload_new_file[0] # TODO: read list of paths
                        # elif new_source_sel == 'Указать директорию':
                        #     source_desc = upload_new_file2[0] # TODO: read list of paths
                        # else:
                        #     source_desc = new_base_conn_val
                    # else:
                    #     source_desc = existing_conn_val

                    log_text += 'Данные успешно загружены.\n'

                # _LOGGER.info(f"PATH: {source_desc}")
                log_text += f'Обрабатываем файл: {source_desc}\n'

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

                    final_profile2 = run_build_final_prompt(response_json["daRequirements"], 
                                                            card_json)

                    if "daRequirements" in response_json and response_json["daRequirements"]:
                        # info_text += response_json["daRequirements"] + '\n'
                        llm_agent_request["history"]["daRequirements"] = response_json["daRequirements"]

                    log_text += 'Аналитика данных завершена.\n'
                    
                    # print(f"RESULT: {result}")

                    # TODO: !!!! SAVE THIS PROMPT TO prev_context FOR NEXT corrector REQUESTS
                    llm_agent_request["daRequirements"] = False
                    llm_agent_request["task"] = f"{preview}'\n'{final_profile2}'\n'{cardinality_text}'\n'{parquet_report}'\n'{str(types_json)}'\n'"

                    final_profile_json = csv_profile2json(source_desc)
                    print("VOVA:", final_profile_json, type(final_profile_json))

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

            ddl = ""
            db_type = extract_db_type(response_json['darchRequirements'][:50])
            print("DB_TYPE: " + db_type)
            print("source_desc:", source_desc)
            
            if source_desc.endswith('.csv') or source_desc.endswith('.CSV'):

                if db_type == "PostgreSQL":
                    ddl = csv_get_postgres_ddl(final_profile_json)
                elif db_type == "ClickHouse":
                    ddl = csv_get_clickhouse_ddl(final_profile_json) 

                dbml = csv_get_dbml(final_profile_json)
                dbml_path = save_dbml_file(dbml)
                dbml_svg_path = convert_dbml_to_svg(dbml_path)


                markdown_content = (
                    f"# Результат в виде DBML схемы\n"
                    f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                )

                info_text += "# Итоговый DDL скрипт\n" + md_code_chunk_from_escaped(ddl) + "\n\n"
                info_text += markdown_content

            elif source_desc.endswith('.json') or source_desc.endswith('.JSON'):
                
                if db_type == "PostgreSQL": 
                    ddl = json_get_postgres_ddl(final_profile_json)
                elif db_type == "ClickHouse":
                    ddl_ch = json_get_clickhouse_ddl(final_profile_json)
                
                dbml = json_get_dbml(final_profile_json)
                dbml_path = save_dbml_file(dbml)
                dbml_svg_path = convert_dbml_to_svg(dbml_path)

                markdown_content = (
                    f"# Результат в виде DBML схемы\n"
                    f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                )

                info_text += "# Итоговый DDL скрипт\n" + md_code_chunk_from_escaped(ddl) + "\n\n"
                info_text += markdown_content

            elif source_desc.endswith('.xml') or source_desc.endswith('.XML'):
                
                if db_type == "PostgreSQL": 
                    ddl = xml_get_postgres_ddl(final_profile_json)
                elif db_type == "ClickHouse":
                    ddl = xml_get_clickhouse_ddl(final_profile_json)

                dbml = xml_get_dbml(final_profile_json)
                dbml_path = save_dbml_file(dbml)
                dbml_svg_path = convert_dbml_to_svg(dbml_path)

                markdown_content = (
                    f"# Результат в виде DBML схемы\n"
                    f"<img src=\"data:image/svg+xml;base64,{image_to_base64(dbml_svg_path)}\" width=\"450\"/>\n"
                )

                info_text += "# Итоговый DDL скрипт\n" + md_code_chunk_from_escaped(ddl) + "\n\n"
                info_text += markdown_content

            md_file_path = save_markdown_file(info_text)

            log_text += 'DDL сгенерирован.\n'
            print("ARSENIY", ddl)
            log_text += 'Отчет готов.\n'

            return final_profile_json, ddl, db_type, gr.update(visible=True), log_text, info_text, gr.update(value=llm_agent_request), gr.update(value=data_path), gr.update(value=chat_history, visible=True), gr.update(visible=True), gr.update(visible=True), gr.update(visible=True, value=md_file_path)


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
        

        @create_connect_btn.click(inputs=[new_db_type, new_db_name, ddl_script, bd_list, final_profile, start_choice, new_source_choice, upload_file_new, existing_conn, log_display, info_box],
                                 outputs=[log_display, bd_list, listbox])
        def on_create_connect(db_type, db_name, ddl, bd_list_ui, profile, start_choice_val, new_source_sel, upload_new_file, existing_conn_val, log_text, info_text):
            source_desc = 'unknown'
            if start_choice_val == 'Создать новое хранилище':
                if new_source_sel == 'Загрузить файлы (CSV/JSON/XML)':
                    source_desc = upload_new_file[0] # TODO: read list of paths
            #     elif new_source_sel == 'Указать директорию':
            #         source_desc = upload_new_file2[0] # TODO: read list of paths
            #     else:
            #         source_desc = new_base_conn_val
            # else:
            #     source_desc = existing_conn_val
                
                # log_text += "Подключение к хранилищу успешно выполнено.\n"
            
            print("(on_create_connect)", ddl, profile)

            URI = ""
            if db_type == "PostgreSQL":
                URI = f"postgresql://myuser:mypass@db:5432/{db_name}"
                run_etl_pg(URI, db_name, ddl, profile, source_desc)
            elif db_type == "ClickHouse":
                URI = f"http://127.0.0.1:8123/{db_name}"    
                run_etl_ch(db_name, ddl, profile, source_desc)

            log_text += "Успешно создано хранилище: " + URI + "\n"
            
            # bd_list_ui.append(URI)
            new_bd_list = bd_list_ui + [URI]

            return log_text, new_bd_list, gr.update(choices=new_bd_list)


    return demo

if __name__ == '__main__':
    _LOGGER.info("Starting app...")
    # try:
    demo = run_web_interface()
    demo.launch(server_name="0.0.0.0", server_port=7862)
    # except:
        # reboot_system()
