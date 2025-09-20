import time, hashlib, os
import gradio as gr

from modules import DataProvider, UserData, PipeLine, Summarizer, TextSplitter
from modules import customLogger
import nltk
from settings import NLTK_DATA
import base64
import requests
import json, io
from datetime import datetime
import tempfile

from bpmn_utils import convert_bpmn_to_image, validate_bpmn, save_xml_file, imporve_bpmn_layout, read_xml_file
from utils import css_utils

nltk.download(NLTK_DATA)

_LOGGER = customLogger.getLogger(__name__)

# Функция для хэширования пароля
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

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

def save_markdown_file(md_content: str) -> str:
    fname = datetime.now().strftime("md_%Y%m%d_%H%M%S_%f.md")
    file_path = os.path.join(tempfile.gettempdir(), fname)
    if not file_path.endswith('.md'):
        file_path += '.md'
    
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(md_content)
    print(f"Файл {file_path} успешно сохранён.")
    return file_path

def image_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

# Основной интерфейс приложения
class AppInterface:
    def __init__(self):
        self.text_splitter = TextSplitter()
        self.pipeline = PipeLine()
        self.data_provider = DataProvider()
        self.summarizer = Summarizer()

    def load_users(self):
        return self.data_provider.load_users()
   
    def register_user(self, username, password):
        users = self.load_users()
        if username in users:
            return "Пользователь уже существует."

        hashed_password = hash_password(password)
        self.data_provider.save_user(username, hashed_password)

        return "Регистрация прошла успешно."

    def authenticate_user(self, username, password):
        users = self.load_users()
        return username in users and users[username] == hash_password(password)

    def login_user(self, username, password):
        if self.authenticate_user(username, password):
            db_user_data = self.data_provider.load_data(username)
            _LOGGER.info("Данные загружены в кэш")
            
            return "Успешный вход!", True, db_user_data
        else:
            return "Неправильное имя пользователя или пароль.", False, None

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
    def process_element(self, file_path, user_data, is_logged_in):
        doc_name = os.path.basename(file_path)
        _LOGGER.info(f"Начало обработки файла {doc_name}")
        txt = self.extract_text(file_path)
        print(txt)
        _LOGGER.info(f"Файл обработан: {file_path}")

        return txt

    @log_execution_time
    def process_data(self, mic_inputs, file_inputs, user_data, is_logged_in):
        results = []
        txt_res = ""
        if mic_inputs:
            txt_res += self.process_element(mic_inputs, user_data, is_logged_in)
            results.append(f"Аудио, записанное с микрофона, обработано.")
            
        if file_inputs:
            txt_res += self.process_element(file_inputs, user_data, is_logged_in)
            results.append(f"Аудиофайл загружен и обработан: {os.path.basename(file_inputs)}")
                
        return "\n".join(results), txt_res

    @log_execution_time
    def process_llm_agent_request(self, llm_agent_response, user_data, is_logged_in):
        response_json = llm_agent_response.json()
        result = []
        img_result = None
        if "businessRequirements" in response_json and response_json["businessRequirements"]:
            result.append(response_json["businessRequirements"])
        elif "systemRequirements" in response_json and response_json["systemRequirements"]:
            result.append(response_json["systemRequirements"])
        elif "jsonSchema" in response_json and response_json["jsonSchema"]:
            json_str = f"```json\n{response_json['jsonSchema']}\n```"
            result.append(json_str)
        elif "bpmnSchema" in response_json and response_json["bpmnSchema"]:
            bpmn_path = save_xml_file(response_json['bpmnSchema'])

            xml_improved_path = imporve_bpmn_layout(bpmn_path)
            
            xml_improved_layout = read_xml_file(xml_improved_path)
            print("LAYOUT", xml_improved_layout)

            # errors_info = validate_bpmn(bpmn_path)
            # print(errors_info)
            # _LOGGER.info(errors_info)
            
            # try:
            bpmn_png_path = convert_bpmn_to_image(xml_improved_path)

            # print("Конвертация успешно завершена!")
            # except Exception as e:
            #     print(f"Ошибка: {e}")

            xml_str = f"```xml\n{xml_improved_layout}\n```"
            result.append(xml_str)

            import cv2
            img_result = cv2.imread(bpmn_png_path)
            # result.append(f"""<img src="data:image/png;base64,{image_to_base64(bpmn_png_path)}" width="3200">""")

        markdown_content = "\n\n".join(result)

        # ba_requrements = response_json["businessRequirements"]
        # markdown_content = f"{ba_requrements}"
        #image_path = Path("bpmn_diagram.png").absolute()
        #print(image_path)
        # #markdown_content = f"""
        #     # Результат в виде BPMN диаграммы
        
        #     Картиночка:
        
        #     <img src="data:image/png;base64,{image_to_base64(image_path)}" width="600">
        # """

        return markdown_content, img_result


def run_web_interface(app):
    with gr.Blocks(theme=gr.themes.Glass()) as demo:

        user_data = gr.State(UserData())
        is_logged_in = gr.State(False)

        llm_agent_request = gr.State({
                "systemRequirements": False,
                "jsonSchema": True,
                "businessRequirements": False,
                "bpmnSchema": False,
                "needFix": False,
                "history": {},
                "task": "",
                "jsonAgentHistory": [],
                "requestDateTime": "2025-03-29T01:56:37.567Z"
            })

        with gr.Row():
            username_input = gr.Textbox(label="Имя пользователя", placeholder="Введите имя пользователя", lines=1)
            password_input = gr.Textbox(label="Пароль", placeholder="Введите пароль", type="password", lines=1)
            with gr.Column():
                login_button = gr.Button(value="Войти")
                register_button = gr.Button(value="Регистрация")
                display_user = gr.Markdown(value="", visible=False)
                logout_button = gr.Button(value="Выйти", visible=False)

        CHECKBOX_FIELDS = ["systemRequirements", "jsonSchema", "businessRequirements", "bpmnSchema"]
        with gr.Row():
            checkboxes = gr.CheckboxGroup(
                choices=CHECKBOX_FIELDS,
                value=[key for key in CHECKBOX_FIELDS 
                        if llm_agent_request.value[key]],
                label="Выбранные опции"
            )
            with gr.Column():
                process_button = gr.Button("Сгенерировать результат")
                download_button = gr.DownloadButton("Скачать результат", visible=False)
        
        image_display = gr.ImageEditor(visible=False, type="numpy", brush=False, fixed_canvas=True)

        with gr.Row():
            with gr.Column():
                chatbot_ui = gr.Chatbot(label="Чат-бот", type="messages")

                with gr.Row():
                    user_input = gr.Textbox(scale=30, label="", placeholder="Введите текст...", lines=1)
                    submit_button = gr.Button(scale=1, value="➤", elem_id="submit_button")

            with gr.Column():
                markdown_result = gr.Markdown(label="Результат") # lines=17, interactive=False

        with gr.Tab("🎤 Запись с микрофона"):
            mic_input = gr.Audio(sources=["microphone"], type="filepath", label="Говорите...")
            # process_button_micro = gr.Button("Сгенерировать результат")
        
        with gr.Tab("📁 Загрузить файл"):
            file_input = gr.Audio(sources=["upload"], type="filepath", label="Загрузите аудио")
            # process_button_file = gr.Button("Сгенерировать результат")

        output = gr.Textbox(label="Результат обработки", lines=5, interactive=False, visible=False)


        @checkboxes.change(inputs=[checkboxes],
                            outputs=llm_agent_request)
        def update_agent_request(selected_options):
            updated_request = llm_agent_request.value
            for option in CHECKBOX_FIELDS:
                updated_request[option] = (option in selected_options)
            return (gr.update(value=updated_request))

        @mic_input.change(inputs=[mic_input, file_input, user_data, is_logged_in, llm_agent_request, chatbot_ui],
                              outputs=[output, llm_agent_request, chatbot_ui])
        @file_input.change(inputs=[mic_input, file_input, user_data, is_logged_in, llm_agent_request, chatbot_ui],
                              outputs=[output, llm_agent_request, chatbot_ui])
        def process_data_and_save(mic_inputs, file_inputs, user_data, is_logged_in, llm_agent_request, chat_history):
            message, txt_res = app.process_data(mic_inputs, file_inputs, user_data, is_logged_in)
            if txt_res:
                chat_history.append({"role":"user", "content":txt_res})
            llm_agent_request["task"] += txt_res + " "
            return (
                message, gr.update(value=llm_agent_request), chat_history
            )

        @register_button.click(inputs=[username_input, password_input])
        def register_and_message(username, password):
            message = app.register_user(username, password)
            gr.Info(message)

        @login_button.click(inputs=[username_input, password_input, user_data, is_logged_in],
                            outputs=[is_logged_in, user_data, username_input, password_input, login_button, register_button, display_user, logout_button])
        def login_and_update_ui(username, password, user_data, login):
            # Логика логина пользователя
            message, authenticated, db_user_data = app.login_user(username, password)
            gr.Info(message)
            if authenticated:
                user_data.user_name = username
                user_data.user_password = password
                user_data.load_from_db(db_user_data)
                _LOGGER.info(f"Пользователь {user_data.user_name} успешно залогинен")
                
                file_paths = json.loads(db_user_data.video_path)
                file_names = [os.path.basename(path) for path in file_paths]
                _LOGGER.info(f"{file_names}")
                
                return (
                    authenticated,                                              # is__LOGGER_in
                    user_data,                                                  # user_data
                    gr.update(visible=False),                                   # username_input
                    gr.update(visible=False),                                   # password_input
                    gr.update(visible=False),                                   # login_button
                    gr.update(visible=False),                                   # register_button
                    gr.update(visible=True, value="Привет, " + username + "!"), # display_user
                    gr.update(visible=True),                                    # logout_button
                )
            else:
                return (
                    authenticated,                          # is__LOGGER_in
                    user_data,                              # user_data
                    gr.update(),                            # username_input
                    gr.update(),                            # password_input
                    gr.update(visible=True),                # login_button
                    gr.update(visible=True),                # register_button
                    gr.update(visible=False),               # display_user
                    gr.update(visible=False),               # logout_button
                )

        @logout_button.click(inputs=[is_logged_in, user_data],
                             outputs=[user_data, is_logged_in, username_input, password_input, login_button, register_button, display_user, logout_button])
        def logout_and_update_ui(is_logged_in, user_data):
            gr.Info("Выход из аккаунта!")
            _LOGGER.info(f"Пользователь {user_data.user_name} вышел из приложения")
            return (
                UserData(),                             # user_data
                is_logged_in,                           # is_logged_in
                gr.update(visible=True),                # username_input
                gr.update(visible=True),                # password_input
                gr.update(visible=True),                # login_button
                gr.update(visible=True),                # register_button
                gr.update(visible=False),               # display_user
                gr.update(visible=False),               # logout_button
            )

        @submit_button.click(inputs=[user_input, chatbot_ui, user_data, llm_agent_request], outputs=[chatbot_ui, user_input, llm_agent_request])
        @user_input.submit(inputs=[user_input, chatbot_ui, user_data, llm_agent_request], outputs=[chatbot_ui, user_input, llm_agent_request])
        def submit_and_get_response(user_text, chat_history, user_data, llm_agent_request):
            llm_agent_request["task"] = user_text
            print(llm_agent_request)
            response = "Информация записана!"#user_data.get_answer(user_text)
            chat_history.append({"role": "user", "content": user_text})
            chat_history.append({"role": "assistant", "content": response})
            _LOGGER.info(f"Запрос пользователя: {user_text}")
            return chat_history, "", gr.update(value=llm_agent_request)

        @process_button.click(inputs=[user_data, is_logged_in, chatbot_ui, llm_agent_request],
                              outputs=[markdown_result, download_button, user_data, chatbot_ui, llm_agent_request, image_display])
        def process_llm_agent_request(user_data, is_logged_in, chat_history, llm_agent_request):

            if llm_agent_request["task"]:
                llm_host = "http://agent_app:7861/llm_agents"
                headers = {"Content-Type": "application/json"}
                print(f"REQUEST: {json.dumps(llm_agent_request)}")
                response = requests.post(llm_host, data=json.dumps(llm_agent_request), verify=False, headers=headers) #timeout=120)
                print(f"RESPONSE: {response}")
                response_json = response.json()

                if "businessRequirements" in response_json and response_json["businessRequirements"]:
                    llm_agent_request["history"]["businessRequirements"] = response_json["businessRequirements"]
                if "systemRequirements" in response_json and response_json["systemRequirements"]:
                    llm_agent_request["history"]["systemRequirements"] = response_json["systemRequirements"]
                if "jsonSchema" in response_json and response_json["jsonSchema"]:
                    llm_agent_request["history"]["jsonSchema"] = response_json["jsonSchema"]
                if "bpmnSchema" in response_json and response_json["bpmnSchema"]:
                    llm_agent_request["history"]["bpmnSchema"] = response_json["bpmnSchema"]

                llm_agent_request["task"] = ""

                if "needFix" in llm_agent_request and llm_agent_request["needFix"]:
                    if "message" in response_json and response_json["message"]:
                        msg = response_json["message"]
                        chat_history.append({"role": "assistant", "content": msg})
                    md_content, img_result = app.process_llm_agent_request(response, user_data, is_logged_in)
                    file_path = save_markdown_file(md_content)
                    make_image_visible = not img_result is None
                    return (
                        gr.update(value=md_content),                # markdown_result
                        gr.update(visible=True, value=file_path),   # download_button
                        gr.update(value=llm_agent_request),         # user_data
                        chat_history,                               # chatbot_ui
                        gr.update(value=llm_agent_request),         # llm_agent_request
                        gr.update(value=img_result, visible=make_image_visible)   # image_display
                    )
                if "needInfo" in response_json and response_json["needInfo"]:
                    msg = response_json["message"]
                    chat_history.append({"role": "assistant", "content": msg})
                    llm_agent_request["jsonAgentHistory"] = response_json["jsonAgentHistory"]
                    return (
                        gr.update(),
                        gr.update(),
                        gr.update(value=llm_agent_request),
                        chat_history,
                        gr.update(value=llm_agent_request),
                        gr.update()
                    )
                else:
                    md_content, img_result = app.process_llm_agent_request(response, user_data, is_logged_in)
                    file_path = save_markdown_file(md_content)
                    make_image_visible = not img_result is None
                    llm_agent_request["needFix"] = True
                    return (
                        gr.update(value=md_content),               # markdown_result
                        gr.update(visible=True, value=file_path),  # download_button
                        gr.update(value=llm_agent_request),        # user_data
                        chat_history,                              # chatbot_ui
                        gr.update(value=llm_agent_request),        # llm_agent_request
                        gr.update(value=img_result, visible=make_image_visible)                # image_display
                    )
            else:
                gr.Info("Для генерации необходимо ввести описание задачи!")
                return (
                    gr.update(), 
                    gr.update(),
                    gr.update(value=llm_agent_request),
                    gr.update(value=llm_agent_request),
                    chat_history,
                    gr.update(value=llm_agent_request),
                    gr.update()
                )

        # download_button.click(outputs=[download_button])

    # CSS для выравнивания блоков
    demo.css = css_utils.css_settings

    return demo

if __name__ == "__main__":
    _LOGGER.info("Starting app...")
    # try:
    app_instance = AppInterface()
    demo = run_web_interface(app_instance)
    demo.launch(server_name="0.0.0.0", server_port=7862)
    # except:
        # reboot_system()
