from .singleton import Singleton
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage
import base64
import os
from settings import OpenAISettings

class Summarizer(Singleton):
  def _setup(self):
    self.prompt_text = """Ты прекрасно суммаризируешь любую информацию. Суммаризируй информацию и выделите главное. Ответ дай на языке входного контекста. {element}"""
    self.prompt_picture = """Опиши изображение"""
    self.prompt_title = """Тебе на вход подается текст. Придумай название для текста из одного предложения. Ответ дай на языке входного контекста. {element}"""

    self.model_name = OpenAISettings.model
    self.model = ChatOpenAI(temperature=0, model=self.model_name)
    self.summarize_parameters = {"max_concurrency": 4 }

  def generate_summaries(self, objects, summarize=False):
    """
    Функция для создания суммаризации текста и таблиц с использованием модели GPT.

    Аргументы:
    objects: Список строк (тексты), которые нужно суммировать.
    summarize: Булев флаг, указывающий, нужно ли суммировать текстовые элементы.

    Возвращает:
    Список суммаризаций
    """
    summaries = objects.copy()
    if summarize:
      prompt = ChatPromptTemplate.from_template(self.prompt_text)
      summarize_chain = {"element": lambda x: x} | prompt | self.model | StrOutputParser()

      summaries = summarize_chain.batch(objects, self.summarize_parameters)

    return summaries

  def generate_title(self, text):
    """
    Функция для создания названия текста с использованием модели GPT.

    Аргументы:
    objects: Список строк (тексты), которые нужно суммировать.

    Возвращает:
    Список суммаризаций
    """
    prompt = ChatPromptTemplate.from_template(self.prompt_title)
    title_chain = {"element": lambda x: x} | prompt | self.model | StrOutputParser()
    title = title_chain.invoke(text)
    return title

  def generate_summary(self, text):
    """
    Функция для создания названия текста с использованием модели GPT.

    Аргументы:
    objects: Список строк (тексты), которые нужно суммировать.

    Возвращает:
    Список суммаризаций
    """
    prompt = ChatPromptTemplate.from_template(self.prompt_text)
    summary_chain = {"element": lambda x: x} | prompt | self.model | StrOutputParser()
    summary = summary_chain.invoke(text)
    return summary

  # Функция кодирования изображения в формат base64
  def encode_image(image_path):
    """
    Функция для кодирования изображения в формат base64.

    Аргументы:
    image_path: Строка, путь к изображению, которое нужно закодировать.

    Возвращает:
    Закодированное в формате base64 изображение в виде строки.
    """
    with open(image_path, "rb") as image_file:
        # Читаем файл изображения в бинарном режиме и кодируем в base64
        return base64.b64encode(image_file.read()).decode("utf-8")


  def image_summarize(self, img_base64):
    """
    Функция для получения суммаризации изображения с использованием GPT модели.

    Аргументы:
    img_base64: Строка, изображение закодированное в формате base64.

    Возвращает:
    Суммаризация изображения, возвращенная моделью GPT.
    """
    prompt = self.prompt_picture

    msg = self.model.invoke(
        [
            HumanMessage(
                content=[
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{img_base64}"},
                    },
                ]
            )
        ]
    )
    return msg.content


  def generate_img_summaries(self, path):
    """
    Функция для генерации суммаризаций изображений из указанной директории.

    Аргументы:
    path: Строка, путь к директории с изображениями формата .jpg.

    Возвращает:
    Два списка:
    - img_base64_list: Список закодированных изображений в формате base64.
    - image_summaries: Список суммаризаций для каждого изображения.
    """
    img_base64_list = []
    image_summaries = []

    for img_file in sorted(os.listdir(path)):
        if img_file.endswith(".jpg"):
            img_path = os.path.join(path, img_file)
            base64_image = self.encode_image(img_path)
            img_base64_list.append(base64_image)
            image_summaries.append(self.image_summarize(base64_image))

    return img_base64_list, image_summaries