from settings import OpenAISettings
import base64
import re
from PIL import Image
import io

from langchain_core.runnables import RunnableLambda, RunnablePassthrough
from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.documents import Document

from langchain.retrievers.multi_vector import MultiVectorRetriever
from langchain.storage import InMemoryStore
from langchain_community.vectorstores import Chroma

from langchain_openai import OpenAIEmbeddings
from langchain_openai import ChatOpenAI

class ContentBasedSystemQA:

    def __init__(self):
        """
        Создает RAG цепочку для работы с мультимодальными запросами, включая текст и изображения.

        Возвращает:
        Цепочка для обработки запросов с учетом текста и изображений.
        """
        self.model_name = OpenAISettings.model
        self.id_key = "doc_id"

        self.retriever = MultiVectorRetriever(
            vectorstore=Chroma(collection_name="custom_rag_collection",
                               embedding_function=OpenAIEmbeddings()),
            docstore=InMemoryStore(),
            id_key=self.id_key,
        )

        self.chain = (
            {
                "context": self.retriever | self.prepare_context_data,
                "question": RunnablePassthrough(),
            }
            | RunnableLambda(self.prompt_func)
            | ChatOpenAI(temperature=0, model=self.model_name, max_tokens=1024)
            | StrOutputParser()
        )

    def get_answer(self, query):
        return self.chain.invoke(query)

    def update_vectorstore(self, summary_docs):
        self.retriever.vectorstore.add_documents(summary_docs)

    def update_docstore(self, contents):
        self.retriever.docstore.mset(contents)

    def prompt_func(self, data_dict):
        """
        Формирует запрос к модели с учетом изображений и текста.

        Аргументы:
        data_dict: Словарь, содержащий тексты и изображения, а также вопрос пользователя.

        Возвращает:
        Список сообщений для отправки модели.
        """
        formatted_texts = "\n".join(data_dict["context"]["texts"])
        messages = []
        if data_dict["context"]["images"]:
            for image in data_dict["context"]["images"]:
                image_message = {
                    "type": "image_url",
                    "image_url": {"url": f"data:image/jpeg;base64,{image}"},
                }
                messages.append(image_message)

        text_message = {
            "type": "text",
            "text": (
                "Сгенерируй ответ на вопрос пользователя, опираясь на текст."
                "Если пользователь просит придумать или сгенерировать ответ, используй текст для генерации."
                "Если пользователь задает вопрос, дай ответ с исвользованием текста."
                "Если текст не содержит полезной информации для ответа на вопрос, ответь Нет информации."
                "Ответ дай на языке запроса.\n\n"
                f"Вопрос пользователя: {data_dict['question']}\n\n"
                "Tекст:\n"
                f"{formatted_texts}"
            ),
        }
        messages.append(text_message)
        return [HumanMessage(content=messages)]

    def prepare_context_data(self, docs):
        """
        Разделяет документы на изображения и текстовые данные.

        Аргументы:
        docs: Список документов, содержащих изображения (в формате base64) и текст.

        Возвращает:
        Словарь с двумя списками: изображения и тексты.
        """
        b64_images = []
        texts = []
        for doc in docs:
            if isinstance(doc, Document):
                doc = doc.page_content
                texts.append(doc)
            if self.looks_like_base64(doc) and self.is_image_data(doc):
                doc = self.resize_base64_image(doc, size=(1300, 600))
                b64_images.append(doc)
            else:
                texts.append(doc)
        return {"texts": texts, "images": b64_images}

    def looks_like_base64(self, sb):
        """
        Проверяет, выглядит ли строка как base64.

        Аргументы:
        sb: Строка для проверки.

        Возвращает:
        True, если строка выглядит как base64, иначе False.
        """
        return re.match("^[A-Za-z0-9+/]+[=]{0,2}$", sb) is not None

    def is_image_data(self, b64data):
        """
        Проверяет, является ли base64 данные изображением, проверяя сигнатуры данных.

        Аргументы:
        b64data: Строка base64, представляющая изображение.

        Возвращает:
        True, если данные начинаются с сигнатуры изображения, иначе False.
        """
        image_signatures = {
            b"\xFF\xD8\xFF": "jpg",
            b"\x89\x50\x4E\x47\x0D\x0A\x1A\x0A": "png",
            b"\x47\x49\x46\x38": "gif",
            b"\x52\x49\x46\x46": "webp",
        }
        try:
            header = base64.b64decode(b64data)[:8]
            for sig, format in image_signatures.items():
                if header.startswith(sig):
                    return True
            return False
        except Exception:
            return False

    def resize_base64_image(self, base64_string, size=(128, 128)):
        """
        Изменяет размер изображения, закодированного в формате base64.

        Аргументы:
        base64_string: Строка base64, представляющая изображение.
        size: Новый размер изображения.

        Возвращает:
        Закодированное в формате base64 изображение нового размера.
        """
        img_data = base64.b64decode(base64_string)
        img = Image.open(io.BytesIO(img_data))

        # Изменение размера изображения с использованием алгоритма LANCZOS для улучшения качества
        resized_img = img.resize(size, Image.LANCZOS)

        buffered = io.BytesIO()
        resized_img.save(buffered, format=img.format)

        return base64.b64encode(buffered.getvalue()).decode("utf-8")