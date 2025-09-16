from .dataProvider import DataProvider
from .qa import ContentBasedSystemQA
from .cutomLogger import customLogger

import uuid, json
from copy import deepcopy

from langchain_core.documents import Document

_LOGGER = customLogger.getLogger(__name__)

class UserData:
    def __init__(self):
        self.provider = DataProvider()
        self.qa_system = ContentBasedSystemQA()
        self.user_name = "basic"
        self.user_password = "basic"

    def __deepcopy__(self, memo):
        copied = UserData()
        # Скопируйте все остальные состояния
        # copied.some_other_state = deepcopy(self.some_other_state, memo)
        return copied
    
    def get_answer(self, user_text):
        return self.qa_system.get_answer(user_text)

    def add_data_to_retriever(self, video_path, doc_summaries, doc_contents):
        """
        Функция для добавления документов и их метаданных в базу данных.

        Аргументы:
        doc_summaries: Список суммаризаций документов.
        doc_contents: Список исходных содержимых документов.
        """
        
        # Генерируем уникальные идентификаторы для каждого документа
        doc_ids = [str(uuid.uuid4()) for _ in doc_contents]

        # Создаем документы для векторного хранилища из суммаризаций
        summary_docs = [
            Document(page_content=s, metadata={self.qa_system.id_key: doc_ids[i]})
            for i, s in enumerate(doc_summaries)
        ]

        # Добавляем документы в векторное хранилище
        self.qa_system.update_vectorstore(summary_docs)

        # Добавляем метаданные документов в хранилище
        self.qa_system.update_docstore(list(zip(doc_ids, doc_contents)))

        _LOGGER.info(f"Успешно загружено локально:{video_path}")

    def load_from_db(self, data):
        # TODO: check for a few data elements
        try:
            data_count = len(json.loads(data.video_path))
        except:
            data_count = 0
        _LOGGER.info(f"Found {data_count} data elements.")
        if data_count:
            self.video_path_data = json.loads(data.video_path)
            self.doc_summaries_data = json.loads(data.doc_summaries)
            self.doc_contents_data = json.loads(data.doc_contents)
            self.add_data_to_retriever(self.video_path_data, 
                                       self.doc_summaries_data,
                                       self.doc_contents_data)
            
            _LOGGER.info("Данные успешно загружены из базы данных.")

    def save_to_db(self, video_path, doc_summaries, doc_contents, username, password):
        self.provider.save_data(username, password, video_path, doc_summaries, doc_contents)
        _LOGGER.info("Успешно загружено в базу данных.")