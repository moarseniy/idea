from .singleton import Singleton
from settings import StorageSettings
import uuid
import json

from langchain.retrievers.multi_vector import MultiVectorRetriever
from langchain.storage import InMemoryStore
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings

class Storage(Singleton):
    def _setup(self):
        self.storage = InMemoryStore()
        self.id_key = "doc_id"
        self.vectorstore = Chroma(collection_name="custom_rag_collection", embedding_function=OpenAIEmbeddings())
        self.retriever = MultiVectorRetriever(
            vectorstore=self.vectorstore,
            docstore=self.storage,
            id_key=self.id_key,
        )

    def add_documents(self, doc_summaries, doc_contents):
        """
        Функция для добавления документов и их метаданных в ритривер.

        Аргументы:
        doc_summaries: Список суммаризаций документов.
        doc_contents: Список исходных содержимых документов.
        """
        # Генерируем уникальные идентификаторы для каждого документа
        doc_ids = [str(uuid.uuid4()) for _ in doc_contents]

        # Создаем документы для векторного хранилища из суммаризаций
        summary_docs = [
            Document(page_content=s, metadata={self.id_key: doc_ids[i]})
            for i, s in enumerate(doc_summaries)
        ]

        # Добавляем документы в векторное хранилище
        self.retriever.vectorstore.add_documents(summary_docs)

        # Добавляем метаданные документов в хранилище
        self.retriever.docstore.mset(list(zip(doc_ids, doc_contents)))

    def save(self, documents):

        with open(StorageSettings.dump_path, "r") as f:
            data = json.load(f)

        with open(StorageSettings.dump_path, "w") as f:
            for key in documents.keys():
              data[key] = documents[key]
            json.dump(data, f)

    def load(self):
        with open(StorageSettings.dump_path, "r") as f:
            documents = json.load(f)
            for doc in documents.keys():
                doc_summaries, doc_contents = documents[doc]["doc_summaries"], documents[doc]["doc_contents"]
                self.add_documents(doc_summaries, doc_contents)
        print("Данные успешно загружены")


