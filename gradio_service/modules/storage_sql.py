from .singleton import Singleton
from settings import StorageSettings as StorSet

import uuid
import json

from langchain.retrievers.multi_vector import MultiVectorRetriever
from langchain.storage import InMemoryStore
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings

from sqlalchemy import create_engine, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

def create_database():
    base_url = f"postgresql://{StorSet.username}:{StorSet.password}@{StorSet.host}:{StorSet.port}/{StorSet.database_name}"
    
    # postgres_url = f"postgresql://{username}:{password}@{host}:{port}/postgres"
    # postgres_engine = create_engine(postgres_url)

    print(f"Connecting to PostgreSQL at {base_url}")
    # with postgres_engine.connect() as connection:
    #     # Проверяем, существует ли база данных
    #     connection.connection.set_isolation_level(0)  # Устанавливаем уровень изоляции для выполнения команды
    #     try:
    #         result = connection.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'"))
    #         exists = result.scalar() is not None
            
    #         if exists:
    #             print(f"База данных '{database_name}' уже существует.")
    #         else:
    #             connection.connection.commit()  # Отключаем транзакционный режим для создания базы данных
    #             connection.execute(text(f"CREATE DATABASE {database_name}"))
    #             print(f"База данных '{database_name}' создана.")
    #     finally:
    #         # Восстанавливаем исходный уровень изоляции
    #         connection.connection.set_isolation_level(1)

    return create_engine(base_url)

engine = create_database()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class DocumentDB(Base):
    __tablename__ = "documents"
    user_name = Column(String, primary_key=True, nullable=False)
    user_password = Column(String, nullable=False)
    video_path = Column(Text, nullable=True)
    doc_contents = Column(Text, nullable=True)
    doc_summaries = Column(Text, nullable=True)
    
# Создание таблиц
# Base.metadata.drop_all(bind=engine)
# Base.metadata.create_all(bind=engine)

class StorageSQL(Singleton):
    def _setup(self):
        self.db = SessionLocal()
        self.id_key = "doc_id"
        self.clear_retriever_storage()

    def clear_retriever_storage(self):
        """Очистка данных из векторного хранилища и хранилища документов."""
        try:
            new_vectorstore = Chroma(collection_name="custom_rag_collection", embedding_function=OpenAIEmbeddings())
            new_docstore = InMemoryStore()
            self.retriever = MultiVectorRetriever(
                vectorstore=new_vectorstore,
                docstore=new_docstore,
                id_key=self.id_key,
            )

            # self.retriever.vectorstore.delete_collection()  # Удаляем все векторы

            print("Retriever успешно очищен.")
        except Exception as e:
            print(f"Ошибка при очистке retriever: {e}")

    def load_users(self):
        with SessionLocal() as session:
            users = session.query(DocumentDB).all()
            return {user.user_name: user.user_password for user in users}

    def save_user(self, user_name, user_password):
        new_user = DocumentDB(user_name=user_name, 
                              user_password=user_password,
                              video_path=json.dumps([]),
                              doc_contents=json.dumps([]),
                              doc_summaries=json.dumps([]))
        
        with SessionLocal() as session:
            session.add(new_user)
            session.commit()

    def add_documents(self, video_path, doc_summaries, doc_contents, user_name=None, user_password=None):
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
            Document(page_content=s, metadata={self.id_key: doc_ids[i]})
            for i, s in enumerate(doc_summaries)
        ]

        # Добавляем документы в векторное хранилище
        self.retriever.vectorstore.add_documents(summary_docs)

        # Добавляем метаданные документов в хранилище
        self.retriever.docstore.mset(list(zip(doc_ids, doc_contents)))

        if user_name:
            self.save_data(video_path, doc_summaries, doc_contents, user_name, user_password)

        print("Успешно загружен", video_path)

    def save_data(self, video_path, doc_summaries, doc_contents, user_name, user_password):
        try:
            user_data = self.load_data(user_name)
            if user_data:
                user_data = user_data[0]  # Если пользователь существует, берем первый (и единственный) объект
                user_data.video_path = json.dumps(json.loads(user_data.video_path) + [video_path])
                user_data.doc_contents = json.dumps(json.loads(user_data.doc_contents) + doc_contents)
                user_data.doc_summaries = json.dumps(json.loads(user_data.doc_summaries) + doc_summaries)
            else:
                user_data = DocumentDB(
                    user_name=user_name,
                    user_password=user_password,
                    video_path=json.dumps([video_path]),
                    doc_contents=json.dumps(doc_contents),
                    doc_summaries=json.dumps(doc_summaries)
                )
            
            self.db.add(user_data)
            self.db.commit()
            print("Данные успешно сохранены.")
        except SQLAlchemyError as e:
            self.db.rollback()
            print(f"Ошибка при добавлении документов: {e}")
        
    def load_data(self, user_name):
        try:
            documents = self.db.query(DocumentDB).filter(DocumentDB.user_name == user_name).all()
            # print(documents[0].user_name, documents[0].user_password, documents[0].video_path)
            
            data_count = len(json.loads(documents[0].video_path))
            print(f"Found {data_count} data elements.")
            # print(json.loads(documents[0].doc_summaries), json.loads(documents[0].doc_contents))
            # for i in range(data_count):
            if data_count:
                self.add_documents(json.loads(documents[0].video_path), 
                                   json.loads(documents[0].doc_summaries),
                                   json.loads(documents[0].doc_contents))
                
                print("Данные успешно загружены.")
            return documents
        except SQLAlchemyError as e:
            print(f"Ошибка при получении документов: {e}")
            return []
        
