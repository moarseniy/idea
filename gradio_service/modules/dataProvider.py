from .singleton import Singleton
from .cutomLogger import customLogger
from settings import StorageSettings
import json
import os


from sqlalchemy import create_engine, Column, String, Text
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

_LOGGER = customLogger.getLogger(__name__)

class Base(DeclarativeBase): pass

class DocumentDB(Base):
    __tablename__ = "documents"
    user_name = Column(String, primary_key=True, nullable=False)
    user_password = Column(String, nullable=False)
    video_path = Column(Text, nullable=True)
    doc_contents = Column(Text, nullable=True)
    doc_summaries = Column(Text, nullable=True)

class SummaryDB(Base):
    __tablename__ = "summaries"
    user_name = Column(String, primary_key=True, nullable=False)
    doc_name = Column(String, nullable=False)
    doc_title = Column(Text, nullable=True)
    doc_summary = Column(Text, nullable=True)

class DataProvider(Singleton):
    def _setup(self):
        _LOGGER.info(f"Connecting to PostgreSQL at {StorageSettings.db_url}")
        __engine = create_engine(StorageSettings.db_url)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=__engine)
        self.db = self.SessionLocal()
        # Base.metadata.create_all(bind=__engine)

    def load_users(self):
        with self.SessionLocal() as session:
            users = session.query(DocumentDB).all()
            return {user.user_name: user.user_password for user in users}

    def save_user(self, user_name, user_password):
        new_user = DocumentDB(user_name=user_name, 
                              user_password=user_password,
                              video_path=json.dumps([]),
                              doc_contents=json.dumps([]),
                              doc_summaries=json.dumps([]))
        
        with self.SessionLocal() as session:
            session.add(new_user)
            session.commit()

    def save_data(self, user_data, video_path, doc_summaries, doc_contents):
        def __check_and_add(previous, new):
            new = new if isinstance(new, list) else [new]
            if previous:
                result = json.dumps(json.loads(previous) + new)
            else:
                result = json.dumps(new)
            return result

        try:
            user_name = user_data.user_name
            user_password = user_data.user_password
            user_data_content = self.load_data(user_name)
            _LOGGER.info(f"USER_: {user_data_content.user_name}")
            if isinstance(user_data_content, DocumentDB):
                _LOGGER.info("TUT!")
                user_data_content.video_path = __check_and_add(user_data_content.video_path, video_path)
                user_data_content.doc_contents = __check_and_add(user_data_content.doc_contents, doc_contents)
                user_data_content.doc_summaries = __check_and_add(user_data_content.doc_summaries, doc_summaries)
            else:
                _LOGGER.info("TUT@@@")
                user_data_content = DocumentDB(
                    user_name=user_name,
                    user_password=user_password,
                    video_path=json.dumps([video_path]),
                    doc_contents=json.dumps(doc_contents),
                    doc_summaries=json.dumps(doc_summaries)
                )
                self.db.add(user_data_content)
            self.db.commit()
            _LOGGER.info("Данные успешно сохранены.")
        except SQLAlchemyError as e:
            self.db.rollback()
            _LOGGER.error(f"Ошибка при добавлении документов: {e}")
        
    def load_data(self, user_name):
        documents = None
        try:
            documents = self.db.query(DocumentDB).filter(DocumentDB.user_name == user_name).first()
        except SQLAlchemyError as e:
            _LOGGER.error(f"Ошибка при получении документов: {e}")
        return documents

    def save_summary(self, user_name, doc_name, doc_summary, doc_title=None):
        summary_user = SummaryDB(user_name=user_name,
                                 doc_name=doc_name,
                                 doc_title=doc_title,
                                 doc_summary=doc_summary,
                              )
        try:
            self.db.add(summary_user)
            self.db.commit()
        except Exception as err:
            _LOGGER.error(f"Ошибка при добавлении суммаризации: {err}")
            self.db.rollback()

    def load_summary(self, user_name, doc_name):
        summary = None
        try:
            summary_row = self.db.query(SummaryDB)\
                             .filter(SummaryDB.user_name==user_name, SummaryDB.doc_name==doc_name)\
                             .first()
            summary = summary_row.doc_summary
        except Exception as err:
            _LOGGER.error(f"Ошибка при получении суммаризации: {err}")
            self.db.rollback()
        summary = "Саммари не найдено" if not summary else summary
        return summary

    def check_files_in(self, user_name, doc_name):
        result = False
        try:
            documents_row = self.db.query(DocumentDB)\
                             .filter(DocumentDB.user_name==user_name)\
                             .first()
            documents_set = set([os.path.basename(tmp_doc) for tmp_doc in eval(documents_row.video_path)])
            doc_name = os.path.basename(doc_name)
            result = doc_name in documents_set
        except Exception as err:
            _LOGGER.error(f"Ошибка при обращении к БД: {err}")
            self.db.rollback()
        return result

