import re
import shutil

# from pytubefix import YouTube
# from pytubefix.cli import on_progress

from settings import ExtractorSettings
from .cutomLogger import customLogger
# from unstructured.partition.pdf import partition_pdf
import os

_LOGGER = customLogger.getLogger(__name__)

class Extractor:

    class FileLoader:
        @classmethod
        def load_file(cls, url:str, resolution:str="high", file_extension='mp4'):
            try:
                # mode = "local" if not url.startswith("http") else "global"
                # if mode == "local":
                filename = re.search(r"[0-9a-zA-Zа-яА-Я_\.\-\?\!\s]+\.(mp3|wav|mp4|pdf)", url).group(0)
                file_save_path = os.path.join(ExtractorSettings.source_path, filename)
                shutil.copy2(url, file_save_path)
                # else:
                #     yt = YouTube(url, on_progress_callback = on_progress)
                #     _LOGGER.info(f"{yt.title} is loading...")
                #     ys = yt.streams.get_highest_resolution() if resolution == "high" else yt.streams.filter(file_extension=file_extension)
                #     ys.download(output_path=ExtractorSettings.source_path)
                #     filename = yt.title + "." + file_extension
                return filename
            except Exception as err:
                _LOGGER.error(err)

    # class TextExtractor:
    #     @staticmethod
    #     def extract_pdf_elements(doc_path):
    #
    #         return partition_pdf(
    #             strategy='hi_res',
    #             filename=doc_path,  # Путь к файлу, который нужно обработать
    #             extract_images_in_pdf=True,  # Указание на то, что из PDF нужно извлечь изображения
    #             infer_table_structure=True,  # Автоматическое определение структуры таблиц в документе
    #             chunking_strategy="by_title",  # Стратегия разбиения текста на части
    #             max_characters=2000,  # Максимальное количество символов в одном чанке текста
    #             new_after_n_chars=1800,  # Число символов, после которого начинается новый чанк текста
    #             combine_text_under_n_chars=1500,  # Минимальное количество символов, при котором чанки объединяются
    #             image_output_dir_path=ExtractorSettings.pic_path,  # Путь, куда будут сохраняться извлеченные изображения
    #             languages=['eng', 'rus']
    #         )
    #
    #     @staticmethod
    #     def categorize_elements(raw_pdf_elements):
    #         tables = []  # Список для хранения элементов типа "таблица"
    #         texts = []  # Список для хранения текстовых элементов
    #         for element in raw_pdf_elements:
    #             # Проверка типа элемента. Если элемент является таблицей, добавляем его в список таблиц
    #             if "unstructured.documents.elements.Table" in str(type(element)):
    #                 tables.append(str(element))
    #             # Если элемент является композитным текстовым элементом, добавляем его в список текстов
    #             elif "unstructured.documents.elements.CompositeElement" in str(type(element)):
    #                 texts.append(str(element))
    #         return texts, tables  # Возвращаем списки с текстами и таблицами
    #
    #     @classmethod
    #     def process_txt(cls, doc_path):
    #         raw_pdf_elements = cls.extract_pdf_elements(doc_path)
    #         texts, tables = cls.categorize_elements(raw_pdf_elements)
    #         return texts

    class Deleter:
        @classmethod
        def delete(cls, path):
            if os.path.exists(path):
                os.remove(path)
