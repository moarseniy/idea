import os
import re

from .singleton import Singleton
from .cutomLogger import customLogger
from .infoExtractor import Extractor
from .speechRecognition import SpeechRecognitionModule
from settings import ExtractorSettings

_LOGGER = customLogger.getLogger(__name__)

class PipeLine(Singleton):
    def _setup(self):
        self.fileLoader = Extractor.FileLoader()
        # self.textExtractor = Extractor.TextExtractor()
        self.recognizer = SpeechRecognitionModule()
        self.deleter = Extractor.Deleter()

    def is_chunk_filename(self, base_filename, check_filename):
        return re.match(r"chunk_[0-9]+_{}".format(base_filename), check_filename) is not None

    def process_chunk(self, filename):
        audio_filename = self.audioExtractor.convert_to_mp3(filename)
        audio_path = os.path.join(ExtractorSettings.audio_path, audio_filename)
        text = self.recognizer.recognize(audio_path)
        self.deleter.delete(audio_path)
        return text

    def process(self, path:str):
        source_filename = self.fileLoader.load_file(path)
        source_full_path = os.path.join(ExtractorSettings.source_path, source_filename)
        processed_full_path = source_full_path
        text = None

        if source_filename is None:
            _LOGGER.info("File was not downloaded sucessfully")
            return
        _LOGGER.info(f"File {source_filename} successfully saved into {ExtractorSettings.source_path}")

        if source_filename.endswith(".mp3") or source_filename.endswith(".wav"):
            processed_full_path = os.path.join(ExtractorSettings.source_path, source_filename)
            text = self.recognizer.recognize(processed_full_path)
        elif source_filename.endswith(".pdf"):
            processed_full_path = os.path.join(ExtractorSettings.source_path, source_filename)
            text_array = self.textExtractor.process_txt(processed_full_path)
            text = "".join(text_array)
            text = "".join([txt for txt in text.split("\n")])

        # _LOGGER.info(f"TEXT: {text}")

        self.deleter.delete(source_full_path)
        self.deleter.delete(processed_full_path)
        _LOGGER.info("Загруженные файлы успешно удалены")
        return text
