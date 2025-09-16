import os
import re, time
from concurrent.futures import ThreadPoolExecutor

from .singleton import Singleton
from .cutomLogger import customLogger
from .infoExtractor import Extractor
from .speechRecognition import SpeechRecognitionModule
from settings import ExtractorSettings

_LOGGER = customLogger.getLogger(__name__)

class PipeLine(Singleton):
    def _setup(self):
        self.videoExtractor = Extractor.VideoLoader()
        self.audioExtractor = Extractor.AudioExtractor()
        self.recognizer = SpeechRecognitionModule()

    def is_chunk_filename(self, base_filename, check_filename):
        return re.match(r"chunk_[0-9]+_{}".format(base_filename), check_filename) is not None

    def process_chunk(self, chunk_filename):
        tmp_audio_filename = self.audioExtractor.convert_to_mp3(chunk_filename)
        tmp_audio_path = os.path.join(ExtractorSettings.audio_path, tmp_audio_filename)
        tmp_text = self.recognizer.recognize(tmp_audio_path)
        return tmp_text

    def process(self, url:str):
        video_filename = self.videoExtractor.load_video(url)
        
        if video_filename is None:
            _LOGGER.info("Video was not downloaded sucessfully")
            return
        _LOGGER.info(f"Video successfully saved into {ExtractorSettings.video_path}")
        
        if ExtractorSettings.chunked:
            start_time = time.time()
            self.videoExtractor.video_cutter(video_filename, ExtractorSettings.video_chunk_time)
            chunk_filenames = [filename for filename in os.listdir(ExtractorSettings.video_path)
                               if self.is_chunk_filename(video_filename, filename)]
            _LOGGER.info("video_cutter", time.time() - start_time)
            
            texts = []
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(self.process_chunk, chunk_filename) for chunk_filename in chunk_filenames]
                for future in futures:
                    texts.append(future.result())
            text = "\n".join(texts)
        else:
            start_time = time.time()
            audio_filename = self.audioExtractor.convert_to_mp3(video_filename)
            mp3_time = time.time() - start_time
            _LOGGER.info(f"convert_to_mp3: {mp3_time}")

            audio_path = os.path.join(ExtractorSettings.audio_path, audio_filename)
            text = self.recognizer.recognize(audio_path)
            _LOGGER.info(f"recognize {time.time() - start_time}")

        return text
