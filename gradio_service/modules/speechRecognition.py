from .singleton import Singleton
from .cutomLogger import customLogger

import os

import torch
import requests
from transformers import AutoModelForSpeechSeq2Seq, AutoProcessor, pipeline
from settings import speechRegognitionSettings

_LOGGER = customLogger.getLogger(__name__)

class SpeechRecognitionModule(Singleton):
    def _setup(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        _LOGGER.info(f"DEVICE: {self.device}")
        # self.torch_dtype = torch.float16 if torch.cuda.is_available() else torch.float32
        # self.model_id = speechRegognitionSettings.model_id
        # self.model_settings = speechRegognitionSettings.model_settings
        #
        # self.model = AutoModelForSpeechSeq2Seq.from_pretrained(
        #     self.model_id,
        #     torch_dtype=self.torch_dtype,
        #     **self.model_settings
        # )
        # self.model.to(self.device)
        #
        # self.processor = AutoProcessor.from_pretrained(self.model_id)
        #
        # self.pipe = pipeline(
        #     speechRegognitionSettings.pipeline,
        #     model=self.model,
        #     tokenizer=self.processor.tokenizer,
        #     feature_extractor=self.processor.feature_extractor,
        #     torch_dtype=self.torch_dtype,
        #     device=self.device,
        #     return_timestamps=True
        # )
    
    def recognize(self, file:str):
        import requests

        # Замените на свой OpenAI API ключ
        api_key = os.getenv("OPENAI_API_KEY")

        # Путь к аудиофайлу
        audio_file_path = file

        # Открываем аудиофайл
        with open(audio_file_path, 'rb') as f:
            files = {
                'file': (audio_file_path, f, 'audio/wav')
            }
            data = {
                'model': 'whisper-1',
                'language': 'ru',  # Явно указываем язык
                'response_format': 'text'
            }
            headers = {
                'Authorization': f'Bearer {api_key}'
            }

            response = requests.post(
                'https://api.openai.com/v1/audio/transcriptions',
                headers=headers,
                data=data,
                files=files
            )

        # Выводим результат
        if response.ok:
            print("Результат транскрибации:")
            print(response.text)
            return response.text
        else:
            print("Ошибка:")
            print(response.status_code, response.text)
            return None
        # try:
        #     result = self.pipe(file)
        #     text = result["text"]
        #     return text
        # except Exception as err:
        #     _LOGGER.error(err)
        
