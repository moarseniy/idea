import os

a_key = "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"
v_key = "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"
os.environ["OPENAI_API_KEY"] = v_key

NLTK_DATA = ['punkt_tab', "averaged_perceptron_tagger_eng"]

class speechRegognitionSettings:
    model_id: str = "openai/whisper-large-v3-turbo"
    model_settings: dict = {
        "low_cpu_mem_usage": True,
        "use_safetensors": True
    }
    pipeline: str = "automatic-speech-recognition"

class ExtractorSettings:
    base_dir = os.getcwd()
    source_path:str = os.path.join(base_dir, "source_data")
    audio_path:str = os.path.join(base_dir, "audio_data")
    pic_path:str = os.path.join(base_dir, "pic_data")
    video_chunk_time:int = 30 # seconds
    chunked = False # use chunks
    for path in [source_path, audio_path, pic_path]:
        if not os.path.exists(path):
            os.mkdir(path)

class OpenAISettings:
    model = "gpt-4o-mini"

class StorageSettings:
    # SQL database params
    username = os.getenv("db_user")
    password = os.getenv("db_password")
    host = "localhost"
    port = "5432"
    database_name = "omnias_db"
    db_url = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"


LOG_FILE = os.getcwd() + "/logs/app.log"

