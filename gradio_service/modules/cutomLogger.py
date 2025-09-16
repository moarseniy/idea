import logging
import settings
# from gradio_service import settings


class customLogger:
    @classmethod
    def getLogger(cls, name:str):
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(settings.LOG_FILE)
        ch = logging.StreamHandler()

        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger