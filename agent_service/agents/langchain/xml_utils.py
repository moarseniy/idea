import requests
import subprocess
import os
from pathlib import Path
import shlex
from datetime import datetime
import tempfile

def read_xml_file(file_path: str) -> str:
    """
    Читает XML-файл и возвращает его содержимое в виде строки.
    
    Args:
        file_path (str): Путь к XML-файлу
    
    Returns:
        str: Содержимое XML-файла
        
    Raises:
        FileNotFoundError: Если файл не существует
        IsADirectoryError: Если указанный путь является директорией
        PermissionError: Если нет прав на чтение файла
    """
    # Проверяем существование файла
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    # Проверяем, что это файл, а не директория
    if os.path.isdir(file_path):
        raise IsADirectoryError(f"Указанный путь является директорией: {file_path}")
    
    try:
        # Открываем файл и читаем содержимое
        with open(file_path, 'r', encoding='utf-8') as file:
            xml_content = file.read()
        return xml_content
    except UnicodeDecodeError:
        # Пробуем другие кодировки, если utf-8 не сработала
        with open(file_path, 'r', encoding='latin-1') as file:
            xml_content = file.read()
        return xml_content
    except PermissionError:
        raise PermissionError(f"Нет прав на чтение файла: {file_path}")


if __name__ == "__main__":
    print("main")