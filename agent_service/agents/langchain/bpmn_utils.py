import requests
import subprocess
import os
from pathlib import Path
import shlex
from datetime import datetime
import tempfile

def save_xml_file(xml_content: str) -> str:
    fname = datetime.now().strftime("xml_%Y%m%d_%H%M%S_%f.md")
    file_path = os.path.join(tempfile.gettempdir(), fname)
    if not file_path.endswith('.xml'):
        file_path += '.xml'
    
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(xml_content)
    print(f"Файл {file_path} успешно сохранён.")
    return file_path

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

def imporve_bpmn_layout(
    bpmn_path: str,
    bpmn_layout_path: str = "bpmn-auto-layout-cli",
    timeout: int = 10,
    check_output: bool = True
) -> str:
    try:
        # Проверка входного файла
        input_path = Path(bpmn_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input BPMN file not found: {input_path}")
        
        fname = datetime.now().strftime("improved_xml_%Y%m%d_%H%M%S_%f.md")
        file_path = os.path.join(tempfile.gettempdir(), fname)
        if not file_path.endswith('.xml'):
            file_path += '.xml'

        # Подготовка команды
        cmd = f"{bpmn_layout_path} {shlex.quote(bpmn_path)} > {shlex.quote(file_path)}"
        # print(cmd)
        try:
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                timeout=timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("Conversion timed out") from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Conversion failed (code {e.returncode}): {e.stderr.decode().strip()}"
            ) from e
        
        return file_path # result.stdout
    
    except subprocess.CalledProcessError as e:
        error_msg = f"Error {e.returncode}: {e.stderr.decode()}"
        raise RuntimeError(error_msg) from e
    except Exception as e:
        raise RuntimeError(f"Conversion error: {str(e)}") from e


def convert_bpmn_to_image(
    bpmn_path: str,
    output_path: str = None,
    bpmn_to_image_path: str = "bpmn-to-image",
    timeout: int = 10,
    check_output: bool = True
) -> str:
    """
    Конвертирует BPMN в изображение через bpmn-to-image
    с поддержкой синтаксиса input.bpmn:output.png
    
    Параметры:
        input_spec: строка в формате "файл.bpmn:результат.png"
        bpmn_to_image_path: путь к утилите bpmn-to-image
        timeout: таймаут выполнения в секундах
        check_output: проверять ли существование выходного файла
        
    Возвращает:
        True если конвертация успешна
        
    Исключения:
        ValueError: некорректный формат input_spec
        subprocess.SubprocessError: ошибка выполнения
        FileNotFoundError: входной файл не существует
    """
    try:
        # Проверка входного файла
        input_path = Path(bpmn_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input BPMN file not found: {input_path}")
        
        # Проверка выходного файла
        if not output_path:
            fname = datetime.now().strftime("bpmn_image_%Y%m%d_%H%M%S_%f.md")
            output_path = os.path.join(tempfile.gettempdir(), fname)
            if not output_path.endswith('.png'):
                output_path += '.png'

        # Подготовка команды
        cmd = f"{bpmn_to_image_path} {shlex.quote(bpmn_path)}:{shlex.quote(output_path)}"
        # print(cmd)
        try:
            subprocess.run(
                cmd,
                shell=True,
                check=True,
                timeout=timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        except subprocess.TimeoutExpired:
            raise RuntimeError("Conversion timed out") from None
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"Conversion failed (code {e.returncode}): {e.stderr.decode().strip()}"
            ) from e
        
        # Проверка результата
        if check_output and not Path(output_path).exists():
            raise RuntimeError(f"Output file was not created: {output_path}")
        
        return output_path
    
    except subprocess.CalledProcessError as e:
        error_msg = f"Error {e.returncode}: {e.stderr.decode()}"
        raise RuntimeError(error_msg) from e
    except Exception as e:
        raise RuntimeError(f"Conversion error: {str(e)}") from e


def validate_bpmn(
    bpmn_file: str,
    bpmnlint_path: str = "bpmnlint", 
    timeout: int = 10
) -> str:
    """
    Валидирует BPMN-файл и возвращает полный вывод bpmnlint
    
    Параметры:
        bpmn_file: путь к BPMN-файлу
        bpmnlint_path: путь к утилите bpmnlint
        timeout: таймаут выполнения в секундах
        
    Возвращает:
        Полный вывод команды в виде строки
        
    Исключения:
        FileNotFoundError: файл не существует
        RuntimeError: ошибки выполнения
    """
    try:
        # Проверка существования файла
        if not Path(bpmn_file).exists():
            raise FileNotFoundError(f"BPMN file not found: {bpmn_file}")

        # Формирование команды
        cmd = f"{bpmnlint_path} {shlex.quote(bpmn_file)}"
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                check=False,
                timeout=timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True
            )
            return result.stdout
            
        except subprocess.TimeoutExpired as e:
            raise RuntimeError("Validation timed out") from e
    
    except subprocess.CalledProcessError as e:
        error_msg = f"Error {e.returncode}: {e.stderr.decode()}"
        raise RuntimeError(error_msg) from e
    except Exception as e:
        raise RuntimeError(f"Validation error: {str(e)}") from e

def validate_with_camunda(file_path: str, camunda_url: str) -> bool:
    """Отправляет BPMN-файл на валидацию в Camunda."""
    with open(file_path, "rb") as f:
        response = requests.post(
            f"{camunda_url}/engine-rest/process-definition/validate",
            files={"file": (file_path, f)}
        )
    return response.status_code == 204


if __name__ == "__main__":

    bpmn_path = "/home/arseniy/python-dev/bpmn/invoice.v1.bpmn"
    output_path = "/home/arseniy/python-dev/bpmn/invoice.v1.png"

    try:
        output = validate_bpmn(bpmn_path)
        print("Результат валидации:")
        print(output)
        
    except FileNotFoundError as e:
        print(f"Ошибка: {str(e)}")
    except RuntimeError as e:
        print(f"Ошибка выполнения: {str(e)}")

    try:
        xml_path = imporve_bpmn_layout(bpmn_path)
        print("Улучшение успешно завершено!")
    except Exception as e:
        print(f"Ошибка: {e}")

    print(xml_path)
    # bpmn_path = save_xml_file(xml_improved_layout)

    try:
        convert_bpmn_to_image(xml_path, output_path)
        print("Конвертация успешно завершена!")
    except Exception as e:
        print(f"Ошибка: {e}")


    # CAMUNDA
    # camunda_url = "http://localhost:8080"
    # if validate_with_camunda(bpmn_path, camunda_url):
    #     print("Camunda подтвердил валидность!")
    # else:
    #     print("Ошибка валидации в Camunda.")
