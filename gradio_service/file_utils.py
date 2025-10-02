import requests
import subprocess
import os
from pathlib import Path
from typing import Union, IO
import shlex
import datetime
import tempfile
import base64
import re, sys

def image_to_base64(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def md_code_chunk_from_escaped(s: str, lang: str = "sql") -> str:
    # превращаем литералы \r\n / \n / \r в реальные переводы строк
    normalized = (s.replace("\\r\\n", "\n")
                   .replace("\\n", "\n")
                   .replace("\\r", "\n"))
    # если внутри встречаются ``` — увеличим «забор»
    fence = "````" if "```" in normalized else "```"
    return f"{fence}{lang}\n{normalized}\n{fence}\n"
    

def extract_db_type(content: str) -> str:
    match = re.search(r"\*(.*?)\*", content)
    return(match.group(1))

def extract_json_data(text: str):
    pattern = r"```json\s*(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
    return "\n\n".join(m.strip() for m in matches)#[m.strip() for m in matches]

def extract_sql_data(text: str):
    pattern = r"```sql\s*(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
    return "\n\n".join(m.strip() for m in matches)#[m.strip() for m in matches]

def save_markdown_file(md_content: str) -> str:
    fname = datetime.datetime.now().strftime("md_%Y%m%d_%H%M%S_%f.md")
    file_path = os.path.join(tempfile.gettempdir(), fname)
    if not file_path.endswith('.md'):
        file_path += '.md'
    
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(md_content)
    print(f"Файл {file_path} успешно сохранён.")
    return file_path

def save_sql_file(sql_content: str) -> str:
    fname = datetime.datetime.now().strftime("sql_%Y%m%d_%H%M%S_%f.sql")
    file_path = os.path.join(tempfile.gettempdir(), fname)
    if not file_path.endswith('.sql'):
        file_path += '.sql'
    
    # Сохранение файла
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(sql_content)
    print(f"Файл {file_path} успешно сохранён.")
    
    return file_path

def save_dbml_file(sql_content: str) -> str:
    fname = datetime.datetime.now().strftime("dbml_%Y%m%d_%H%M%S_%f.dbml")
    file_path = os.path.join(tempfile.gettempdir(), fname)
    if not file_path.endswith('.dbml'):
        file_path += '.dbml'
    
    # Сохранение файла
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(sql_content)
    print(f"Файл {file_path} успешно сохранён.")
    
    return file_path

def convert_dbml_to_svg(
    dbml_path: str,
    output_path: str = None,
    renderer_path: str = "dbml-renderer",
    timeout: int = 10,
    check_output: bool = True
) -> str:
    """
    Конвертирует DBML в SVG через dbml-renderer.

    Параметры:
        dbml_path: путь к входному .dbml файлу
        output_path: путь к выходному .svg файлу (если не задан — создаётся во временной папке)
        renderer_path: путь к утилите dbml-renderer (по умолчанию "dbml-renderer")
        timeout: таймаут выполнения в секундах
        check_output: проверять ли существование выходного файла

    Возвращает:
        Путь к созданному SVG файлу

    Исключения:
        FileNotFoundError: входной файл не существует
        RuntimeError: ошибка выполнения или отсутствует результат
    """
    try:
        # Проверка входного файла
        input_path = Path(dbml_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input DBML file not found: {input_path}")

        # Проверка выходного файла
        if not output_path:
            fname = datetime.datetime.now().strftime("dbml_%Y%m%d_%H%M%S_%f.svg")
            output_path = os.path.join(tempfile.gettempdir(), fname)
        else:
            if not str(output_path).endswith(".svg"):
                output_path = str(output_path) + ".svg"

        # Подготовка команды
        cmd = [
            renderer_path,
            "-i", str(input_path),
            "-o", str(output_path),
        ]

        # Запуск процесса
        try:
            subprocess.run(
                cmd,
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

    except Exception as e:
        raise RuntimeError(f"Conversion error: {str(e)}") from e


def convert_sql_to_dbml(
    sql_path: str,
    output_path: str = None,
    sql2dbml_path: str = "sql2dbml",
    dialect: str = "postgres",
    timeout: int = 10,
    check_output: bool = True
) -> str:
    """
    Конвертирует SQL DDL в DBML через sql2dbml.

    Параметры:
        sql_path: путь к входному .sql файлу
        output_path: путь к выходному .dbml файлу (если не задан — создаётся во временной папке)
        sql2dbml_path: путь к утилите sql2dbml (по умолчанию "sql2dbml")
        dialect: диалект SQL (postgres, mysql, mssql, oracle, sqlite)
        timeout: таймаут выполнения в секундах
        check_output: проверять ли существование выходного файла

    Возвращает:
        Путь к созданному DBML файлу

    Исключения:
        FileNotFoundError: входной файл не существует
        RuntimeError: ошибка выполнения или отсутствует результат
    """
    try:
        # Проверка входного файла
        input_path = Path(sql_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input SQL file not found: {input_path}")

        # Проверка выходного файла
        if not output_path:
            fname = datetime.datetime.now().strftime("schema_%Y%m%d_%H%M%S_%f.dbml")
            output_path = os.path.join(tempfile.gettempdir(), fname)
        else:
            if not str(output_path).endswith(".dbml"):
                output_path = str(output_path) + ".dbml"

        # Подготовка команды
        cmd = [
            sql2dbml_path,
            f"--{dialect.lower()}",
            str(input_path)
        ]

        # Запуск процесса и запись результата в файл
        try:
            result = subprocess.run(
                cmd,
                check=True,
                timeout=timeout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result.stdout.decode())
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

    except Exception as e:
        raise RuntimeError(f"Conversion error: {str(e)}") from e



def clean_clickhouse_ddl(sql: str) -> str:
    """
    Простая и безопасная очистка ClickHouse DDL без регексов:
    - оставляет CREATE TABLE ... ( ... );
    - удаляет опции после закрывающей ')' до ближайшего ';'
    - учитывает строки и комментарии, чтобы не сломать типы с парен-/запятыми.
    """
    if not sql:
        return sql

    s = sql
    lower = s.lower()
    out = []
    i = 0
    n = len(s)

    def skip_string(idx, quote_char):
        j = idx + 1
        while j < n:
            ch = s[j]
            if ch == '\\':   # эскейп
                j += 2
                continue
            if ch == quote_char:
                return j + 1
            j += 1
        return n

    def skip_line_comment(idx):
        j = s.find('\n', idx+2)
        return n if j == -1 else j+1

    def skip_block_comment(idx):
        j = s.find('*/', idx+2)
        return n if j == -1 else j+2

    while True:
        pos = lower.find('create table', i)
        if pos == -1:
            out.append(s[i:])
            break

        # добавим всё до CREATE TABLE
        out.append(s[i:pos])

        # найдем '(' после CREATE TABLE
        open_paren = s.find('(', pos)
        if open_paren == -1:
            # нет тела - добавляем остаток и выходим
            out.append(s[pos:])
            break

        # пройдём от open_paren, учитывая строки/комментарии, до парной ')'
        depth = 0
        j = open_paren
        while j < n:
            ch = s[j]
            # комментарии/строки
            if ch == '-' and j+1 < n and s[j+1] == '-':
                j = skip_line_comment(j)
                continue
            if ch == '/' and j+1 < n and s[j+1] == '*':
                j = skip_block_comment(j)
                continue
            if ch == "'" or ch == '"':
                j = skip_string(j, ch)
                continue
            # скобки
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    # найдено соответствие
                    j += 1  # включаем закрывающую скобку
                    break
            j += 1

        if depth != 0:
            # если не нашли корректную закрывающую, добавляем остаток и выходим
            out.append(s[pos:])
            break

        # table_def — от CREATE ... до найденной ')'
        table_def = s[pos:j]

        # теперь пропустим всё до ближайшей точки с запятой (;) не внутри строк/комментариев
        k = j
        while k < n:
            ch = s[k]
            if ch == '-' and k+1 < n and s[k+1] == '-':
                k = skip_line_comment(k)
                continue
            if ch == '/' and k+1 < n and s[k+1] == '*':
                k = skip_block_comment(k)
                continue
            if ch in ("'", '"'):
                k = skip_string(k, ch)
                continue
            if ch == ';':
                k += 1  # включаем ';'
                break
            k += 1

        # добавляем table_def и ';' если был
        # если k > j и s[k-1] == ';' — значит мы на ';'
        if k > j and s[k-1] == ';':
            out.append(table_def.rstrip() + ';')
        else:
            out.append(table_def.rstrip())

        i = k  # продолжаем с позиции после ';' (или после закрывающей скобки)

    # финальная чистка — привести пустые строки и пробелы в порядок
    res = ''.join(out)
    res = res.replace('\r\n', '\n').replace('\r', '\n')
    # убрать лишние пустые строки (необязательно)
    import re
    res = re.sub(r'\n{3,}', '\n\n', res)
    return res.strip()


def skip_string(s, i):
    quote = s[i]
    j = i + 1
    n = len(s)
    while j < n:
        if s[j] == '\\':
            j += 2
            continue
        if s[j] == quote:
            return j + 1
        j += 1
    return n

def skip_line_comment(s, i):
    j = s.find('\n', i+2)
    return len(s) if j == -1 else j+1

def skip_block_comment(s, i):
    j = s.find('*/', i+2)
    return len(s) if j == -1 else j+2

def find_matching_paren(s, start):
    """Find ')' matching '(' at index start (ignores strings/comments)."""
    depth = 0
    i = start
    n = len(s)
    while i < n:
        ch = s[i]
        if ch == '-' and i+1 < n and s[i+1] == '-':
            i = skip_line_comment(s, i); continue
        if ch == '/' and i+1 < n and s[i+1] == '*':
            i = skip_block_comment(s, i); continue
        if ch in ("'", '"'):
            i = skip_string(s, i); continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1

# split columns by top-level commas (keeps parentheses and strings intact)
def split_columns(body):
    cols = []
    buf = []
    depth = 0
    i = 0
    n = len(body)
    while i < n:
        ch = body[i]
        if ch == '-' and i+1<n and body[i+1]=='-':
            j = body.find('\n', i+2)
            if j == -1:
                i = n
                continue
            buf.append(body[i:j+1]); i = j+1; continue
        if ch == '/' and i+1<n and body[i+1]=='*':
            j = body.find('*/', i+2)
            j = n-2 if j==-1 else j+2
            buf.append(body[i:j]); i = j; continue
        if ch in ("'", '"'):
            start = i
            i = skip_string(body, i)
            buf.append(body[start:i]); continue
        if ch == '(':
            depth += 1; buf.append(ch); i += 1; continue
        if ch == ')':
            depth -= 1; buf.append(ch); i += 1; continue
        if ch == ',' and depth == 0:
            cols.append(''.join(buf).strip()); buf = []; i += 1; continue
        buf.append(ch); i += 1
    if buf and ''.join(buf).strip():
        cols.append(''.join(buf).strip())
    return cols

# map simple ClickHouse types -> DBML types + detect Nullable(...)
def map_type(ch_type):
    t = ch_type.strip()
    nullable = False
    m = re.match(r'Nullable\s*\(\s*(.+)\s*\)\s*$', t, flags=re.I)
    if m:
        nullable = True
        t = m.group(1).strip()
    m = re.match(r'LowCardinality\s*\(\s*(.+)\s*\)\s*$', t, flags=re.I)
    if m:
        t = m.group(1).strip()
    # simple mappings
    if re.match(r'Int64\b', t, flags=re.I):
        base = 'bigint'
    elif re.match(r'Int(8|16|32)\b', t, flags=re.I):
        base = 'int'
    elif re.match(r'UInt8\b', t, flags=re.I):
        base = 'boolean'
    elif re.match(r'UInt', t, flags=re.I):
        base = 'int'
    elif re.match(r'DateTime64?\b', t, flags=re.I):
        base = 'timestamp'
    elif re.match(r'Date\b', t, flags=re.I):
        base = 'date'
    elif re.match(r'Decimal', t, flags=re.I):
        base = t
    elif re.match(r'Float', t, flags=re.I):
        base = 'float'
    elif re.match(r'String\b', t, flags=re.I):
        base = 'varchar'
    elif re.match(r'FixedString', t, flags=re.I):
        base = 'varchar'
    elif re.match(r'JSON\b', t, flags=re.I):
        base = 'json'
    else:
        base = t
    return base, nullable

# parse a single column line -> dict {name,type,comment}
def parse_column(col_text):
    comment = None
    m = re.search(r'\bCOMMENT\b\s*(?:\'([^\']*)\'|"([^"]*)")', col_text, flags=re.I)
    if m:
        comment = m.group(1) or m.group(2)
        col_text = col_text[:m.start()].strip()
    parts = col_text.strip().split(None, 1)
    if not parts:
        return None
    name = parts[0].strip().strip('`"')
    typ = parts[1].strip() if len(parts) > 1 else 'String'
    return {'name': name, 'type': typ, 'comment': comment}

# parse all CREATE TABLE statements in SQL text
def parse_create_tables(sql_text):
    s = sql_text
    pos = 0
    res = []
    create_re = re.compile(r'CREATE\s+TABLE\b', re.I)
    while True:
        m = create_re.search(s, pos)
        if not m:
            break
        open_idx = s.find('(', m.end())
        if open_idx == -1:
            pos = m.end(); continue
        close_idx = find_matching_paren(s, open_idx)
        if close_idx == -1:
            pos = m.end(); continue
        header = s[m.end():open_idx].strip()
        tbl_name = header.split()[-1].strip().strip('`"')
        body = s[open_idx+1:close_idx]
        cols_raw = split_columns(body)
        cols = [parse_column(c) for c in cols_raw if parse_column(c)]
        semi_idx = s.find(';', close_idx+1)
        options = s[close_idx+1:semi_idx if semi_idx!=-1 else close_idx+1]
        order_cols = None
        morder = re.search(r'ORDER\s+BY\s*(\([^)]+\)|[^\n;]+)', options, re.I)
        if morder:
            oc = morder.group(1).strip()
            if oc.startswith('(') and oc.endswith(')'):
                oc = oc[1:-1]
            order_cols = [c.strip().strip('`"') for c in oc.split(',')]
        mpart = re.search(r'PARTITION\s+BY\s*(\([^)]+\)|[^\n;]+)', options, re.I)
        partition = None
        if mpart:
            partition = mpart.group(1).strip()
            if partition.startswith('(') and partition.endswith(')'):
                partition = partition[1:-1].strip()
        res.append({'name': tbl_name, 'columns': cols, 'order': order_cols, 'partition': partition})
        pos = (semi_idx+1) if semi_idx != -1 else (close_idx+1)
    return res

# --- heuristic FK detection ---
def detect_foreign_keys(tables):
    """
    Heuristic:
      - for each table A and column c that ends with '_id', find table B which has column with same name;
      - prefer B where that column is PK (i.e. B.order == [col]);
      - if no exact name match, try entity-name match: col 'event_id' -> look for table with 'event' token.
    Returns list of tuples: (src_table, src_col, dst_table, dst_col)
    """
    # build column -> list of (table_name, is_pk) mapping
    col_index = {}
    table_by_name = {t['name']: t for t in tables}
    for t in tables:
        pk_col = None
        if t.get('order') and len(t['order']) == 1:
            pk_col = t['order'][0]
        for col in t['columns']:
            name = col['name']
            is_pk = (pk_col == name)
            col_index.setdefault(name, []).append((t['name'], is_pk))

    refs = []
    seen = set()
    for t in tables:
        src = t['name']
        for col in t['columns']:
            cname = col['name']
            # skip if column is the PK of the same table
            if t.get('order') and len(t['order'])==1 and t['order'][0] == cname:
                continue
            # candidate if endswith _id or is exactly id
            if not (cname.lower().endswith('_id') or cname.lower() == 'id'):
                continue
            candidates = col_index.get(cname, [])
            chosen = None
            # prefer candidate where it's PK
            for tbl_name, is_pk in candidates:
                if tbl_name == src:
                    continue
                if is_pk:
                    chosen = (tbl_name, cname); break
            # otherwise pick first candidate in different table
            if chosen is None and candidates:
                for tbl_name, is_pk in candidates:
                    if tbl_name != src:
                        chosen = (tbl_name, cname); break
            # if still none, try matching by token: event_id -> look for table with 'event' token
            if chosen is None:
                token = cname[:-3] if cname.lower().endswith('_id') else cname
                token = token.lower()
                # try to find table whose name contains token as whole part (split by _ or camel)
                best = None
                for tbl in tables:
                    if tbl['name'] == src:
                        continue
                    # check tokens
                    name_tokens = re.split(r'[_\W]+', tbl['name'].lower())
                    if token in name_tokens:
                        # if this table has a column named cname, prefer that
                        cols = [c['name'] for c in tbl['columns']]
                        if cname in cols:
                            best = (tbl['name'], cname)
                            break
                        # else keep as fallback
                        if best is None:
                            # try to guess candidate column name as token + '_id' or 'id'
                            if token + '_id' in cols:
                                best = (tbl['name'], token + '_id')
                            elif 'id' in cols:
                                best = (tbl['name'], 'id')
                if best:
                    chosen = best
            if chosen:
                dst_tbl, dst_col = chosen
                key = (src, cname, dst_tbl, dst_col)
                if key not in seen:
                    seen.add(key)
                    refs.append(key)
    return refs

# build DBML output including refs
def to_dbml_with_refs(tables):
    # produce table blocks
    lines = []
    for t in tables:
        lines.append(f"Table {t['name']} " + "{")
        pk = None
        if t.get('order') and len(t['order']) == 1:
            pk = t['order'][0]
        for col in t['columns']:
            mapped, nullable = map_type(col['type'])
            attrs = []
            if pk and col['name'] == pk:
                attrs.append('pk'); attrs.append('not null')
            note = None
            if col.get('comment'):
                note = col['comment'].replace("'", "\\'")
            attr_parts = []
            if attrs:
                attr_parts.extend(attrs)
            if note:
                attr_parts.append(f"note: '{note}'")
            if attr_parts:
                lines.append(f"  {col['name']} {mapped} [{', '.join(attr_parts)}]")
            else:
                lines.append(f"  {col['name']} {mapped}")
        notes = []
        if t.get('order'):
            notes.append("ordering: " + ", ".join(t['order']))
        if t.get('partition'):
            notes.append("partitioning: " + t['partition'])
        if notes:
            note_text = "\\n".join(notes).replace("'", "\\'")
            lines.append(f"  Note: '{note_text}'")
        lines.append("}\n")
    # detect refs
    refs = detect_foreign_keys(tables)
    if refs:
        lines.append("// Foreign Keys")
        for src_tbl, src_col, dst_tbl, dst_col in refs:
            lines.append(f"Ref: {src_tbl}.{src_col} > {dst_tbl}.{dst_col}")
    return "\n".join(lines)








def _read_text(src: Union[str, bytes, Path, IO, None]) -> str:
    if src is None:
        return ""
    if isinstance(src, bytes):
        return src.decode("utf-8", errors="ignore")
    if isinstance(src, Path):
        return src.read_text(encoding="utf-8")
    if hasattr(src, "read") and callable(getattr(src, "read")):
        content = src.read()
        return content.decode("utf-8", errors="ignore") if isinstance(content, bytes) else str(content)
    return str(src)

def _normalize_types_block(s: str) -> str:
    # Снимаем обёртки и заменяем типы на SQL-подходящие
    s = re.sub(r'LowCardinality\(\s*String\s*\)', 'varchar', s, flags=re.I)
    s = re.sub(r'LowCardinality\(\s*([A-Za-z0-9_(),\s]+)\s*\)', r'\1', s, flags=re.I)
    s = re.sub(r'Nullable\(\s*([A-Za-z0-9_(),\s]+)\s*\)', r'\1', s, flags=re.I)
    s = re.sub(r'FixedString\(\s*\d+\s*\)', 'varchar', s, flags=re.I)
    s = re.sub(r'Decimal\(\s*\d+\s*,\s*\d+\s*\)', 'decimal', s, flags=re.I)
    s = re.sub(r'Array\(\s*([A-Za-z0-9_(),\s]+)\s*\)', r'\1[]', s, flags=re.I)

    # Базовый маппинг типов
    mapping = [
        (r'\bInt64\b', 'bigint'),
        (r'\bInt32\b', 'int'),
        (r'\bInt16\b', 'smallint'),
        (r'\bInt8\b', 'tinyint'),
        (r'\bUInt8\b', 'tinyint'),
        (r'\bUInt16\b', 'smallint'),
        (r'\bUInt32\b', 'int'),
        (r'\bUInt64\b', 'bigint'),
        (r'\bString\b', 'varchar'),
        (r'\bDateTime\b', 'datetime'),
        (r'\bDate\b', 'date'),
        (r'\bFloat32\b', 'float'),
        (r'\bFloat64\b', 'float'),
        (r'\bUUID\b', 'uuid'),
    ]
    for pat, rep in mapping:
        s = re.sub(pat, rep, s, flags=re.I)
    return s

def _split_top_level_columns(block: str):
    """
    Разбивает содержимое скобок на отдельные определения колонок/constraints,
    корректно обрабатывая вложенные скобки.
    Возвращает список строк (каждая — один элемент внутри CREATE TABLE(...)).
    """
    cols = []
    cur = []
    depth = 0
    i = 0
    while i < len(block):
        ch = block[i]
        if ch == '(':
            depth += 1
            cur.append(ch)
        elif ch == ')':
            depth -= 1
            cur.append(ch)
        elif ch == ',' and depth == 0:
            piece = ''.join(cur).strip()
            if piece:
                cols.append(piece)
            cur = []
        else:
            cur.append(ch)
        i += 1
    last = ''.join(cur).strip()
    if last:
        cols.append(last)
    return cols


def clean_clickhouse_sql_for_sql2dbml(raw_sql: Union[str, bytes, Path, IO, None]) -> str:
    """
    Очищает ClickHouse DDL и нормализует его так, чтобы it можно было подать в sql2dbml.

    Возвращает объединённую строку SQL, содержащую один или несколько корректных
    CREATE TABLE ...; блоков.
    """
    sql = _read_text(raw_sql)
    if not sql:
        return ""

    # Удалим BOM и нормализуем переносы
    sql = sql.lstrip("\ufeff")
    # Удалим комментарии (/* ... */ и -- ...)
    sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.S)
    sql = re.sub(r'--.*?$', '', sql, flags=re.M)

    # Уберём лишние пробелы (не критично)
    sql = re.sub(r'\r\n', '\n', sql)

    # Найдём все CREATE TABLE ... ; блоки (жадно до ближайшей ';' после закрывающей скобки)
    # Используем простой поиск: locate "CREATE TABLE" и найдём соответствующую закрывающую ')' перед ';'
    out_blocks = []
    create_iter = re.finditer(r'CREATE\s+TABLE\s+', sql, flags=re.I)
    pos = 0
    for m in create_iter:
        start = m.start()
        # отрезок от start до следующего ';' будет кандидатом; но сначала нужно корректно найти парную закрывающую скобку
        # возьмём оставшуюся часть и найдём первую '(' после CREATE TABLE
        remain = sql[start:]
        paren_pos = re.search(r'\(', remain)
        if not paren_pos:
            # нет блока, пропустим
            continue
        open_idx = start + paren_pos.start()
        # найдем соответствующую закрывающую скобку, отслеживая вложенность
        idx = open_idx
        depth = 0
        end_idx = None
        while idx < len(sql):
            ch = sql[idx]
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    # нашли закрывающую скобку для CREATE TABLE
                    end_idx = idx
                    break
            idx += 1
        if end_idx is None:
            # если не нашли — берем до ближайшего ';' как резерв
            semi = sql.find(';', open_idx)
            end_idx = semi-1 if semi != -1 else len(sql)-1

        # теперь найдём ближайший ';' после end_idx
        semi_pos = sql.find(';', end_idx)
        block_end = semi_pos if semi_pos != -1 else end_idx + 1
        block_text = sql[start:block_end+1]  # включаем ';' если он есть

        # очистим директивы ClickHouse (ENGINE, PARTITION BY, ORDER BY, SETTINGS, TTL)
        block_text = re.sub(r'\bENGINE\s*=\s*[^;\n]+', '', block_text, flags=re.I)
        block_text = re.sub(r'\bPARTITION\s+BY\s+[^;\n]+', '', block_text, flags=re.I)
        # сохраним ORDER BY значение как комментарий (опционально) — иногда полезно
        obm = re.search(r'\bORDER\s+BY\s+([^;\n]+)', block_text, flags=re.I)
        order_comment = None
        if obm:
            order_comment = obm.group(1).strip()
        block_text = re.sub(r'\bORDER\s+BY\s+[^;\n]+', '', block_text, flags=re.I)
        block_text = re.sub(r'\bSETTINGS\s+[^;\n]+', '', block_text, flags=re.I)
        block_text = re.sub(r'\bTTL\s+[^;\n]+', '', block_text, flags=re.I)

        # Разделим на заголовок (до первой '('), тело (внутри скобок) и хвост (после закрывающей ')')
        hdr_match = re.match(r'(CREATE\s+TABLE\s+[^\(]+)\(', block_text, flags=re.I)
        if not hdr_match:
            # если что-то странное — просто пропускаем добавляя очищенный блок целиком
            cleaned_block = _normalize_types_block(block_text)
            out_blocks.append(cleaned_block.strip())
            continue
        header = hdr_match.group(1).strip()
        inside_and_tail = block_text[hdr_match.end():]  # часть после '('
        # найдем позицию закрывающей скобки (последний ')') — но лучше искать первое парное, уже вычисляли end_idx
        # Мы уже вычислили end_idx relative to sql, но проще найти индекс последней ')' в inside_and_tail before ';'
        # Обрежем tail от первой occurrence of ')' that closes block_text
        # Используем ранее найден end position relative to sql to extract inner block
        # compute local end
        # find matching paren in inside_and_tail
        depth = 1
        inner_chars = []
        for ch in inside_and_tail:
            if ch == '(':
                depth += 1
                inner_chars.append(ch)
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    break
                inner_chars.append(ch)
            else:
                inner_chars.append(ch)
        inner_block = ''.join(inner_chars)
        # tail: substring after the matching ')', up to ';'
        tail_start = inside_and_tail.find(')')  # first ')'
        # but there could be nested; instead find position where inner parsing stopped
        # fallback: find last ')' before ';'
        tail = inside_and_tail[len(inner_block) + 1:]
        # Normalize types in inner_block
        inner_block_norm = _normalize_types_block(inner_block)

        # Split top-level columns
        cols = _split_top_level_columns(inner_block_norm)

        # Clean each column/definition line
        clean_cols = []
        for c in cols:
            part = c.strip().rstrip(',')
            # пропустить пустые строки
            if not part:
                continue
            # убрать лишние пробелы
            part = re.sub(r'\s+', ' ', part).strip()
            # иногда встречаются закрывающие или открывающие мусорные скобки — удалим нач/конечные
            part = part.strip().strip(',')
            clean_cols.append(part)

        # Соберём блок заново с красивым форматированием
        new_block = header + " (\n"
        new_block += ",\n".join("  " + cl for cl in clean_cols)
        new_block += "\n)"

        # Добавим комментарий об ORDER BY если был найден (чтобы не терять инфу)
        if order_comment:
            new_block += f" /* ORDER BY: {order_comment} */"

        # Убедимся, что в конце стоит ';'
        new_block = new_block.strip()
        if not new_block.endswith(';'):
            new_block += ';'

        out_blocks.append(new_block)

        # переместим позицию поиска дальше
        pos = start + len(block_text)

    # Если не найдено ни одного CREATE TABLE с помощью итератора — попробуем определить один основной блок целиком
    if not out_blocks:
        # минимальная обработка всего SQL как единый блок: удалить директивы и нормализовать типы
        sql_simple = re.sub(r'\bENGINE\s*=\s*[^;\n]+', '', sql, flags=re.I)
        sql_simple = re.sub(r'\bPARTITION\s+BY\s+[^;\n]+', '', sql_simple, flags=re.I)
        sql_simple = re.sub(r'\bORDER\s+BY\s+[^;\n]+', '', sql_simple, flags=re.I)
        sql_simple = re.sub(r'\bSETTINGS\s+[^;\n]+', '', sql_simple, flags=re.I)
        sql_simple = _normalize_types_block(sql_simple)
        # исправим двойные скобки '));' -> ');'
        sql_simple = re.sub(r'\)\s*\)\s*;', ');', sql_simple)
        if not sql_simple.strip().endswith(';'):
            sql_simple = sql_simple.strip() + ';'
        return sql_simple.strip()

    # Соберём все очищенные блоки вместе, отделив пустой строкой
    result = "\n\n".join(b for b in out_blocks if b)
    # Нормализуем несколько пустых строк
    result = re.sub(r'\n\s*\n+', '\n\n', result).strip()
    return result + "\n"




"""
sql_to_dbml_unified.py
Конвертер DDL -> DBML с поддержкой ClickHouse и Postgres-ish SQL.
Usage:
    python3 sql_to_dbml_unified.py input.sql output.dbml
"""

# Расширяемая маппинг-функция типов
def normalize_type(t):
    t = t.strip()
    # убрать множества пробелов
    t = re.sub(r'\s+', ' ', t)
    # ClickHouse wrappers
    t = re.sub(r'LowCardinality\(\s*([A-Za-z0-9_() ,]+)\s*\)', r'\1', t, flags=re.I)
    t = re.sub(r'Nullable\(\s*([A-Za-z0-9_() ,]+)\s*\)', r'\1', t, flags=re.I)
    t = re.sub(r'FixedString\(\s*\d+\s*\)', 'varchar', t, flags=re.I)
    t = re.sub(r'Decimal\(\s*\d+\s*,\s*\d+\s*\)', 'decimal', t, flags=re.I)
    t = re.sub(r'Array\(\s*([A-Za-z0-9_() ,]+)\s*\)', r'\1[]', t, flags=re.I)
    # базовый маппинг
    replacements = [
        (r'Int64', 'bigint'),
        (r'Int32', 'int'),
        (r'Int16', 'smallint'),
        (r'UInt8', 'tinyint'),
        (r'UInt16', 'smallint'),
        (r'UInt32', 'int'),
        (r'UInt64', 'bigint'),
        (r'Int8', 'tinyint'),
        (r'String', 'varchar'),
        (r'VARCHAR', 'varchar'),
        (r'CHAR\(\d+\)', 'varchar'),
        (r'Text', 'text'),
        (r'DateTime', 'datetime'),
        (r'Date', 'date'),
        (r'TIMESTAMP', 'timestamp'),
        (r'BOOLEAN|Bool', 'boolean'),
        (r'UUID', 'uuid'),
        (r'Float32|Float64', 'float'),
        (r'Decimal', 'decimal'),
        (r'LOWER\(|UPPER\(', 'varchar')  # fallback weird
    ]
    for pat, out in replacements:
        t = re.sub(pat, out, t, flags=re.I)
    # trim trailing things
    t = re.sub(r'\s+unsigned\b', '', t, flags=re.I)
    t = t.strip()
    return t

def extract_create_blocks(sql):
    # ищем CREATE TABLE ... ( ... ) ... ;  (не идеальный SQL-парсер, но рабочий для DDL)
    blocks = []
    # Уберём комментарии типа -- и /* ... */
    sql_clean = re.sub(r'--.*', '', sql)
    sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.S)
    # Найдём все CREATE TABLE ... ( ... )
    pattern = re.compile(r'CREATE\s+TABLE\s+([`"]?[\w\.]+[`"]?)\s*\((.*?)\)\s*([^;]*);', re.I | re.S)
    for m in pattern.finditer(sql_clean):
        name = m.group(1).strip('`"')
        inside = m.group(2)
        tail = m.group(3)
        # найти ORDER BY (ClickHouse) или PRIMARY KEY (Postgres)
        pk = None
        ob = re.search(r'ORDER\s+BY\s+([^\n,;]+)', tail, re.I)
        if ob:
            pk_candidate = ob.group(1).strip()
            pk_candidate = re.sub(r'[\(\)]','', pk_candidate).split(',')[0].strip()
            pk = pk_candidate.strip('`"')
        pk2 = re.search(r'PRIMARY\s+KEY\s*\(\s*([^\)]+)\)', inside, re.I)
        if pk2:
            pk = pk2.group(1).split(',')[0].strip(' `"\n')
        blocks.append((name, inside, pk))
    return blocks

def split_columns(block):
    # осторожно разбиваем колонки по запятым в верхнем уровне скобок
    cols = []
    cur = ''
    depth = 0
    for ch in block:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            cols.append(cur)
            cur = ''
        else:
            cur += ch
    if cur.strip():
        cols.append(cur)
    return cols

def parse_column_def(s):
    s = s.strip()
    if not s:
        return None
    # пропускаем CONSTRAINT/INDEX/PRIMARY KEY внутри подблоков (они уже обработаны)
    if re.match(r'PRIMARY\s+KEY|UNIQUE|KEY|INDEX|CONSTRAINT', s, re.I):
        return None
    # match name + type
    m = re.match(r'[`"]?([\w_]+)[`"]?\s+(.+)', s, re.S)
    if not m:
        return None
    name = m.group(1)
    rest = m.group(2).strip()
    # убрать DEFAULT ... , COMMENT ... , AFTER ... и т.д.
    rest = re.split(r'\bDEFAULT\b|\bCOMMENT\b|\bAFTER\b|\bENGINE\b|\bSTORED\b|\bNOT\s+NULL\b', rest, flags=re.I)[0].strip()
    # останется тип+возможно дополнительные атрибуты: берем первое слово/скобочную конструкцию
    # но тип может быть "LowCardinality(String)" или "VARCHAR(100)" или "Decimal(18,4)"
    type_match = re.match(r'([A-Za-z_0-9\(\),\s]+)', rest)
    col_type = type_match.group(1).strip() if type_match else rest
    col_type = normalize_type(col_type)
    return (name, col_type)

def to_dbml(table_name, cols, pk=None):
    lines = []
    lines.append(f"Table {table_name} {{")
    for name, typ in cols:
        pk_tag = ""
        if pk and name.lower() == pk.lower():
            pk_tag = " [pk]"
        lines.append(f"  {name} {typ}{pk_tag}")
    lines.append("}\n")
    return "\n".join(lines)

def convert(sql):
    blocks = extract_create_blocks(sql)
    out = []
    for name, inside, pk in blocks:
        parts = split_columns(inside)
        cols = []
        for p in parts:
            parsed = parse_column_def(p)
            if parsed:
                cols.append(parsed)
        out.append(to_dbml(name, cols, pk=pk))
    return "\n".join(out)




def main(argv):

    path = argv[1]
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
    tables = parse_create_tables(sql)
    dbml = to_dbml_with_refs(tables)
    print(dbml)

if __name__ == '__main__':
    main(sys.argv)


