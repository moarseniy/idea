import json

from scripts.xml_scripts.xml2graph import build_graph_from_address
from scripts.xml_scripts.profile2entities import profile_to_entities
from scripts.xml_scripts.final_profile import make_final_profile
from scripts.xml_scripts.dbml_minimal import generate_dbml_minimal
from scripts.xml_scripts.ddl_postgres import generate_postgres_ddl
from scripts.xml_scripts.ddl_clickhouse import generate_clickhouse_ddl

def run_compute_xml_profile(path):
    profile = build_graph_from_address(path)  # или URL, или XML-строка
    entities = profile_to_entities(profile, table_name_style="short")
    return entities


def run_final_xml_profile(profile, json_answer_patch, path):
    
    # patch - это  ответ от LLM - Просим нам LLM правильно назвать сущности и поля бизнесово.
    # result = validate_rename_patch(profile, json_answer_patch)  # критик - он уже сам скажет какие ошибки в ответе и если все норм, то SUCCESS
    # print("(validate_rename_patch):", result)
    
    final_profile = make_final_profile(profile, json_answer_patch, path)

    return final_profile


def xml_get_postgres_ddl(final_spec):
    pg_sql = generate_postgres_ddl(final_spec, schema="public") # emit_unique=False
    return pg_sql


def xml_get_clickhouse_ddl(final_spec):
    ch_sql = generate_clickhouse_ddl(final_spec, database="raw") # include_unique_comments=True
    return ch_sql


def xml_get_dbml(final_spec):
    dbml = generate_dbml_minimal(final_spec, with_project=False)
    return dbml