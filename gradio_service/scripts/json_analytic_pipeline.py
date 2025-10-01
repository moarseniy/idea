import json

from scripts.json_scripts.json_profile_generator import generate_profile
# from scripts.json_scripts.validate_rename_patch import validate_rename_patch
from scripts.json_scripts.final_profile import build_final_profile

from scripts.json_scripts.ddl_postgres import emit_ddl_pg
from scripts.json_scripts.ddl_clickhouse import emit_ddl_ch

from scripts.json_scripts.dbml_from_profile import emit_dbml, save_dbml


def run_compute_json_profile(path):
    profile = generate_profile(path)
    return profile


def run_final_json_profile(profile, json_answer_patch):
    
    # patch - это  ответ от LLM - Просим нам LLM правильно назвать сущности и поля бизнесово.
    # result = validate_rename_patch(profile, json_answer_patch)  # критик - он уже сам скажет какие ошибки в ответе и если все норм, то SUCCESS
    # print("(validate_rename_patch):", result)
    
    final_profile = build_final_profile(profile, json_answer_patch)

    return final_profile


def json_get_clickhouse_ddl(final_prof):
    ddl_ch = emit_ddl_ch(final_prof, types_yaml_path="config/types.yaml", database="mydb")  # database опционально
    return ddl_ch


def json_get_postgres_ddl(final_prof):
    ddl_pg = emit_ddl_pg(final_prof, types_yaml_path="config/types.yaml")
    return ddl_pg


def json_get_dbml(final_prof):
    dbml_text = emit_dbml(final_prof, project_database_type="Generic")
    print(dbml_text[:500], "...")

    # или сохранить в файл
    # save_dbml(final_prof, "schema.dbml", project_database_type="Generic")
    return dbml_text