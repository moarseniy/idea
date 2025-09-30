# dags/ie_registry_ingest.py
from __future__ import annotations
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Any

from airflow import DAG
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
}

@dag(
    dag_id="ie_registry_ingest",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,               # поставьте '0 3 * * *' для ежедневного прогона
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "ie", "json"],
)
def ie_registry_ingest():

    @task
    def read_json_to_raw() -> Dict[str, Any]:
        """
        Читает JSON-файл и кладёт payload как есть в raw.ingest_registry.
        Путь к файлу берём из Airflow Variable: IE_JSON_PATH.
        """
        json_path = Variable.get("IE_JSON_PATH")  # например: /opt/airflow/data/ie.json
        with open(json_path, "r", encoding="utf-8") as f:
            payload = json.load(f)

        pg = PostgresHook(postgres_conn_id="postgres_default")
        pg.run(
            "INSERT INTO raw.ingest_registry (source_file, payload) VALUES (%s, %s)",
            parameters=(json_path, json.dumps(payload, ensure_ascii=False)),
        )
        return {"count": len(payload)}

    @task
    def upsert_core_tables(_meta: Dict[str, Any]) -> int:
        """
        Разбирает последний payload из raw.ingest_registry и апсертом пишет:
        - core.individual_entrepreneur
        - core.individual_entrepreneur_okved_opt
        Возвращает кол-во обработанных записей.
        """
        pg = PostgresHook(postgres_conn_id="postgres_default")
        # берём последний payload
        records = pg.get_first("SELECT id, payload FROM raw.ingest_registry ORDER BY id DESC LIMIT 1")
        if not records:
            return 0
        _, payload = records
        data: List[Dict[str, Any]] = payload

        # Подготовим батчи
        ie_rows = []
        okved_rows = []

        def hsh(obj: Dict[str, Any]) -> bytes:
            return hashlib.sha256(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).digest()

        for r in data:
            # Основная карточка
            ie = {
                "innfl": r.get("innfl"),
                "ogrnip": r.get("ogrnip"),
                "surname": r.get("inf_surname_ind_entrep_surname"),
                "firstname": r.get("inf_surname_ind_entrep_firstname"),
                "midname": r.get("inf_surname_ind_entrep_midname"),
                "sex": r.get("inf_surname_ind_entrep_sex"),
                "dob": r.get("dob"),
                "id_card": r.get("id_card"),
                "id_card_alien": r.get("id_card_alien_for_rus"),
                "cit_kind": r.get("citizenship_kind"),
                "cit_name": r.get("citizenship_name"),
                "date_ogrnip": r.get("date_ogrnip"),
                "auth_code": r.get("inf_authority_reg_ind_entrep_code"),
                "auth_name": r.get("inf_authority_reg_ind_entrep_name"),
                "reg_tax":  r.get("inf_reg_tax_ind_entrep"),
                "status_code": r.get("inf_status_ind_entrep_code"),
                "status_name": r.get("inf_status_ind_entrep_name"),
                "stop_code": r.get("inf_stop_ind_entrep_code"),
                "stop_name": r.get("inf_stop_ind_entrep_name"),
                "okved_code": r.get("inf_okved_code"),
                "okved_name": r.get("inf_okved_name"),
                "insured_pf": r.get("insured_pf"),
                "insuref_fss": r.get("insuref_fss"),
                "email": r.get("email_ind_entrep"),
                "date_exec": r.get("date_exec"),
                "process_dttm": r.get("process_dttm"),
                "error_code": r.get("error_code"),
                "hash_last_payload": hsh(r),
            }
            ie_rows.append(ie)

            # Вложенные доп. ОКВЭД
            for opt in (r.get("inf_okved_opt") or []):
                d = opt.get("ГРНИПДата") or {}
                okved_rows.append({
                    "innfl": r.get("innfl"),
                    "grnip_id": d.get("attr_ГРНИП"),
                    "grnip_date": d.get("attr_ДатаЗаписи"),
                    "code": opt.get("attr_КодОКВЭД"),
                    "name": opt.get("attr_НаимОКВЭД"),
                    "ver":  opt.get("attr_ПрВерсОКВЭД"),
                    "date_exec": r.get("date_exec"),
                })

        # Апсерты
        # 1) Карточки
        upsert_ie_sql = """
        INSERT INTO core.individual_entrepreneur (
            innfl, ogrnip,
            inf_surname_ind_entrep_surname,
            inf_surname_ind_entrep_firstname,
            inf_surname_ind_entrep_midname,
            inf_surname_ind_entrep_sex,
            dob, id_card, id_card_alien_for_rus,
            citizenship_kind, citizenship_name,
            date_ogrnip,
            inf_authority_reg_ind_entrep_code,
            inf_authority_reg_ind_entrep_name,
            inf_reg_tax_ind_entrep,
            inf_status_ind_entrep_code,
            inf_status_ind_entrep_name,
            inf_stop_ind_entrep_code,
            inf_stop_ind_entrep_name,
            inf_okved_code, inf_okved_name,
            insured_pf, insuref_fss, email_ind_entrep,
            date_exec, process_dttm, error_code,
            hash_last_payload, updated_at
        )
        VALUES (
            %(innfl)s, %(ogrnip)s,
            %(surname)s, %(firstname)s, %(midname)s, %(sex)s,
            %(dob)s, %(id_card)s, %(id_card_alien)s,
            %(cit_kind)s, %(cit_name)s,
            %(date_ogrnip)s,
            %(auth_code)s, %(auth_name)s, %(reg_tax)s,
            %(status_code)s, %(status_name)s,
            %(stop_code)s, %(stop_name)s,
            %(okved_code)s, %(okved_name)s,
            %(insured_pf)s, %(insuref_fss)s, %(email)s,
            %(date_exec)s, %(process_dttm)s, %(error_code)s,
            %(hash_last_payload)s, now()
        )
        ON CONFLICT (innfl) DO UPDATE
        SET
            ogrnip = EXCLUDED.ogrnip,
            inf_surname_ind_entrep_surname = EXCLUDED.inf_surname_ind_entrep_surname,
            inf_surname_ind_entrep_firstname = EXCLUDED.inf_surname_ind_entrep_firstname,
            inf_surname_ind_entrep_midname = EXCLUDED.inf_surname_ind_entrep_midname,
            inf_surname_ind_entrep_sex = EXCLUDED.inf_surname_ind_entrep_sex,
            dob = EXCLUDED.dob,
            id_card = EXCLUDED.id_card,
            id_card_alien_for_rus = EXCLUDED.id_card_alien_for_rus,
            citizenship_kind = EXCLUDED.citizenship_kind,
            citizenship_name = COALESCE(EXCLUDED.citizenship_name, core.individual_entrepreneur.citizenship_name),
            date_ogrnip = EXCLUDED.date_ogrnip,
            inf_authority_reg_ind_entrep_code = EXCLUDED.inf_authority_reg_ind_entrep_code,
            inf_authority_reg_ind_entrep_name = EXCLUDED.inf_authority_reg_ind_entrep_name,
            inf_reg_tax_ind_entrep = EXCLUDED.inf_reg_tax_ind_entrep,
            inf_status_ind_entrep_code = EXCLUDED.inf_status_ind_entrep_code,
            inf_status_ind_entrep_name = EXCLUDED.inf_status_ind_entrep_name,
            inf_stop_ind_entrep_code = EXCLUDED.inf_stop_ind_entrep_code,
            inf_stop_ind_entrep_name = EXCLUDED.inf_stop_ind_entrep_name,
            inf_okved_code = EXCLUDED.inf_okved_code,
            inf_okved_name = EXCLUDED.inf_okved_name,
            insured_pf = EXCLUDED.insured_pf,
            insuref_fss = COALESCE(EXCLUDED.insuref_fss, core.individual_entrepreneur.insuref_fss),
            email_ind_entrep = EXCLUDED.email_ind_entrep,
            date_exec = EXCLUDED.date_exec,
            process_dttm = EXCLUDED.process_dttm,
            error_code = EXCLUDED.error_code,
            hash_last_payload = EXCLUDED.hash_last_payload,
            updated_at = now()
        WHERE
            core.individual_entrepreneur.process_dttm IS NULL
            OR EXCLUDED.process_dttm > core.individual_entrepreneur.process_dttm;
        """

        # 2) Доп. ОКВЭД: «мягкий» апсерт — на ключ (innfl, okved_code, grnip_record_id, date_exec)
        upsert_okved_sql = """
        INSERT INTO core.individual_entrepreneur_okved_opt (
            innfl, grnip_record_id, grnip_record_date,
            okved_code, okved_name, okved_version, date_exec
        )
        VALUES (
            %(innfl)s, %(grnip_id)s, %(grnip_date)s,
            %(code)s, %(name)s, %(ver)s, %(date_exec)s
        )
        ON CONFLICT (innfl, okved_code, grnip_record_id, date_exec) DO UPDATE
        SET okved_name = EXCLUDED.okved_name,
            okved_version = EXCLUDED.okved_version,
            grnip_record_date = EXCLUDED.grnip_record_date;
        """

        # батчево
        pg.insert_rows(
            table="core.individual_entrepreneur",
            rows=[tuple(),],    # не используем, но метод требует сигнатуру — вместо этого run_many ниже
            target_fields=[],
            commit_every=0
        )  # no-op, просто чтобы иметь доступ к соединению
        # Используем низкоуровневый cursor для executemany
        conn = pg.get_conn()
        with conn.cursor() as cur:
            cur.executemany(upsert_ie_sql, ie_rows)
            if okved_rows:
                cur.executemany(upsert_okved_sql, okved_rows)
        conn.commit()

        return len(ie_rows)

    @task
    def dq_checks(processed_count: int):
        """
        Простые проверки качества.
        """
        if processed_count == 0:
            raise ValueError("Ни одной записи не обработано")

        pg = PostgresHook(postgres_conn_id="postgres_default")
        # пример: нет дублей по innfl
        dup = pg.get_first("""
            SELECT COUNT(*) FROM (
                SELECT innfl, COUNT(*) c FROM core.individual_entrepreneur GROUP BY innfl HAVING COUNT(*) > 1
            ) t
        """)[0]
        if dup and dup > 0:
            raise ValueError(f"Обнаружены дубли innfl: {dup}")

    meta = read_json_to_raw()
    cnt  = upsert_core_tables(meta)
    dq_checks(cnt)

ie_registry_ingest()
