# ch_reset.py
from typing import Any, Dict, Optional
from ddl_clickhouse import emit_ddl_ch
from load_clickhouse import insert_into_ch
from ch_exec import ch_exec_many

def drop_ch_tables_for_profile(http_url: str, profile: Dict[str, Any], database: str,
                               user: Optional[str] = None, password: Optional[str] = None,
                               trust_env: bool = False) -> None:
    parts = []
    # дети -> родитель
    for e in sorted(profile.get("entities", []), key=lambda x: x.get("depth", 0), reverse=True):
        t = e["name"]
        parts.append(f"DROP TABLE IF EXISTS `{database}`.`{t}`")
    ddl = ";\n".join(parts) + ";"
    ch_exec_many(http_url, ddl, database=None, user=user, password=password, trust_env=trust_env)

def recreate_and_load_ch(http_url: str, profile: Dict[str, Any], records,
                         database: str, types_yaml_path: str = "config/types.yaml",
                         batch_size: int = 100_000, cast: bool = True,
                         user: Optional[str] = None, password: Optional[str] = None,
                         trust_env: bool = False) -> None:
    ddl = emit_ddl_ch(profile, types_yaml_path=types_yaml_path, database=database)
    ch_exec_many(http_url, ddl, database=database, user=user, password=password, trust_env=trust_env)
    insert_into_ch(http_url, profile, records, database=database, batch_size=batch_size,
                   cast=cast, user=user, password=password, trust_env=trust_env)
