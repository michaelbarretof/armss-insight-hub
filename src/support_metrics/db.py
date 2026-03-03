from __future__ import annotations

import ssl
import traceback
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

DEFAULT_SCHEMA = "armss"

def _make_engine_from_url(url, connect_args: dict | None = None) -> Engine:
    return sqlalchemy.create_engine(
        url,
        connect_args=connect_args or {},
        pool_size=5,
        max_overflow=2,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        future=True,
    )

def _make_engine_from_parts(drivername: str, username: str, password: str, host: str, port: int, database: str, connect_args: dict | None = None) -> Engine:
    url = URL.create(
        drivername=drivername,
        username=username,
        password=password,
        host=host,
        port=int(port),
        database=database,
    )
    return _make_engine_from_url(url, connect_args=connect_args)

def connect_pool_local(settings) -> Engine:
    if settings.pg_dsn:
        return _make_engine_from_url(settings.pg_dsn)

    missing = [k for k, v in {
        "PG_HOST": settings.pg_host,
        "PG_DB": settings.pg_db,
        "PG_USER": settings.pg_user,
        "PG_PASS": settings.pg_pass,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars for local Postgres: {', '.join(missing)}")

    return _make_engine_from_parts(
        drivername=settings.pg_driver,
        username=settings.pg_user,
        password=settings.pg_pass,
        host=settings.pg_host,
        port=settings.pg_port,
        database=settings.pg_db,
        connect_args={},
    )

def connect_pool_supabase(settings) -> Engine:
    # Supabase normalmente requiere SSL; con pg8000 se puede pasar ssl_context. [web:250]
    connect_args = {}
    if settings.supa_ssl:
        connect_args["ssl_context"] = ssl.create_default_context()

    if settings.supa_pg_dsn:
        return _make_engine_from_url(settings.supa_pg_dsn, connect_args=connect_args)

    missing = [k for k, v in {
        "SUPA_HOST": settings.supa_host,
        "SUPA_PASS": settings.supa_pass,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars for Supabase Postgres: {', '.join(missing)}")

    return _make_engine_from_parts(
        drivername=settings.supa_driver,
        username=settings.supa_user,
        password=settings.supa_pass,
        host=settings.supa_host,
        port=settings.supa_port,
        database=settings.supa_db,
        connect_args=connect_args,
    )

def exe_query(sql: str, pool: Engine, params: dict | None = None):
    try:
        with pool.connect() as conn:
            res = conn.execute(text(sql), params or {}).fetchall()
        return res
    except Exception as ex:
        raise Exception("Error en connectionDB -> exe_query: " + str(ex)) from ex

def exe_non_query(sql: str, pool: Engine, params: dict | None = None) -> None:
    try:
        with pool.begin() as conn:
            conn.execute(text(sql), params or {})
    except Exception as e:
        tb = "".join(traceback.format_tb(e.__traceback__))
        raise Exception(tb + " Exception: " + str(e)) from e

def exe_non_query_many(sql: str, rows: list[dict], pool: Engine) -> None:
    if not rows:
        return
    try:
        with pool.begin() as conn:
            conn.execute(text(sql), rows)
    except Exception as e:
        tb = "".join(traceback.format_tb(e.__traceback__))
        raise Exception(tb + " Exception: " + str(e)) from e

def get_state(pool: Engine, key: str, schema: str = DEFAULT_SCHEMA) -> str | None:
    sql = f"select value from {schema}.etl_state where key = :k"
    rows = exe_query(sql, pool, {"k": key})
    return rows[0][0] if rows else None

def set_state(pool: Engine, key: str, value: str, schema: str = DEFAULT_SCHEMA) -> None:
    sql = f"""
    insert into {schema}.etl_state(key, value)
    values (:k, :v)
    on conflict (key) do update set value = excluded.value, updated_at = now()
    """
    exe_non_query(sql, pool, {"k": key, "v": value})
