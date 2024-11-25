from typing import Type

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env',
                                      env_file_encoding='utf-8')
    postgres_user: str
    postgres_password: str
    postgres_db: str
    sql_host: str
    sql_port: int
    elastic_host: str


BATCH_SIZE = 100

settings = Settings()

DSL = {
    'dbname': settings.postgres_db,
    'user': settings.postgres_user,
    'password': settings.postgres_password,
    'host': settings.sql_host,
    'port': settings.sql_port,
}