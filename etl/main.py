from contextlib import closing
from datetime import datetime as dt

import psycopg
import backoff
from elasticsearch import Elasticsearch
from psycopg import ClientCursor
from psycopg import Connection
from psycopg.rows import dict_row

from config import DSL, settings, TABLES
from extracters import PostgresExtracter
from loaders import ElasticLoader
from states import JsonFileStorage, State
from transformers import TransformToElastic


def load_to_elastic(pg_conn: Connection, elastic: Elasticsearch, state: State):
    postgres_extracter = PostgresExtracter(pg_conn)
    elastic_loader = ElasticLoader(elastic)
    for table in TABLES:
        modified = dt.fromisoformat(
            state.get_state(table, dt.min.isoformat())
        )
        postgres_extracter.set_last_modified(modified)
        for batch in postgres_extracter.get_all_data(table):
            batch_for_elastic = TransformToElastic.transform(batch)
            elastic_loader.load_data(batch_for_elastic)
        state.set_state(table,
                        postgres_extracter.get_last_modified().isoformat())


@backoff.on_exception(backoff.expo, psycopg.errors.OperationalError,
                      max_time=60)
def get_pgconnection(**DSL):
    return psycopg.connect(**DSL, row_factory=dict_row,
                           cursor_factory=ClientCursor)


if __name__ == '__main__':
    json_file_storage = JsonFileStorage(settings.file_storage)
    state = State(json_file_storage)
    with closing(get_pgconnection(**DSL)) as pg_conn:
        elastic = Elasticsearch(settings.elastic_host)
        load_to_elastic(pg_conn, elastic, state)
