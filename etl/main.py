import datetime as dt
from contextlib import closing

import psycopg
import backoff
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg import ClientCursor
from psycopg import Connection
from psycopg.rows import dict_row

from config import DSL, settings
from extracters import PostgresExtracter
from loader import ElasticLoader
from transformers import TransformToElastic


def load_to_elastic(pg_conn: Connection, elastic: Elasticsearch):
    postgres_extracter = PostgresExtracter(pg_conn)
    elastic_loader = ElasticLoader(elastic)
    for batch in postgres_extracter.get_all_data('film_work', dt.datetime.min):
        batch_for_elastic = TransformToElastic.transform(batch)
        elastic_loader.load_data(batch_for_elastic)

@backoff.on_exception(backoff.expo, psycopg.errors.OperationalError,
                      max_time=60)
def get_pgconnection(**DSL):
    # print(f'Try connect {dt.datetime.now()}')
    #with (closing(
    #    psycopg.connect(**DSL, row_factory=dict_row,
    #                    cursor_factory=ClientCursor)
    #) as pg_conn):
    #    return pg_conn
    return psycopg.connect(**DSL, row_factory=dict_row,
                           cursor_factory=ClientCursor)


if __name__ == '__main__':
    pg_conn = get_pgconnection(**DSL)
    elastic = Elasticsearch(settings.elastic_host)
    load_to_elastic(pg_conn, elastic)
