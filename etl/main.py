from contextlib import closing
from datetime import datetime as dt
from time import sleep

import psycopg
from elasticsearch import Elasticsearch
from psycopg import ClientCursor
from psycopg.rows import dict_row

from config import DSL, settings, TABLES
from extracters import PostgresExtracter
from loaders import ElasticLoader
from logger import logger
from services import backoff, JsonFileStorage, State
from transformers import TransformDataToElastic


def load_to_elastic(postgres_extracter: PostgresExtracter,
                    elastic_loader: ElasticLoader, state: State):
    """Loading updates to elastic based on updates in tables."""
    for table in TABLES:
        modified = dt.fromisoformat(
            state.get_state(table, dt.min.isoformat())
        )
        logger.info(f'Start update elastic: table - {table}, '
                    f'last modified - {modified}')
        postgres_extracter.set_last_modified(modified)
        for batch in postgres_extracter.get_film_work_data(table):
            batch_for_elastic = TransformDataToElastic.transform(batch)
            elastic_loader.load_data(batch_for_elastic)
        state.set_state(table,
                        postgres_extracter.get_last_modified().isoformat())


@backoff(exceptions=psycopg.errors.OperationalError)
def get_pgconnection(**DSL):
    logger.info(f'Connecting to DB: {DSL["dbname"]}')
    return psycopg.connect(**DSL, row_factory=dict_row,
                           cursor_factory=ClientCursor)


def main():
    """Main function for elastic load data."""
    try:
        state = State(JsonFileStorage(settings.file_storage))
        with closing(get_pgconnection(**DSL)) as pg_conn:
            postgres_extracter = PostgresExtracter(pg_conn)
            elastic_loader = ElasticLoader(
                Elasticsearch(settings.elastic_host), settings.index
            )
            while True:
                load_to_elastic(postgres_extracter, elastic_loader, state)
                logger.info('Update elastic success')
                sleep(15)
    except psycopg.errors.OperationalError as error:
        logger.exception(error)
        logger.info('Restart elastic load data')
        main()
    except Exception as e:
        logger.exception(e)


if __name__ == '__main__':
    logger.info('Start elastic load data')
    main()
