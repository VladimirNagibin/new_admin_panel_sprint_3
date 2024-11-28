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
    total_updated = 0
    for table in TABLES:
        modified = dt.fromisoformat(
            state.get_state(table, dt.min.isoformat())
        )
        updated = 0
        logger.info(f'Start of receiving data. Source table - <{table}>, '
                    f'last modified - {modified}')
        postgres_extracter.set_last_modified(modified)
        for batch in postgres_extracter.get_film_work_data(table):
            batch_for_elastic = TransformDataToElastic.transform(batch)
            updated += elastic_loader.load_data(batch_for_elastic)
        modified = postgres_extracter.get_last_modified()
        logger.info(f'Finish of receiving data. Source table - <{table}>, '
                    f'new last modified - {modified}, loading {updated}')
        state.set_state(table, modified.isoformat())
        total_updated += updated
    return total_updated


@backoff(exceptions=psycopg.errors.OperationalError)
def main():
    """Main function for elastic load data."""
    state = State(JsonFileStorage(settings.file_storage))
    logger.info(f'Start connecting to DB: {DSL["dbname"]} ...')
    with closing(
        psycopg.connect(**DSL, row_factory=dict_row,
                        cursor_factory=ClientCursor)
    ) as pg_conn:
        postgres_extracter = PostgresExtracter(pg_conn)
        logger.info('Created postgres extractor')
        elastic_loader = ElasticLoader(
            Elasticsearch(settings.elastic_host), settings.index
        )
        logger.info('Created elastic loader')
        counter = 1
        while True:
            logger.info(f'Start elastic load data. Iteration: {counter}')
            total_updated = load_to_elastic(postgres_extracter,
                                            elastic_loader, state)
            logger.info(f'Loading {total_updated} entries. '
                        f'Iteration: {counter}')
            counter += 1
            sleep(15)


if __name__ == '__main__':
    logger.info('Start elastic load data')
    try:
        main()
    except Exception as e:
        logger.exception(e)
