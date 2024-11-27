from elasticsearch import (ApiError, ConnectionError, ConnectionTimeout,
                           Elasticsearch)
from elasticsearch.helpers import bulk, BulkIndexError
import backoff

from logger import logger


class ElasticLoader():
    def __init__(self, elastic: Elasticsearch, index: str) -> None:
        self.elastic = elastic
        self.index = index

    @backoff.on_exception(backoff.expo, (ConnectionError, ConnectionTimeout),
                          max_time=600)
    def load_data(self, data: list[dict]):
        """Loading data to elasticsearch."""
        logger.info(f'Start loading to elastic a batch of {len(data)} items')
        try:
            success, failed = bulk(self.elastic, data, index=self.index)
        except (ApiError, BulkIndexError) as e:
            raise e
        logger.info(f'Successfully uploaded {success}, failed {failed}')
