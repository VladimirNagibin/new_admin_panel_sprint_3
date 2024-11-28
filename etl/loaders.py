from elasticsearch import (ApiError, ConnectionError, ConnectionTimeout,
                           Elasticsearch)
from elasticsearch.helpers import bulk, BulkIndexError

from config import settings
from logger import logger
from services import backoff, get_dict_from_file


class ElasticLoader():
    def __init__(self, elastic: Elasticsearch, index: str) -> None:
        self.elastic = elastic
        self.index = index
        self.check_index()

    @backoff(exceptions=(ConnectionError, ConnectionTimeout))
    def load_data(self, data: list[dict]):
        """Loading data to elasticsearch."""
        logger.info(f'Start loading to elastic a batch of {len(data)} items')
        try:
            success, failed = bulk(self.elastic, data, index=self.index)
        except (ApiError, BulkIndexError) as e:
            raise e
        logger.info(f'Successfully uploaded {success}, failed {failed}')

    @backoff(exceptions=(ConnectionError, ConnectionTimeout))
    def check_index(self):
        """Check index and create if not exist"""
        if not self.elastic.indices.exists(index=self.index):
            logger.info(f'Try to create elastic index {self.index}')
            index_scheme = get_dict_from_file(settings.file_index_scheme)
            self.elastic.indices.create(index=self.index, ignore=400,
                                        body=index_scheme)
            logger.info(f'Elastic index {self.index} created')
