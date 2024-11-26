from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elastic_transport import ConnectionError, ConnectionTimeout

import backoff


class ElasticLoader():
    def __init__(self, elastic: Elasticsearch, index: str = 'movies') -> None:
        self.elastic = elastic
        self.index = index

    @backoff.on_exception(backoff.expo, (ConnectionError, ConnectionTimeout),
                          max_time=60)
    def load_data(self, data: list[dict]):
        success, failed = bulk(self.elastic, data, index=self.index)
        # print(success)
        if failed:
            raise Exception(failed)
