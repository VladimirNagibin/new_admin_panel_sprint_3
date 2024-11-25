from elasticsearch import Elasticsearch


class ElasticLoader():
    def __init__(self, elastic: Elasticsearch) -> None:
        self.elastic = elastic

    def load_data(self, data: list[dict]):
        bulk_data = []
        for document in data:
            bulk_data.append(
                {"index": {"_index": "movies",  "_id": document['id']}}
            )
            bulk_data.append(document)
            #  re, res = bulk(e, bulk_data)
            r = self.elastic.bulk(index='movies', body=bulk_data)
            # print(r)
