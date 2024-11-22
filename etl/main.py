from contextlib import closing

import psycopg
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from psycopg import ClientCursor
from psycopg import connection as _connection
from psycopg.rows import dict_row

from settings import DSL


with (closing(psycopg.connect(
    **DSL, row_factory=dict_row, cursor_factory=ClientCursor
     )) as pg_conn):
    with closing(pg_conn.cursor()) as pg_cursor:
        query = ("""SELECT id, modified
                    FROM content.person
                    WHERE modified > '2000-06-16T20:14:09.309765+0000'
                    ORDER BY modified
                    LIMIT 100;""")
        res = pg_cursor.execute(query)
        person_ids = res.fetchall()
        per1 = []
        for person in person_ids:
            per1.append("'" + str(person['id']) + "'")
        per = ' ,'.join(per1)
        query = ('SELECT fw.id, fw.modified '
                 'FROM content.film_work fw '
                 'LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id '
                 f'WHERE pfw.person_id IN ({per}) '
                 'ORDER BY fw.modified '
                 'LIMIT 100;')
        res = pg_cursor.execute(query)
        films_ids = res.fetchall()
        films_ids_unique = set([film['id'] for film in films_ids])
        print(films_ids_unique)
        per1 = []
        for film in films_ids:
            per1.append("'" + str(film['id']) + "'")
        per = ' ,'.join(per1)
        query = ('SELECT '
                 'fw.id as fw_id,'
                 'fw.title, '
                 'fw.description, '
                 'fw.rating, '
                 'fw.type, '
                 'fw.created, '
                 'fw.modified, '
                 'pfw.role, '
                 'p.id, '
                 'p.full_name,'
                 'g.name '
                 'FROM content.film_work fw '
                 'LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id '
                 'LEFT JOIN content.person p ON p.id = pfw.person_id '
                 'LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id '
                 'LEFT JOIN content.genre g ON g.id = gfw.genre_id '
                 f'WHERE fw.id IN ({per}); ')
        res = pg_cursor.execute(query)
        films_data = res.fetchall()
        #print(films_data)
        films_for_elastic = []
        #print(type(films_ids))
        for film_dict in films_ids_unique:
            #print(film_dict['id'])
            film_data = [film for film in films_data if film.get('fw_id') == film_dict]
            if film_data:
                film_for_elastic = {
                    'id': str(film_data[0]['fw_id']),
                    'imdb_rating': film_data[0]['rating'],
                    'genres': [],
                    'title': film_data[0]['title'],
                    'description': film_data[0]['description'],
                    'directors_names': [],
                    'actors_names': [],
                    'writers_names': [],
                    'directors': [],
                    'actors': [],
                    'writers': [],
                }
                for film_detail in film_data:
                    name = film_detail['name']
                    role = film_detail['role']
                    id = film_detail['id']
                    full_name = film_detail['full_name']
                    if name not in film_for_elastic['genres']:
                        film_for_elastic['genres'].append(name)
                    if full_name not in film_for_elastic[f'{role}s_names']:
                        film_for_elastic[f'{role}s_names'].append(full_name)
                        film_for_elastic[f'{role}s'].append(
                            {'id': str(id), 'name': full_name}
                        )
            films_for_elastic.append(film_for_elastic)
#print(films_for_elastic)
e = Elasticsearch('http://localhost:9200')  # no args, connect to localhost:9200
#if not e.indices.exists(INDEX):
#    raise RuntimeError('index does not exists, use `curl -X PUT "localhost:9200/%s"` and try again'%INDEX)

bulk_data = []
for document in films_for_elastic:
    id = document['id']
    bulk_data.append({"index": {"_index": "movies",  "_id": id}})
    bulk_data.append(document)
# print(bulk_data)
#re, res = bulk(e, bulk_data)

# Check the response
#print(re, res)
r = e.bulk(index='movies', body=bulk_data) # return a dict
print(r)
