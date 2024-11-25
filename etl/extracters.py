from contextlib import closing
from datetime import datetime
from typing import Generator

from psycopg import Connection, sql

from models import FilmworkElastic, IdModified

BATCH_SIZE = 100


class PostgresExtracter:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def get_id_modified(
            self, table: str, modified: datetime
    ) -> Generator[list[dict], None, None]:
        """Get all id, modified from table with modified > modified"""
        with closing(self.conn.cursor()) as pg_cursor:
            query = sql.SQL(
                'SELECT id, modified '
                'FROM content.{table} '
                'WHERE modified > {modified} '
                'ORDER BY modified'
            ).format(
                table=sql.Identifier(table),
                modified=modified,
            )
            pg_cursor.execute(query)
            while results := pg_cursor.fetchmany(BATCH_SIZE):
                yield results

    def get_all_id_modified(
            self, table: str, records: list[IdModified]
    ) -> Generator[list[dict], None, None]:
        """Get id, modified from film_work"""
        with closing(self.conn.cursor()) as pg_cursor:
            query = ('SELECT DISTINCT fw.id, fw.modified '
                     'FROM content.film_work fw '
                     f'LEFT JOIN content.{table}_film_work tfw '
                     'ON tfw.film_work_id = fw.id '
                     f'WHERE tfw.{table}_id = ANY(%s) '
                     'ORDER BY fw.modified')
            pg_cursor.execute(query, [[record.id for record in records]])
            while results := pg_cursor.fetchmany(BATCH_SIZE):
                yield results

    def transform_id_modified(seif, data: list[dict]) -> list[IdModified]:
        """Transform dict in pydentic class"""
        records = []
        for record in data:
            records.append(IdModified(**record))
        return records

    def transform_filmwork(seif, data: list[dict]) -> list[FilmworkElastic]:
        """Transform dict in pydentic class"""
        records = []
        for record in data:
            records.append(FilmworkElastic(**record))
        return records

    def get_id_modified_for_load(
            self, table: str, modified: datetime
    ) -> Generator[list[IdModified], None, None]:
        """Get id, modified from updated film_work"""
        for batch in self.get_id_modified(table, modified):
            records = self.transform_id_modified(batch)
            if table == 'film_work':
                yield records
            else:
                for batch_films in self.get_all_id_modified(table, records):
                    yield self.transform_id_modified(batch_films)

    def get_all_data(
            self, table: str, modified: datetime
    ):# -> list[FilmworkElastic]:
        """Get id, modified from updated film_work"""
        for batch in self.get_id_modified_for_load(table, modified):
            with closing(self.conn.cursor()) as pg_cursor:
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
                         'LEFT JOIN content.person_film_work pfw '
                         'ON pfw.film_work_id = fw.id '
                         'LEFT JOIN content.person p '
                         'ON p.id = pfw.person_id '
                         'LEFT JOIN content.genre_film_work gfw '
                         'ON gfw.film_work_id = fw.id '
                         'LEFT JOIN content.genre g ON g.id = gfw.genre_id '
                         'WHERE fw.id = ANY(%s)')
                pg_cursor.execute(query, [[record.id for record in batch]])
                res = pg_cursor.fetchall()
                yield self.transform_filmwork(res)
