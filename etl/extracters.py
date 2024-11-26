from contextlib import closing
from datetime import datetime
from typing import Generator

from psycopg import Connection, sql

from config import BATCH_SIZE
from models import FilmworkElastic, IdModified


class PostgresExtracter:
    def __init__(self, conn: Connection,
                 last_modified: datetime | None = None) -> None:
        self.conn = conn
        self.last_modified = last_modified

    def set_last_modified(self, modified: datetime) -> None:
        self.last_modified = modified

    def get_last_modified(self) -> datetime | None:
        return self.last_modified

    def get_id_modified(self, table: str) -> Generator[list[dict], None, None]:
        """Get all id, modified from table with modified > modified"""
        with closing(self.conn.cursor()) as pg_cursor:
            query = sql.SQL(
                'SELECT id, modified '
                'FROM content.{table} '
                'WHERE modified > {modified} '
                'ORDER BY modified'
            ).format(
                table=sql.Identifier(table),
                modified=self.last_modified,
            )
            pg_cursor.execute(query)
            while results := pg_cursor.fetchmany(BATCH_SIZE):
                yield results

    def get_film_work_id_modified(
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

    @staticmethod
    def transform_id_modified(
            data: list[dict], cls: IdModified | FilmworkElastic
         ) -> list[IdModified | FilmworkElastic]:
        """Transform dict in pydentic class"""
        records = []
        for record in data:
            records.append(cls(**record))
        return records

    def get_id_modified_for_load(
            self, table: str
         ) -> Generator[list[IdModified], None, None]:
        """Get id, modified from updated film_work"""
        for batch in self.get_id_modified(table):
            records = self.transform_id_modified(batch, IdModified)
            self.set_last_modified(records[-1].modified)
            if table == 'film_work':
                yield records
            else:
                for batch_films in self.get_film_work_id_modified(table,
                                                                  records):
                    yield self.transform_id_modified(batch_films, IdModified)

    def get_all_data(
            self, table: str
         ) -> Generator[list[FilmworkElastic], None, None]:
        """Get all data for updated film_work"""
        for batch in self.get_id_modified_for_load(table):
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
                yield self.transform_id_modified(res, FilmworkElastic)
