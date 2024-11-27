from contextlib import closing
from datetime import datetime
from typing import Generator, Optional

import psycopg
from psycopg import Connection, sql

from config import BATCH_SIZE
from logger import logger
from models import Filmwork, IdModified


class PostgresExtracter:
    def __init__(self, conn: Connection,
                 last_modified: Optional[datetime] = None) -> None:
        self.conn = conn
        self.last_modified = last_modified

    def set_last_modified(self, modified: datetime) -> None:
        self.last_modified = modified

    def get_last_modified(self) -> Optional[datetime]:
        return self.last_modified

    def get_data(
            self, query: sql.Composed | str, params: Optional[list] = None
       ) -> Generator[list[dict], None, None]:
        """Get a generator with query result."""
        try:
            with closing(self.conn.cursor()) as pg_cursor:
                pg_cursor.execute(query, params)
                while results := pg_cursor.fetchmany(BATCH_SIZE):
                    yield results
        except psycopg.Error as e:
            raise e

    def get_id_modified(self, table: str) -> Generator[list[dict], None, None]:
        """Get id, modified from the table updated after last_modified."""
        query = sql.SQL(
            'SELECT id, modified '
            'FROM content.{table} '
            'WHERE modified > {modified} '
            'ORDER BY modified'
        ).format(
            table=sql.Identifier(table),
            modified=self.last_modified,
        )
        logger.info(f'Get id, modified from {table} updated '
                    f'after {self.last_modified}')
        return self.get_data(query)

    def get_film_work_id_modified(
            self, table: str, records: list[IdModified]
    ) -> Generator[list[dict], None, None]:
        """
        Get id, modified from the film_work which are related
        to the values from the table updated after last_modified.
        """
        query = ('SELECT DISTINCT fw.id, fw.modified '
                 'FROM content.film_work fw '
                 f'LEFT JOIN content.{table}_film_work tfw '
                 'ON tfw.film_work_id = fw.id '
                 f'WHERE tfw.{table}_id = ANY(%s) '
                 'ORDER BY fw.modified')
        logger.info('Get id, modified from film_work for update '
                    f'which related to {table}')
        return self.get_data(query,
                             params=[[record.id for record in records]])

    @staticmethod
    def transform_to_model(
            data: list[dict], cls: IdModified | Filmwork
         ) -> list[IdModified | Filmwork]:
        """Transform dict into pydantic class."""
        records = []
        for record in data:
            try:
                records.append(cls(**record))
            except ValueError as e:
                raise e
        return records

    def get_id_modified_for_load(
            self, table: str
         ) -> Generator[list[IdModified], None, None]:
        """
        Get id, modified from the film_work updated after last_modified or
        related to updated table.
        """
        for batch in self.get_id_modified(table):
            records = self.transform_to_model(batch, IdModified)
            self.set_last_modified(records[-1].modified)
            if table == 'film_work':
                yield records
            else:
                for batch_films in self.get_film_work_id_modified(table,
                                                                  records):
                    yield self.transform_to_model(batch_films, IdModified)

    def get_film_work_data(
            self, table: str
         ) -> Generator[list[Filmwork], None, None]:
        """Get films data to update."""
        for batch in self.get_id_modified_for_load(table):
            try:
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
                             'LEFT JOIN content.genre g '
                             'ON g.id = gfw.genre_id '
                             'WHERE fw.id = ANY(%s)')
                    pg_cursor.execute(query, [[record.id for record in batch]])
                    yield self.transform_to_model(pg_cursor.fetchall(),
                                                  Filmwork)
            except psycopg.Error as e:
                raise e
