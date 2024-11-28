from typing import Any

from config import settings
from models import Filmwork, FilmworkElastic, Person


class TransformDataToElastic():

    @staticmethod
    def check_attr(fw_elastic: FilmworkElastic, attr_name: str, val: Any):
        """Checking attribute for addition."""
        try:
            attr = getattr(fw_elastic, attr_name)
        except AttributeError as e:
            raise e
        if val not in attr:
            attr.append(val)
            return True

    @staticmethod
    def transform(films: list[Filmwork]) -> list[dict]:
        """Transformation data for elastic."""
        films_for_elastic = []
        films_ids_unique = {film.fw_id for film in films}
        for film_id in films_ids_unique:
            film_data = [film for film in films if film.fw_id == film_id]
            if film_data:
                film_detail = film_data[0]
                fw_elastic = FilmworkElastic(
                    id=film_id,
                    imdb_rating=film_detail.rating,
                    title=film_detail.title,
                    description=film_detail.description,
                )
                for film_detail in film_data:
                    name = film_detail.name
                    role = film_detail.role
                    full_name = film_detail.full_name
                    TransformDataToElastic.check_attr(fw_elastic,
                                                      'genres', name)
                    if role:
                        if TransformDataToElastic.check_attr(
                            fw_elastic, f'{role}s_names', full_name
                        ):
                            TransformDataToElastic.check_attr(
                                fw_elastic, f'{role}s',
                                Person(id=film_detail.id, name=full_name)
                            )
                film_for_elastic = (
                    {'_index': settings.index, '_id': film_id, } |
                    fw_elastic.model_dump()
                )
            films_for_elastic.append(film_for_elastic)
        return films_for_elastic
