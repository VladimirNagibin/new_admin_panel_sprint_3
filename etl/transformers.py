from config import settings
from models import Filmwork, FilmworkElastic


class TransformDataToElastic():

    @staticmethod
    def transform(films: list[Filmwork]) -> list[FilmworkElastic]:
        films_for_elastic = []
        films_ids_unique = {film.fw_id for film in films}
        for film_id in films_ids_unique:
            film_data = [film for film in films if film.fw_id == film_id]
            if film_data:
                film_detail = film_data[0]
                film_for_elastic = {
                    '_index': settings.index,
                    '_id': film_id,
                    'id': film_id,
                    'imdb_rating': film_detail.rating,
                    'genres': [],
                    'title': film_detail.title,
                    'description': film_detail.description,
                    'directors_names': [],
                    'actors_names': [],
                    'writers_names': [],
                    'directors': [],
                    'actors': [],
                    'writers': [],
                }
                for film_detail in film_data:
                    name = film_detail.name
                    role = film_detail.role
                    id = film_detail.id
                    full_name = film_detail.full_name
                    if name not in film_for_elastic['genres']:
                        film_for_elastic['genres'].append(name)
                    if role:
                        if full_name not in film_for_elastic[f'{role}s_names']:
                            film_for_elastic[f'{role}s_names'].append(
                                full_name
                            )
                            film_for_elastic[f'{role}s'].append(
                                {'id': str(id), 'name': full_name}
                            )
        #    fw = FilmworkElastic(**film_for_elastic)
            #print(fw)
        #    print('================================================')
            #print(fw.model_dump_json())
        #    res = {'_index': settings.index, '_id': film_id, } | fw.model_dump_json()
        #    print(res)
            films_for_elastic.append(film_for_elastic)
        return films_for_elastic
