import uuid
from datetime import datetime

from pydantic import BaseModel


class IdModified(BaseModel):
    id: uuid.UUID
    modified: datetime


class Filmwork(BaseModel):
    fw_id: uuid.UUID
    title: str
    description: str | None
    rating: float | None
    type: str
    created: datetime
    modified: datetime
    role: str | None
    id: uuid.UUID | None
    full_name: str | None
    name: str | None


class Person(BaseModel):
    id: uuid.UUID
    name: str


class FilmworkElastic(BaseModel):
    id: uuid.UUID
    imdb_rating: float | None
    genres: list[str] = []
    title: str
    description: str | None
    directors_names: list[str] = []
    actors_names: list[str] = []
    writers_names: list[str] = []
    directors: list[Person] = []
    actors: list[Person] = []
    writers: list[Person] = []
