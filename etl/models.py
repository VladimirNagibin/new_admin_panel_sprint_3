import sqlite3
import uuid
from datetime import date, datetime

#from dateutil import parser
from pydantic import BaseModel, Field, field_validator

#from logger import logger


class IdModified(BaseModel):
    id: uuid.UUID
    modified: datetime
    
    #@classmethod
    #def get_values(cls, d: sqlite3.Row) -> tuple:
    #    try:
    #        item = cls(**dict(d))
    #        return tuple(dict(item).values())
    #    except TypeError as error:
    #        logger.error('Ошибка создания объекта %s: %s', dict(d), error)
    #        raise error


class FilmworkElastic(BaseModel):
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
