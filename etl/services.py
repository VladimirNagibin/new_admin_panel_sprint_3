import abc
import json
import random
from functools import wraps
from time import sleep
from typing import Any, Dict, Tuple


class BaseStorage(abc.ABC):
    """Abstract state storage."""

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Save the state to the storage."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Get the state from the storage."""


class JsonFileStorage(BaseStorage):
    """
    Implementation of a storage using a local file.
    Storage format: JSON
    """

    def __init__(self, file_path: str) -> None:
        self.file_path = file_path

    def save_state(self, state: Dict[str, Any]) -> None:
        """Save the state to the storage."""
        with open(self.file_path, 'w', encoding='utf-8') as f:
            json.dump(state, f)

    def retrieve_state(self) -> Dict[str, Any]:
        """Get the state from the storage."""
        try:
            with open(self.file_path, 'r', encoding='utf-8') as f:
                state = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            state = {}
        return state


class State:
    """Class for working with states."""

    def __init__(self, storage: BaseStorage) -> None:
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Set the state for a key."""
        state = self.storage.retrieve_state()
        state[key] = value
        self.storage.save_state(state)

    def get_state(self, key: str, default: Any) -> Any:
        """Get the state for a key."""
        state = self.storage.retrieve_state()
        return state.get(key, default)


def backoff(exceptions: Tuple[Exception, ...] | Exception,
            start_sleep_time=0.1, factor=2, border_sleep_time=10, jitter=0.1):
    """Function to re-execute the function after a while if an error occurs."""
    def func_wrapper(func):

        @wraps(func)
        def inner(*args, **kwargs):
            n = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    delay = min(start_sleep_time * (factor ** n),
                                border_sleep_time) + random.uniform(0, jitter)
                    sleep(delay)
                    n += 1
        return inner
    return func_wrapper


def get_dict_from_file(file_path):
    """Get dict from json file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        raise e
