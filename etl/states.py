import abc
import json
from typing import Any, Dict


class BaseStorage(abc.ABC):
    """Abstract state storage."""

    @abc.abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        """Save the state to the storage."""

    @abc.abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        """Get the state from the storage."""


class JsonFileStorage(BaseStorage):
    """Implementation of a storage using a local file.

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
