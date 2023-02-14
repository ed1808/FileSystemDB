from abc import ABC, abstractmethod
from typing import Any, Dict


class FileSystemDbAbstract(ABC):
    @abstractmethod
    def __init__(self, db_name: str) -> None:
        self._db_name: str = db_name

    
    @abstractmethod
    def create_file_table(self, file_table_name: str) -> bool:
        raise NotImplementedError


    @abstractmethod
    def fetch_all(self, table_name: str) -> Any:
        raise NotImplementedError


    @abstractmethod
    def fetch_one(self, table_name: str, filter_key: str) -> Any:
        raise NotImplementedError


    @abstractmethod
    def update(self, table_name: str, update_fields: Dict) -> bool:
        raise NotImplementedError


    @abstractmethod
    def insert(self, table_name: str, data_to_insert: Dict) -> Any:
        raise NotImplementedError


    @abstractmethod
    def delete(self, table_name: str, delete_key: str) -> bool:
        raise NotImplementedError

    
    @abstractmethod
    def _create_db(self) -> bool:
        raise NotImplementedError