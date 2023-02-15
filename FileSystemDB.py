import os
import pickle

from json import dumps, loads
from pathlib import Path
from typing import Any, Dict, List
from uuid import uuid4

from FSDBAbstract import FileSystemDbAbstract


class FileSystemDB(FileSystemDbAbstract):
    """
    Creates a new instance of a FileSystemDB object.

    :param db_name: str = The name of the database (directory) that will contain the tables
    """

    __db_path: Path = Path()

    def __init__(self, db_name: str) -> None:
        super().__init__(db_name)

        if self._create_db():
            print('Database created successfully!')
        elif db_name in os.listdir(Path.cwd()):
            self.__db_path = Path(__file__).resolve().parent.joinpath(db_name)

    def create_file_table(self, file_table_name: str) -> bool:
        """
        It creates a file table in the database
        
        :param file_table_name: str
        :type file_table_name: str
        :return: A boolean value.
        """
        try:
            table: Path = self.__db_path.joinpath(f'{file_table_name}.pickle')

            with open(table, 'wb'):
                pass

            return True

        except FileExistsError:
            return False

    def fetch_all(self, table_name: str) -> List:
        """
        It opens a file, reads the file, and appends the data to a list
        
        :param table_name: str = 'table_name'
        :type table_name: str
        :return: A list of all the rows in the table.
        """
        result_list: List = []

        with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'rb') as f:
            while True:
                try:
                    result_list.append(loads(pickle.load(f)))
                except EOFError:
                    break

            f.close()

        return result_list

    def fetch_one(self, table_name: str, register_id: str) -> Dict:
        """
        It opens a file, reads it line by line, and if the line contains the key, it returns the line
        
        :param table_name: str = The name of the table you want to fetch from
        :type table_name: str
        :param register_id: str = '1'
        :type register_id: str
        :return: A dictionary
        """
        result: Dict = {}
        temp: Any = Any
        with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'rb') as f:
            while True:
                try:
                    temp = loads(pickle.load(f))
                    if register_id in temp.keys():
                        result = temp
                        break
                except EOFError:
                    break

        return result

    def update(self, table_name: str, register_id: str, update_field: Dict) -> bool:
        """
        It opens a file, reads it, updates the data, and writes it back to the file
        
        :param table_name: str = The name of the table you want to update
        :type table_name: str
        :param register_id: str = 'id'
        :type register_id: str
        :param update_field: Dict = {'name': 'John Doe'}
        :type update_field: Dict
        :return: A boolean value.
        """
        register_list: List = []
        temp_list: List = []
        jsonified_data: str = ''

        try:
            with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'rb') as f:
                while True:
                    try:
                        register_list.append(loads(pickle.load(f)))
                    except EOFError:
                        break
                f.close()

            with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'wb') as f:
                for register in register_list:
                    if register_id in register.keys():
                        temp_list = list(register.values())
                        temp_list[0].update(update_field)

                    jsonified_data = dumps(register)

                    f.write(pickle.dumps(jsonified_data,
                            pickle.HIGHEST_PROTOCOL))

                f.close()

            return True

        except Exception as error:
            raise(error)

    def insert(self, table_name: str, data_to_insert: Dict) -> str:
        """
        It takes a table name and a dictionary of data to insert, and returns the id of the inserted
        data
        
        :param table_name: str = The name of the table you want to insert data into
        :type table_name: str
        :param data_to_insert: Dict = {
        :type data_to_insert: Dict
        :return: A string
        """
        try:
            register_id: str = str(uuid4())

            register: Dict = {
                register_id: data_to_insert
            }

            jsonified_data: str = dumps(register)

            with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'ab') as f:
                pickle.dump(jsonified_data, f, pickle.HIGHEST_PROTOCOL)
                f.close()

            return register_id

        except Exception as e:
            raise(e)

    def delete(self, table_name: str, delete_key: str) -> bool:
        """
        It deletes a register from a table
        
        :param table_name: str = The name of the table you want to delete the register from
        :type table_name: str
        :param delete_key: str = 'id'
        :type delete_key: str
        :return: A boolean value.
        """
        register_list: List = []
        jsonified_data: str = ''
        reg_index: int = 0

        try:
            with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'rb') as f:
                while True:
                    try:
                        register_list.append(loads(pickle.load(f)))
                    except EOFError:
                        break
                f.close()

            for idx, register in enumerate(register_list):
                if delete_key in register.keys():
                    reg_index = idx
                    break

            with open(self.__db_path.joinpath(f'{table_name}.pickle'), 'wb') as f:
                for idx, register in enumerate(register_list):
                    if idx == reg_index:
                        continue

                    jsonified_data = dumps(register)

                    f.write(pickle.dumps(jsonified_data,
                            pickle.HIGHEST_PROTOCOL))

                f.close()

            return True
        except Exception as error:
            raise(error)

    def _create_db(self) -> bool:
        try:
            db_path: Path = Path(__file__).resolve(
            ).parent.joinpath(f'{self._db_name}')
            os.mkdir(db_path)

            self.__db_path = db_path

            return True

        except FileExistsError:
            return False


if __name__ == '__main__':

    # Create the database
    test_db = FileSystemDB('TestDb')

    # Create the test table
    test_db.create_file_table('test')

    # Insert two registers
    test_id: str = test_db.insert('test', {'hola': 'mundo'})
    test_id_2: str = test_db.insert('test', {'username': 'edward1808'})

    # Update first register
    test_db.update('test', test_id, {'hola': 'Mundo'})
    print(test_db.fetch_all('test'))

    # Update first and second register
    test_db.update('test', test_id, {'hola': 'Mundo 2'})
    test_db.update('test', test_id_2, {'username': 'ed1808'})
    print(test_db.fetch_all('test'))

    # Delete first register
    test_db.delete('test', test_id)
    print(test_db.fetch_all('test'))
