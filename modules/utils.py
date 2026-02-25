from os.path import exists
import json
from typing import Any, Dict, Iterator
import pymysql
from pymysql.constants import FIELD_TYPE
from pymysql.converters import conversions


import os
from dotenv import load_dotenv

load_dotenv()  # This loads the environment variables from .env file

class JsonFile:
    """ Helper class for dealing with json files """

    def __init__(self, path: str, default_value: Dict[str, Any]):
        self._path = path
        self._readFile(self._path, default_value)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any):
        self._data[key] = value

    def __getattr__(self, name: str) -> Any:
        return getattr(self._data, name)

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def _readFile(self, path: str, default_value: Dict[str, Any]):
        if not exists(path):
            self._saveFile(path, default_value)

        with open(path, "r", encoding="utf-8") as file:
            self._data = json.load(file)

    def _saveFile(self, path: str, data: Dict[str, Any]):
        with open(path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, default=str)

    def save(self):
        """ Saves the internal (modified?) data into the file path that were passed to the __init__. """
        self._saveFile(self._path, self._data)




conv = conversions.copy()
conv[FIELD_TYPE.DECIMAL] = float
conv[FIELD_TYPE.NEWDECIMAL] = float
conv[FIELD_TYPE.DATETIME] = str
conv[FIELD_TYPE.TIME] = str
conv[FIELD_TYPE.NEWDATE] = str
conv[FIELD_TYPE.TIMESTAMP] = str

class Connection:
    def __init__(self):
        db_password = os.getenv('DB_PASSWD') or os.getenv('DB_PASSWORD')
        db_port = int(os.getenv('DB_PORT', '3306'))
        self.con = pymysql.connect(
            host=os.getenv('DB_HOST'),
            port=db_port,
            user=os.getenv('DB_USER'),
            password=db_password,
            db=os.getenv('DB_NAME'),
            charset=os.getenv('DB_CHARSET', 'utf8mb4'),
            conv=conv,
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True

            )
        self.cur = self.con.cursor()

    def select(self,sql,args=None):
        try:
            self.cur.execute(sql,args)
            return self.cur.fetchone()
        finally:
            self.cur.close()
            self.con.close()

    def selectAll(self,sql,args=None):
        try:
            self.cur.execute(sql,args)
            return self.cur.fetchall()
        finally:
            self.cur.close()
            self.con.close()

    def insert(self,sql,args=None):
        self.ins = self.cur.execute(sql,args)
        return self.ins

    def update(self,sql,args=None):
        self.upd = self.cur.execute(sql,args)
        return self.upd

    def delete(self, sql, args=None):
        self.delete = self.cur.executemany(sql,args)
        return self.delete

    #def truncate(self, sql, args=None):
    #    self.truncate = self.cur.execute(sql,args)
    #    return self.truncate


####

# http://docs.python-requests.org/en/master/api/
import requests

class RequestsApi:
    def __init__(self, base_url, **kwargs):
        self.base_url = base_url
        self.session = requests.Session()
        for arg in kwargs:
            if isinstance(kwargs[arg], dict):
                kwargs[arg] = self.__deep_merge(getattr(self.session, arg), kwargs[arg])
            setattr(self.session, arg, kwargs[arg])

    def request(self, method, url, **kwargs):
        return self.session.request(method, self.base_url+url, **kwargs)

    def head(self, url, **kwargs):
        return self.session.head(self.base_url+url, **kwargs)

    def get(self, url, **kwargs):
        return self.session.get(self.base_url+url, **kwargs)

    def post(self, url, **kwargs):
        return self.session.post(self.base_url+url, **kwargs)

    def put(self, url, **kwargs):
        return self.session.put(self.base_url+url, **kwargs)

    def patch(self, url, **kwargs):
        return self.session.patch(self.base_url+url, **kwargs)

    def delete(self, url, **kwargs):
        return self.session.delete(self.base_url+url, **kwargs)

    @staticmethod
    def __deep_merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict):
                node = destination.setdefault(key, {})
                RequestsApi.__deep_merge(value, node)
            else:
                destination[key] = value
        return destination
