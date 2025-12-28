from functools import lru_cache
from pymongo import MongoClient

@lru_cache(maxsize=1)
def get_mongo_client(uri: str):
    return MongoClient(uri, maxPoolSize=100, minPoolSize=0)