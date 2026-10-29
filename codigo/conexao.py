import os
from dotenv import load_dotenv
from pymongo import MongoClient
import redis

load_dotenv()

def get_db():
    client = MongoClient(os.getenv("MONGO_URI"), tz_aware=True)
    return client[os.getenv("MONGO_DB")]

def get_redis():
    return redis.Redis(host=os.getenv("REDIS_HOST"),
                       port=int(os.getenv("REDIS_PORT")),
                       decode_responses=True)