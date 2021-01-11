import codecs
import json
import logging

from bson import ObjectId
from dotenv import load_dotenv
from pprint import pp
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
import os

load_dotenv(".env", override=True)
log_level = os.getenv("LOG_LEVEL", "INFO")

seed_db = os.getenv("SEED_DB", "0")

logging.basicConfig(level=log_level)
log = logging.getLogger(__name__)

db_name = "vivina"
wines_collection_name = "wines"


def connect(host: str, port: int) -> MongoClient:
    client = MongoClient(host, port)
    return client


def create_db(client: MongoClient, name: str) -> Database:
    return client[name]


def create_collection(db: Database, name: str) -> Collection:
    return db[name]


def insert_document(collection: Collection, data):
    return collection.insert_one(data)


def seed():
    db = create_db(mc, db_name)
    col = create_collection(db, wines_collection_name)
    data_dir = r"C:\temp\0-10"
    data_files = os.listdir(data_dir)

    for i, f in enumerate(data_files):
        with codecs.open(os.path.join(data_dir, f), "r", "utf-8") as reader:
            data = reader.read()
            data_json = json.loads(data)
            i_id = insert_document(col, data_json).inserted_id
            log.info(f"Inserted doc {i + 1} of {len(data_files)}. Id = {i_id}.")


def get_document_by_id(collection: Collection, oid: str):
    return collection.find_one({"_id": ObjectId(oid)})


if __name__ == "__main__":
    mc = connect("localhost", 27017)

    if seed_db != "0":
        seed()
    else:
        pass
        # doc_id = input("Enter a document id: ")
        # wines_collection = mc[db_name][wines_collection_name]
        # pp(get_document_by_id(wines_collection, doc_id))

    wines_collection = mc[db_name][wines_collection_name]
    result = wines_collection.find({"summary.region": {"$eq": "California"}})
    c = result.count()
    for doc in result:
        pp(doc)
