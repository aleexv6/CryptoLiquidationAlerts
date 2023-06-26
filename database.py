import config
import certifi
from pymongo import MongoClient

def get_database():
    CONNECTION_STRING = "mongodb+srv://"+config.USER+":"+config.PASS+"@cluster0.1hg3mpx.mongodb.net/test"
    ca = certifi.where()
    client = MongoClient(CONNECTION_STRING, tlsCAFile = ca, uuidRepresentation='standard', connectTimeoutMS=120000, socketTimeoutMS=120000)
    return client['CodeReaderLiquidations']