import sys 
from  json import loads
from typing import Collection
from pandas import DataFrame
from pymongo.database import Database
import pandas as pd
from pymongo import MongoClient
from shipment.constant import DB_URL
from shipment.exception import shippingException
from shipment.logger import logging




class mongoDBOperation:
    def __init__(self):
        self.DB_URL = DB_URL
        self.client = MongoClient(self.DB_URL)


    def get_database(self, db_name) -> Database:

        """"
        method_Name: get_database

        description: This method gets databases from mongoDB cluster as per the database name.

        Returns : A Database
        """

        logging.info("Entered the get_database method of mongoDBOperation class")
        try:

            db = self.client[db_name]
            logging.info(f"created the database {db_name} in mongoDB cluster")
            logging.info("Exited the get_database method of mongoDBOperation class")

            return db
        except Exception as e:
            logging.error(f"Error occurred while getting the database: {e}")
            raise shippingException(e, sys) from e
        


@staticmethod
def get_collection(Database, collection_name) -> Collection:
    """_summary_

    Args:
        database (_type_): _description_
        collection_name (_type_): _description_

    Returns:
        Collection: _description_
    """
    logging.info("Entered the get_collection method of mongoDBOperation class")
    try:
        collection = Database[collection_name]
        logging.info(f"created the collection {collection_name} in mongoDB cluster")
        logging.info("Exited the get_collection method of mongoDBOperation class")

        return collection
    except Exception as e:
        logging.error(f"Error occurred while getting the collection: {e}")
        raise shippingException(e, sys) from e
    
def get_collection_as_dataframe(self, db_name, collection_name) -> DataFrame:
    """
    method_Name: get_collection_data_as_dataframe

    description: This method gets collection data from mongoDB cluster as per the collection name.

    Returns : A DataFrame
    """
    logging.info("Entered the get_collection_data_as_dataframe method of mongoDBOperation class")
    try:
        
        #get the database
        database = self.get_database(db_name = db_name)

        #get collection
        Collection  = database.get_collection(collection_name)

        #convert to pd.DataFrame
        df = pd.DataFrame(list(Collection.find()))

        if "_id" in df.columns.to_list():
            df = df.drop(columns = ["_id"],axis=1)
        
        return df
    
    except Exception as e:
        logging.error(f"Error occurred while getting the collection data as dataframe: {e}")
        raise shippingException(e, sys) from e
    




def insert_dataframe_as_record(self,data_frame,db_name,collection_name):
    """
    method_Name: insert_dataframe_as_record

    description: This method inserts the data frame as record in the collection.

    Returns : None
    """
    logging.info("Entered the insert_dataframe_as_record method of mongoDBOperation class")
    try:
        #Converting dataframe into json
        records = loads(data_frame.T.to_json().replace()).values()
        logging.info(f"Converted dataframe to json records")

        #Getting the database and collection
        database = self.get_database(db_name)
        collection = database.get_collection(collection_name)
        logging.info(f"Inserting records to MongoDb")

        #inserting data to MongoDB database
        collection.insert_many(records)
        logging.info(f"Inserted records to MongoDB successfully")
        logging.info("Exited the insert_dataframe_as_record method of mongoDBOperation class")
    except Exception as e:
        logging.error(f"Error occurred while inserting the data frame as record: {e}")
        raise shippingException(e, sys) from e