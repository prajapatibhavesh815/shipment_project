import os 
import pickle
import sys
from io import StringIO
from typing import List,Union
from shipment.constant import *
import boto3
from shipment.exception import shippingException
from botocore.exceptions import ClientError
from mypy_boto3_s3.service_resource import Bucket
from pandas import DataFrame ,read_csv
from shipment.logger import logging




class S3Operation:

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.s3_resource = boto3.resource('s3')

    
    @staticmethod
    def read_object(object_name:str,decode:bool = True,make_readable:bool = False) -> Union[StringIO,str]:

        """        method_Name: read_object
        description: This method reads the object from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the read_object method of S3Operation class")
        try:
            func = (
                lambda: object_name.get()['Body'].read().decode('utf-8')
                if decode is True
                else object_name.get()['Body'].read()
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()
            logging.info("Exited the read_object method of S3Operation class")
            return conv_func()
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def get_bucket(self,bucket_name:str) -> Bucket:
        """        method_Name: get_bucket
        description: This method gets the bucket from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the get_bucket method of S3Operation class")
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of S3Operation class")
            return bucket
        except Exception as e:
            raise shippingException(e, sys) from e
        
    
    def is_model_present(self,bucket_name:str,s3_model_key:str) -> bool:
        """        method_Name: is_model_present
        description: This method checks if the model is present in the S3 bucket.

        output:  The column name is required
        """
        try:
            bucket = self.get_bucket(bucket_name)
            file_objects = [
                file_objects
                for file_objects in bucket.objects.filter(Prefix=s3_model_key)
            ]
            if len(file_objects) > 0:
                return True
            else:
                return False
            
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def get_file_object(
            self, filename:str,bucket_name:str
    ) -> Union[List[object],object]:
        """        method_Name: get_file_object
        description: This method gets the file object from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the get_file_object method of S3Operation class")
        try:
            bucket = self.get_bucket(bucket_name)
            lst_objs = [object for object in bucket.objects.filter(Prefix=filename)]
            func = lambda x : x[0] if len(x) == 1 else x
            file_objs = func(lst_objs)
            logging.info("Exited the get_file_object method of S3Operation class")
            return file_objs
            
        except Exception as e:
            raise shippingException(e, sys) from e
        

    def load_model(self, model_name:str, bucket_name:str,model_dir:str = None) -> object:
        """        
        method_Name: load_model
        description: This method loads the model from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the load_model method of S3Operation class")
        try:
            func = (lambda: model_name
                    if model_dir is None 
                    else model_dir + "/" + model_name
            )
            model_file = func()
            f_obj = self.get_file_object(model_file,bucket_name)
            model_obj = self.read_object(f_obj,decode=False)
            model = pickle.loads(model_obj)
            logging.info("Exited the load_model method of S3Operation class")
            return model
        
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def create_model(self,folder_name: str , bucket_name: str) -> None:
        """        
        method_Name: create_model
        description: This method creates the model in S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the create_model method of S3Operation class")
        try:
            self.s3_resource.object(bucket_name, folder_name).load()
        
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                folder_obj = folder_name + "/"
                self.s3_client.put_object(
                    Bucket=bucket_name, Key=folder_obj
                )
            else:
                pass
                logging.info("Exited the create_model method of S3Operation class")
    

    def upload_file(
            self,
            from_filename:str,
            to_filename:str,
            bucket_name:str,
            remove:bool = True,
    ) -> None:
        """        
        method_Name: upload_file
        description: This method uploads the file to S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the upload_file method of S3Operation class")
        try:
            logging.info(f"Uploading {from_filename} to {to_filename} file in {bucket_name} bucket")
            self.s3_resource.meta.client.upload_file(
                from_filename, bucket_name, to_filename
            )
            logging.info(f"Uploaded {from_filename} to {to_filename} file in {bucket_name} bucket")
            if remove is True:
                os.remove(from_filename)
                logging.info(f"Remove is set to {remove} , deleted the file")
            else:
                logging.info(f"Remove is set to {remove} , not deleted the file")
            logging.info("Exited the upload_file method of S3Operation class")
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def upload_folder(self,folder_name:str,bucket_name:str) -> None:
        """        
        method_Name: upload_folder
        description: This method uploads the folder to S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the upload_folder method of S3Operation class")
        try:
            lst = os.listdir(folder_name)
            for f in lst:
                local_f = os.path.join(folder_name, f)
                dest_f = f
                self.upload_file(local_f, dest_f, bucket_name, remove=False)


            logging.info("Exited the upload_folder method of S3Operation class")
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def upload_df_to_csv(
            self,
            data_frame:DataFrame,
            local_filename:str,
            bucket_filename:str,
            bucket_name:str,
    ) -> None:
        """        
        method_Name: upload_df_to_csv
        description: This method uploads the dataframe to S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the upload_df_to_csv method of S3Operation class")
        try:
            data_frame.to_csv(local_filename, index=None, header=True)
            self.upload_file(local_filename, bucket_filename, bucket_name)
            logging.info("Exited the upload_df_to_csv method of S3Operation class")
        except Exception as e:
            raise shippingException(e, sys) from e
        
    def get_df_from_object(self,object_:object) -> DataFrame:
        """        
        method_Name: get_df_from_object
        description: This method gets the dataframe from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the get_df_from_object method of S3Operation class")
        try:
            content = self.read_object(object_, make_readable=True)
            df = read_csv(content , na_values="na")
            logging.info("Exited the get_df_from_object method of S3Operation class")
            return df
        
        except Exception as e:
            raise shippingException(e, sys) from e
            

    def read_csv(self ,filename:str,bucket_name:str) -> DataFrame:
        """        
        method_Name: read_csv
        description: This method reads the csv file from S3 bucket and returns the content of the object.

        output:  The column name is required
        """
        
        logging.info("Entered the read_csv method of S3Operation class")
        try:
            csv_obj = self.get_file_object(filename, bucket_name)
            df = self.get_df_from_object(csv_obj)
            logging.info("Exited the read_csv method of S3Operation class")
            return df
        
        except Exception as e:
            raise shippingException(e, sys) from e
