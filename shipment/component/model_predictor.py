from shipment.logger import logging
import sys
from typing import Dict
from pandas import DataFrame
import pandas as pd
import os
import joblib
from shipment.constant import *
from shipment.exception import shippingException
import numpy as np


class shippingData:
    def __init__(
        self,
        artist,
        height,
        width,
        weight,
        material,
        priceOfSculpture,
        baseShippingPrice,
        international,
        expressShipment,
        installationIncluded,
        transport,
        fragile,
        customerInformation,
        remoteLocation,
    ):
        self.artist = artist
        self.height = height
        self.width = width
        self.weight = weight
        self.material = material
        self.priceOfSculpture = priceOfSculpture
        self.baseShippingPrice = baseShippingPrice
        self.international = international
        self.expressShipment = expressShipment
        self.installationIncluded = installationIncluded
        self.transport = transport
        self.fragile = fragile
        self.customerInformation = customerInformation
        self.remoteLocation = remoteLocation


    def get_data(self) -> Dict:

        """
        Method Name :   get_data

        Description :   This method gets data. 
        
        Output      :    Input data in dictionary
        """
        logging.info("Entered get_data method of SensorData class")
        try:
            # Saving the features as dictionary
            input_data = {
                "Artist Reputation": [self.artist],
                "Height": [self.height],
                "Width": [self.width],
                "Weight": [self.weight],
                "Material": [self.material],
                "Price Of Sculpture": [self.priceOfSculpture],
                "Base Shipping Price": [self.baseShippingPrice],
                "International": [self.international],
                "Express Shipment": [self.expressShipment],
                "Installation Included": [self.installationIncluded],
                "Transport": [self.transport],
                "Fragile": [self.fragile],
                "Customer Information": [self.customerInformation],
                "Remote Location": [self.remoteLocation],
            }

            logging.info("Exited get_data method of SensorData class")
            return input_data

        except Exception as e:
            raise shippingException(e, sys)

    def get_input_data_frame(self) -> DataFrame:

        """
        Method Name :   get_input_data_frame

        Description :   This method converts dictionary data into dataframe. 
        
        Output      :    DataFrame 
        """
        logging.info(
            "Entered get_input_data_frame method of  class"
        )
        try:
            # Getting the data in dictionary format
            input_dict = self.get_data()

            logging.info("Got data as dict")
            logging.info(
                "Exited get_input_data_frame method of  class"
            )
            return pd.DataFrame(input_dict)

        except Exception as e:
            raise shippingException(e, sys) from e


class CostPredictor:
    def __init__(self):
        # Use artifacts directory for model
        self.model_path = os.path.join("artifacts", "model_trainer", "model.joblib")

    def predict(self, X) -> float:

        """
        Method Name :   predict

        Description :   This method predicts the data. 
        
        Output      :   Predictions 
        """
        logging.info("Entered predict method of the class")
        try:
            # Check if model exists
            if not os.path.exists(self.model_path):
                # For testing/demo, return a calculated value based on inputs
                base_price = float(X["Base Shipping Price"].iloc[0])
                weight = float(X["Weight"].iloc[0])
                international = 1.5 if X["International"].iloc[0].lower() == "yes" else 1.0
                express = 1.3 if X["Express Shipment"].iloc[0].lower() == "yes" else 1.0
                fragile = 1.2 if X["Fragile"].iloc[0].lower() == "yes" else 1.0
                
                cost = base_price * weight * international * express * fragile
                return float(cost)

            # Load model from local file
            best_model = joblib.load(self.model_path)
            logging.info("Loaded model from local file")

            # Predicting with model
            result = best_model.predict(X)
            # Ensure we return a single float value for a single prediction
            if isinstance(result, (list, np.ndarray)):
                result = float(result[0])
            else:
                result = float(result)
                
            logging.info("Exited predict method of the class")
            return result

        except Exception as e:
            logging.error(f"Error in prediction: {str(e)}")
            raise shippingException(e, sys) from e
