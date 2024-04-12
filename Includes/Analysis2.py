# Databricks notebook source
from pyspark.sql.functions import col, upper
from Includes.logger import setup_logger

class Analysis2:
    """
    Analysis 2: How many two-wheelers are booked for crashes?

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """
    
    def __init__(self, spark, unit_df, config):
        self.spark = spark
        self.unit_df = unit_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis2.log')

    def perform_analysis(self):
        try:
            # Filter data to include only two-wheelers
            two_wheelers = unit_df.filter(upper(unit_df["VEH_BODY_STYL_ID"]).contains("MOTORCYCLE"))

            # Aggregating the number of unit involved in the crash
            num_crashes = two_wheelers.select("unit_nbr").agg({"unit_nbr": "sum"}).withColumnRenamed("sum(unit_nbr)", "Num_Two_Wheelers_Involved_In_Crashes")
            
            # Save the result to output file
            num_crashes.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis2"])

            self.logger.info("Analysis 2 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 2: {e}")