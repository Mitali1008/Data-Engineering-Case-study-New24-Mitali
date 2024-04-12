# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis4:
    """
    Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run. 

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """

    def __init__(self, spark, unit_df, primary_person_df, config):
        self.spark = spark
        self.unit_df = unit_df
        self.primary_person_df = primary_person_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis4.log')

    def perform_analysis(self):
        try:
            # Filter data to include only records where the unit was involved in a hit and run
            hit_and_run_df = unit_df.select('CRASH_ID','UNIT_NBR','VIN').filter(col("VEH_HNR_FL") == "Y")

            # Filter data to include only records where the driver had a valid license
            valid_license_df = primary_person_df.select('CRASH_ID','UNIT_NBR').filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("DRVR_LIC_TYPE_ID").isin(["UNLICENSED", "UNKNOWN", "NA"]) == False))

            # Join the Unit and Primary Person tables on CRASH_ID and UNIT_NBR
            joined_df = hit_and_run_df.join(valid_license_df, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Count the number of unique vehicles
            num_vehicles = joined_df.select("VIN").distinct().count()
            
            # Save the result to output file
            result_df = spark.createDataFrame([(num_vehicles,)], ["Num_Vehicles_With_Valid_Licenses_In_Hit_And_Run"])
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis4"])

            self.logger.info("Analysis 4 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 4: {e}")