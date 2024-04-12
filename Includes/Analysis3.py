# Databricks notebook source
from pyspark.sql.functions import col,upper
from Includes.logger import setup_logger

class Analysis3:
    """
    Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes 
    in which driver died and Airbags did not deploy.

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """
    
    def __init__(self, spark, unit_df, primary_person_df, config):
        self.spark = spark
        self.primary_person_df = primary_person_df
        self.unit_df = unit_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis3.log')

    def perform_analysis(self):
        try:
            # Filter data to include only records where the driver died and airbags did not deploy
            updated_unit_df = unit_df.filter(unit_df['DEATH_CNT']>0)
            updated_primary_person_df = primary_person_df.filter(primary_person_df['PRSN_AIRBAG_ID']== 'NOT DEPLOYED')

            # Count the number of units for each vehicle make
            top_vehicle_makes = updated_unit_df.join(updated_primary_person_df, ["CRASH_ID", "UNIT_NBR"], "inner").groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)
            
            # Save the result to output file
            result_df = top_vehicle_makes.select('VEH_MAKE_ID')
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis3"])

            self.logger.info("Analysis 3 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 3: {e}")