# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis8:
    """
    Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

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
        self.logger = setup_logger(__name__, 'log/analysis8.log')

    def perform_analysis(self):
        try:            
            # Filter motorcycles, for df to contain cars
            unit_df2 = unit_df.filter(~col("VEH_BODY_STYL_ID").contains('MOTORCYCLE'))

            # Filter data to include only records where alcohol was a contributing factor
            alcohol_related_df = unit_df2.filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))

            # Remove Null Driver Zip Code
            primary_person_df2 = primary_person_df.dropna(subset=['DRVR_ZIP'])

            # Join the Unit and Primary Person tables 
            joined_df = alcohol_related_df.join(primary_person_df2, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Count the number of crashes for each driver's zip code
            zip_code_counts = joined_df.groupBy("DRVR_ZIP").count().withColumnRenamed("count", "Num_Crashes")

            # Select the top 5 zip codes with the highest number of crashes
            top_zip_codes = zip_code_counts.orderBy(col("Num_Crashes").desc()).limit(5)
            
            # Save the result to output file
            top_zip_codes.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis8"])

            self.logger.info("Analysis 8 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 8: {e}")