# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis1:
    """
    Analysis 1: Find the number of crashes (accidents) in which the number of males killed is greater than 2.

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """
    def __init__(self, spark, primary_person_df,config):
        self.spark = spark
        self.primary_person_df = primary_person_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis1.log')

    def perform_analysis(self):
        try:
            # Filter data to include only males with death count > 2
            crashes_with_male_deaths = self.primary_person_df.filter(
                (self.primary_person_df["PRSN_GNDR_ID"] == "MALE") & (self.primary_person_df["DEATH_CNT"] > 2)
            )

            # Count the number of unique crash IDs
            num_crashes = crashes_with_male_deaths.select("CRASH_ID").distinct().count()

            # Save the result to output file
            result_df = spark.createDataFrame([(num_crashes,)], ["Num_Crashes_With_Male_Deaths_Greater_Than_2"])
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis1"])

            self.logger.info("Analysis 1 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 1: {e}")