# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis5:
    """
    Analysis 5: Which state has highest number of accidents in which females are not involved?

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """

    def __init__(self, spark, primary_person_df, config):
        self.spark = spark
        self.primary_person_df = primary_person_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis5.log')

    def perform_analysis(self):
        try:
            # Filter data to include only records where the gender is not female
            not_female_df = primary_person_df.filter(col("PRSN_GNDR_ID") != "FEMALE")

            # Count the number of unique crash IDs for each state
            state_accident_counts = not_female_df.groupBy("DRVR_LIC_STATE_ID").agg({"CRASH_ID": "count"}).withColumnRenamed("count(CRASH_ID)", "Num_Accidents")

            # Find the state with the highest count
            highest_state = state_accident_counts.orderBy(col("Num_Accidents").desc()).select("DRVR_LIC_STATE_ID").withColumnRenamed("DRVR_LIC_STATE_ID","State_With_Highest_Num_Accidents_Not_Involving_Females").limit(1)

            # Save the result to output file
            highest_state.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis5"])

            self.logger.info("Analysis 5 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 5: {e}")