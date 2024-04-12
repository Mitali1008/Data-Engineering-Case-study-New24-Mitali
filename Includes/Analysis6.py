# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis6:
    """
    Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

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
        self.logger = setup_logger(__name__, 'log/analysis6.log')

    def perform_analysis(self):
        try:
            # Count the total number of injuries, including death, for each vehicle make
            vehicle_injuries = unit_df.groupBy("VEH_MAKE_ID").sum("TOT_INJRY_CNT", "DEATH_CNT").withColumnRenamed("sum(TOT_INJRY_CNT)", "Total_Injuries").withColumnRenamed("sum(DEATH_CNT)", "Total_Deaths")

            # Calculate the total injuries, including death, for each vehicle make
            total_injuries = vehicle_injuries.withColumn("Total_Injuries_Deaths", col("Total_Injuries") + col("Total_Deaths"))

            # Select the top 5 vehicle makes based on the total injuries, including death
            top_vehicle_makes = total_injuries.orderBy(col("Total_Injuries_Deaths").desc()).limit(5)

            # Select the 3rd to 5th vehicle makes from the top 5
            third_to_fifth_vehicle_makes = top_vehicle_makes.orderBy(col("Total_Injuries_Deaths")).limit(3).select("VEH_MAKE_ID")

            # Save the result to output file
            third_to_fifth_vehicle_makes.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis6"])

            self.logger.info("Analysis 6 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 6: {e}")