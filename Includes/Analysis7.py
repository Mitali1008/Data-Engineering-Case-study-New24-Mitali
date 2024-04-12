# Databricks notebook source
from pyspark.sql.functions import col,rank
from pyspark.sql import Window
from Includes.logger import setup_logger

class Analysis7:
    """
    Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

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
        self.logger = setup_logger(__name__, 'log/analysis7.log')

    def perform_analysis(self):
        try:
            # Removing None body style
            unit_df2 = unit_df.filter(col("VEH_BODY_STYL_ID").isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])==False)

            # Join the Unit and Primary Person tables 
            joined_df = unit_df2.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Calculate the count of persons for each body style and ethnic group
            body_style_ethnic_counts = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()

            # Use window function to rank the ethnic groups for each body style
            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
            ranked_df = body_style_ethnic_counts.withColumn("rn", rank().over(window_spec))

            # Filter to keep only the top ethnic group for each body style
            top_ethnic_groups = ranked_df.filter(col("rn") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

            # Save the result to output file
            top_ethnic_groups.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis7"])

            self.logger.info("Analysis 7 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 7: {e}")