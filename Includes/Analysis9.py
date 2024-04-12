# Databricks notebook source
from pyspark.sql.functions import col, regexp_extract
from Includes.logger import setup_logger

class Analysis9:
    """
    Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """

    def __init__(self, spark, unit_df, damages_df, config):
        self.spark = spark
        self.unit_df = unit_df
        self.damages_df = damages_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis9.log')

    def perform_analysis(self):
        try:
            # Extract the numeric part of the 'VEH_DMAG_SCL_1_ID' and 'VEH_DMAG_SCL_2_ID'
            unit_df_num = unit_df.withColumn('VEH_DMAG_SCL_1_ID_num', regexp_extract(col('VEH_DMAG_SCL_1_ID'), r'\d+', 0).cast('int')).withColumn('VEH_DMAG_SCL_2_ID_num', regexp_extract(col('VEH_DMAG_SCL_2_ID'), r'\d+', 0).cast('int'))

            # Filter data to include only records where the damage level is above 4, and the car has insurance
            filtered_unit_df = unit_df_num.filter((col("VEH_DMAG_SCL_1_ID_num") > 4) & (col("VEH_DMAG_SCL_1_ID").isin(["INVALID VALUE", "NA", "NO DAMAGE"])==False) & (col("FIN_RESP_TYPE_ID") != "NA") | ((col("VEH_DMAG_SCL_2_ID_num") > 4) & (col("VEH_DMAG_SCL_2_ID").isin(["INVALID VALUE", "NA", "NO DAMAGE"])==False)))

            # Filter data to include only records where no damaged property was observed
            filtered_damages_df = damages_df.filter(col("DAMAGED_PROPERTY")=='NONE')

            # Join the Damages and Unit tables
            joined_df = filtered_unit_df.join(filtered_damages_df, "CRASH_ID", "inner")

            # Count the number of distinct crash IDs
            count = joined_df.select("CRASH_ID").distinct().count()

            # Save the result to output file
            result_df = spark.createDataFrame([(count,)], ["Num_Crash_IDs"])
            result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis9"])

            self.logger.info("Analysis 9 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 9: {e}")