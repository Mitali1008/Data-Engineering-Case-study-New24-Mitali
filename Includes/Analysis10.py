# Databricks notebook source
from pyspark.sql.functions import col
from Includes.logger import setup_logger

class Analysis10:
    """
    Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, 
    used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

    Attributes:
    - spark: SparkSession object
    - config: Configuration object
    - logger: Logger object for logging

    Methods:
    - perform_analysis(): Performs the analysis and writes the result to an output CSV file.
    """

    def __init__(self, spark, primary_person_df, unit_df, charges_df, config):
        self.spark = spark
        self.primary_person_df = primary_person_df
        self.unit_df = unit_df
        self.charges_df = charges_df
        self.config = config
        self.logger = setup_logger(__name__, 'log/analysis10.log')

    def perform_analysis(self):
        try:
            # Filter data to include only records where drivers are charged with speeding-related offenses
            speeding_offenses_df = charges_df.filter(col("CHARGE").contains("SPEED"))

            # Filter data to include only records where drivers have valid driver's licenses
            licensed_drivers_df = primary_person_df.filter(col("DRVR_LIC_TYPE_ID").isin(["COMMERCIAL DRIVER LIC.","DRIVER LICENSE"]))

            # Join the Charges, Primary Person and Unit
            joined_df = unit_df.join(licensed_drivers_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
                .join(speeding_offenses_df, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Get the top 10 used vehicle colors
            top_10_colors = unit_df.filter(col("VEH_COLOR_ID")!="NA").groupBy("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).select("VEH_COLOR_ID")

            # Filter data to include only records where the vehicle color is among the top 10 used vehicle colors
            top_10_colors_df = joined_df.join(top_10_colors, "VEH_COLOR_ID", "inner")

            # Determine the top 25 states with the highest number of offenses
            top_25_states = unit_df.groupBy("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).select("VEH_LIC_STATE_ID")

            # Filter data to include only records where cars are licensed in the top 25 states
            top_25_states_df = top_10_colors_df.join(top_25_states, "VEH_LIC_STATE_ID", "inner")

            # Count the number of occurrences for each vehicle make
            vehicle_make_counts = top_25_states_df.groupBy("VEH_MAKE_ID").count()

            # Select the top 5 vehicle makes based on the count
            top_vehicle_makes = vehicle_make_counts.orderBy(col("count").desc()).limit(5).select('VEH_MAKE_ID')

            # Save the result to output file
            top_vehicle_makes.coalesce(1).write.mode("overwrite").option("header", "true").csv(self.config["output_path_analysis10"])

            self.logger.info("Analysis 10 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 10: {e}")