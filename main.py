# Databricks notebook source
from pyspark.sql import SparkSession
from Includes.logger import logger
from Includes.config import read_config
from Includes.Analysis1 import Analysis1
from Includes.Analysis2 import Analysis2
from Includes.Analysis3 import Analysis3
from Includes.Analysis4 import Analysis4
from Includes.Analysis5 import Analysis5
from Includes.Analysis6 import Analysis6
from Includes.Analysis7 import Analysis7
from Includes.Analysis8 import Analysis8
from Includes.Analysis9 import Analysis9
from Includes.Analysis10 import Analysis10


if __name__ == "__main__":
    # Create a Spark session
    spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

    config = read_config()
    input_path_primary_person = config["input_path_primary_person"]
    input_path_unit = config['input_path_unit']
    input_path_charges = config['input_path_charges']
    input_path_endorse = config['input_path_endorse']
    input_path_restrict = config['input_path_restrict']
    input_path_damages = config['input_path_damages']

    # Load the csv data into a DataFrame
    primary_person_df = spark.read.format("csv").option("header", "true").load(input_path_primary_person)
    unit_df = spark.read.format("csv").option("header", "true").load(input_path_unit)
    charges_df = spark.read.format("csv").option("header", "true").load(input_path_charges)
    endorse_df = spark.read.format("csv").option("header", "true").load(input_path_endorse)
    restrict_df = spark.read.format("csv").option("header", "true").load(input_path_restrict)
    damages_df = spark.read.format("csv").option("header", "true").load(input_path_damages)


    # Perform Analysis 1:
    analysis1 = Analysis1(spark, primary_person_df,config)
    analysis1.perform_analysis()

    # Perform Analysis 2: 
    analysis2 = Analysis2(spark, unit_df, config)
    analysis2.perform_analysis()

    # Perform Analysis 3: 
    analysis3 = Analysis3(spark, unit_df, primary_person_df, config)
    analysis3.perform_analysis()

    # Perform Analysis 4: 
    analysis4 = Analysis4(spark, unit_df, primary_person_df, config)
    analysis4.perform_analysis()

    # Perform Analysis 5: 
    analysis5 = Analysis5(spark, primary_person_df,config)
    analysis5.perform_analysis()

    # Perform Analysis 6: 
    analysis6 = Analysis6(spark, unit_df, config)
    analysis6.perform_analysis()

    # Perform Analysis 7:
    analysis7 = Analysis7(spark, unit_df, primary_person_df, config)
    analysis7.perform_analysis()

    # Perform Analysis 8:
    analysis8 = Analysis8(spark, unit_df, primary_person_df, config)
    analysis8.perform_analysis()

    # Perform Analysis 9: 
    analysis9 = Analysis9(spark, unit_df, damages_df, config)
    analysis9.perform_analysis()

    # Perform Analysis 10:
    analysis10 = Analysis10(spark, primary_person_df, unit_df, charges_df, config)
    analysis10.perform_analysis()

    spark.stop()
















