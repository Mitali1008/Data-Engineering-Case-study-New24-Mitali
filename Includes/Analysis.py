# Databricks notebook source
from pyspark.sql.functions import col
from logger import setup_logger
from config import read_config

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
        self.logger = setup_logger(__name__, 'log/analysis11.log')

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
            display(result_df)
            # result_df.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis1"])

            self.logger.info("Analysis 1 completed successfully")
        except Exception as e:
            self.logger.error(f"Error during Analysis 1: {e}")

spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()
config = read_config()
# Load the primary person data into a DataFrame
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)


# Create an instance of Analysis1 and pass the primary_person_df
analysis1 = Analysis1(spark, primary_person_df,config)

# Perform Analysis 1
analysis1.perform_analysis()

# COMMAND ----------

spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load the primary person data into a DataFrame
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)


# Create an instance of Analysis1 and pass the primary_person_df
analysis1 = Analysis1(spark, primary_person_df)

# Perform Analysis 1
analysis1.perform_analysis()

# COMMAND ----------

from pyspark.sql.functions import col, upper
from logger import setup_logger
from config import read_config

class Analysis2:
    """
    Analysis 2: How many two-wheelers are booked for crashes?

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
        # self.logger = setup_logger(__name__, 'log/analysis2.log')

    def perform_analysis(self):
        try:
            # Filter data to include only two-wheelers
            two_wheelers = unit_df.filter(upper(unit_df["VEH_BODY_STYL_ID"]).contains("MOTORCYCLE"))
            num_crashes = two_wheelers.select("unit_nbr").agg({"unit_nbr": "sum"}).withColumnRenamed("sum(unit_nbr)", "Num_Two_Wheelers_Involved_In_Crashes")
            
            # Save the result to output file
            # num_crashes.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis2"])
            display(num_crashes)
            # self.logger.info("Analysis 2 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 2: {e}")
            pass


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')


# Create an instance of Analysis1 and pass the primary_person_df
analysis1 = Analysis2(spark, unit_df, config)

# Perform Analysis 1
analysis1.perform_analysis()



# COMMAND ----------

from pyspark.sql.functions import col,upper
from logger import setup_logger
from config import read_config

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
        self.logger = setup_logger(__name__, 'log/analysis33.log')

    def perform_analysis(self):
        try:
            # Filter data to include only records where the driver died and airbags did not deploy
            updated_unit_df = unit_df.filter(unit_df['DEATH_CNT']>0)
            updated_primary_person_df = primary_person_df.filter(primary_person_df['PRSN_AIRBAG_ID']== 'NOT DEPLOYED')

            # Count the number of units for each vehicle make
            top_vehicle_makes = updated_unit_df.join(updated_primary_person_df, ["CRASH_ID", "UNIT_NBR"], "inner").groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)
            result_df = top_vehicle_makes.select('VEH_MAKE_ID')

            display(result_df)

            # result_df.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis3"])

            # self.logger.info("Analysis 3 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 3: {e}")
            pass


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')
primary_person_df.createOrReplaceTempView('primary_person_df')


# Create an instance of Analysis1 and pass the primary_person_df
analysis3 = Analysis3(spark, unit_df, primary_person_df, config)

# Perform Analysis 3
analysis3.perform_analysis()


# COMMAND ----------

from pyspark.sql.functions import col
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis4.log')

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
            # result_df.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis4"])
            display(result_df)

            # self.logger.info("Analysis 4 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 4: {e}")
            pass


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')
primary_person_df.createOrReplaceTempView('primary_person_df')


# Create an instance of Analysis1 and pass the primary_person_df
analysis4 = Analysis4(spark, unit_df, primary_person_df, config)

# Perform Analysis 4
analysis4.perform_analysis()






# COMMAND ----------

from pyspark.sql.functions import col
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis5.log')

    def perform_analysis(self):
        try:

            # select DRVR_LIC_STATE_ID from (select DRVR_LIC_STATE_ID,count(1) ct from primary_person_df where PRSN_GNDR_ID!='FEMALE' group by 1 order by ct desc limit 1 )

            # Filter data to include only records where the gender is not female
            not_female_df = primary_person_df.filter(col("PRSN_GNDR_ID") != "FEMALE")

            # Count the number of unique crash IDs for each state
            state_accident_counts = not_female_df.groupBy("DRVR_LIC_STATE_ID").agg({"CRASH_ID": "count"}).withColumnRenamed("count(CRASH_ID)", "Num_Accidents")

            # Find the state with the highest count
            highest_state = state_accident_counts.orderBy(col("Num_Accidents").desc()).select("DRVR_LIC_STATE_ID").withColumnRenamed("DRVR_LIC_STATE_ID","State_With_Highest_Num_Accidents_Not_Involving_Females").limit(1)

            # Save the result to output file
            display(highest_state)

            # highest_state.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis5"])

            # self.logger.info("Analysis 5 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 5: {e}")
            pass


config = read_config()

primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
primary_person_df.createOrReplaceTempView('primary_person_df')


# Create an instance of Analysis5 and pass the primary_person_df
analysis5 = Analysis5(spark,  primary_person_df, config)

# Perform Analysis 5
analysis5.perform_analysis()

# COMMAND ----------

# DBTITLE 1,Analysis6:
from pyspark.sql.functions import col
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis6.log')

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
            display(third_to_fifth_vehicle_makes)

            # Save the result to output file
            # third_to_fifth_vehicle_makes.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis6"])

            # self.logger.info("Analysis 6 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 6: {e}")
            pass




config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')


# Create an instance of Analysis6 and pass the primary_person_df
analysis6 = Analysis6(spark, unit_df, config)

# Perform Analysis 6
analysis6.perform_analysis()

# COMMAND ----------

from pyspark.sql.functions import col,rank
from pyspark.sql import Window

from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis7.log')

    def perform_analysis(self):
        try:
            # Removing None body style
            unit_df2 = unit_df.filter(col("VEH_BODY_STYL_ID").isin(["NA", "UNKNOWN", "NOT REPORTED", "OTHER  (EXPLAIN IN NARRATIVE)"])==False)

            # Join the Unit and Primary Person tables on CRASH_ID
            joined_df = unit_df2.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Calculate the count of persons for each body style and ethnic group
            body_style_ethnic_counts = joined_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()

            # Use window function to rank the ethnic groups for each body style
            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
            ranked_df = body_style_ethnic_counts.withColumn("rn", rank().over(window_spec))

            # Filter to keep only the top ethnic group for each body style
            top_ethnic_groups = ranked_df.filter(col("rn") == 1).select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")
            display(top_ethnic_groups )

            # Save the result to output file
            # top_ethnic_groups.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis7"])

            # self.logger.info("Analysis 7 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 7: {e}")
            print(e)



config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')
primary_person_df.createOrReplaceTempView('primary_person_df')
# display(unit_df)

# Create an instance of Analysis7 and pass the primary_person_df
analysis7 = Analysis7(spark, unit_df, primary_person_df, config)

# Perform Analysis 7
analysis7.perform_analysis()

# COMMAND ----------

from pyspark.sql.functions import col
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis8.log')

    def perform_analysis(self):
        try:            
            # Filter motorcycles, for df to contain cars
            unit_df2 = unit_df.filter(~col("VEH_BODY_STYL_ID").contains('MOTORCYCLE'))

            # Filter data to include only records where alcohol was a contributing factor
            alcohol_related_df = unit_df2.filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL"))

            # Remove Null Driver Zip Code
            primary_person_df2 = primary_person_df.dropna(subset=['DRVR_ZIP'])

            # Join the Unit and Primary Person tables on CRASH_ID
            joined_df = alcohol_related_df.join(primary_person_df2, ["CRASH_ID", "UNIT_NBR"], "inner")

            # Count the number of crashes for each driver's zip code
            zip_code_counts = joined_df.groupBy("DRVR_ZIP").count().withColumnRenamed("count", "Num_Crashes")

            # Select the top 5 zip codes with the highest number of crashes
            top_zip_codes = zip_code_counts.orderBy(col("Num_Crashes").desc()).limit(5).select("DRVR_ZIP")
            display(top_zip_codes)
            
            # Save the result to output file
            # top_zip_codes.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis8"])

            # self.logger.info("Analysis 8 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 8: {e}")
            print(e)


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')
primary_person_df.createOrReplaceTempView('primary_person_df')


# Create an instance of Analysis8 and pass the primary_person_df
analysis8 = Analysis8(spark, unit_df, primary_person_df, config)

# Perform Analysis 8
analysis8.perform_analysis()

# COMMAND ----------

from pyspark.sql.functions import col,regexp_extract
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis9.log')

    def perform_analysis(self):
        try:
            # Extract the numeric part of the 'VEH_DMAG_SCL_1_ID' and 'VEH_DMAG_SCL_2_ID'
            unit_df_num = unit_df.withColumn('VEH_DMAG_SCL_1_ID_num', regexp_extract(col('VEH_DMAG_SCL_1_ID'), r'\d+', 0).cast('int')).withColumn('VEH_DMAG_SCL_2_ID_num', regexp_extract(col('VEH_DMAG_SCL_2_ID'), r'\d+', 0).cast('int'))

            # Filter data to include only records where the damage level is above 4, and the car has insurance
            filtered_unit_df = unit_df_num.filter((col("VEH_DMAG_SCL_1_ID_num") > 4) & (col("VEH_DMAG_SCL_1_ID").isin(["INVALID VALUE", "NA", "NO DAMAGE"])==False) & (col("FIN_RESP_TYPE_ID") != "NA") | ((col("VEH_DMAG_SCL_2_ID_num") > 4) & (col("VEH_DMAG_SCL_2_ID").isin(["INVALID VALUE", "NA", "NO DAMAGE"])==False)))

            # Filter data to include only records where no damaged property was observed
            filtered_damages_df = damages_df.filter(col("DAMAGED_PROPERTY")=='NONE')

            # Join the Damages and Unit tables on CRASH_ID
            joined_df = filtered_unit_df.join(filtered_damages_df, "CRASH_ID", "inner")

            # Count the number of distinct crash IDs
            count = joined_df.select("CRASH_ID").distinct().count()

            # Save the result to output file
            result_df = spark.createDataFrame([(count,)], ["Num_Crash_IDs"])
            display(result_df)

            # result_df.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis9"])

            # self.logger.info("Analysis 9 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 9: {e}")
            print(e)


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
damages_df = spark.read.csv("dbfs:/FileStore/tables/Damages_use.csv", header=True,inferSchema=True)
unit_df.createOrReplaceTempView('unit_df')
damages_df.createOrReplaceTempView('damages_df')


# Create an instance of Analysis9 and pass the damages_df
analysis9 = Analysis9(spark, unit_df, damages_df, config)

# Perform Analysis 9
analysis9.perform_analysis()


# COMMAND ----------

from pyspark.sql.functions import col
from config import read_config
from logger import setup_logger

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
        # self.logger = setup_logger(__name__, 'log/analysis10.log')

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
            display(top_vehicle_makes)

            # Save the result to output file
            # top_vehicle_makes.coalesce(1).write.mode("overwrite").csv(self.config["output_path_analysis10"])

            # self.logger.info("Analysis 10 completed successfully")
        except Exception as e:
            # self.logger.error(f"Error during Analysis 10: {e}")
            print(e)


config = read_config()

unit_df = spark.read.csv("dbfs:/FileStore/tables/Units_use.csv", header=True,inferSchema=True)
primary_person_df = spark.read.csv("dbfs:/FileStore/tables/Primary_Person_use.csv", header=True,inferSchema=True)
primary_person_df.createOrReplaceTempView('primary_person_df')
unit_df.createOrReplaceTempView('unit_df')
charges_df = spark.read.csv("dbfs:/FileStore/tables/Charges_use.csv", header=True,inferSchema=True)
charges_df.createOrReplaceTempView('charges_df')


# Create an instance of Analysis10 and pass the damages_df
analysis10 = Analysis10(spark, primary_person_df, unit_df, charges_df, config)

# Perform Analysis 10
analysis10.perform_analysis()

# COMMAND ----------

