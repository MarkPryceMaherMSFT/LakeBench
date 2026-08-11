# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0cd682b4-1ad4-436f-9470-ec7091f19c2e",
# META       "default_lakehouse_name": "Lakebench",
# META       "default_lakehouse_workspace_id": "83e610e2-36d5-481d-8777-57be7ecd5c98",
# META       "known_lakehouses": [
# META         {
# META           "id": "0cd682b4-1ad4-436f-9470-ec7091f19c2e"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Lakebench Jumpstart
# 
# This jumpstart enables you to run the TPC-DS dataset and benchmarks on Fabric Spark
# 
# Follow the instructions below to go from zero to a fully running benchmark
# 
# 
# LakeBench is the first Python-based, multi-modal benchmarking framework designed to evaluate performance across multiple lakehouse compute engines and ELT scenarios. Supporting a variety of engines and both industry-standard and novel benchmarks, LakeBench enables comprehensive, apples-to-apples comparisons in a single, extensible Python library.
# 
# To explore and learn more about Lakebench then go [here.](https://github.com/microsoft/LakeBench/blob/main/README.md)

# MARKDOWN ********************


# MARKDOWN ********************

# ## Installing the dependancies
# 
# These are the peices of code that install all the dependancies and code libraries for TCP-DS

# CELL ********************

# Install all the depenancies
!pip install lakebench[duckdb,polars,tpcds_datagen,tpch_datagen,sparkmeasure]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate the data
# 
# Change the scale_factor to change the size of the dataset
# 
# | Scale | Dataset Size |
# | - | - |
# | 1 | 1 GB |
# | 10 | 10 GB |
# | 100 | 100 GB |
# | 1000 | 1 TB |
# 
# Warning: The larger the dataset, the more time every takes.  So if this is the first time doing this, then 1GB is a great starting point

# MARKDOWN ********************


# CELL ********************

#Generate TCP-DS data
from lakebench.datagen import TPCDSDataGenerator

datagen = TPCDSDataGenerator(
    scale_factor=1,
    target_folder_uri='/lakehouse/default/Files/tpcds_sf1'
)
datagen.run()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_folder = '/lakehouse/default/Files/tpcds_sf1'
tcpds_location = "abfss://Gittest@onelake.dfs.fabric.microsoft.com/Lakebench.Lakehouse/Files/tpcds_sf1"
results_location = "abfss://Gittest@onelake.dfs.fabric.microsoft.com/Lakebench.Lakehouse/Tables/lakebench/results"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Turn the Parquet files into Delta Tables

# MARKDOWN ********************


# CELL ********************

# Run the TPC
from lakebench.engines import FabricSpark
from lakebench.benchmarks import ELTBench

engine = FabricSpark(
    lakehouse_name="Lakebench", lakehouse_schema_name="spark_eltbench_test", spark_measure_telemetry=False
)

benchmark = ELTBench(
    engine=engine,
    scenario_name="SF1",
    input_parquet_folder_uri=tcpds_location,
    save_results=True,
    result_table_uri=results_location,
)
benchmark.run(mode="light")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ETL Results
# How long did it take to load the data?

# CELL ********************

df = spark.sql("SELECT * FROM Lakebench.lakebench.results LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run TPCDS power_test (Load tables and run all queries)

# CELL ********************

from lakebench.engines import FabricSpark
from lakebench.benchmarks import TPCDS

engine = FabricSpark(lakehouse_name="Lakebench", lakehouse_schema_name="spark_tpcds_sf1", spark_measure_telemetry=False)

benchmark = TPCDS(
    engine=engine,
    scenario_name="SF1 - Power Test",
    input_parquet_folder_uri=tcpds_location,
    save_results=True,
    result_table_uri=results_location,
)
benchmark.run(mode="power_test")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run TPCDS query test: q1 run 4 times

# CELL ********************

from lakebench.engines import FabricSpark
from lakebench.benchmarks import TPCDS

engine = FabricSpark(lakehouse_name="lakebench", lakehouse_schema_name="spark_tpcds_sf1", spark_measure_telemetry=False)

benchmark = TPCDS(
    engine=engine,
    scenario_name="SF1 - Q4*4",
    input_parquet_folder_uri=tcpds_location,
    save_results=True,
    result_table_uri=results_location,
    query_list=["q1"] * 4,
)
benchmark.run(mode="query")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
