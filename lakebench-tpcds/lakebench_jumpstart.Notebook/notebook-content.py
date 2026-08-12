# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "91f19c2e-ec70-9470-436f-1ad40cd682b4",
# META       "default_lakehouse_name": "Lakebench",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "91f19c2e-ec70-9470-436f-1ad40cd682b4",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # LakeBench Jumpstart
# 
# This jumpstart walks you through running the industry-standard **TPC-DS** benchmark against **Fabric Spark**, using the open-source **LakeBench** Python library.
# 
# **Objective:** go from an empty Lakehouse to a fully-run TPC-DS benchmark, with ELT load timings and query performance results saved to a Delta table you can query and chart.
# 
# **What you need to do:** run the cells below in order, top to bottom. Each section explains what it does before you run it.
# 
# LakeBench is a Python-based, multi-modal benchmarking framework designed to evaluate performance across multiple lakehouse compute engines and ELT scenarios. Supporting a variety of engines and both industry-standard and novel benchmarks, LakeBench enables comprehensive, apples-to-apples comparisons in a single, extensible Python library.
# 
# To explore and learn more about LakeBench, see the [project README](https://github.com/microsoft/LakeBench/blob/main/README.md).

# MARKDOWN ********************

# ## 0. Verify your Fabric Spark runtime
# 
# This jumpstart is validated against **Fabric Runtime 2.0 (Apache Spark 4.x)**. Running on an older runtime (e.g. Runtime 1.3 / Spark 3.5) can cause confusing errors later in this notebook.
# 
# The cell below checks the Spark version of your **current session** and warns you if it's below 4.x. If you get a warning:
# 1. Go to **Workspace settings → Data Engineering/Science → Spark settings → Environment** tab, set **Runtime version** to **2.0 Public Preview (Spark 4.1, Delta 4.2)**, and save.
# 2. Restart this notebook's session (close and reopen, or use **Stop session** then run cells again) so it picks up the new default.
# 
# Note: changing the workspace or environment runtime setting only affects *new* Spark sessions — it can't upgrade a session that's already running, which is why this is a check-and-warn rather than an automatic fix.

# CELL ********************

# Check the active Spark session's runtime version and warn if it's older than Fabric Runtime 2.0 (Spark 4.x)
current_spark_version = spark.version
current_major_version = int(current_spark_version.split(".")[0])
required_major_version = 4  # Fabric Runtime 2.0 ships Apache Spark 4.x

if current_major_version < required_major_version:
    print(
        f"WARNING: This session is running Apache Spark {current_spark_version}, which is an older Fabric runtime "
        f"(Runtime 2.0 ships Spark 4.x).\n"
        f"Later cells in this notebook are validated against Fabric Runtime 2.0 and may behave unexpectedly on this version.\n\n"
        f"To fix: Workspace settings > Data Engineering/Science > Spark settings > Environment tab > Runtime version > "
        f"select '2.0 Public Preview (Spark 4.1, Delta 4.2)', save, then restart this notebook's session."
    )
else:
    print(f"Running Apache Spark {current_spark_version} (Fabric Runtime 2.0 or later) - looks good.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Install the dependencies
# 
# This installs LakeBench and the optional extras needed for TPC-DS/TPC-H data generation and benchmarking.

# CELL ********************

# Install LakeBench and its optional extras
!pip install lakebench[duckdb,polars,tpcds_datagen,tpch_datagen,sparkmeasure]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Generate the TPC-DS data
# 
# Change `scale_factor` to change the size of the generated dataset:
# 
# | Scale | Dataset Size |
# | - | - |
# | 1 | 1 GB |
# | 10 | 10 GB |
# | 100 | 100 GB |
# | 1000 | 1 TB |
# 
# Warning: the larger the dataset, the longer everything takes. If this is your first run, `scale_factor=1` (1 GB) is a great starting point.

# CELL ********************

# Generate TPC-DS data into the attached Lakehouse's Files area
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

# MARKDOWN ********************

# ## 3. Resolve OneLake paths dynamically
# 
# We resolve the current workspace and Lakehouse by **ID** (via `sempy.fabric`) rather than hardcoding a workspace name. This keeps the notebook portable so it works no matter what the workspace or Lakehouse happens to be named after deployment.

# CELL ********************

import sempy.fabric as fabric

workspace_id = fabric.get_workspace_id()
lakehouse_id = fabric.get_lakehouse_id()

target_folder = '/lakehouse/default/Files/tpcds_sf1'
tcpds_location = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/tpcds_sf1"
results_location = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/lakebench/results"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Load the Parquet files into Delta tables (ELT benchmark)
# 
# This runs LakeBench's `ELTBench` against Fabric Spark to load the generated Parquet files into Delta tables, timing each stage of the load.

# CELL ********************

# Run the TPC-DS ELT benchmark (light mode) to load Parquet into Delta tables
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

# ## ETL results
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

# ## 5. Run the TPC-DS power test (load tables and run all queries)

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

# ## How fast was the TPD-DS power test?
# 
# Filter by the scenario and sub_phase.

# CELL ********************

df = spark.sql("SELECT * FROM Lakebench.lakebench.results where scenario = 'SF1 - Power Test' and sub_phase = 'load' LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# How long did each query take to run?

# CELL ********************

df = spark.sql("SELECT * FROM Lakebench.lakebench.results where scenario = 'SF1 - Power Test' and sub_phase is NULL  LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Query Results
# 
# Showing the query id, the average , min and max query duration

# CELL ********************

# MAGIC %%sql
# MAGIC select test_item , avg(duration_ms) avg_duration_ms, min(duration_ms) min_duration_ms, max(duration_ms) max_duration_ms  
# MAGIC from  Lakebench.lakebench.results where scenario = 'SF1 - Power Test' and phase = 'Query'
# MAGIC group by test_item
# MAGIC order by test_item


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Run a targeted query test (q1, repeated 4 times)

# CELL ********************

from lakebench.engines import FabricSpark
from lakebench.benchmarks import TPCDS

engine = FabricSpark(lakehouse_name="Lakebench", lakehouse_schema_name="spark_tpcds_sf1", spark_measure_telemetry=False)

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

# CELL ********************

df = spark.sql("SELECT * FROM Lakebench.lakebench.results where scenario = 'SF1 - Q4*4' LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## What's next?
# 
# All benchmark timings from every run above are saved to the `Lakebench.lakebench.results` Delta table, so you can re-run the `SELECT * FROM Lakebench.lakebench.results` cell any time to compare runs.
# 
# From here you can:
# - Increase `scale_factor` in step 2 to benchmark at a larger data size (10 GB, 100 GB, 1 TB).
# - Point a Power BI report or semantic model at the `lakebench.results` table to visualize and compare benchmark runs over time.
# - Add more scenarios by calling `TPCDS`/`ELTBench` with different `query_list` or `mode` values — see the [LakeBench README](https://github.com/microsoft/LakeBench/blob/main/README.md) for the full list of options.
