# Databricks notebook source
import pandas as pd

# Read the CSV
loan_book = pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/loan_book.csv")  # works in notebooks only
loan_book

# Convert to Spark DataFrame
spark_loan_book = spark.createDataFrame(loan_book)

# Register as a temporary SQL table

spark_loan_book.createOrReplaceTempView("loan_book")


# COMMAND ----------

loan_level_panel= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/loan_level_panel.csv")
loan_level_panel

spark_loan_level_panel= spark.createDataFrame(loan_level_panel)
spark_loan_level_panel.createOrReplaceTempView("loan_level_panel")



# COMMAND ----------

macro= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/macro.csv")
macro

spark_macro= spark.createDataFrame(macro)
spark_macro.createOrReplaceTempView("macro")


# COMMAND ----------

macro_scenario_inputs= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/macro_scenario_inputs.csv")
macro_scenario_inputs

spark_macro_scenario_inputs= spark.createDataFrame(macro_scenario_inputs)
spark_macro_scenario_inputs.createOrReplaceTempView("macro_scenario_inputs")

# COMMAND ----------

stress_position = pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/stress_position.csv")
stress_position

spark_stress_position= spark.createDataFrame(stress_position)
spark_stress_position.createOrReplaceTempView("stress_position")

# COMMAND ----------

trading_book= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/trading_book.csv")
trading_book

spark_trading_book= spark.createDataFrame(trading_book)
spark_trading_book.createOrReplaceTempView("trading_book")

# COMMAND ----------

isg_lending= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/isg_lending.csv")
isg_lending

spark_isg_lending= spark.createDataFrame(isg_lending)
spark_isg_lending.createOrReplaceTempView("isg_lending")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from loan_book

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from trading_book
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from isg_lending
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_type, sum(exposure_amt) as total_exposure from isg_lending group by product_type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select fico_bin, avg(default_rate) as avg_default from loan_level_panel group by fico_bin

# COMMAND ----------

isg_exposures= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/isg_exposures.csv")
isg_exposures

spark_isg_exposures= spark.createDataFrame(isg_exposures)
spark_isg_exposures.createOrReplaceTempView("isg_exposures")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from isg_exposures

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view isg_exposures_view as select *, cast(replace(notional, ',', '') as double)  as notional_double from isg_exposures

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from isg_exposures_view

# COMMAND ----------

# DBTITLE 1,ioa
# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC select sector, sum(notional_double) as total_exposure from isg_exposures_view group by sector;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view stress_position_view as select * , cast(replace(exposure, ',', '') as double) as exposure_double from stress_position

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from stress_position_view

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select product_type , sum(exposure_double * expected_loss) as total_expected_loss from stress_position_view group by product_type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan_book

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view loan_book_view as select * , cast(replace(exposure, ',', '') as double) as exposure_double from loan_book

# COMMAND ----------

# MAGIC %sql
# MAGIC select as_of_date, sum(exposure_double) as total_exposure from loan_book_view group by as_of_date order by as_of_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trading_book where as_of_date= '3/31/2025'

# COMMAND ----------

bond_position= pd.read_csv("/Workspace/Users/rohankumarlnu@gmail.com/bond_positions.csv")
bond_position

# COMMAND ----------

spark_bond_position= spark.createDataFrame(bond_position)
spark_bond_position.createOrReplaceTempView("bond_position")

# COMMAND ----------

# MAGIC %sql
# MAGIC select bond_id, market_price - prior_price as price_change, accrued_interest from bond_position

# COMMAND ----------

# MAGIC %sql
# MAGIC select as_of_date, sum(exposure_amt) as total_exposure from isg_lending group by as_of_date order by as_of_date

# COMMAND ----------

# MAGIC %sql
# MAGIC select loan_id, count(*) from loan_book_view group by loan_id having count(*) >1