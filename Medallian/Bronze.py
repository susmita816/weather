# Databricks notebook source
'''
1. Upload the json file into adls
2. Create notebook called bronze. Read the json file from adls.
4. Create training catalog
5. create medallion schema
6. create bronze table and load the json data as delta table.
7. create silver notebook, read the bronze table
8. Drop the columns WindDir3pm, WindSpeed3pm, Humidity3pm, Pressure3pm, Cloud3pm
9. Filter the rows where MinTemp > 10 and MaxTemp < 28
10. Find the count of rows where rainfall is 0 and WindDir9am = 'SE'
11. Find the average Humidity9am where Evaporation > 6.8
12. Create silver table under medallion and store the transformed data
13. Read the silver table, get the average rainfall when MinTemp > 10.3 and MaxTemp <= 27?
14. Create gold table and load the data into it.
15. Create workflow to automate the entire medallion process?
16. Create Git repository called weather, connect to databricks
17. Load the bronze, silver and gold notebooks under folder called medallion?
18. Create Key vault service? store your adls-container and storage account?
19. Integrate with databricks using secret scope
20. Retrieve the keys from key-vault and try to read the json file by replacing the hardcoded values In the path?

Key-vault to Databricks connection youtube video: Key-valut to DB connection
'''

# COMMAND ----------

file_path = 'abfss://debemo12@dedemose1.dfs.core.windows.net/rafloder/weather.json'


# COMMAND ----------

df = spark.read.json(path=file_path)
df.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists training_catalog.medallion;

# COMMAND ----------

df.write.mode('append').saveAsTable('training_catalog.medallion.bronze')

# COMMAND ----------

df.display()

# COMMAND ----------

bronze_df = spark.table("training_catalog.medallion.bronze")

# COMMAND ----------

bronze_df.display()
