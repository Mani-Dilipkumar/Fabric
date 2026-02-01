# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0b5b2116-ced1-4bc4-90c2-c76a2adcddf9",
# META       "default_lakehouse_name": "source_LH",
# META       "default_lakehouse_workspace_id": "dc7a4a37-37f2-4ac6-bb91-4f695956fe87",
# META       "known_lakehouses": [
# META         {
# META           "id": "0b5b2116-ced1-4bc4-90c2-c76a2adcddf9"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Pyspark Transformation


# MARKDOWN ********************

# ### Data Reading

# CELL ********************

df1=spark.read.format("csv")\
        .option("header", "true")\
        .load("abfss://source_WS@onelake.dfs.fabric.microsoft.com/source_LH.Lakehouse/Files/source2_data/olist_customers_dataset.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# SELECT

# CELL ********************

df1= df1.select("customer_id","customer_unique_id",)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# with column rename

# CELL ********************

df_customers=df1.withColumnRenamed("customer_unique_id","unique_id")\
                .withColumnRenamed("customer_id","customer_name_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Type Casting

# MARKDOWN ********************

# It helps to modify existing column and create a new column

# CELL ********************

df_customers=df_customers.withColumn("customer_name_id",col("customer_name_id").cast(IntegerType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_customers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customers.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfo=spark.read.format("csv")\
            .option("header","true")\
            .load("abfss://source_WS@onelake.dfs.fabric.microsoft.com/source_LH.Lakehouse/Files/source2_data/olist_order_items_dataset.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfo.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfo=dfo.withColumn("shipping_limit_date",col("shipping_limit_date").cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfo.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfo=dfo.withColumn("price",col("price").cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(dfo)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2= spark.read.format("csv")\
                .option("header","true")\
                .load("abfss://source_WS@onelake.dfs.fabric.microsoft.com/source_LH.Lakehouse/Files/source2_data/olist_orders_dataset.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df2.withColumn("order_status",regexp_replace(col("order_status"), "delivered", "success"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2=df2.fillna({"customer_id":"unknown"},{"order_status":"unknown"})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
