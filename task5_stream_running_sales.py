from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window, count, sum as spark_sum, expr, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("RealTimeTransactionMonitoringRunningSales").getOrCreate()

# Suppress unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# Read data from socket
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse the incoming data
transactions = lines.withColumn("Timestamp", split(col("value"), ",").getItem(0)) \
                    .withColumn("Product", split(col("value"), ",").getItem(1)) \
                    .withColumn("Quantity", split(col("value"), ",").getItem(2).cast("int")) \
                    .withColumn("Price", split(col("value"), ",").getItem(3).cast("float")) \
                    .withColumn("TotalSale", expr("Quantity * Price"))

transactions.printSchema()
#transactions.writeStream.outputMode("append").format("console").option("truncate", False).start()


# Calculate running total sales per product
#running_sales = transactions.groupBy("Product").agg({"Quantity": "sum", "Price": "sum"})
running_sales = transactions.groupBy("Product").agg(
    spark_sum("Quantity").alias("TotalQuantity"),
    spark_sum("Price").alias("TotalPrice"),
    spark_sum("TotalSale").alias("TotalSales")
)
running_sales.printSchema()
#running_sales = transactions.groupBy("Product").agg(
#    spark_sum("Quantity").alias("TotalQuantity"), spark_sum("Price").alias("TotalPrice")
#)
# Calculate running total sales per product
# running_sales = transactions.groupBy("Product").agg(
#     spark_sum("Quantity").alias("sum_Quantity"),
#     spark_sum("Price").alias("sum_Price")
# )

# Rename columns explicitly to match expected schema
# running_sales = running_sales.select(
#     col("Product"),
#     col("sum(Quantity)").alias("TotalQuantity"),
#     col("sum(Price)").alias("TotalSales")
# )


# Write results to SQLite database
def write_to_db(df, epoch_id):
    print(f"Writing running_sales to database for epoch {epoch_id}")
    # Define database connection
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "running_sales"

    # # Write running sales
    try:
        # Show the DataFrame for debugging
        if df.count() > 0:  # Prevent writing empty DataFrame
            df.show()
            df.write.format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table_name) \
                .option("driver", "org.sqlite.JDBC") \
                .mode("append") \
                .save()
            print(f"Data written successfully to {table_name}")
    except Exception as e:
        print(f"Error writing running_sales: {e}")
        
#write the data to the database file 
running_sales.writeStream \
    .foreachBatch(write_to_db) \
    .outputMode("complete") \
    .start() \
    .awaitTermination()