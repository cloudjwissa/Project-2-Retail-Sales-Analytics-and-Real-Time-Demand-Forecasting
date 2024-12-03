from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, window, count, sum as spark_sum, expr, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("RealTimeTransactionMonitoringAnomalies").getOrCreate()

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

anomalies = transactions.filter((col("Quantity") > 60) | (col("Price") > 700))

#write the anomalies
def write_anomalies_to_db(df, epoch_id):
    # Define database connection
    print(f"Writing anomalies to database for epoch {epoch_id}")
    db_url = "jdbc:sqlite:/workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/retail_data.db"
    table_name = "anomalies"
    
    try:
        if df.count() > 0:  # Prevent writing empty DataFrame
            df.show()
            df.write.format("jdbc") \
                .option("url", db_url) \
                .option("dbtable", table_name) \
                .option("driver", "org.sqlite.JDBC") \
                .mode("append") \
                .save()
            print(f"Anomalies written successfully to {table_name}")
    except Exception as e:
        print(f"Error writing anomalies: {e}")

#write the data to the database file
anomalies.writeStream \
    .foreachBatch(write_anomalies_to_db) \
    .outputMode("append") \
    .start() \
    .awaitTermination()