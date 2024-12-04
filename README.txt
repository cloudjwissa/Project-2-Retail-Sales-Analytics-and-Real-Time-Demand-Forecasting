To run the task 5 files:

install the packages:
pip install pyspark
pip install dash

1. Open a terminal and run python generate_retail_two.py to start the live data stream

2. Open a new terminal, while the streaming is happening, and run the file that calculates the running sales to be written in a database file, run this command:
spark-submit --jars /workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/jars/sqlite-jdbc-3.47.1.0.jar task5_stream_running_sales.py

3. Open another terminal, while the streaming is happening, and run the file that detecets anomalies and write them to the database file
spark-submit --jars /workspaces/Project-2-Retail-Sales-Analytics-and-Real-Time-Demand-Forecasting/jars/sqlite-jdbc-3.47.1.0.jar task5_stream_anomalies.py

4. Open another terminal to open the dashboard application that takes the database file and makes visualizations. You can open it while the streaming is still happening.
python dashboard_app.py

5. When you are done streaming, hit ctrl+C on every terminals. Close the ones that are ingesting the live stream before the one generating the live stream.

6. You can verify if the tables are added to the database file
 - 1. sqlite3 retail_data.db
 - 2. .tables
 - 3. SELECT * from running_sales/anomalies;
 Hit ctrl+Z to exit