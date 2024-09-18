from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# Initialize a Spark Session
spark = SparkSession.builder.appName("Broadcast Join with CSV Example").getOrCreate()

# Load CSV files into DataFrames
# Let's create a small DataFrame (right) and a larger DataFrame (left) from CSV files
right_csv_path = "tackle/tackles/tackles.csv"  # Replace with your actual path to a small CSV file
right_df = spark.read.csv(right_csv_path, header=True)

left_csv_path = "players/players.csv"  # Replace with your actual path to a larger CSV file
left_df = spark.read.csv(left_csv_path, header=True)

# Perform a broadcast join by broadcasting the smaller DataFrame (right_df)
broadcast_df = right_df.join(broadcast(left_df), "nflId", "inner")

# Show the result of the broadcast join
broadcast_df.show()

broadcast_df.explain()
# Stop the Spark session
spark.stop()

