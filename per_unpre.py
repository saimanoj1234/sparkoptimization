from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr,split

# Define a filter_input_data function
def filter_input_data(initial_data):
    df_with_split_height = df.withColumn("height", split(col("height"), "-"))
    df_with_split_height = df_with_split_height.withColumn("feet", df_with_split_height["height"].getItem(0).cast("int"))
    df_with_split_height = df_with_split_height.withColumn("inches", df_with_split_height["height"].getItem(1).cast("int"))

    # Filter the data based on height
    filtered_data = df_with_split_height.filter((col('feet') > 6) | ((col('feet') == 6) & (col('inches') >= 4)))


    return filtered_data

# Initialize a Spark session
spark = SparkSession.builder.appName("CSV Read Example").getOrCreate()

# Read the CSV file (replace with your actual file path)
csv_file_path = "players/players.csv"
df = spark.read.csv(csv_file_path, header=True)

# Show the first few rows of the DataFrame
df.show()

# Persist the DataFrame in memory
df.persist()

# Check if a DataFrame is persisted
if df.is_cached:
    print("DataFrame is persisted in memory.")
else:
    print("DataFrame is not persisted in memory.")

# Perform data filtering (height > 6-4)
filtered_df = filter_input_data(df)

# Now, you have a list of objects, and you want to perform computations on the filtered data for each object.
# Example: Compute the count of rows in the filtered data
row_count = filtered_df.count()
print(f"Number of rows where height > 5-0: {row_count}")

# Unpersist the DataFrame to release memory
df.unpersist()

# Stop the Spark session when done
spark.stop()
