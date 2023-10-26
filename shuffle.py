from pyspark.sql import SparkSession

# Initialize a Spark session and set the number of shuffle partitions
spark = SparkSession.builder.appName("Change Shuffle Partitions Example") \
    .config("spark.sql.shuffle.partitions", 100) \
    .getOrCreate()

# Create a DataFrame
data = [('1', 'true'), ('2', 'false'), ('1', 'true'), ('2', 'false'),
        ('1', 'true'), ('2', 'false'), ('1', 'true'), ('2', 'false'), ('1', 'true'), ('2', 'false')]
df = spark.createDataFrame(data, ["_1", "flag"])

# Show the initial number of partitions
initial_partitions = df.rdd.getNumPartitions()
print(f"Initial number of partitions: {initial_partitions}")

# Perform a groupBy operation
group_df = df.groupBy("_1").count()
group_df.show()

# Show the number of partitions after the groupBy operation
final_partitions = group_df.rdd.getNumPartitions()
print(f"Number of partitions after groupBy: {final_partitions}")

# Stop the Spark session
spark.stop()
