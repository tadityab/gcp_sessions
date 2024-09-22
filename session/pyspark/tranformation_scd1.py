# SCD Type 1: Slowly Changing Dimension (SCD) Type 1 is a method
# used in data warehousing to manage changes to dimension data.
# In SCD Type 1, when a change occurs in a dimension record
# (such as a customer or product attribute), the record is updated by
# overwriting the existing data. No history of previous data is kept in this method,
# meaning you only retain the latest information.
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

if __name__ == '__main__':
    # create a spark session
    spark = SparkSession.builder.master("local[*]") \
        .appName("Transformation SCD1").getOrCreate()

    # Reading existing file and create a dataframe
    existing_df = spark.read.csv(r"D:\GCP2024\gcp_sessions\session\inputdata\employee.csv", header=True)

    # Create a dataframe on new file
    new_df = spark.read.csv(r"D:\GCP2024\gcp_sessions\session\inputdata\employee_new.csv", header=True)

    # join both the dataframe
    joined_df = existing_df.alias("tgt").join(new_df.alias("src"), on="id", how="outer")
    # id,name,city,state
    latest_df = joined_df.select(
        coalesce(new_df.id, existing_df.id).alias("id"),
        coalesce(new_df.name, existing_df.name).alias("name"),
        coalesce(new_df.city, existing_df.city).alias("city"),
        coalesce(new_df.state, existing_df.state).alias("state")
    )

    # write a data at target location
    latest_df.write.mode("overwrite").csv(r"D:\GCP2024\gcp_sessions\session\outputdata\scd1output")
