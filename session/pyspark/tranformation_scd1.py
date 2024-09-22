# SCD Type 1: Slowly Changing Dimension (SCD) Type 1 is a method
# used in data warehousing to manage changes to dimension data.
# In SCD Type 1, when a change occurs in a dimension record
# (such as a customer or product attribute), the record is updated by
# overwriting the existing data. No history of previous data is kept in this method,
# meaning you only retain the latest information.
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce
import os

# Using below block dynamically defining input and output location present in project.
# Get parent folder  location,
current_folder = os.path.dirname(os.path.abspath(__file__))

# Get parent folder name
parent_folder = os.path.dirname(current_folder)

# Initialize input location.
existing_file = f"{parent_folder}/inputdata/employee.csv"
new_file = f"{parent_folder}/inputdata/employee_new.csv"

# Initialize output location
output_location = f"{parent_folder}/outputdata/scd1output"


if __name__ == '__main__':
    # create a spark session
    spark = SparkSession.builder.master("local[*]") \
        .appName("Transformation SCD1").getOrCreate()

    # Reading existing file and create a dataframe
    existing_df = spark.read.csv(fr"{existing_file}", header=True)

    # Create a dataframe on new file
    new_df = spark.read.csv(fr"{new_file}", header=True)

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
    latest_df.write.mode("overwrite").csv(fr"{output_location}")
    print(f"Successfully written output at location : {output_location}")

    # Logic using Spark SQL
    # Below code will run in Databricks
    print(f"Currently Running logic using SparkSQL")
    # Here instead of view create a Delta table.
    existing_df.createOrReplaceTempView("existing_view")
    new_df.createOrReplaceTempView("new_view")

    scd1_df = spark.sql("""
                MERGE INTO existing_view e
                USING new_view n 
                ON e.id = n.id 
                WHEN MATCHED THEN 
                    UPDATE SET e.name = n.name, e.city=n.city, e.state=n.state
                WHEN NOT MATCHED THEN 
                    INSERT (id, name, city, state) 
                    VALUES (n.id,n.name,n.city,n.state)
    """)

    scd1_df.write.mode("overwrite").csv(f"{output_location}")