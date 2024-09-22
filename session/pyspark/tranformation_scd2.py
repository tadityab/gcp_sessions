from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, current_date, lit, when
import os

# Using below block dynamically defining input and output location present in project.
# Get parent folder  location,
current_folder = os.path.dirname(os.path.abspath(__file__))

# Get parent folder name
parent_folder = os.path.dirname(current_folder)

# Initialize input location.
existing_file = f"{parent_folder}/inputdata/employee_scd2.csv"
new_file = f"{parent_folder}/inputdata/employee_new.csv"

# Initialize output location
output_location = f"{parent_folder}/outputdata/scd2output"
if __name__ == '__main__':
    # Initializing spark Session
    spark = SparkSession.builder.master("local[*]").appName("Transformation SCD Type2").getOrCreate()

    # Generate a Dataframe using reading Existing data
    existing_df = spark.read.csv(f"{existing_file}", header=True)

    # Generate a dataframe using reading new data
    new_df = spark.read.csv(f"{new_file}", header=True)

    # Adding current date as start date in new dataframe
    new_df = new_df.withColumn("start_date", current_date().cast("string"))

    # converting column to Date type
    # existing_df = existing_df.withColumn("start_date", to_date(col("start_date"))).withColumn("end_date", to_date(col("end_date")))

    existing_df = existing_df.alias("old") \
        .join(new_df.alias("new"), on="id", how="left").withColumn("end_date", new_df.start_date).select(existing_df.id,existing_df.name,existing_df.city,existing_df.state,existing_df.start_date,"end_date")

    # Update value of end_date if it is null
    existing_df = existing_df.withColumn("end_date", when(col("end_date").isNull(),lit("2999-12-31")).otherwise(col("end_date")))

    # Add column end date in new df
    new_df = new_df.withColumn("end_date", lit("2999-12-31"))

    # Perform union to combine both dataframe
    scd2_df = existing_df.unionByName(new_df).orderBy("id", "end_date")

    # write output at target location
    scd2_df.write.mode("overwrite").csv(f"{output_location}")

