from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    df = spark.read.csv(r"C:\Users\tadit\Downloads\Electronic_sales_Sep2023-Sep2024.csv", header=True)

    df = df.select('Customer ID', 'Age', 'Gender', 'Loyalty Member', 'Product Type', 'SKU', 'Rating', 'Order Status', 'Payment Method', 'Total Price', 'Unit Price', 'Quantity', 'Purchase Date', 'Shipping Type', struct(col('Add-ons Purchased'), col('Add-on Total')).alias('add-ons'))

    df.coalesce(1).write.mode("overwrite").json(r"C:\Users\tadit\Downloads\Electronic_sales")


