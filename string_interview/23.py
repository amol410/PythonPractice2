import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def process_sales_data():
    # Create Spark session
    spark = SparkSession.builder.getOrCreate()

    # 1. Combine the data from both regions
    print("Reading data files...")
    data_a = spark.read.excel("C:/Users/Amol/Desktop/Assessment/order_region_a.xlsx")
    data_b = spark.read.excel("C:/Users/Amol/Desktop/Assessment/order_region_b.xlsx")

    # Add region column (Rule 3)
    data_a = data_a.withColumn("region", lit("A"))
    data_b = data_b.withColumn("region", lit("B"))

    # Combine data (Rule 1)
    combined_data = data_a.union(data_b)

    # 2. Calculate total_sales
    print("Calculating sales...")
    # Convert numeric columns to ensure proper calculation
    combined_data = combined_data.select(
        "*",
        col("QuantityOrdered").cast("integer").alias("QuantityOrdered"),
        col("ItemPrice").cast("float").alias("ItemPrice"),
        col("PromotionDiscount").cast("float").alias("PromotionDiscount")
    )

    # Calculate total_sales (Rule 2)
    combined_data = combined_data.withColumn("total_sales", col("QuantityOrdered") * col("ItemPrice"))

    # 4. Remove duplicates based on OrderId
    # Keep first occurrence of each OrderId
    print("Removing duplicates...")
    combined_data = combined_data.dropDuplicates(["OrderId"])

    # 5. Calculate net_sale
    combined_data = combined_data.withColumn("net_sale", col("total_sales") - col("PromotionDiscount"))

    # 6. Exclude orders with negative or zero net_sale
    print("Filtering out negative or zero net sales...")
    combined_data = combined_data.where(col("net_sale") > 0)

    # Print summary before saving to database
    print("\nData Summary:")
    print(f"Total records: {combined_data.count()}")
    print("\nRegion distribution:")
    combined_data.groupBy("region").count().show()

    return combined_data

def load_to_database(data):
    # 7. Load to SQLite database
    print("\nLoading data to SQLite database...")
    data.write.mode("overwrite").format("jdbc") \
        .option("url", "jdbc:sqlite:C:/Users/Amol/Desktop/Assessment/sales_data.db") \
        .option("dbtable", "sales") \
        .save()

    # Verify data loading
    conn = sqlite3.connect("C:/Users/Amol/Desktop/Assessment/sales_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT region, COUNT(*) FROM sales GROUP BY region")
    results = cursor.fetchall()
    print("\nDatabase Summary:")
    for region, count in results:
        print(f"Region {region}: {count} records")
    conn.close()

if __name__ == "__main__":
    print("Starting data processing...")
    # Process data following business rules
    processed_data = process_sales_data()

    # Load to database
    load_to_database(processed_data)

    print("\nData processing and loading completed successfully!")