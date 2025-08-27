from pyspark.sql.functions import count, col
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder.appName("MySpark").getOrCreate()

data = [
  ('C1','2025-01-01',200),
  ('C1','2025-01-10',300),
  ('C2','2025-01-05',150),
  ('C3','2025-01-02',100),
  ('C3','2025-01-04',200),
]

cls = ['customer_id','purchase_date','amount']

sparkdf = spark.createDataFrame(data, cls)

sparkdf.show()

sparkdf.createOrReplaceTempView("user_purchase")

sparkdf_sql = spark.sql("""
 
select customer_id, count(customer_id) from user_purchase group by customer_id  having count(customer_id) == 1 
""")

sparkdf_sql.show()

sparkdf = sparkdf.groupBy('customer_id').agg(count('customer_id').alias('purchase_count'))

sparkdf.filter(col('purchase_count') == 1).select('customer_id').show()