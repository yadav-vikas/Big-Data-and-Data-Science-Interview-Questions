from pyspark.sql.functions import col, lag, datediff
from pyspark.sql import Window
from pyspark.sql import SparkSession

# Create a Spark Session
spark = SparkSession.builder.appName("MySpark").getOrCreate()

data = [
  ('U1','2025-01-01'),
  ('U1','2025-01-05'),
  ('U1','2025-01-10'),
  ('U2','2025-01-02'),
  ('U2','2025-01-04'),
]

cols = ['user_id','session_date']

sparkdf = spark.createDataFrame(data, cols)

sparkdf.show()

sparkdf.createOrReplaceTempView("user_session")

sparkdf_sql = spark.sql("""

with temp_session as (
  select user_id, session_date, lag(session_date) over(partition by user_id order by session_date) as prev_session from user_session
)
select user_id, session_date, datediff(session_date, prev_session) as session_diff from temp_session

""")

sparkdf_sql.show()

sparkdf = sparkdf.withColumn("session_date", col("session_date").cast('date'))
sparkdf = sparkdf.withColumn("prev_session", lag("session_date").over(Window.partitionBy("user_id").orderBy("session_date")))
sparkdf = sparkdf.withColumn("session_diff", datediff("session_date", "prev_session"))

sparkdf.show()