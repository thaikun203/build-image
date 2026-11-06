from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("WordCount-SparkSQL") \
    .getOrCreate()

# Dữ liệu sinh sẵn
data = [
    "Apache Spark is an open-source distributed computing system",
    "Spark provides an interface for programming entire clusters",
    "WordCount example with Spark"
]

# Tạo DataFrame từ dữ liệu
df = spark.createDataFrame([(line,) for line in data], ["line"])

# Tách từ và tính số lượng từ bằng Spark SQL
df_words = df.selectExpr("explode(split(line, ' ')) as word")
df_word_counts = df_words.groupBy("word").count()

# Hiển thị kết quả
df_word_counts.show()

# Dừng SparkSession
spark.stop()
