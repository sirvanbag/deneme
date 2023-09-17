import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, sum, when, udf
from pyspark.sql.types import IntegerType
import re


def lw_specificword_count(word):
    pattern = "spark"
    result = re.findall(pattern, word)
    return len(result)

def cpt_specificword_count(word):
    pattern = "Spark"
    result = re.findall(pattern, word)
    return len(result)


findspark.init("/opt/spark")

spark = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("bentego-akademi-quiz") \
    .getOrCreate()

readme_file = "file:///opt/spark/README.md"

convertUDF_lw = udf(lambda word: lw_specificword_count(word), IntegerType())

convertUDF_cpt = udf(lambda word: cpt_specificword_count(word), IntegerType())

readMeData = spark.read.text(readme_file).cache()

word_data = readMeData \
    .select(explode(split(readMeData.value, "\s+")) \
            .alias("word"))

spark_word_data = word_data \
    .withColumn("lwSpark", convertUDF_lw(col("word"))) \
    .withColumn("cptSpark", convertUDF_cpt(col("word")))

cnt_lwSpark = spark_word_data.agg(sum(col("lwSpark"))).first()[0]

cnt_cptSpark = spark_word_data.agg(sum(col("cptSpark"))).first()[0]


print("README.md dosyasinda spark kelimesinin geçme sayisi: {} "
      "Spark kelimesinin geçme sayisi {}".format(cnt_lwSpark, cnt_cptSpark))

