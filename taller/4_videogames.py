from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr,regexp_replace,explode,avg,count


spark = SparkSession.builder.appName('lab_SparkApi').getOrCreate()

sc = spark.sparkContext

df = spark.read.csv('input/games.csv',header=True, inferSchema=True)

df = df.withColumn(
    "Genres", expr("split(regexp_replace(Genres, '[\\\[\\\] ]', ''), ',')").cast("array<string>")
)

df_exp = df.withColumn('Genre',explode(col('Genres')))

res = df_exp.groupBy('Genre').agg(avg('Rating').alias('avg_rating'),
	count('Title').alias('cant'))

res = res.filter(col('avg_rating').isNotNull())

res.show()

df_exp.printSchema()

res.write.csv('output/resultadosgeneros')
