from pyspark.sql import SparkSession
from pyspark.sql.functions import col,expr,regexp_replace,explode,avg,count,max


spark = SparkSession.builder.appName('lab_SparkApi').getOrCreate()

sc = spark.sparkContext

df = spark.read.csv('input/games.csv',header=True, inferSchema=True)

#df.show()

df = df.withColumn(
    "Genres", expr("split(regexp_replace(Genres, '[\\\[\\\] ]', ''), ',')").cast("array<string>")
)

df_exp_g = df.withColumn('Genre',explode(col('Genres')))

gens = df_exp_g.groupBy('Genre').agg(max('Rating'))

gens.show()

df = df.withColumn(
    "Team", expr("split(regexp_replace(Team, '[\\\[\\\] ]', ''), ',')").cast("array<string>")
)

df_exp_t = df.withColumn('team_e',explode(col('Team')))

ts = df_exp_t.groupBy('team_e').agg(max('Rating'))

ts.show()


#res = df_exp.groupBy('Genre').agg(avg('Rating').alias('avg_rating'),
#	count('Title').alias('cant'))

#res = res.filter(col('avg_rating').isNotNull())

#res.show()

#df_exp.printSchema()

#res.write.csv('output/resultadosgeneros')

