from pyspark import SparkContext

sc = SparkContext('local','NASDAQ')

nsdata = sc.textFile('input/NASDAQsample.csv').map(lambda l: l.split(','))

cdata = sc.textFile('input/companylist.tsv').map(lambda l: l.split('\t'))

nsops = nsdata.map(lambda x: (x[1],x[2].split('-')[0]))

cops = cdata.map(lambda x : (x[0],x[3]))

jdata = nsops.join(cops)

count = jdata.map(lambda x:(x[1], 1)).reduceByKey(lambda a,b : a + b )

maxsec = count.map(lambda x: (x[0][0],(x[0][1],x[1]))).reduceByKey(lambda a,b : a if a[1]>b[1] else b)

salida = maxsec.map(lambda x: f'{x[1][0]},{x[0]},{x[1][1]}')

salida.saveAsTextFile('output1/out')

sc.stop()

