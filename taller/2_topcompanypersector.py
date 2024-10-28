from pyspark import SparkContext
from datetime import datetime

sc = SparkContext('local','NASDAQ')

nsdata = sc.textFile('input/NASDAQsample.csv').map(lambda l: l.split(','))

cdata = sc.textFile('input/companylist.tsv').map(lambda l: l.split('\t'))

nsops = nsdata.map(lambda x: ((x[1],x[2].split('-')[0]),(datetime.strptime(x[2],'%Y-%m-%d'),x[3],x[6])))

nsmins = nsops.reduceByKey(lambda a,b: a if a[0]<b[0] else b).map(lambda x: (x[0],x[1][1]))

nsmaxs = nsops.reduceByKey(lambda a,b: a if a[0]>b[0] else b).map(lambda x:(x[0],x[1][2]))

nsjoin = nsmins.join(nsmaxs)


def porcs(x):
	ap = float(x[0])
	ci = float(x[1])
	if ap == 0 : return 0
	raz = ci/ap
	raz = raz -1 
	raz = raz*100
	return raz


nsporc = nsjoin.map(lambda x: (x[0],porcs(x[1])))

cops = cdata.map(lambda x : (x[0],x[3]))

nsporc2 = nsporc.map(lambda x: (x[0][0],(x[0][1],x[1])))

totjoin = nsporc2.join(cops).map(lambda x: ((x[1][0][0],x[1][1]),(x[1][0][1],x[0])))

tots = totjoin.reduceByKey(lambda a,b: a if a[0]>b[0] else b)

tots = tots.map(lambda x: '{},{},{},{:.2f}%'.format(x[0][1],x[0][0],x[1][1],x[1][0]))

tots.saveAsTextFile('output2/porcjoined')


sc.stop()

