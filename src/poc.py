if __name__ == '__main__':
	sc = pyspark.SparkContext(appName="myAppName")

	from pyspark.sql import SparkSession
	spark = SparkSession(sc)

