from pyspark import SparkContext
sc = SparkContext()

# Create the initial RDD from file hosted on GCP Storage
initial_rdd = sc.textFile('gs://neiloconnor-pyspark-bucket/example_data.txt')

# Simply print the contents of the file
print(initial_rdd.collect())