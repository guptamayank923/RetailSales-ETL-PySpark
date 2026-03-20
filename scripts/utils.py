from pyspark.sql import SparkSession
import glob

def get_spark_session(app_name="RetailETL"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_raw_data(path_pattern, spark):
    files = glob.glob(path_pattern + "*.csv")
    
    if not files:
        raise FileNotFoundError("No raw data files found!")

    df = None
    for i, file in enumerate(files):
        tmp = spark.read.csv(file, header=True, inferSchema=True)
        
        if i == 0:
            df = tmp
        else:
            df = df.union(tmp)

    return df