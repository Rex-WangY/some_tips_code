from pyspark.sql import SparkSession

if __name__ == "__main__":
    #0. create perfermance entry 
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    
    sc = spark.sparkContest

    # read dataset
    schema = StructType().add("user_id", StringType(), nullable=True).\
        add("movie_id", IntegerType(), nullable=True).\
        add("rank", IntegerType(), nullable=True).\
        add("ts", StringType(), nullable=True)
    
    df = spark.read.format("csv").\
        option("sep", "\t").\
        option("header", False).\
        option("encoding", "utf-8").\
        schema(schema=schema).\
        load('../data/input/sql/u.data')
    
        
