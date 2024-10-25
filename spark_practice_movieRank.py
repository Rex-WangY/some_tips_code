from pyspark.sql import SparkSession
import pyspark.sql.functions as F
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

    #TODO 1: user average rank
    df.groupBy('user_id').\
        avg('rank').\
        withColumnRenamed("avg(rank)", "avg_rank").\
        withColumn("avg_rank", F.round("avg_rank",2)).\
        orderBy("avg_rank", ascending=False).\
        show()

    #TODO 2: movie average rank
    df.groupBy('movie_id').\
        avg('rank').\
        withColumnRename("avg(rank)","avg_rank").\
        withColumn("avg_rank", F.round("avg_rank",2)).\
        orderBy('avg_rank',ascending = False).\
        show()

    #TODO 3 check the movie which movie rank biger than average rank
    df.createTempView("movie")
    spark.sql(
        """
           WITH 
           avg_rank AS (
            SELECT AVG(rank) as avg_rank FROM movie GROUP BY customer_id
           )
           SELECT movie_id FROM movie where rank > (select * from avg_rank)
        """
    )

