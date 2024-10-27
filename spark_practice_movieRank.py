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

    #TODO 3.2 check the movie which movie rank biger than average rank
    df.where(df['rank'] > df.select(F.avg(df['rank'])).first(['avg(rank)']).count())

    #TODO 4 Checkt the users with the highest number of ratings in high-scoreing movies,the average score of this person
    #Find the user
    user_is = df.where("rank > 3").\
        groupBy('user_id').\
        count().\
        withColumnRenamed("count","cnt").\
        orderBy('cnt',acsending=False).\
        first()['user_id']
    #calculate the average of the users we find
    df.filter(df['user'] == user_id).\
        select(F.round(F.avg("rank"),2)).show()

    #TODO 5 Check the every user's highest, aveerg and lowest rank
    df.createTempview("movie")
    spark.sql(
        """
            SELECT 
                user_id,
                avg(rank) as avg_rank,
                min(rank) as min_rank,
                max(rank) as max_rank
            FROM movie as m
            GROUP BY user_id
        """
    )
    #TODO 5.2 Check the every user's highest, aveerg and lowest rank
    df.groupBy("user_id").agg(
        F.round(F.avg('rank'),2).alias('avg_rank'),
        F.round(F.min('rank'),2).alias('min_rank'),
        F.round(F.max('rank'),2).alias('max_rank'),
    ).show()

    #TODO 6 Check the movie whick ranking above 100 times,  and then the top 10 average
    df.groupBy('movie_id').agg(
        F.count('movie_id').alias('cnt'),
        F.Round(F.avg('rank'),2).alias('avg_rank')
    ).where("cnt > 100")/\
    orderBy("avg_rank",ascending=False).\
    limit(10).\
    show()

        