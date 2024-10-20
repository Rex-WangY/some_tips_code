from pyspark import SparkConf, SparkContext
import json
import os

os.environ["HADOOP_CONF_DIR"] = "hadoop_path"

if __name__ == "__main__":
  conf = SparkConf().setAppName("test").setMaster("yarn")
  sc.SparkContest(conf = conf)
  #這個conf.set的作用主是因爲我們的集群需要知道主文件運行的時候，你其他的依賴在哪裏，因爲我們可能會把一些比較難寫的方法拆分去其他的文件維護，那麽在集群中當我們運行文件的時候，發現報錯，找不到相應的model，是因爲我們的集群沒有上傳相應的文件罷了
  conf.set("spark.submit.pyFiles", "path")
  #read data file
  # fild_rdd = sc.textFile ("path")
  file_rdd = sc.textFile('hdfs://......')
  #follow the "|" symbol splits RDD data
  json_rdd = fild_rdd.flatMap(lambda line: line.split("|"))

  #usr json package to covert the str to json
  dict_rdd = json_rdd.map(lambda json_str: json.loads(json_str))

  #filter data, which data area is in beijin
  beijing_rdd = dict_rdd.fileter(lambda d : d['areaName'] == "BJ")

  #combine place names and product names to form new strings
  category_rdd = beijing_rdd.map(lambda x: x['areaName'] + "_" + x['category'])

  #remove duplication
  result_rdd = category_rdd.distinct()

  #print
  print(result_rdd.collect())
