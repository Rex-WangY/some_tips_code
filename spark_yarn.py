from pyspark import SparkConf, SparkContext

if __name__ == "__main__":\
  conf = SparkConf().setAppName("test").setMaster("yarn")
  sc.SparkContest(conf = conf)

  #read data file
  fild_rdd = sc.textFile ("path")
  
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
