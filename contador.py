import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","Meu-Desafio-Dataproc")
    words = sc.textFile("gs://bucket-meu-desafio-dataproc/livro.txt").flatMap(lambda line: line.split(" ")).filter(lambda x: len(x) > 0)
    wordcounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    sc.parallelize(wordcounts.collect()[:10]).saveAsTextFile("gs://bucket-meu-desafio-dataproc/meu-resultado")
