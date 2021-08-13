import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","Análise enem 2016")
    enem = sc.textFile("gs://bucket-meu-desafio-dataproc/microdados_enem.ipynb")
    enem.saveAsTextFile("gs://bucket-meu-desafio-dataproc/meu-resultado")