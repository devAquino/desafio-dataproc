# desafio-dataproc
Conclusão do desafio dataproc que consiste na criação de um jop pyspark que leia um arquivo de texto e salve as 10 palavras que mais se repete em outro arquivo.
O arquivo *contador.py* foi modificado para salvar as 10 palavras que mais se repete no arquivo *resultado.txt*, também foi feito o tratamento para desconsiderar espaços em brancos, que também estava entre as 10 palavras mais repetidas, o job gerou o arquivo *part-00000*.
Foi criado um outro job onde faço a análise de dados do dataset *enem.csv* que, usando ETL, fiz todo o tratamento necessário e salvei como *microdados_enem.csv*. A partir daqui foi usado pyspark para manipulação dos dados, o jobo execulta o arquivo *analis_enem.py* e gera uma arquivo parquet como resultado
