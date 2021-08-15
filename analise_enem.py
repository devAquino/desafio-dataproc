#!/usr/bin/python3
# coding: utf-8

# # Análise de dados do enem 2016

# In[6]:

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local','Análise Microdados Enem').getOrCreate()
spark = SparkSession(sc)


# In[44]:


import pandas as pd
import pandera as pa


# In[28]:


df = pd.read_csv('gs://bucket-meu-desafio-dataproc/enem_2016.csv')


# visualizando as colunas do dataframe

# In[11]:


df.columns.values


# Como não pretendo trabalhar com todos as colunas, farei uma seleção das colunas de meu interesse

# In[29]:


colunas_selecionadas = ['NU_INSCRICAO', 'NU_ANO',
       'NO_MUNICIPIO_RESIDENCIA','SG_UF_RESIDENCIA',
       'NU_IDADE', 'TP_SEXO', 'TP_ESTADO_CIVIL', 'TP_COR_RACA',
       'IN_GESTANTE','NO_MUNICIPIO_PROVA', 'SG_UF_PROVA',
       'NU_NOTA_CN', 'NU_NOTA_CH', 'NU_NOTA_LC',
       'NU_NOTA_MT', 'NU_NOTA_REDACAO', 'Q001', 'Q002']


# Criando um dataframe com as colunas selecionadas

# In[30]:


df_enem = df.filter(items=colunas_selecionadas)


# Renomeando as colunas para melhorar a legibilidade

# In[31]:


df_enem.rename(columns={'NU_INSCRICAO':'inscricao', 'NU_ANO':'ano',
       'NO_MUNICIPIO_RESIDENCIA':'municipio_residencia','SG_UF_RESIDENCIA':'uf_residencia',
       'NU_IDADE':'idade', 'TP_SEXO':'sexo', 'TP_ESTADO_CIVIL':'estado_civil', 'TP_COR_RACA':'raca',
       'IN_GESTANTE':'gestante',
       'NO_MUNICIPIO_PROVA':'municipio_prova', 'SG_UF_PROVA':'uf_prova',
       'NU_NOTA_CN':'nota_ciencias_da_naturaza', 'NU_NOTA_CH':'nota_ciencias_humanas', 'NU_NOTA_LC':'nota_linguagens_e_codigos',
       'NU_NOTA_MT':'nota_matematica','NU_NOTA_REDACAO':'nota_redacao', 'Q001':'questionario_pai', 
       'Q002':'questionario_mae'}, inplace=True)


# Visualizando o tipo de cada coluna

# In[32]:


df_enem.dtypes


# Podemos observar que a coluna raca, gestante e estado_civil tem seus valores representado por numeros inteiro, inteiro e float respectivamente. Se quisermos conferir o que cada um representa teriamos que analizamos o dicionário de dados. Então vou criar uma melhor representação desses valores e das outras colunas semelhantes como questionario_pai e questionario_mae

# In[33]:


df_enem.head(5)


# Dicionário para alterar o valor da coluna raca

# In[34]:


dict_raca = {
    0:'Não declarado',
    1:'Branca',
    2:'Preta',
    3:'Parda',
    4:'Amarela',
    5:'Indígina'
}


# Alterando os valores da coluna raca

# In[35]:


df_enem.raca = [dict_raca[rc] for rc in df_enem.raca]


# Dicionário para alterar o valor da coluna gestante

# In[36]:


dict_gestante = {
    1:'Sim',
    0:'Não',
}


# Alterando os valores da coluna gestante

# In[37]:


df_enem.gestante = [dict_gestante[gest] for gest in df_enem.gestante]


# Criando dicionário para alterar os valores da coluna estado_civil

# In[38]:


dict_estado_civil = {
    0.0:'Não informado',
    1.0:'Solteiro(a)',
    2.0:'Casado(a)/Mora com companheiro(a)',
    3.0:'Divorciado(a)/Desquitado(a)/Separado(a)',
    4.0:'Viúvo(a)',
}


# *Preenchendo os valores NAN da coluna estado_civil com 0 para que a iteração seja possivel*

# In[39]:


df_enem.estado_civil.fillna(0,inplace=True)


# *Alterando os valores da coluna estado_civil*

# In[40]:


df_enem.estado_civil = [dict_estado_civil[estado] for estado in df_enem.estado_civil]


# Validando as colunas

# In[45]:


schema = pa.DataFrameSchema(
    columns = {
        'inscricao': pa.Column(pa.String),
        'ano': pa.Column(pa.Int),
        'municipio_residencia':pa.Column(pa.String),
        'uf_residencia': pa.Column(pa.String),
        'idade': pa.Column(pa.Int),                     
        'sexo':pa.Column(pa.String),            
        'estado_civil':pa.Column(pa.String),              
        'raca':pa.Column(pa.String),                      
        'gestante':pa.Column(pa.String),                  
        'municipio_prova':pa.Column(pa.String),           
        'uf_prova':pa.Column(pa.String),                  
        'nota_ciencias_da_naturaza':pa.Column(pa.String), 
        'nota_ciencias_humanas':pa.Column(pa.String),      
        'nota_linguagens_e_codigos':pa.Column(pa.String),  
        'nota_matematica':pa.Column(pa.String),            
        'nota_redacao':pa.Column(pa.String),               
        'questionario_pai':pa.Column(pa.String),           
        'questionario_mae':pa.Column(pa.String),           

    }
)


# Criando um dicionario para alterar os valores das colunas questionario_pai e questionario_mae, também melhorará a legibilidade

# In[46]:


questionario = {
    'A':'Nunca estudou',
    'B':'Não completou a 4ª série/5º ano do Ensino Fundamental.',
    'C':'Completou a 4ª série/5º ano, mas não completou a 8ª série/9º ano do Ensino Fundamental.',
    'D':'Completou a 8ª série/9º ano do Ensino Fundamental, mas não completou o Ensino Médio.',
    'E':'Completou o Ensino Médio, mas não completou a Faculdade.',
    'F':'Completou a Faculdade, mas não completou a Pós-graduação.',
    'G':'Completou a Pós-graduação.',
    'H':'Não sei.',
}


# Iterando sobre a coluna questionario_mae, capturando os valores do dicionario questionario e armazenando na coluna questionario_mae

# In[47]:


df_enem.questionario_mae = [questionario[resp] for resp in df_enem.questionario_mae]


# Iterando sobre a coluna questionario_pai, capturando os valores do dicionario questionario e armazenando na coluna questionario_pai

# In[48]:


df_enem['questionario_pai'] = [questionario[resp] for resp in df_enem.questionario_pai]


# Agora temos a seguinte representação

# In[49]:


df_enem['questionario_mae']


# Substituindo todos os valores NaN do dataframe por 0

# In[50]:


df_enem.fillna(0, inplace=True)


# Savando o dataframe já transformado como microdados_enem.csv

# In[73]:


df_enem.to_csv('gs://bucket-meu-desafio-dataproc/microdados_enem.csv')


# # A partir de agora será usado pyspark para a manipulação dos dados

# In[5]:


rdd_enem = spark.read.csv('gs://bucket-meu-desafio-dataproc/microdados_enem.csv', header=True)


# Analisando as colunas e seus tipos

# In[80]:


rdd_enem.printSchema()


# In[81]:


rdd_enem.take(3)


# In[82]:


rdd_enem.show()


# Selecionando uma coluna específica

# In[83]:


rdd_enem.select('municipio_residencia').show()


# *Quantidade de município onde as provas foram realizadas*

# In[84]:


rdd_enem.select('municipio_prova').distinct().count()


# Criando uma tabela temporária para manipulação dos dados

# In[89]:


rdd_enem.createOrReplaceTempView('temp_microdados_enem')
spark.read.table("temp_microdados_enem").show(3)


# Analizando os alunos pela inscricao e suas respectivas notas

# In[90]:


nota_alunos = spark.sql('SELECT inscricao, nota_ciencias_da_naturaza, nota_ciencias_humanas, nota_linguagens_e_codigos, nota_matematica, nota_redacao FROM temp_microdados_enem').show()


# Obtendo todos os candidatos, pela inscrição, que obtiveram nota maior que 900 na redação

# In[91]:


spark.sql('SELECT inscricao, nota_redacao FROM temp_microdados_enem WHERE nota_redacao > 900').show()


# Obtendo a quantidade de inscrito por cidade de cada estado ordenado pela maio quantidade de inscritos para a menor.

# In[92]:


spark.sql('SELECT municipio_residencia, uf_residencia, COUNT(inscricao) FROM temp_microdados_enem GROUP BY municipio_residencia, uf_residencia ORDER BY COUNT(inscricao) DESC').show()


# Obtendo a quantidade de inscritos no estado de Tocantins.

# In[95]:


spark.sql('SELECT count(inscricao) FROM temp_microdados_enem WHERE uf_residencia = "TO"').show()


# Obtendo as maiores notas em nota_linguagens_e_codigos de cada cidade do estado de RJ.

# In[96]:


spark.sql('SELECT uf_residencia, municipio_residencia, MAX(nota_linguagens_e_codigos) nota_linguagens_e_codigos FROM temp_microdados_enem WHERE uf_residencia = "RJ" GROUP BY uf_residencia, municipio_residencia').show()


# Como podemos ver, a maior nota de matemática é do candidato que reside em Fortaleza.

# In[98]:


spark.sql('SELECT max(nota_matematica) FROM temp_microdados_enem WHERE uf_residencia = "CE" ').show()


# In[7]:


rdd_enem.write.save('gs://bucket-meu-desafio-dataproc/dados-enem/analise_enem.ipynb')


# In[ ]:




