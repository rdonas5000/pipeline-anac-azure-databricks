# Databricks notebook source
dfAnac = spark.read.parquet('/mnt/anac/Silver/V_OCORRENCIA_AMPLA.parquet/')
print('Dataframe criado')

# COMMAND ----------

colunas = ['Aerodromo_de_Destino', 'Aerodromo_de_Origem','Classificacao_da_Ocorrência', 'Danos_a_Aeronave', 'Data_da_Ocorrencia','Municipio','UF','Regiao','Tipo_de_Aerodromo', 'Tipo_de_Ocorrencia','Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']
dfAnac = dfAnac.select(colunas)
print('colunas selecionadas')

# COMMAND ----------

colunas_a_somar = [
    "Lesoes_Desconhecidas_Passageiros",
    "Lesoes_Desconhecidas_Terceiros",
    "Lesoes_Desconhecidas_Tripulantes",
    "Lesoes_Fatais_Passageiros",
    "Lesoes_Fatais_Terceiros",
    "Lesoes_Fatais_Tripulantes",
    "Lesoes_Graves_Passageiros",
    "Lesoes_Graves_Terceiros",
    "Lesoes_Graves_Tripulantes",
    "Lesoes_Leves_Passageiros",
    "Lesoes_Leves_Terceiros",
    "Lesoes_Leves_Tripulantes"
]
dfAnac = dfAnac.withColumn("Total_Lesoes", sum(dfAnac[somarcolunas] for somarcolunas in colunas_a_somar))
print('Campo total criado')

# COMMAND ----------

dfAnac = dfAnac\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')\
    .withColumnRenamed('Aerodromo_de_Origem', 'Origem')\
    .withColumnRenamed('Classificacao_da_Ocorrência', 'Classificacao')\
    .withColumnRenamed('Danos_a_Aeronave', 'Danos')\
    .withColumnRenamed('Data_da_Ocorrencia', 'Data')\
    .withColumnRenamed('UF', 'Estado')\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')\
    .withColumnRenamed('Aerodromo_de_Destino', 'Destino')
print('Colunas renomeadas')

# COMMAND ----------

filtroClassificacoes = ['Indeterminado', 'Sem Informação', 'Exterior']
dfAnac = dfAnac.filter(~dfAnac['Estado'].isin(filtroClassificacoes))
print('Filtro de estado aplicado')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, date_format, from_utc_timestamp
dfAnac = dfAnac.withColumn('Data Atualização', date_format(from_utc_timestamp(current_timestamp(), 'America/Sao_Paulo'), 'yyyy-MM-dd HH:mm:ss'))
print('Campo data atualização criado')

# COMMAND ----------

dfAnac.write\
    .mode('overwrite')\
    .format('parquet')\
    .partitionBy('Estado')\
    .save('/mnt/anac/Gold/anac_estado')
print('Arquivo salvo com sucesso')
