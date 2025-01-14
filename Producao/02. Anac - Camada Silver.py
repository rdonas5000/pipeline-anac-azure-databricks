# Databricks notebook source
dfAnac = spark.read.json('/mnt/anac/Bronze/V_OCORRENCIA_AMPLA.json')
print('Dataframe Anac criado')

# COMMAND ----------

columns = ['Aerodromo_de_Destino','Aerodromo_de_Origem','CLS','Categoria_da_Aeronave','Classificacao_da_Ocorrência','Danos_a_Aeronave','Data_da_Ocorrencia','Descricao_do_Tipo','Fase_da_Operacao','Historico','Hora_da_Ocorrência','ICAO','Ilesos_Passageiros','Ilesos_Tripulantes','Latitude','Longitude','Matricula','Modelo','Municipio','Nome_do_Fabricante','Numero_da_Ficha','Numero_da_Ocorrencia','Numero_de_Assentos','Operacao','Operador','Operador_Padronizado','PMD','PSSO','Regiao','Tipo_ICAO','Tipo_de_Aerodromo','Tipo_de_Ocorrencia','UF']
for c in columns:
    dfAnac = dfAnac.fillna('Sem Informação', subset=[c])
print('Colunas String Nulas ajustadas')

# COMMAND ----------

columns = ['Lesoes_Desconhecidas_Passageiros', 'Lesoes_Desconhecidas_Terceiros', 'Lesoes_Desconhecidas_Tripulantes', 'Lesoes_Fatais_Passageiros', 'Lesoes_Fatais_Terceiros', 'Lesoes_Fatais_Tripulantes', 'Lesoes_Graves_Passageiros', 'Lesoes_Graves_Terceiros', 'Lesoes_Graves_Tripulantes', 'Lesoes_Leves_Passageiros', 'Lesoes_Leves_Terceiros', 'Lesoes_Leves_Tripulantes']
for c in columns:
    dfAnac = dfAnac.withColumn(c, dfAnac[c].cast('int')).fillna(0, subset=[c])
print('Colunas Int convertidas')

# COMMAND ----------

dfAnac.write.mode('overwrite').parquet('/mnt/anac/Silver/V_OCORRENCIA_AMPLA.parquet')
print('Arquivo Silver parquet salvo')
