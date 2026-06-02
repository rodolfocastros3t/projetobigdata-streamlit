import os
import sys
import json

os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("ETL_BigData_Projeto") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

contratos = spark.read.csv("contratos.csv", header=True, inferSchema=True)
fornecedores = spark.read.csv("fornecedores.csv", header=True, inferSchema=True)
pagamentos = spark.read.csv("pagamentos.csv", header=True, inferSchema=True)

# -------------------------------------------------------------
# [DATA QUALITY] 1. Análise de Nulos e Negativos
# -------------------------------------------------------------
nulos_contratos = contratos.filter(F.col("id_contrato").isNull() | F.col("valor_contrato").isNull()).count()
nulos_pagamentos = pagamentos.filter(F.col("id_pagamento").isNull() | F.col("valor_pago").isNull()).count()
valores_negativos = pagamentos.filter(F.col("valor_pago") < 0).count()

# [DATA QUALITY] 2. Cronologia Invertida
contratos_valida_data = contratos \
    .withColumn("dt_ini", F.try_to_date(F.col("data_inicio"), "yyyy-MM-dd")) \
    .withColumn("dt_fim", F.try_to_date(F.col("data_fim"), "yyyy-MM-dd"))
cronologia_invertida = contratos_valida_data.filter(F.col("dt_fim") < F.col("dt_ini")).count()

# [DATA QUALITY] 3. Pagamentos Duplicados
pagamentos_suspeitos = pagamentos.groupBy("id_contrato", "data_pagamento", "valor_pago") \
    .agg(F.count("id_pagamento").alias("recorrencias")) \
    .filter(F.col("recorrencias") > 1)
total_pagamentos_duplicados = pagamentos_suspeitos.count()
# -------------------------------------------------------------

pagos_por_contrato = pagamentos.groupBy("id_contrato").agg(F.sum("valor_pago").alias("total_pago"))

df_contratos_pagos = contratos.join(pagos_por_contrato, "id_contrato", "left").fillna(0, subset=["total_pago"])
df_contratos_pagos = df_contratos_pagos.withColumn("saldo_contratual", F.col("valor_contrato") - F.col("total_pago"))

res_gerais = df_contratos_pagos.select(
    F.sum("valor_contrato").alias("total_contratado"),
    F.sum("total_pago").alias("total_pago"),
    F.sum("saldo_contratual").alias("saldo_contratual"),
    F.count(F.when(F.col("status") == "ATIVO", 1)).alias("contratos_ativos")
).collect()[0]

metricas_globais = {
    "total_contratado": res_gerais["total_contratado"],
    "total_pago": res_gerais["total_pago"],
    "saldo_contratual": res_gerais["saldo_contratual"],
    "contratos_ativos": res_gerais["contratos_ativos"],
    "fornecedores_unicos": contratos.select("id_fornecedor").distinct().count()
}

top_orgaos = contratos.groupBy("orgao").agg(F.sum("valor_contrato").alias("valor")).orderBy(F.desc("valor")).limit(5)

top_fornecedores = pagamentos.join(contratos, "id_contrato", "inner") \
    .join(fornecedores, "id_fornecedor", "inner") \
    .groupBy("nome_fornecedor") \
    .agg(F.sum("valor_pago").alias("valor")) \
    .orderBy(F.desc("valor")).limit(5)

pagamentos_sem_contrato = pagamentos.join(contratos, "id_contrato", "left_anti")
total_pagamentos_sem_contrato = pagamentos_sem_contrato.select(F.sum("valor_pago")).collect()[0][0] or 0.0

pagamentos_acima = df_contratos_pagos.filter(F.col("total_pago") > F.col("valor_contrato"))
total_pagamentos_acima = pagamentos_acima.count()

pagamentos_data = pagamentos.withColumn("data_pagamento", F.try_to_date(F.col("data_pagamento"), "yyyy-MM-dd"))
contratos_data = contratos.withColumn("data_fim", F.try_to_date(F.col("data_fim"), "yyyy-MM-dd"))

contratos_encerrados_Incorretos = pagamentos_data.join(contratos_data, "id_contrato", "inner") \
    .filter(
        (F.col("status") == "ENCERRADO") & 
        (F.col("data_pagamento").isNotNull()) & 
        (F.col("data_fim").isNotNull()) & 
        (F.col("data_pagamento") > F.col("data_fim"))
    )
total_encerrados_com_erro = contratos_encerrados_Incorretos.select("id_contrato").distinct().count()

# Consolidação das Métricas Finais
metricas_globais["pagamentos_sem_contrato"] = total_pagamentos_sem_contrato
metricas_globais["pagamentos_acima_do_valor"] = total_pagamentos_acima
metricas_globais["encerrados_com_pagamento_posterior"] = total_encerrados_com_erro

# Injeção das novas chaves de Data Quality
metricas_globais["linhas_com_nulos"] = nulos_contratos + nulos_pagamentos
metricas_globais["cronologia_invertida"] = cronologia_invertida
metricas_globais["pagamentos_duplicados"] = total_pagamentos_duplicados
metricas_globais["valores_negativos"] = valores_negativos

with open("metricas_globais.json", "w") as f:
    json.dump(metricas_globais, f)

top_orgaos.write.mode("overwrite").parquet("top_orgaos.parquet")
top_fornecedores.write.mode("overwrite").parquet("top_fornecedores.parquet")

print("🔥 Processamento com Spark concluído com sucesso e dados salvos!")
spark.stop()