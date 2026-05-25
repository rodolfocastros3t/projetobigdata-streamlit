import os
import sys

# Força o PySpark a utilizar o Java 17 do Temurin instalado no Mac
os.environ['JAVA_HOME'] = '/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home'

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Inicializar o Spark
spark = SparkSession.builder \
    .appName("ETL_BigData_Projeto") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# 2. Carregar os dados
contratos = spark.read.csv("contratos.csv", header=True, inferSchema=True)
fornecedores = spark.read.csv("fornecedores.csv", header=True, inferSchema=True)
pagamentos = spark.read.csv("pagamentos.csv", header=True, inferSchema=True)

# --- PROCESSAMENTO DAS MÉTRICAS ---

# Agregando pagamentos por contrato para métricas cruzadas
pagos_por_contrato = pagamentos.groupBy("id_contrato").agg(
    F.sum("valor_pago").alias("total_pago")
)

# Cruzando Contratos com Pagamentos
df_contratos_pagos = contratos.join(pagos_por_contrato, "id_contrato", "left").fillna(0, subset=["total_pago"])
df_contratos_pagos = df_contratos_pagos.withColumn("saldo_contratual", F.col("valor_contrato") - F.col("total_pago"))

# Métrica 1, 2, 3 e 4: Totais Gerais e Contratos Ativos
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
    "contratos_ativos": res_gerais["contratos_ativos"]
}

# Métrica 5: Fornecedores únicos
fornecedores_unicos = contratos.select("id_fornecedor").distinct().count()
metricas_globais["fornecedores_unicos"] = fornecedores_unicos

# Métrica 6: Top órgãos por valor contratado
top_orgaos = contratos.groupBy("orgao").agg(F.sum("valor_contrato").alias("valor")).orderBy(F.desc("valor")).limit(5)

# Métrica 7: Top fornecedores por valor pago
top_fornecedores = pagamentos.join(contratos, "id_contrato", "inner") \
    .join(fornecedores, "id_fornecedor", "inner") \
    .groupBy("nome_fornecedor") \
    .agg(F.sum("valor_pago").alias("valor")) \
    .orderBy(F.desc("valor")).limit(5)

# Métrica 8: Pagamentos sem contrato
pagamentos_sem_contrato = pagamentos.join(contratos, "id_contrato", "left_anti")
total_pagamentos_sem_contrato = pagamentos_sem_contrato.select(F.sum("valor_pago")).collect()[0][0] or 0.0

# Métrica 9: Pagamentos acima do valor contratado
pagamentos_acima = df_contratos_pagos.filter(F.col("total_pago") > F.col("valor_contrato"))
total_pagamentos_acima = pagamentos_acima.count()

# Métrica 10: Contratos encerrados com pagamentos posteriores à vigência
# Modificado para try_to_date para tratar de forma robusta strings inválidas (ex: 31 de fevereiro) transformando-as em NULL
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

metricas_globais["pagamentos_sem_contrato"] = total_pagamentos_sem_contrato
metricas_globais["pagamentos_acima_do_valor"] = total_pagamentos_acima
metricas_globais["encerrados_com_pagamento_posterior"] = total_encerrados_com_erro

# --- SALVANDO RESULTADOS PARA O STREAMLIT ---
import json
with open("metricas_globais.json", "w") as f:
    json.dump(metricas_globais, f)

top_orgaos.toPandas().to_csv("top_orgaos.csv", index=False)
top_fornecedores.toPandas().to_csv("top_fornecedores.csv", index=False)

print("🔥 Processamento com Spark concluído com sucesso e dados salvos!")
spark.stop()