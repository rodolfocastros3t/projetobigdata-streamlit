import streamlit as st
import pandas as pd
import json

st.set_page_config(page_title="Dashboard Big Data", layout="wide")

st.title("📊 Painel de Análise de Contratos e Pagamentos")
st.subheader("Disciplina: Implementação de Projetos de Big Data")

# Carregar os dados calculados pelo Spark
with open("metricas_globais.json", "r") as f:
    metricas = json.load(f)

top_orgaos = pd.read_parquet("top_orgaos.parquet")
top_fornecedores = pd.read_parquet("top_fornecedores.parquet")

# --- LINHA 1: Métricas Financeiras Principais ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Contratado", f"R$ {metricas['total_contratado']:,.2f}")
col2.metric("Total Pago", f"R$ {metricas['total_pago']:,.2f}")
col3.metric("Saldo Contratual", f"R$ {metricas['saldo_contratual']:,.2f}")
col4.metric("Contratos Ativos", f"{metricas['contratos_ativos']:,}")

st.markdown("---")

# --- LINHA 2: Alertas de Auditoria/Compliance ---
st.markdown("### ⚠️ Indicadores de Compliance e Auditoria")
col_a, col_b, col_c, col_d = st.columns(4)

col_a.metric("Fornecedores Únicos", f"{metricas['fornecedores_unicos']}")
col_b.metric("Pagamentos s/ Contrato", f"R$ {metricas['pagamentos_sem_contrato']:,.2f}", delta="Crítico", delta_color="inverse")
col_c.metric("Contratos estourados (pago > contrato)", f"{metricas['pagamentos_acima_do_valor']} contratos", delta="Atenção", delta_color="off")
col_d.metric("Pagamentos pós-encerramento", f"{metricas['encerrados_com_pagamento_posterior']} contratos", delta="Inconformidade", delta_color="inverse")

# --- LINHA EXTRA: Qualidade dos Dados (Data Quality / Data Cleansing) ---
st.markdown("### 🕵️‍♂️ Governança e Qualidade de Dados (Data Quality)")
col_q1, col_q2, col_q3, col_q4 = st.columns(4)

col_q1.metric("Registros com Valores Nulos", f"{metricas['linhas_com_nulos']} colunas", delta="Campos Vazios", delta_color="off")
col_q2.metric("Cronologia Invertida (Fim < Início)", f"{metricas['cronologia_invertida']} contratos", delta="Erro Lógico", delta_color="inverse")
col_q3.metric("Pagamentos Duplicados Suspeitos", f"{metricas['pagamentos_duplicados']} ocorrências", delta="Risco / Fraude", delta_color="inverse")
col_q4.metric("Valores Negativos Encontrados", f"{metricas['valores_negativos']} linhas", delta="Erro de Sinal", delta_color="off")

st.markdown("---")

# --- LINHA 3: Gráficos (Visualização de Dados) ---
col_g1, col_g2 = st.columns(2)

with col_g1:
    st.write("### 🏢 Top 5 Órgãos por Valor Contratado")
    st.bar_chart(data=top_orgaos, x="orgao", y="valor", use_container_width=True)

with col_g2:
    st.write("### 🚚 Top 5 Fornecedores por Valor Recebido")
    st.bar_chart(data=top_fornecedores, x="nome_fornecedor", y="valor", use_container_width=True)