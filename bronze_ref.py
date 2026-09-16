# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Descrição do pipeline bronze - tabelas de referência
# MAGIC %md
# MAGIC Lê os CSVs de referência do volume `voebem.bronze.arquivos/referencias/` e materializa tabelas bronze.
# MAGIC
# MAGIC ## Arquivos de referência:
# MAGIC * **Empresas Aéreas** - 2 arquivos: empresas nacionais e estrangeiras (ICAO, razão social, país)
# MAGIC * **Aeródromos** - Cadastro de aeroportos públicos (ICAO, nome, cidade, estado)
# MAGIC
# MAGIC Regras da camada bronze
# MAGIC - **sem tipagem** - tudo como string, exatamente como veio do arquivo
# MAGIC - **nada de filtro** - nenhuma linha é descartada
# MAGIC - **colunas de auditoria** - de qual arquivo veio e quando foi ingerido
# MAGIC - **idempotente** - rodar duas vezes não duplica

# COMMAND ----------

# DBTITLE 1,Configuração
from pyspark.sql import functions as F

CAMINHO_EMPRESAS_NAC = "/Volumes/voebem/bronze/arquivos/referencias/pda_empresas_aereas_nacionais.csv"
CAMINHO_EMPRESAS_EST = "/Volumes/voebem/bronze/arquivos/referencias/pda_empresas_aereas_estrangeiros.csv"
CAMINHO_AERODROMOS = "/Volumes/voebem/bronze/arquivos/referencias/AerodromosPublicos.csv"

TABELA_EMPRESAS = "voebem.bronze.ref_empresas"
TABELA_AERODROMOS = "voebem.bronze.ref_aerodromos"

# COMMAND ----------

# DBTITLE 1,1. Empresas Aéreas
# MAGIC %md
# MAGIC ## 1. Empresas Aéreas
# MAGIC
# MAGIC Cadastro de empresas aéreas com código ICAO, razão social, serviços e informações de contato.
# MAGIC
# MAGIC Dois arquivos: nacionais e estrangeiras.

# COMMAND ----------

# DBTITLE 1,Carrega empresas nacionais
# Lê arquivo de empresas nacionais
empresas_nac = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ";")
    .option("skipRows", 1)  # Pula linha "Atualizado em:"
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")  # bronze não descarta nenhuma linha
    .load(CAMINHO_EMPRESAS_NAC)
)

print("✅ Empresas nacionais - colunas lidas do arquivo:")
for c in empresas_nac.columns:
    print(f"  {c!r}")

# COMMAND ----------

# DBTITLE 1,Carrega empresas estrangeiras
# Lê arquivo de empresas estrangeiras
empresas_est = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ";")
    .option("skipRows", 1)  # Pula linha "Atualizado em:"
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")  # bronze não descarta nenhuma linha
    .load(CAMINHO_EMPRESAS_EST)
)

print("✅ Empresas estrangeiras - colunas lidas do arquivo:")
for c in empresas_est.columns:
    print(f"  {c!r}")

# COMMAND ----------

# DBTITLE 1,Une as duas fontes de empresas
# Une empresas nacionais e estrangeiras
bruto_empresas = empresas_nac.unionByName(empresas_est)

print(f"\n✅ Total de empresas (nacionais + estrangeiras): {bruto_empresas.count():,}")

# COMMAND ----------

# DBTITLE 1,2. Aeródromos
# MAGIC %md
# MAGIC ## 2. Aeródromos
# MAGIC
# MAGIC Cadastro de aeroportos públicos com código ICAO, nome, município, coordenadas e informações operacionais.
# MAGIC
# MAGIC **Nota:** Este arquivo usa encoding **Latin-1** (não UTF-8).

# COMMAND ----------

# DBTITLE 1,Carrega aeródromos
# Lê arquivo de aeródromos (encoding diferente!)
bruto_aerodromos = (
    spark.read.format("csv")
    .option("header", True)
    .option("sep", ";")
    .option("skipRows", 1)  # Pula linha "Atualizado em:"
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "ISO-8859-1")  # Latin-1!
    .option("mode", "PERMISSIVE")  # bronze não descarta nenhuma linha
    .load(CAMINHO_AERODROMOS)
)

print("✅ Aeródromos - colunas lidas do arquivo:")
for c in bruto_aerodromos.columns:
    print(f"  {c!r}")

# COMMAND ----------

# DBTITLE 1,Normalização de nomes de colunas
# MAGIC %md
# MAGIC # Nomes de coluna: o Delta não aceita espaço
# MAGIC
# MAGIC `Código OACI` é um nome de coluna válido em CSV e inválido em Delta — o caractere de espaço (` `) está na lista de caracteres proibidos pela [especificação de nomenclatura de colunas do Delta Lake](https://docs.delta.io/latest/delta-batch.html#column-naming): `. ; { } ( ) \n \r \t = ,`
# MAGIC
# MAGIC Portanto, antes de materializar a tabela Delta na camada bronze, **precisamos normalizar os nomes das colunas**, substituindo espaços por underscores (`_`) e removendo caracteres inválidos. A regra adotada nesta camada bronze é simples:
# MAGIC
# MAGIC | Caractere original | Ação tomada |
# MAGIC |---|---|
# MAGIC | Espaço (` `)        | Substituir por `_` |
# MAGIC | Acentos (`á`, `ç`, …) | Remover (ex.: `Operação` → `Operacao`) |
# MAGIC | Demais proibidos      | Remover |
# MAGIC
# MAGIC > **Observação:** mesmo após a normalização, nenhum dado é alterado — apenas os **nomes das colunas** mudam. Os valores continuam como strings brutas, conforme a regra da bronze de "sem tipagem".

# COMMAND ----------

# DBTITLE 1,Função de normalização
import unicodedata

# --- Função de normalização de nome de coluna ---
# Regra bronze: minúsculo, espaços -> _, acentos removidos, demais
# caracteres proibidos pelo Delta removidos.
_PROIBIDOS = set(".;{}()\n\r\t=,")

def normalizar_nome(col: str) -> str:
    # remove acentos (á -> a, ç -> c, etc.)
    sem_acento = unicodedata.normalize("NFKD", col)
    sem_acento = "".join(
        c for c in sem_acento if not unicodedata.combining(c)
    )
    # minúsculo + espaços -> _
    nome = sem_acento.lower().replace(" ", "_")
    # remove demais caracteres proibidos pelo Delta
    nome = "".join(c for c in nome if c not in _PROIBIDOS)
    return nome

# COMMAND ----------

# DBTITLE 1,Normaliza colunas de empresas
# --- Mapeamento original -> normalizado (empresas) ---
renome_emp = {c: normalizar_nome(c) for c in bruto_empresas.columns}

print("Mapeamento de colunas (empresas):")
for original, novo in renome_emp.items():
    marca = "  ✓" if original != novo else ""
    print(f'  {original!r:30s} -> {novo!r} {marca}')

# --- Aplica o rename ---
bruto_empresas = bruto_empresas.toDF(*[renome_emp[c] for c in bruto_empresas.columns])

# COMMAND ----------

# DBTITLE 1,Normaliza colunas de aeródromos
# --- Mapeamento original -> normalizado (aeródromos) ---
renome_aero = {c: normalizar_nome(c) for c in bruto_aerodromos.columns}

print("Mapeamento de colunas (aeródromos):")
for original, novo in renome_aero.items():
    marca = "  ✓" if original != novo else ""
    print(f'  {original!r:30s} -> {novo!r} {marca}')

# --- Aplica o rename ---
bruto_aerodromos = bruto_aerodromos.toDF(*[renome_aero[c] for c in bruto_aerodromos.columns])

# COMMAND ----------

# DBTITLE 1,Adiciona colunas de auditoria - empresas
# Adiciona colunas de auditoria para empresas
# Nota: Como lemos 2 arquivos separados, adicionamos manualmente os nomes
bronze_empresas = bruto_empresas \
  .withColumn("_arquivo_origem", 
    F.when(F.col("estrangeira") == "S", F.lit("pda_empresas_aereas_estrangeiros.csv"))
     .otherwise(F.lit("pda_empresas_aereas_nacionais.csv"))
  ) \
  .withColumn("_ingerido_em", F.current_timestamp())

display(bronze_empresas.limit(5))

# COMMAND ----------

# DBTITLE 1,Adiciona colunas de auditoria - aeródromos
# Adiciona colunas de auditoria para aeródromos
bronze_aerodromos = bruto_aerodromos \
  .withColumn("_arquivo_origem", F.lit("AerodromosPublicos.csv")) \
  .withColumn("_ingerido_em", F.current_timestamp())

display(bronze_aerodromos.limit(5))

# COMMAND ----------

# DBTITLE 1,Escrita idempotente
# MAGIC %md
# MAGIC # Escrita idempotente
# MAGIC
# MAGIC ## Estratégia: Full refresh determinístico
# MAGIC
# MAGIC As tabelas bronze são materializadas com `.mode("overwrite")`, reprocessando **todos** os arquivos de referência a cada execução.
# MAGIC
# MAGIC ### Por que overwrite para tabelas de referência?
# MAGIC
# MAGIC | Critério | Full refresh (overwrite) |
# MAGIC |---|---|
# MAGIC | **Simplicidade** | ✅ Uma única operação: lê tudo, escreve tudo |
# MAGIC | **Velocidade** | ✅ Rápido para datasets de referência (< 1 MB) |
# MAGIC | **Consistência** | ✅ O estado final reflete **exatamente** os arquivos atuais |
# MAGIC | **Idempotência** | ✅ Rodar 2x produz o mesmo resultado |
# MAGIC | **Natureza dos dados** | ✅ Dados de referência mudam raramente e são pequenos |
# MAGIC
# MAGIC Tabelas de referência são **pequenas e estáticas**. Não há necessidade de append ou merge incremental - full refresh é a abordagem mais simples e eficiente.

# COMMAND ----------

# DBTITLE 1,Materializa tabela de empresas
# Materializa a tabela Delta de empresas com full refresh
bronze_empresas.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(TABELA_EMPRESAS)

print(f"✅ Tabela {TABELA_EMPRESAS} materializada com sucesso!")
print(f"   Total de registros: {spark.table(TABELA_EMPRESAS).count():,}")

# COMMAND ----------

# DBTITLE 1,Materializa tabela de aeródromos
# Materializa a tabela Delta de aeródromos com full refresh
bronze_aerodromos.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(TABELA_AERODROMOS)

print(f"✅ Tabela {TABELA_AERODROMOS} materializada com sucesso!")
print(f"   Total de registros: {spark.table(TABELA_AERODROMOS).count():,}")

# COMMAND ----------

# DBTITLE 1,Adiciona comentários nas tabelas
# Adiciona comentários descritivos nas tabelas
spark.sql(f"""
    COMMENT ON TABLE {TABELA_EMPRESAS} IS
    'Bronze - Cadastro de empresas aéreas da ANAC (nacionais e estrangeiras).
        Dado bruto: todas as colunas string, nenhuma linha descartada.
        Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/referencias/.'
""")

spark.sql(f"""
    COMMENT ON TABLE {TABELA_AERODROMOS} IS
    'Bronze - Cadastro de aeródromos (aeroportos) da ANAC.
        Dado bruto: todas as colunas string, nenhuma linha descartada.
        Carga full refresh idempotente a partir de /Volumes/voebem/bronze/arquivos/referencias/.'
""")

print("✅ Comentários adicionados nas tabelas!")

# COMMAND ----------

# DBTITLE 1,Resumo das tabelas criadas
# Resumo das tabelas criadas
print("📋 Tabelas bronze de referência:")
print()

for tabela in [TABELA_EMPRESAS, TABELA_AERODROMOS]:
    df = spark.table(tabela)
    count = df.count()
    colunas = len(df.columns)
    print(f"✅ {tabela}")
    print(f"   Registros: {count:,}")
    print(f"   Colunas: {colunas}")
    print()