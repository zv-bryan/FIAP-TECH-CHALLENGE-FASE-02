# Job da camada Silver
#
# Minha ideia aqui foi não transformar demais o dado ainda.
# A Bronze fica como cópia bruta e a Silver recebe os tratamentos mínimos:
# tipo de dado, nomes de coluna, separador do arquivo de aluno e padronização da rede.

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, when

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

spark = (
    SparkSession.builder
    .appName(args["JOB_NAME"])
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

bronze_path = f"s3://{bucket}/bronze/inep/alfabetizacao"
silver_path = f"s3://{bucket}/silver/inep/alfabetizacao"


def read_csv(entity):
    """Lê uma entidade da Bronze.

    Ponto de atenção que eu encontrei:
    - os CSVs da Base dos Dados usam vírgula;
    - o TS_ALUNO usa ponto e vírgula.

    Por isso deixei essa regra separada, para não carregar o arquivo de aluno como
    uma única coluna gigante.
    """
    reader = spark.read.option("header", True).option("inferSchema", True)

    if entity == "aluno":
        reader = reader.option("sep", ";").option("encoding", "ISO-8859-1")
    else:
        reader = reader.option("sep", ",").option("encoding", "UTF-8")

    return reader.csv(f"{bronze_path}/{entity}/")


def cast_percentuais(df):
    # Essas colunas representam percentuais/indicadores. Deixei como double para evitar
    # problema de comparação e cálculo depois.
    for nome_coluna in df.columns:
        if (
            nome_coluna.startswith("taxa_")
            or nome_coluna.startswith("meta_")
            or nome_coluna.startswith("percentual_")
            or nome_coluna.startswith("proporcao_")
        ):
            df = df.withColumn(nome_coluna, col(nome_coluna).cast("double"))

    return df


def padronizar_tipos_basicos(df):
    # Não coloquei todas as colunas possíveis aqui, só as chaves que uso nos joins e análises.
    if "ano" in df.columns:
        df = df.withColumn("ano", col("ano").cast("int"))

    if "id_municipio" in df.columns:
        df = df.withColumn("id_municipio", col("id_municipio").cast("int"))

    if "serie" in df.columns:
        df = df.withColumn("serie", col("serie").cast("int"))

    if "sigla_uf" in df.columns:
        df = df.withColumn("sigla_uf", trim(col("sigla_uf").cast("string")))

    return df


def padronizar_rede(df):
    """Cria uma versão textual da rede de ensino.

    Tive que fazer isso porque uma base vem com rede = 3/5 e outra vem como
    Municipal/Pública. Sem essa coluna, o join da Gold não encontra as metas.
    """
    if "rede" not in df.columns:
        return df

    df = df.withColumn("rede", trim(col("rede").cast("string")))

    return (
        df
        .withColumn("rede_codigo", col("rede").cast("int"))
        .withColumn(
            "rede_descricao",
            when(col("rede_codigo") == 0, lit("Total"))
            .when(col("rede_codigo") == 1, lit("Federal"))
            .when(col("rede_codigo") == 2, lit("Estadual"))
            .when(col("rede_codigo") == 3, lit("Municipal"))
            .when(col("rede_codigo") == 4, lit("Privada"))
            .when(col("rede_codigo") == 5, lit("Pública"))
            .otherwise(col("rede"))
        )
    )


def tratar_base_agregada(entity):
    # Fiz uma função simples para reaproveitar o mesmo tratamento nas bases agregadas.
    df = read_csv(entity)
    df = cast_percentuais(df)
    df = padronizar_tipos_basicos(df)
    df = padronizar_rede(df)
    return df


def salvar_silver(df, entity, partition_cols):
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy(*partition_cols)
        .save(f"{silver_path}/{entity}")
    )


# Bases agregadas da Base dos Dados
meta_brasil = tratar_base_agregada("meta_brasil")
meta_uf = tratar_base_agregada("meta_uf")
meta_municipio = tratar_base_agregada("meta_municipio")
uf = tratar_base_agregada("uf")
municipio = tratar_base_agregada("municipio")

# Microdados de aluno
# Aqui eu renomeei só as colunas que uso na Gold e nas validações.
aluno = (
    read_csv("aluno")
    .withColumnRenamed("NU_ANO_AVALIACAO", "ano")
    .withColumnRenamed("CO_UF", "id_uf")
    .withColumnRenamed("SG_UF", "sigla_uf")
    .withColumnRenamed("ID_ALUNO", "id_aluno")
    .withColumnRenamed("TP_SERIE", "serie")
    .withColumnRenamed("ID_ESCOLA", "id_escola")
    .withColumnRenamed("TP_DEPENDENCIA", "rede")
    .withColumnRenamed("CO_MUNICIPIO", "id_municipio")
    .withColumnRenamed("NO_MUNICIPIO", "nome_municipio")
    .withColumnRenamed("VL_PESO_ALUNO_LP", "peso_aluno_lp")
    .withColumnRenamed("VL_PROFICIENCIA_LP", "proficiencia_lp")
    .withColumnRenamed("IN_ALFABETIZADO", "in_alfabetizado")
)

aluno = (
    aluno
    .withColumn("ano", col("ano").cast("int"))
    .withColumn("id_uf", col("id_uf").cast("int"))
    .withColumn("sigla_uf", trim(col("sigla_uf").cast("string")))
    .withColumn("id_municipio", col("id_municipio").cast("int"))
    .withColumn("id_aluno", col("id_aluno").cast("string"))
    .withColumn("id_escola", col("id_escola").cast("int"))
    .withColumn("serie", col("serie").cast("int"))
    .withColumn("peso_aluno_lp", col("peso_aluno_lp").cast("double"))
    .withColumn("proficiencia_lp", col("proficiencia_lp").cast("double"))
    .withColumn("in_alfabetizado", col("in_alfabetizado").cast("int"))
    .withColumn(
        "fl_alfabetizado_calculado",
        when(col("proficiencia_lp") >= 743, lit(1)).otherwise(lit(0))
    )
)

aluno = padronizar_rede(aluno)

# Escrita final da Silver.
# Usei partição por ano porque as análises quase sempre filtram período.
salvar_silver(meta_brasil, "meta_brasil", ["ano"])
salvar_silver(meta_uf, "meta_uf", ["ano"])
salvar_silver(meta_municipio, "meta_municipio", ["ano"])
salvar_silver(uf, "uf", ["ano"])
salvar_silver(municipio, "municipio", ["ano"])
salvar_silver(aluno, "aluno", ["ano", "sigla_uf"])

print("Silver finalizada com sucesso.")
