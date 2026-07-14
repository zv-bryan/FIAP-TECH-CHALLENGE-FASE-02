# Job da camada Gold
#
# A Gold é a camada que eu deixei pronta para análise.
# Não tentei criar muitas tabelas diferentes, só as principais para responder o desafio:
# município, UF, Brasil e uma visão agregada dos microdados.

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct, lit, round as spark_round, when

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

spark = (
    SparkSession.builder
    .appName(args["JOB_NAME"])
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

silver_path = f"s3://{bucket}/silver/inep/alfabetizacao"
gold_path = f"s3://{bucket}/gold/inep/alfabetizacao"


def ler_silver(entity):
    return spark.read.parquet(f"{silver_path}/{entity}")


def adicionar_status_meta(df):
    """Cria a meta do ano, o gap e o status.

    Fiz com when mesmo para ficar explícito qual coluna de meta é usada em cada ano.
    Para 2023, a meta pode ficar nula porque as metas oficiais começam mais para frente.
    """
    df = df.withColumn(
        "meta_ano",
        when(col("ano") == 2024, col("meta_alfabetizacao_2024"))
        .when(col("ano") == 2025, col("meta_alfabetizacao_2025"))
        .when(col("ano") == 2026, col("meta_alfabetizacao_2026"))
        .when(col("ano") == 2027, col("meta_alfabetizacao_2027"))
        .when(col("ano") == 2028, col("meta_alfabetizacao_2028"))
        .when(col("ano") == 2029, col("meta_alfabetizacao_2029"))
        .when(col("ano") == 2030, col("meta_alfabetizacao_2030"))
    )

    df = df.withColumn(
        "gap_meta",
        spark_round(col("taxa_alfabetizacao_resultado") - col("meta_ano"), 2)
    )

    df = df.withColumn(
        "status_meta",
        when(col("meta_ano").isNull(), lit("SEM_META"))
        .when(col("gap_meta") >= 0, lit("ATINGIU_META"))
        .otherwise(lit("ABAIXO_META"))
    )

    return df


def salvar_gold(df, name, partitions):
    (
        df.write
        .mode("overwrite")
        .format("parquet")
        .option("compression", "snappy")
        .partitionBy(*partitions)
        .save(f"{gold_path}/{name}")
    )


aluno = ler_silver("aluno")
municipio = ler_silver("municipio")
meta_municipio = ler_silver("meta_municipio")
uf = ler_silver("uf")
meta_uf = ler_silver("meta_uf")
meta_brasil = ler_silver("meta_brasil")

# 1) Visão pelos microdados de alunos.
# Aqui eu recalculo a taxa usando a flag oficial e também usando o corte 743.
gold_aluno_municipio = (
    aluno
    .groupBy("ano", "sigla_uf", "id_municipio", "nome_municipio", "rede_descricao")
    .agg(
        countDistinct("id_aluno").alias("qtd_alunos"),
        spark_round(avg("proficiencia_lp"), 4).alias("media_proficiencia_lp"),
        spark_round(avg("in_alfabetizado") * 100, 2).alias("taxa_alfabetizacao_microdados"),
        spark_round(avg("fl_alfabetizado_calculado") * 100, 2).alias("taxa_alfabetizacao_calculada_743"),
    )
    .withColumnRenamed("rede_descricao", "rede")
)

# 2) Resultado oficial por município x metas.
# O detalhe mais importante é o join por rede_descricao, porque as fontes não usam o mesmo formato.
gold_indicador_municipio = (
    municipio.alias("m")
    .join(
        meta_municipio.alias("mm"),
        on=[
            col("m.ano") == col("mm.ano"),
            col("m.id_municipio") == col("mm.id_municipio"),
            col("m.rede_descricao") == col("mm.rede_descricao"),
        ],
        how="left",
    )
    .select(
        col("m.ano"),
        col("m.id_municipio"),
        col("m.serie"),
        col("m.rede_descricao").alias("rede"),
        col("m.taxa_alfabetizacao").alias("taxa_alfabetizacao_resultado"),
        col("m.media_portugues"),
        col("mm.meta_alfabetizacao_2024"),
        col("mm.meta_alfabetizacao_2025"),
        col("mm.meta_alfabetizacao_2026"),
        col("mm.meta_alfabetizacao_2027"),
        col("mm.meta_alfabetizacao_2028"),
        col("mm.meta_alfabetizacao_2029"),
        col("mm.meta_alfabetizacao_2030"),
        col("mm.percentual_participacao"),
    )
)
gold_indicador_municipio = adicionar_status_meta(gold_indicador_municipio)

# 3) Resultado oficial por UF x metas.
gold_indicador_uf = (
    uf.alias("u")
    .join(
        meta_uf.alias("mu"),
        on=[
            col("u.ano") == col("mu.ano"),
            col("u.sigla_uf") == col("mu.sigla_uf"),
            col("u.rede_descricao") == col("mu.rede_descricao"),
        ],
        how="left",
    )
    .select(
        col("u.ano"),
        col("u.sigla_uf"),
        col("u.serie"),
        col("u.rede_descricao").alias("rede"),
        col("u.taxa_alfabetizacao").alias("taxa_alfabetizacao_resultado"),
        col("u.media_portugues"),
        col("mu.meta_alfabetizacao_2024"),
        col("mu.meta_alfabetizacao_2025"),
        col("mu.meta_alfabetizacao_2026"),
        col("mu.meta_alfabetizacao_2027"),
        col("mu.meta_alfabetizacao_2028"),
        col("mu.meta_alfabetizacao_2029"),
        col("mu.meta_alfabetizacao_2030"),
        col("mu.percentual_participacao"),
    )
)
gold_indicador_uf = adicionar_status_meta(gold_indicador_uf)

# 4) Resultado nacional.
# A base Brasil já vem agregada, então aqui não tem join.
gold_brasil = meta_brasil.select(
    "ano",
    col("rede_descricao").alias("rede"),
    col("taxa_alfabetizacao").alias("taxa_alfabetizacao_resultado"),
    "meta_alfabetizacao_2024",
    "meta_alfabetizacao_2025",
    "meta_alfabetizacao_2026",
    "meta_alfabetizacao_2027",
    "meta_alfabetizacao_2028",
    "meta_alfabetizacao_2029",
    "meta_alfabetizacao_2030",
    "percentual_participacao",
)
gold_brasil = adicionar_status_meta(gold_brasil)

salvar_gold(gold_aluno_municipio, "aluno_municipio", ["ano"])
salvar_gold(gold_indicador_municipio, "indicador_municipio", ["ano"])
salvar_gold(gold_indicador_uf, "indicador_uf", ["ano"])
salvar_gold(gold_brasil, "indicador_brasil", ["ano"])

print("Gold finalizada com sucesso.")
