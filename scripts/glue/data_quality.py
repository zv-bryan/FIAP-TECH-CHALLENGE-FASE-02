# Job simples de qualidade de dados
#
# Eu deixei validações mais diretas, porque a ideia é mostrar controle de qualidade
# sem transformar isso em um framework muito grande.

import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])
bucket = args["BUCKET"]

spark = SparkSession.builder.appName(args["JOB_NAME"]).getOrCreate()
silver_path = f"s3://{bucket}/silver/inep/alfabetizacao"


def ler_silver(entity):
    return spark.read.parquet(f"{silver_path}/{entity}")


def reprovar_se(condicao, mensagem):
    if condicao:
        raise ValueError(mensagem)


aluno = ler_silver("aluno")
municipio = ler_silver("municipio")
meta_municipio = ler_silver("meta_municipio")

# 1) Duplicidade de aluno.
# Estou usando ano + id_aluno, porque o mesmo identificador poderia aparecer em anos diferentes.
dup_aluno = (
    aluno
    .groupBy("ano", "id_aluno")
    .agg(count("*").alias("qtd"))
    .filter(col("qtd") > 1)
    .count()
)
reprovar_se(dup_aluno > 0, f"Encontrei duplicidade no aluno: {dup_aluno}")

# 2) Percentuais fora de 0 a 100.
# Nulos são aceitos, porque algumas metas ou resultados não existem para todos os anos.
for nome_df, df in [("municipio", municipio), ("meta_municipio", meta_municipio)]:
    for nome_coluna in df.columns:
        if (
            nome_coluna.startswith("taxa_")
            or nome_coluna.startswith("meta_")
            or nome_coluna.startswith("percentual_")
        ):
            invalidos = df.filter((col(nome_coluna) < 0) | (col(nome_coluna) > 100)).count()
            reprovar_se(
                invalidos > 0,
                f"Valores fora de 0-100 em {nome_df}.{nome_coluna}: {invalidos}",
            )

# 3) Proficiência negativa não faz sentido.
prof_negativa = aluno.filter(col("proficiencia_lp") < 0).count()
reprovar_se(prof_negativa > 0, f"Proficiência negativa: {prof_negativa}")

# 4) Conferência do corte 743.
# Se aparecer alguma diferença pequena, eu aviso. Se for muita coisa, eu quebro o job.
# Fiz assim porque microdados oficiais às vezes podem ter regra de presença/preenchimento junto.
inconsistente = aluno.filter(
    col("proficiencia_lp").isNotNull()
    & col("in_alfabetizado").isNotNull()
    & (
        ((col("proficiencia_lp") >= 743) & (col("in_alfabetizado") != 1))
        | ((col("proficiencia_lp") < 743) & (col("in_alfabetizado") != 0))
    )
).count()

total_alunos = aluno.count()
pct_inconsistente = (inconsistente / total_alunos * 100) if total_alunos > 0 else 0

if inconsistente > 0:
    print(
        f"Aviso: {inconsistente} alunos tiveram diferença entre proficiência e flag oficial "
        f"({pct_inconsistente:.4f}%)."
    )

reprovar_se(
    pct_inconsistente > 1,
    f"Mais de 1% dos alunos estão inconsistentes com o corte 743: {pct_inconsistente:.2f}%",
)

# 5) Município do aluno precisa existir na base agregada em algum momento.
# Não validei por ano porque o TS_ALUNO pode ser 2025 e o agregado pode ter 2023/2024.
municipios_ref = municipio.select("id_municipio").distinct()
municipios_aluno = aluno.select("id_municipio").where(col("id_municipio").isNotNull()).distinct()

sem_ref = municipios_aluno.join(municipios_ref, on=["id_municipio"], how="left_anti").count()
total_municipios_aluno = municipios_aluno.count()
pct_sem_ref = (sem_ref / total_municipios_aluno * 100) if total_municipios_aluno > 0 else 0

reprovar_se(total_municipios_aluno == 0, "Nenhum município encontrado na base de alunos.")

if sem_ref > 0:
    print(
        f"Aviso: {sem_ref} municípios dos alunos não apareceram na base agregada "
        f"({pct_sem_ref:.2f}%)."
    )

reprovar_se(
    pct_sem_ref > 1,
    f"Mais de 1% dos municípios dos alunos estão sem referência: {pct_sem_ref:.2f}%",
)

# 6) Se a rede_descricao ficar nula, o join com meta pode quebrar depois.
rede_sem_descricao = aluno.filter(col("rede_descricao").isNull()).count()
reprovar_se(rede_sem_descricao > 0, f"Registros sem rede_descricao: {rede_sem_descricao}")

print("Validação de qualidade concluída.")
