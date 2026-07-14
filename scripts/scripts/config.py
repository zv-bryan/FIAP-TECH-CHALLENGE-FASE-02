import os

# Centralizei as configurações aqui para não ficar repetindo nome de bucket
# em vários lugares do código.
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
BUCKET = os.getenv("DATALAKE_BUCKET", "tc-alfabetizacao-luiz-genuino")

BRONZE_PREFIX = "bronze/inep/alfabetizacao"
SILVER_PREFIX = "silver/inep/alfabetizacao"
GOLD_PREFIX = "gold/inep/alfabetizacao"

# Esses nomes precisam bater exatamente com os arquivos dentro da pasta DADOS.
# Deixei sem o sufixo "(1)" porque no ZIP final os arquivos estão com nome limpo.
LOCAL_FILES = {
    "meta_brasil": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv",
    "meta_uf": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv",
    "meta_municipio": "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv",
    "uf": "br_inep_avaliacao_alfabetizacao_uf.csv",
    "municipio": "br_inep_avaliacao_alfabetizacao_municipio.csv",
    "aluno": "TS_ALUNO.csv",
}
