-- Tabelas externas da camada Gold.
-- Observação: se o bucket mudar, trocar o nome do bucket nos LOCATIONs abaixo.

CREATE EXTERNAL TABLE IF NOT EXISTS tc_alfabetizacao.gold_indicador_municipio (
    id_municipio int,
    serie int,
    rede string,
    taxa_alfabetizacao_resultado double,
    media_portugues double,
    meta_alfabetizacao_2024 double,
    meta_alfabetizacao_2025 double,
    meta_alfabetizacao_2026 double,
    meta_alfabetizacao_2027 double,
    meta_alfabetizacao_2028 double,
    meta_alfabetizacao_2029 double,
    meta_alfabetizacao_2030 double,
    percentual_participacao double,
    meta_ano double,
    gap_meta double,
    status_meta string
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://tc-alfabetizacao-luiz-genuino/gold/inep/alfabetizacao/indicador_municipio/';

CREATE EXTERNAL TABLE IF NOT EXISTS tc_alfabetizacao.gold_indicador_uf (
    sigla_uf string,
    serie int,
    rede string,
    taxa_alfabetizacao_resultado double,
    media_portugues double,
    meta_alfabetizacao_2024 double,
    meta_alfabetizacao_2025 double,
    meta_alfabetizacao_2026 double,
    meta_alfabetizacao_2027 double,
    meta_alfabetizacao_2028 double,
    meta_alfabetizacao_2029 double,
    meta_alfabetizacao_2030 double,
    percentual_participacao double,
    meta_ano double,
    gap_meta double,
    status_meta string
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://tc-alfabetizacao-luiz-genuino/gold/inep/alfabetizacao/indicador_uf/';

CREATE EXTERNAL TABLE IF NOT EXISTS tc_alfabetizacao.gold_indicador_brasil (
    rede string,
    taxa_alfabetizacao_resultado double,
    meta_alfabetizacao_2024 double,
    meta_alfabetizacao_2025 double,
    meta_alfabetizacao_2026 double,
    meta_alfabetizacao_2027 double,
    meta_alfabetizacao_2028 double,
    meta_alfabetizacao_2029 double,
    meta_alfabetizacao_2030 double,
    percentual_participacao double,
    meta_ano double,
    gap_meta double,
    status_meta string
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://tc-alfabetizacao-luiz-genuino/gold/inep/alfabetizacao/indicador_brasil/';

CREATE EXTERNAL TABLE IF NOT EXISTS tc_alfabetizacao.gold_aluno_municipio (
    sigla_uf string,
    id_municipio int,
    nome_municipio string,
    rede string,
    qtd_alunos bigint,
    media_proficiencia_lp double,
    taxa_alfabetizacao_microdados double,
    taxa_alfabetizacao_calculada_743 double
)
PARTITIONED BY (ano int)
STORED AS PARQUET
LOCATION 's3://tc-alfabetizacao-luiz-genuino/gold/inep/alfabetizacao/aluno_municipio/';

-- Atualiza as partições no catálogo do Athena.
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_municipio;
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_uf;
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_brasil;
MSCK REPAIR TABLE tc_alfabetizacao.gold_aluno_municipio;
