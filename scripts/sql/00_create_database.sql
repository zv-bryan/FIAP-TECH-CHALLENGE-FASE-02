-- Banco usado para consultar a camada Gold no Athena.
-- Se trocar o bucket, trocar também o LOCATION.

CREATE DATABASE IF NOT EXISTS tc_alfabetizacao
LOCATION 's3://tc-alfabetizacao-luiz-genuino/gold/inep/alfabetizacao/';
