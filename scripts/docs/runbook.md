# Runbook de execução

Este arquivo é o passo a passo que eu usaria para rodar o projeto. Ele não é um script automático, é só o guia de execução.

> Observação: estou considerando que o terminal foi aberto dentro da pasta `SCRIPTS`.
> Se você estiver na pasta onde existem `DADOS` e `SCRIPTS`, rode primeiro:
>
> ```powershell
> cd SCRIPTS
> ```

---

## 1. Criar a infraestrutura básica

Na pasta do Terraform:

```bash
cd terraform
terraform init
terraform apply -var="bucket_name=tc-alfabetizacao-luiz-genuino"
```

Depois volte para a pasta `SCRIPTS`:

```bash
cd ..
```

O Terraform aqui cria a base do projeto, como bucket S3, stream Kinesis e role básica do Glue. Os jobs do Glue e as Lambdas ainda podem ser cadastrados manualmente no console.

---

## 2. Enviar os CSVs para a Bronze

No Windows PowerShell:

```powershell
$env:DATALAKE_BUCKET="tc-alfabetizacao-luiz-genuino"
$env:INPUT_DIR="C:\Users\Gui\OneDrive\Anexos\Desktop\Fase2-TechChallenge\DADOS"
python scripts/upload_to_bronze.py
```

No Git Bash ou Linux:

```bash
export DATALAKE_BUCKET=tc-alfabetizacao-luiz-genuino
export INPUT_DIR="/c/Users/Gui/OneDrive/Anexos/Desktop/Fase2-TechChallenge/DADOS"
python scripts/upload_to_bronze.py
```

Arquivos esperados dentro da pasta `DADOS`:

```text
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv
br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv
br_inep_avaliacao_alfabetizacao_uf.csv
br_inep_avaliacao_alfabetizacao_municipio.csv
TS_ALUNO.csv
```

---

## 3. Rodar o Glue da Silver

No Glue Job, passar o argumento:

```text
--BUCKET=tc-alfabetizacao-luiz-genuino
```

Pela AWS CLI:

```bash
aws glue start-job-run \
  --job-name tc-silver-transform \
  --arguments '--BUCKET=tc-alfabetizacao-luiz-genuino'
```

Esse job lê a Bronze e grava a Silver em Parquet.

---

## 4. Rodar a qualidade dos dados

```bash
aws glue start-job-run \
  --job-name tc-data-quality \
  --arguments '--BUCKET=tc-alfabetizacao-luiz-genuino'
```

Esse passo verifica se tem algo muito errado antes de criar a Gold.

---

## 5. Rodar o Glue da Gold

```bash
aws glue start-job-run \
  --job-name tc-gold-transform \
  --arguments '--BUCKET=tc-alfabetizacao-luiz-genuino'
```

Esse job cria as tabelas analíticas finais.

---

## 6. Criar tabelas no Athena

Execute os arquivos nesta ordem:

```text
sql/athena/00_create_database.sql
sql/athena/01_create_external_tables_gold.sql
sql/athena/02_analytical_queries.sql
```

Depois de gerar novas partições, rode ou mantenha os comandos `MSCK REPAIR TABLE` do arquivo de criação das tabelas.

---

## Problemas que eu já mapeei

### Bucket diferente

Se trocar o nome do bucket, precisa trocar em:

```text
scripts/config.py
sql/athena/*.sql
terraform/variables.tf ou comando terraform apply
parâmetro --BUCKET dos Glue Jobs
```

### TS_ALUNO carregando errado

O `TS_ALUNO.csv` usa `;`, diferente dos outros CSVs. O tratamento está na função `read_csv` do `silver_transform.py`.

### Meta vindo nula no join

Isso normalmente acontece por causa da coluna `rede`. Nas tabelas de resultado ela vem como número e nas metas vem como texto. Por isso criei `rede_descricao` na Silver.

### Athena não mostra dados

Pode ser partição não reconhecida. Rode:

```sql
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_municipio;
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_uf;
MSCK REPAIR TABLE tc_alfabetizacao.gold_indicador_brasil;
MSCK REPAIR TABLE tc_alfabetizacao.gold_aluno_municipio;
```
