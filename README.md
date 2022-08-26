# Datapipeline com airflow e spark

## Definir variáveis de ambiente
 - `export AIRFLOW_HOME=$(pwd)/airflow`
 - `export AIRFLOW_VERSION=1.10.14`
 - `export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"`
 - `export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"`
 - Definir variável `BEARER_TOKEN` do twitter.
 - Também é possível ter um arquivo `.env` e usar o comando: `set -a && source .env && set +a && exec $@`

## Instalação
 - `pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"`
 - `airflow initdb`

## Iniciando (obs: É necessário definir variáveis de ambiente antes)
 - `airflow webserver`
 - `airflow scheduler`
 - Definir conexão `twitter_default` como `http` para o link `https://api.twitter.com` com Authorization setando o Bearer token.
 - acessar `localhost:8080`
 

