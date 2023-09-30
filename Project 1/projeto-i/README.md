# Projeto-i

- [Resumo](#resumo)
- [1. Versões de Criação do Código](#1-versões-de-criação-do-código)
    - [1.1. Primeira Versão](#11-primeira-versão)
    - [1.2. Segunda Versão](#12-segunda-versão)
- [2. Desafios Econtrados no Projeto](#2-desafios-econtrados-no-projeto)
    - [2.1 Desafios com CSVs](#21-desafios-com-csv) 
    - [2.2 Desafios no Desenvolvimento](#22-desafios-no-desenvolvimento)
    - [2.3 Modelagem Dimensional](#23-modelagem-do-banco)

## Resumo

Finalizei o Projeto I, levei o mês inteiro para terminar. No começo estava meio perdido, sem ter uma imagem clara na
cabeça do que estava sendo feito, porém, após fazer o projeto ficou mais fácil de entender. Também sinto que foi um
pouco complicado entender e desenvolver o meu modelo dimensional.
Após todo processo terminado, eu refatorei o meu código, deixando muito mais performático e legível.

> Para mais informações sobre o arquivo **'etl.py''**, sugiro que leia **'ETL.md'** após finalizar este README.md.

## 1. Versões de Criação do Código

### 1.1. Primeira versão
Na primeira versão do meu ETL, estava muito pouco claro como o processo inteiro deveria ocorrer. Nunca havia trabalhado com Python, nem Docker. Por outro lado, tenho conhecimento em programação / lógica.

Eu estava usando o pandas para manipular os **CSVs**, porém para inserir dados no Postgres, iterava sobre cada linha do **CSV** e usava um '..**INSERT**..' manual. Resultado: O meu ETL demorou cerca de **30 minutos** para rodar completamente. Consegui fazer as cargas de inserção, mas desempenho **0**.

### 1.2. Segunda Versão
> Depois de um dia inteiro esperando **30 minutos** para cada teste do meu ETL, percebi que provávelmente aquilo não estava certo. Conversei com o meu irmão sobre essa questão de desempenho do código e ele me passou algumas dicas:

* Falou sobre **Prints** no código, os quais ocupam processamento (**I / O**);
* Otimizar memória **reutilizando** DataFrame;
* Também me falou sobre o uso de **Joins** para substituir **Selects**;

Então resolvi refatorar o meu ETL, e comecei a usar o método '**to_sql**', do Pandas para inserir dados no **PostgreSQL**.
Precisei modificar praticamente toda a estrutura do código, pois todas as funções de inserção utilizavam o laço '**for**' e fazia um loop sobre **cada linha** do Dataframe.

> Sugiro que olhe o arquivo **'python_etl/source/etl.py'**, para ver o código final.

O código foi pensado atentamente para utilizar:

* **Tipagem de Dados e Funções**, todas as funções implementadas tem suas respectivas descrições, contendo tipo de retorno, tipo de parâmetros e explicando a função.
* **Tipagem de DataFrames**, eu utilizei **pd.read_csv(/, dtype)** para explicitar ao Pandas qual tipo de dados contém em cada CSV.
* **Try / Except**, o código utiliza uma estrutura try / except, se houver um erro durante algum processo, você será indicado.

> Também recomendo que veja o arquivo **'ETL.md'**, para entender melhor sobre como os dados estão sendo extraídos do **mongodb**, como os metódos de inserção foram criados, e outras infornações sobre o **'etl.py'** estão diponíveis no **'ETL.md**.

Comecei a usar metódos do Pandas para tratar os dados, ao invés de sql e/ou tratamento manual.  Metódos como:

* df.drop
* df.rename
* df.map 
* df.unique
* pd.isna

Resultado: O ETL agorá está rodando em poucos minutos (Se você não tiver uma imagem postgres ou mondodb baixada). Porém se já existir uma imagem postgres e mongodb, o ETL irá rodar em 1 minuto ou menos.

## 2. Desafios Econtrados no Projeto

### 2.1 Desafios com CSV:

Os problemas / desafios trabalhando com CSVs foram:

* 'product_name_**lenght**' - (**length**);
* 'product_description_**lenght**' - (**length**);
* dados nulos;
* dados não utilizados na modelagem (**seller_id**);
* timestamp;

### 2.2 Desafios no Desenvolvimento:

Os problemas e desafios encontrados durante o desenvolvimento do projeto foram:

Criação do Dockerfile e docker-compose.yml: 

* Estrutura de pastas;
* Bibliotecas necessárias;
* Variavéis de ambiente;

Estrutura do Projeto:

> Tentei abstrair partes que não faziam parte do processo de ETL (Extract Transform Load) para seus próprios arquivos:

* **postgres-init/init.sql**: Criação das tabelas no banco PostgreSQL;
* **python_etl/source/postgres_db.py**: Criação da conexão com o banco PostgreSQL, utilizando **sqlalchemy**;
* **python_etl/source/config/settings.toml**: Configurações de conexão com os bancos de dados, utilizando **Dynaconf**;

> Também abstrai as variáveis de ambiente do postgres, usadas no docker-compose.yml, e as bibliotecas utilizadas no projeto para o arquivo requirements.txt, referenciado no Dockerfile.

* **environments/postgres.env**: Variáveis de ambiente postgres;
* **python_etl/requirements.txt**: Bibliotecas utilizadas no desenvolvimento.

### 2.3 Modelagem do Banco:

Foi realmente um desafio para criar a modelagem final, fiz mudanças durante o desenvolvimento para adicionar ou remover dimensões, modificar o tipo e/ou nome das colunas.

> Sugiro que olhe a modelagem a partir do Diagrams (https://app.diagrams.net/), para entender melhor sobre as tabelas dimensões, como os dados se comunicam e como foi pensado a estrutura do Data Warehouse

> O arquivo está em: database_model/fast_pb.drawio

Dimensões e Fato:

* dim_reviews
* dim_payment_types
* dim_dates
* dim_customers
* dim_products
* dim_states
* dim_product_categories
* fact_sales
