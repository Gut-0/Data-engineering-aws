# Projeto-II

- [Resumo](#resumo)
- [1. Etapas do Desenvolvimento](#1-etapas-do-desenvolvimento)
    - [1.1. Ingestão de dados para S3 em modo batch](#11-ingestão-de-dados-para-s3-em-modo-batch)
    - [1.2. Ingestão de dados para S3 usando AWS Lambda](#12-ingestão-de-dados-para-s3-usando-aws-lambda)
    - [1.3. Camada Trusted](#13-camada-trusted)
        - [1.3.1 Crawler / Glue Catalog - Camada Trusted](#131-crawler--glue-catalog---camada-trusted)
    - [1.4. Camada Refined](#14-camada-refined)
        - [1.4.1. Crawler / Glue Catalog - Camada Refined](#141-crawler--glue-catalog---camada-refined)
    - [1.5. Criação de dashboard por meio do AWS QuickSight](#15-criação-de-dashboard-por-meio-do-aws-quicksight)
- [2. Análise e Dashboard](#2-análise-e-dashboard)
    - [2.1. Dashboard](#21-dashboard)
    - [2.2. O Tema Escolhido](#22-o-tema-escolhido)
    - [2.3. Perguntas Avaliadas](#23-perguntas-avaliadas)

## Resumo

A criação do Data Lake integrado aos serviços da AWS foram uma experiência enriquecedora, na qual me aprofundei
em diversos conteúdos, assistindo aos cursos na AWS Skill Builder e após, integrando os serviços necessários para o
desenvolvimento do projeto.

> Para mais informações sobre o **Dashboard final**, sugiro que veja a apresentação, localizada **Project
2/project-II/aws/quicksight** após finalizar este README.md.

## 1. Etapas do Desenvolvimento

### 1.1. Ingestão de dados para S3 em modo batch

O envio dos arquivos CSV para a camada **RAW** foi feito usando Pandas, assim, foi possível passar especificações sobre o arquivo, como: separador, credenciais, index, entre outras.

De primeiro momento iria fazer usando Spark, porém, obtive muitos problemas para rodar o Docker com uma imagem do Spark
por conta das dependências e jdk. Então remodelei o código para usar pandas.

### 1.2. Ingestão de dados para S3 usando AWS Lambda

As funções rodadas no Lambda também foram simples de desenvolver.
Os desafios enfrentados foram: Custos, Policy/Roles de acesso, versões de libs e a criação de uma layer no Lambda.

Para enviar os dados ao S3 usei a biblioteca do Boto3; Para manipular os arquivos json, a biblioteca JSON; e para fazer
requisições ao TMDB, utilizei a biblioteca Requests.

O primeiro desafio surgiu nos testes, houve a necessidade de procurar otimizar os gastos na nuvem, simulando a cloud com Localstack. O Localstack funciona em Docker e você pode simular um Bucket S3, por exemplo, para fazer testes de Read and Write.

Depois do código desenvolvido, coloquei rodar na AWS e o novo desafio de compatibilidade de versões e bibliotecas, foi
preciso criar uma layer para enviar a biblioteca de Requests em arquivo zip.

Como o meu endpoint utiliza o parâmetro de 'page', foi possível **num unico código**, trazer os arquivos necessários para o **S3**.

### 1.3. Camada Trusted

Nesse ponto, eu já estava com uma pré-modelagem do meu schema criado. Para dar continuidade, eu precisava finalizar o
schema.

Desenvolvi dois jobs no Glue, um para processar os **JSON** e outro para os **CSV**, ambos salvando os arquivos em *
*Parquet**.

Todos os arquivos foram particionados por data de envio.

#### 1.3.1 Crawler / Glue Catalog - Camada Trusted

Com os dados disponíveis na camada **Trusted**, rodei um Crawler para mapeamento e criação das tabelas no Athena.

Depois, utilizei o Athena para fazer queries SQL e verificar os dados salvos.

### 1.4. Camada Refined

Para o job da camada Refined, separei e particionei cada dimensão e tabela fato no Data Lake.

Também criei uma função para gerar **ids** de forma sequencial em cada registro das tabelas. Isso foi necessário para depois, nas queries e analises, usar **JOIN** nas dimensões.

Documentei todo o código a fim de ajudar na leitura e entendimento.

#### 1.4.1. Crawler / Glue Catalog - Camada Refined

Fiz o mapeamento dos dados na camada Refined, deixando-os prontos para uso no Athena e Quicksight.

### 1.5. Criação de dashboard por meio do AWS QuickSight

Na hora de fazer as minhas análises, tive uma certa dificuldade para entender o que realmente era necessário apresentar no Dashboard.

Voltei ao Skill Builder e fiz a seguinte aula: **"BI Dashboard Building (#BID) Workshop Session 1 of 3"**, buscando entender melhor sobre a criação de análises e aprendendo as ferramentas que o QuickSight fornece.

Depois de ver a aula, fui aos poucos desenvolvendo e aprimorando meus gráficos, levei alguns dias para finalizar.

Para mim, abstrair as análises de forma coerente e relevante, foi um processo de muito aprendizado.

## 2. Análise e Dashboard

### 2.1. Dashboard

Desenvolvi uma apresentação, que está disponível em pdf ou pptx, para explicação e entendimento das análises feitas.

Considere ver a apresentação após terminar este README, e depois, ver o grafico disponível em **PDF**, assim será
possível entender toda a análise.

[Movie Genre Analysis - Compass pb.pdf](aws%2Fquicksight%2FMovie%20Genre%20Analysis%20-%20Compass%20pb.pdf)

[Movie Genre Analysis - Compass pb.pptx](aws%2Fquicksight%2FMovie%20Genre%20Analysis%20-%20Compass%20pb.pptx)

Considere também acessar o grafico via QuickSight, somente assim terá poder total da ferramenta.

Lembre-se que os gráficos do QuickSight são dinâmicos, então é possível ter analises detalhadas sobre cada gênero.

### 2.2. O Tema Escolhido

O tema da análise é: **Avaliação dos Gêneros de Filmes**.

### 2.3. Perguntas Avaliadas:

Algumas das perguntas que o dashboard busca responder são:

* **Quais são** os gêneros mais bem avaliados?
* **Qual é** a classificação do gênero por anos?
*
    * **Quais são** as classificações médias para gêneros com uma contagem mínima de 10.000 votos?
* Se eu optar por explorar o gênero de **"Ação"**, **há um ano com mais votos do que outros**?
* **Qual foi** o período com avaliações mais baixas? E o mais alto?
* **Qual é** a relação entre as classificações médias e as quantidades de votos?