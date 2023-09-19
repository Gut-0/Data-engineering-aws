## SQL Course Commands - Guide

> **You can use this guide to search for specific commands, challenges, or any SQL concept you need to learn and
practice.**

> **I put some comments along the activities, they are in boxes like this one.**

### Index

- [ ] Unmarked
- [x] [Basic Select Commands](#select-commands)
- [x] [Challenge 1](#challenge-1)
- [x] [Operators](#operators)
- [x] [Challenge 2](#challenge-2)
- [x] [Aggregate Functions](#aggregate-functions)
- [x] [Challenge 3](#challenge-3)
- [x] [Joins](#joins)
- [x] [Challenge 4](#challenge-4)
- [x] [Union](#union)
- [x] [Sub query](#sub-query)
- [x] [Challenge 5](#challenge-5)
- [x] [Data Treatment](#data-treatment)
- [x] [Functions](#functions)
- [x] [Tables](#tables)

<br> 

### *Select Commands*

> Select is a statement used to retrieve data from a database table.

---

````sql
select *
from sales.customers;
````

````sql
select email
from sales.customers;
````

````sql
select brand
from sales.products;
````

````sql
select distinct brand
from sales.products;
````

````sql
select distinct brand, model_year
from sales.products;
````

````sql
select email, state
from sales.customers
where state = 'SC'
   or state = 'MS';
````

````sql
select email, state, birth_date
from sales.customers
where (state = 'SC' or state = 'MS')
  and birth_date < '1991-12-28';
````

````sql
select *
from sales.products
order by price;
````

````sql
select *
from sales.products
order by price;
````

````sql
select distinct state
from sales.customers
order by state;
````

````sql
select *
from sales.products
order by price desc
limit 10;
````

---
<br>

### *Challenge 1*

> In this section, I did the first challenge of the course;

---

1. (Exercício 1)
   Selecione os nomes de cidade distintas que existem no estado de Minas Gerais em ordem alfabética (dados da tabela
   sales.customers).

````sql
select distinct city
from sales.customers
where state = 'MG'
order by city;
````

2. (Exercício 2)
   Selecione o visit_id das 10 compras mais recentes efetuadas (dados da tabela sales.funnel).

````sql
select visit_id
from sales.funnel
order by paid_date desc
limit 10;
````

3. (Exercício 3)
   Selecione todos os dados dos 10 clientes com maior score nascidos após 01/01/2000 (dados da tabela sales.customers).

````sql
select *
from sales.customers
where birth_date > '2000-01-01'
order by score desc
limit 10;
````

---

<br>

### *Operators*

> In this section, an introduction about the operators that we can use to simplify our queries;

---

````sql
select email,
       birth_date,
       (current_date - birth_date) / 365
           as "idade do cliente"
from sales.customers
order by "idade do cliente";
````

````sql
select customer_id,
       first_name,
       professional_status,
       (professional_status = 'clt')
           as client_clt
from sales.customers;
````

````sql
select *
from sales.products
where price
          between 100000 and 200000;
````

````sql
select *
from sales.products
where price
          not between 100000 and 200000;
````

````sql
select *
from sales.products
where brand
          in ('TOYOTA', 'HONDA', 'RENAULT');
````

````sql
select distinct
from sales.products
where brand
          in ('TOYOTA', 'HONDA', 'RENAULT');
````

````sql
select distinct first_name
from sales.customers
where first_name ilike 'ana%';
````

````sql
select *
from temp_tables.regions
where population is null;
````

---
<br>

### *Challenge 2*

> The second course challenge, which involves some math and conditionals;

---

1. (Exercício 1)
   Calcule quantos salários mínimos ganha cada cliente da tabela
   sales.customers. Selecione as colunas de: email, income e a coluna calculada "salários mínimos"
   Considere o salário mínimo igual à R$1200

````sql
select email,
       income,
       (income / 1200)
           as "Salários mínimos"
from sales.customers;
````

2. (Exercício 2)
   Na query anterior acrescente uma coluna informando TRUE se o cliente
   ganha acima de 5 salários mínimos e FALSE se ganha 4 salários ou menos.
   Chame a nova coluna de "acima de 4 salários"

````sql
select email,
       income,
       income / 1200 as "salários mínimos",
       case
           when income / 1200 > 5 then true
           else false
           end       as "acima de 4 salários"
from sales.customers;
````

3. (Exercício 3)
   Na query anterior filtre apenas os clientes que ganham entre
   4 e 5 salários mínimos. Utilize o comando BETWEEN

````sql
select email,
       income,
       income / 1200 as "salários mínimos",
       case
           when income / 1200 > 5 then true
           else false
           end       as "acima de 4 salários"
from sales.customers
where income / 1200 between 4 and 5;
````

4. (Exercício 4)
   Selecine o email, cidade e estado dos clientes que moram no estado de
   Minas Gerais e Mato Grosso.

````sql
select email, city, state
from sales.customers
where state in ('MG', 'MT');
````

5. (Exercício 5)
   Selecine o email, cidade e estado dos clientes que não
   moram no estado de São Paulo.

````sql
select email, city, state
from sales.customers
where state <> 'SP';
````

6. (Exercício 6)
   Selecine os nomes das cidade que começam com a letra Z.
   Dados da tabela temp_table.regions

````sql
select distinct city
from temp_tables.regions
where city ilike 'z%';
````

---
<br>

### *Aggregate Functions*

> Aggregate Functions: sum, average, count, minimum, or maximum.
> Aggregate Functions do not consider null values as zero values.

---

````sql
select count(paid_date)
from sales.funnel;
````

````sql
select count(paid_date)
from sales.funnel;
````

````sql
select count(distinct product_id)
from sales.funnel
where visit_page_date
          between '2021-01-01' and '2021-01-31';
````

````sql
select min(price), max(price), avg(price)
from sales.products;
````

````sql
select *
from sales.products
where price = (select max(price)
               from sales.products);
````

```sql
select state,
       professional_status,
       count(*) as contagem
from sales.customers
group by state, professional_status
order by professional_status, contagem desc;
````

```sql
select state, count(*)
from sales.customers
group by state
having count(*) > 100
   and state <> 'MG';
````

---

<br>

### *Challenge 3*

> The third course challenge is about Aggregate Functions, using more logic to make queries.

---

1. (Exercício 1)
   Conte quantos clientes da tabela sales.customers tem menos de 30 anos

```sql
select *
from sales.customers
where ((current_date - birth_date) / 365) < 30
order by birth_date;
```

2. (Exercício 2)
   Informe a idade do cliente mais velho e mais novo da tabela sales.customers

```sql
select max((current_date - birth_date) / 365)
           as "Cliente mais velho",
       min((current_date - birth_date) / 365)
           as "Cliente mais novo"
from sales.customers;
```

3. (Exercício 3)
   Selecione todas as informações do cliente mais rico da tabela sales.customers (possívelmente a resposta contém mais
   de um cliente)

```sql
select *
from sales.customers
where income = (select max(income)
                from sales.customers);
```

4. (Exercício 4)
   Conte quantos veículos de cada marca tem registrado na tabela sales.products Ordene o resultado pelo nome da marca

```sql
select brand, count(brand)
from sales.products
group by brand
order by brand;
```

5. (Exercício 5)
   Conte quantos veículos existem registrados na tabela sales.products por marca e ano do modelo. Ordene pela nome da
   marca e pelo ano do veículo

```sql
select brand, model_year, count(*)
from sales.products
group by brand, model_year
order by brand, model_year;
```

6. (Exercício 6)
   Conte quantos veículos de cada marca tem registrado na tabela sales.products e mostre apenas as marcas que contém
   mais de 10 veículos registrados

```sql
select brand, count(*)
from sales.products
group by brand
having count(*) > 10
order by count(*)
```

---
<br>

### *Joins*

> SQL joins are used to combine data from database tables based on a related column;

|                      Left join                       |
|:----------------------------------------------------:|
| Full left table and compatible data from right table |

|                      Right join                      |
|:----------------------------------------------------:|
| Full right table and compatible data from left table |

|                Inner join                |
|:----------------------------------------:|
| Full data when both table are compatible |

|         Full join         |
|:-------------------------:|
| Full data from both table |

---

```sql
select table1.cpf, table1.name, table2.state
from temp_tables.tabela_1 as table1
         inner join temp_tables.tabela_2 as table2
                    on table1.cpf = table2.cpf
```

```sql
select customers.professional_status,
       count(funnel.paid_date) as pagamentos
from sales.funnel as funnel
         left join sales.customers customers
                   on funnel.customer_id = customers.customer_id
group by customers.professional_status
order by pagamentos desc;
```

```sql
select ibge.gender, count(funnel.paid_date)
from sales.funnel as funnel
         left join sales.customers as customers
                   on funnel.customer_id = customers.customer_id
         left join temp_tables.ibge_genders as ibge
                   on lower(customers.first_name) = lower(ibge.first_name)
group by ibge.gender;
```

---
<br>

### *Challenge 4*

> The fourth challenge involves SQL queries using data from different tables.

---

1. (Exercício 1)
   Identifique quais as marcas de veículo mais visitada na tabela sales.funnel

```sql
select brand, count(brand) as visitas
from sales.products as products
         left join sales.funnel funnel
                   on products.product_id = funnel.product_id
group by brand
order by visitas desc;
```

2. (Exercício 2)
   Identifique quais as lojas de veículo mais visitadas na tabela sales.funnel

```sql
select stores.store_name, count(store_name) as visitas
from sales.stores as stores
         left join sales.funnel funnel
                   on stores.store_id = funnel.store_id
group by stores.store_name
order by visitas desc;
```

3. (Exercício 3)
   Identifique quantos clientes moram em cada tamanho de cidade (o porte da cidade consta na coluna "size" da tabela
   temp_tables.regions)

```sql
select customers.city, count(customers.city) as contagem
from sales.customers as customers
         left join temp_tables.regions as regions
                   on customers.state = regions.state
group by customers.city
order by contagem desc;
```

```sql
select regions.size,
       count(*) as contagem
from sales.customers as customers
         left join temp_tables.regions as regions
                   on lower(customers.city) = lower(regions.city)
                       and lower(customers.state) = lower(regions.state)
group by regions.size
order by contagem;
```

---
<br>

### *Union*

> The Union operator combines the results of two or more select queries into a single result, removing duplicates.

---

```sql
select *
from sales.products
union all
select *
from temp_tables.products_2;
```

---
<br>

### *Sub query*

> Sub queries retrieve and use data from one query to another query data filtering and operations. It needs to return a
> single value.

---

```sql
select *
from sales.products
where price = (select min(price) from sales.products);
```

```sql
with some_table as (select professional_status, (current_date - birth_date) / 365 as age
                    from sales.customers)

select professional_status, avg(age) as average_age
from some_table
group by professional_status
order by average_age desc;
```

```sql
select professional_status, avg(age) as average_age
from (select professional_status, (current_date - birth_date) / 365 as age
      from sales.customers) as some_table
group by professional_status
order by average_age desc;
```

```sql
select funnel.visit_id,
       funnel.visit_page_date,
       store_name,
       (select count(*)
        from sales.funnel as visitFunnel
        where visitFunnel.visit_page_date <= funnel.visit_page_date
          and visitFunnel.store_id = funnel.store_id)
from sales.funnel as funnel
         left join sales.stores as store
                   on funnel.store_id = store.store_id
order by store.store_name, funnel.visit_page_date;
```

```sql
with first_visit as (select customer_id, min(visit_page_date) as visit_1
                     from sales.funnel
                     group by customer_id)

select funnel.visit_page_date,
       (funnel.visit_page_date <> first_visit.visit_1) as recurring_lead,
       count(*)
from sales.funnel as funnel
         left join first_visit on funnel.customer_id = first_visit.customer_id
group by funnel.visit_page_date, recurring_lead
order by funnel.visit_page_date desc, recurring_lead;
```

```sql
with average_cost as (select brand, avg(price) as average_brand_price
                      from sales.products
                      group by brand)

select visit_id,
       visit_page_date,
       products.brand,
       (products.price * (1 + funnel.discount))                                      as final_price,
       average_cost.average_brand_price,
       ((products.price * (1 + funnel.discount)) - average_cost.average_brand_price) as price_vs_average
from sales.funnel as funnel
         left join sales.products as products
                   on funnel.product_id = products.product_id
         left join average_cost on products.brand = average_cost.brand;

```

---
<br>

### *Challenge 5*

> Challenge 5 is a simple sub query

---

1. (Exercício 1)
   Crie uma coluna calculada com o número de visitas realizadas por cada cliente da tabela sales.customers

````sql
with number_of_visits as (select customer_id, count(*) as total_visits
                          from sales.funnel
                          group by customer_id)

select customer.customer_id,
       customer.first_name,
       customer.cpf,
       customer.email,
       total_visits

from sales.customers as customer
         left join number_of_visits as visits
                   on customer.customer_id = visits.customer_id
order by total_visits desc;
````

---
<br>

### *Data Treatment*

> Data treatment refers to the process of manipulating, cleaning, or transforming data in queries.

---

|                                       Cast(value as value)                                        |
|:-------------------------------------------------------------------------------------------------:|
| CAST (::) is a function that changes the data type or expression to another data type in queries. |

````sql
select '2021-10-01'::date - '2021-02-01'::date;
````

````sql
select '100'::numeric - '10'::numeric;
````

````sql
select replace(1212111::text, '1', 'C');
````

````sql
select cast('2021-10-01' as date) - cast('2021-02-01' as date);
````

|                                         Coalesce                                         |
|:----------------------------------------------------------------------------------------:|
| Coalesce is a function that returns the first non-null value from a list of expressions. |

````sql
select *,
       coalesce(population, (select avg(population) from temp_tables.regions)) as adjusted_population
from temp_tables.regions;
````

````sql
select *,
       case
           when population is not null then population
           else (select avg(population) from temp_tables.regions)
           end as adjusted_population
from temp_tables.regions
where population is null;
````

````sql
with get_income_bracket as (select income,
                                   case
                                       when income < 5000 then '0-5000'
                                       when income >= 5000 and income < 10000 then '5000-10000'
                                       when income >= 10000 and income < 15000 then '10000-15000'
                                       else '15000+'
                                       end as income_bracket
                            from sales.customers)

select income_bracket, count(*)
from get_income_bracket
group by income_bracket;
````

| Text Treatment |
|:--------------:|

````sql
select upper('São Paulo') = 'SÃO PAULO';

select lower('São Paulo') = 'são paulo';

select trim('São Paulo           ') = 'São Paulo';

select replace('SAO PAULO', 'SAO', 'SÃO') = 'SÃO PAULO';
````

| Date Treatment |
|:--------------:|

````sql
select current_date + 10;

select (current_date + interval '10 weeks')::date;

select (current_date + interval '10 months')::date;

select current_date + interval '10 hours';
````

````sql
select date_trunc('month', visit_page_date)::date as visits_per_month,
       count(*)
from sales.funnel
group by visits_per_month
order by visits_per_month desc;
````

````sql
select extract('dow' from visit_page_date) as day_of_week,
       count(*)
from sales.funnel
group by day_of_week
order by day_of_week
````

---
<br>

### *Functions*

> SQL functions are predefined operations that perform specific tasks.

---

````sql
create function datediff(unit varchar, init_date date, end_date date)
    returns integer
    language sql
as

$$
select case
           when unit in ('d', 'day', 'days') then (end_date - init_date)
           when unit in ('w', 'week', 'weeks') then (end_date - init_date) / 7
           when unit in ('m', 'month', 'months') then (end_date - init_date) / 30
           when unit in ('y', 'year', 'years') then (end_date - init_date) / 365
           end as time
$$;
````

````sql
select datediff('m', '2022-09-04', current_date);
````

````sql
drop function datediff
````

---
<br>

### *Tables*

> SQL tables are database structures that store data in rows and columns, organizing information into a structured
> format.

---

````sql
select customer_id,
       datediff('years', birth_date, current_date) as age
into temp_tables.customers_age
from sales.customers;
````

````sql
create table temp_tables.professions
(
    professional_status varchar,
    status_profissional varchar
);
````

````sql
insert into temp_tables.professions (professional_status, status_profissional)
values ('freelancer', 'freelancer'),
       ('retired', 'aposentado(a)'),
       ('clt', 'clt'),
       ('self_employer', 'autonomo(a)'),
       ('other', 'outro'),
       ('businessman', 'empresario(a)'),
       ('civil_servant', 'funcionario publico(a)'),
       ('student', 'estudante');
````

````sql
drop table temp_tables.professions;
````

````sql
insert into temp_tables.professions(professional_status, status_profissional)
values ('unemployed', 'desempregado(a)'),
       ('trainee', 'estagiario(a)');
````

````sql
update temp_tables.professions
set professional_status = 'intern'
where professional_status = 'estagiario(a)';
````

````sql
delete
from temp_tables.professions
where status_profissional = 'desempregado(a)'
   or status_profissional = 'estagiario(a)';
````

````sql
alter table sales.customers
    add customer_age int;
````

````sql
alter table sales.customers
    drop column customer_age;
````

````sql
update sales.customers
set customer_age = datediff('years', birth_date, current_date)
where true;
````

````sql
alter table sales.customers
    alter column customer_age type varchar;
````

````sql
alter table sales.customers
    rename column customer_age to age;
````

````sql
alter table sales.customers
    drop column age;
````

---
