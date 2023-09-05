### Sales Dashboard

> **This is the first project of the course, which involves create a sales dashboard about all database we have explored
in the course**

- [x] [(Result)](#result)

---

1. (Query 1)
   Receita, leads, conversão e ticket médio mês a mês
   <br>
   Colunas: mês, leads (#), vendas (#), receita (k, R$), conversão (%), ticket médio (k, R$)

````sql
with leads as (select date_trunc('month', visit_page_date)::date as visit_page_month, count(*) as visit_page_count
               from sales.funnel
               group by visit_page_month
               order by visit_page_month),

     payments as (select date_trunc('month', fun.paid_date)::date as paid_month,
                         count(fun.paid_date)                     as paid_count,
                         sum(pro.price * (1 + fun.discount))      as receita
                  from sales.funnel as fun
                           left join sales.products as pro
                                     on fun.product_id = pro.product_id
                  where fun.paid_date is not null
                  group by paid_month
                  order by paid_month)

select leads.visit_page_month                                       as "mês",
       leads.visit_page_count                                       as "leads (#)",
       payments.paid_count                                          as "vendas (#)",
       (payments.receita / 1000)                                    as "receita (k, R$)",
       (payments.paid_count::float / leads.visit_page_count::float) as "conversão (%)",
       (payments.receita / payments.paid_count / 1000)              as "ticket médio (k, R$)"
from leads
         left join payments
                   on leads.visit_page_month = paid_month;
````

2. (Query 2) Estados que mais venderam
   <br>
   Colunas: país, estado, vendas (#)

````sql
select 'Brazil'             as país,
       cus.state            as estado,
       count(fun.paid_date) as "vendas (#)"

from sales.funnel as fun
         left join sales.customers as cus
                   on fun.customer_id = cus.customer_id
where paid_date between '2021-08-01' and '2021-08-31'
group by país, estado
order by "vendas (#)" desc
limit 5;
````

3. (Query 3)
   Marcas que mais venderam no mês
   <br>
   Colunas: marca, vendas (#)

````sql
select pro.brand            as marca,
       count(fun.paid_date) as "vendas (#)"

from sales.funnel as fun
         left join sales.products as pro
                   on fun.product_id = pro.product_id
where paid_date between '2021-08-01' and '2021-08-31'
group by marca
order by "vendas (#)" desc
limit 5;
````

4. (Query 4)
   Lojas que mais venderam
   <br>
   Colunas: loja, vendas (#)

````sql
select sto.store_name       as loja,
       count(fun.paid_date) as "vendas (#)"

from sales.funnel as fun
         left join sales.stores as sto
                   on fun.store_id = sto.store_id
where paid_date between '2021-08-01' and '2021-08-31'
group by loja
order by "vendas (#)" desc
limit 5;
````

5. (Query 5)
   Dias da semana com maior número de visitas ao site
   <br>
   Colunas: dia_semana, dia da semana, visitas (#)

````sql
select extract('dow' from visit_page_date) as dia_semana,
       case
           when extract('dow' from visit_page_date) = 0 then 'domingo'
           when extract('dow' from visit_page_date) = 1 then 'segunda'
           when extract('dow' from visit_page_date) = 2 then 'terça'
           when extract('dow' from visit_page_date) = 3 then 'quarta'
           when extract('dow' from visit_page_date) = 4 then 'quinta'
           when extract('dow' from visit_page_date) = 5 then 'sexta'
           when extract('dow' from visit_page_date) = 6 then 'sábado'
           else null end                   as "dia da semana",
       count(*)                            as "visitas (#)"

from sales.funnel
where visit_page_date between '2021-08-01' and '2021-08-31'
group by dia_semana
order by dia_semana
````

## Result

![Exel Graphic result](graphic.png)
---

