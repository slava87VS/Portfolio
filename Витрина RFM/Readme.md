# Витрина RFM

## 1.1. Выясните требования к целевой витрине.

Постановка задачи выглядит достаточно абстрактно - постройте витрину. Первым делом вам необходимо выяснить у заказчика детали. Запросите недостающую информацию у заказчика в чате.

Зафиксируйте выясненные требования. Составьте документацию готовящейся витрины на основе заданных вами вопросов, добавив все необходимые детали.

- Название витриы — dm_rfm_segments
- Период — данные с начала 2022 года
- Место хранения витрины — база — dm, схема — analysis
Их каких полей состоит:
- user_id
- recency (число от 1 до 5)
- frequency (число от 1 до 5)
- monetary_value (число от 1 до 5)
Обновление витрины — не нужны
Успешно выполненный заказ — это заказ со статусом Closed

- Recency (пер. «давность») — сколько времени прошло с момента последнего заказа.
- Frequency (пер. «частота») — количество заказов.
- Monetary Value (пер. «денежная ценность») — сумма затрат клиента.



## 1.2. Изучите структуру исходных данных.

Полключитесь к базе данных и изучите структуру таблиц.

Если появились вопросы по устройству источника, задайте их в чате.

Зафиксируйте, какие поля вы будете использовать для расчета витрины.

- Recency - orders_ts (таблица orders)
- Frequency - orders_id (таблица orders)
- Monetary Value - payment (таблица orders)


## 1.3. Проанализируйте качество данных

Изучите качество входных данных. Опишите, насколько качественные данные хранятся в источнике. Так же укажите, какие инструменты обеспечения качества данных были использованы в таблицах в схеме production.

- orders_ts имеет следующий DDL - ALTER TABLE production.orders ADD order_ts timestamp NOT NULL;
Верный тип данных, отсутствие NULL

- order_id имеет следующий DDL - ALTER TABLE production.orders ADD order_id int4 NOT NULL;
Верный тип данных, отсутствие NULL

- payment имеет следующий DDL - ALTER TABLE production.orders ADD payment numeric(19, 5) NOT NULL DEFAULT 0;
Верный тип данных, отсутствие NULL, а также сумма покупки не должна быть равна нулю


## 1.4. Подготовьте витрину данных

Теперь, когда требования понятны, а исходные данные изучены, можно приступить к реализации.

### 1.4.1. Сделайте VIEW для таблиц из базы production.**

Вас просят при расчете витрины обращаться только к объектам из схемы analysis. Чтобы не дублировать данные (данные находятся в этой же базе), вы решаете сделать view. Таким образом, View будут находиться в схеме analysis и вычитывать данные из схемы production. 

Напишите SQL-запросы для создания пяти VIEW (по одному на каждую таблицу) и выполните их. Для проверки предоставьте код создания VIEW.

```
create or replace view analysis.orderitems as select * from production.orderitems;
create or replace view analysis.orderstatuses as select * from production.orderstatuses;
create or replace view analysis.orderstatuslog as select * from production.orderstatuslog;
create or replace view analysis.products as select * from production.products;
create or replace view analysis. users as select * from production.users;
create or replace view analysis.orders as select * from production.orders;

```

### 1.4.2. Напишите DDL-запрос для создания витрины.**

Далее вам необходимо создать витрину. Напишите CREATE TABLE запрос и выполните его на предоставленной базе данных в схеме analysis.

```

CREATE TABLE analysis.dm_rfm_segments (user_id int PRIMARY KEY,
recency int CHECK(recency > 0 AND recency <= 5),
frequency int CHECK(frequency > 0 AND recency <= 5),
monetary_value int CHECK(monetary_value > 0 AND monetary_value <= 5))

```


### 1.4.3. Напишите SQL запрос для заполнения витрины

Наконец, реализуйте расчет витрины на языке SQL и заполните таблицу, созданную в предыдущем пункте.

Для решения предоставьте код запроса.

```
with y as(with q as (select user_id, order_ts,  payment, order_id
from analysis.orders
where order_ts >= '2022-01-01'
and status = 4)
select user_id,
MAX(order_ts) AS last_date,
SUM(payment) AS total_sum,
count(order_id) as count_order
FROM q
GROUP by user_id)
INSERT INTO analysis.dm_rfm_segments
SELECT analysis.users.id AS user_id,
ntile(5) OVER (ORDER BY (CASE WHEN last_date IS NULL THEN (SELECT MIN(last_date) FROM y) ELSE last_date END) ASC) AS recency,
ntile(5) OVER (ORDER BY (CASE WHEN total_sum IS NULL THEN (SELECT MIN(total_sum) FROM y) ELSE total_sum END) ASC) AS monetary_value,
ntile(5) OVER (ORDER BY (CASE WHEN count_order IS NULL THEN (SELECT MIN(count_order) FROM y) ELSE count_order END) ASC) AS frequency
FROM y
RIGHT JOIN analysis.users ON y.user_id=analysis.users.id
ORDER BY analysis.users.id;


