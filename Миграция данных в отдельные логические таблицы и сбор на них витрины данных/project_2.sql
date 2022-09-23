--1 задание
create table shipping_country_rates (
  id serial,
  shipping_country text,
  shipping_country_base_rate numeric(3,2),
  primary key (id));
create SEQUENCE shipping_country_rates_sequence start 1;
insert into shipping_country_rates (id, shipping_country, shipping_country_base_rate)
select nextval('shipping_country_rates_sequence')::bigint AS id,
shipping_country,
shipping_country_base_rate
FROM (select distinct shipping_country, shipping_country_base_rate from shipping s) as a;
drop SEQUENCE shipping_country_rates_sequence;
--2 задание
CREATE TABLE shipping_agreement (
agreementid BIGINT,
agreement_number VARCHAR(30),
agreement_rate NUMERIC(3,2),
agreement_commission NUMERIC(3,2),
PRIMARY KEY (agreementid));
INSERT INTO shipping_agreement(agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT description[1]::BIGINT AS agreementid,
description[2]::VARCHAR(30) AS agreement_number,
description[3]::NUMERIC(3,2) AS agreement_rate,
description[4]::NUMERIC(3,2) AS agreement_commission
from (SELECT DISTINCT (regexp_split_to_array(vendor_agreement_description , E':')) as description
FROM shipping) AS parse_description;
--3 задание
CREATE TABLE shipping_transfer (
id serial,
transfer_type VARCHAR(5),
transfer_model TEXT,
shipping_transfer_rate  NUMERIC(4,3),
PRIMARY KEY (id));
create sequence shipping_transfer_sequnce start 1;
insert into shipping_transfer (id, transfer_type, transfer_model, shipping_transfer_rate)
select nextval('shipping_transfer_sequnce')::bigint as id,
description[1]::VARCHAR (5) as transfer_type,
description[2]::text as transfer_model,
shipping_transfer_rate::NUMERIC(4,3)
from (select distinct(regexp_split_to_array(shipping_transfer_description, E':')) as description,
shipping_transfer_rate
from shipping s) as description_parse;
--4 задание
--созадем таблицу
CREATE TABLE shipping_info (
shippingid int,
shipping_plan_datetime timestamp,
payment_amount numeric(6,2),
vendorid int);
--добавляем костантные данные
insert into shipping_info (shippingid, shipping_plan_datetime, payment_amount, vendorid)
select distinct  shippingid::int,
shipping_plan_datetime::timestamp,
payment_amount::numeric(6,2),
vendorid::int
from shipping;
---shipping_country_rates
--добавляем столбец внешний ключ
alter table shipping_info add column shipping_country_rates_id int;
--создаем внешний ключ
ALTER TABLE shipping_info 
ADD CONSTRAINT shipping_info_fkey
FOREIGN KEY (shipping_country_rates_id) REFERENCES shipping_country_rates (id) 
ON UPDATE CASCADE;
--заполняем данные
update shipping_info as d set (shipping_country_rates_id) = (select distinct scr.id 
from shipping_country_rates scr
join shipping s
on scr.shipping_country = s.shipping_country
where d.shippingid=s.shippingid);
---shipping_agreement
--добавляем столбец внешний ключ
alter table shipping_info add column agreementid int;
--создаем внешний ключ
ALTER TABLE shipping_info 
ADD CONSTRAINT shipping_agreement_fkey
FOREIGN KEY (agreementid) REFERENCES shipping_agreement (agreementid) 
ON UPDATE CASCADE;
--заполняем данные
update shipping_info as si set (agreementid) =
(SELECT DISTINCT (regexp_split_to_array(vendor_agreement_description , E':'))[1]::BIGINT as agreementid
from shipping s
where s.shippingid=si.shippingid);
---shipping_transfer
--добавляем столбец внешний ключ
alter table shipping_info add column shipping_transfer_id int;
--создаем внешний ключ
ALTER TABLE shipping_info 
ADD CONSTRAINT shipping_transfer_fkey
FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer (id) 
ON UPDATE CASCADE;
--заполняем данные
update shipping_info as d set (shipping_transfer_id) = (select distinct stn.id
from shipping s 
join (select *, transfer_type || ':' || transfer_model as shipping_transfer_description
from shipping_transfer st) as stn
on stn.shipping_transfer_description=s.shipping_transfer_description
where d.shippingid=s.shippingid);
--5 задание
CREATE TABLE shipping_status (
shippingid BIGINT,
status TEXT,
state TEXT,
shipping_start_fact_datetime TIMESTAMP,
shipping_end_fact_datetime TIMESTAMP,
PRIMARY KEY (shippingid));
with ship_max as (select shippingid,
max(case when state = 'booked' then state_datetime else null end) as shipping_start_fact_datetime,
max(case when state = 'recieved' then state_datetime else null end) as shipping_end_fact_datetime,
max(state_datetime) as max_state_datetime
from shipping
group by shippingid)
insert into shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
select sm.shippingid, s.status, s.state, sm.shipping_start_fact_datetime, sm.shipping_end_fact_datetime
from ship_max as sm
left join shipping as s on sm.shippingid = s.shippingid
and sm.max_state_datetime = s.state_datetime
order by shippingid;
--6 задание
create or replace view shipping_datamart as
--временная таблица для расчета полных дней доставки
with full_day as (select shippingid, full_day_at_shipping
from (select shippingid, date_part('day', age (shipping_end_fact_datetime, (LAG(shipping_start_fact_datetime) OVER (PARTITION BY shippingid ORDER BY shipping_start_fact_datetime)))) as full_day_at_shipping
from (select *
from shipping_status
where state = 'booked'
or state = 'recieved'
order by shippingid, state) as q) as w
where full_day_at_shipping is not null),
--временная таблица для is_delay
isdelay as (select shippingid, case when shipping_end_fact_datetime > shipping_plan_datetime then 1 else 0 end as is_delay 
from (select distinct ss.shippingid, shipping_end_fact_datetime, shipping_plan_datetime
from shipping_status ss
join shipping_info si
on ss.shippingid = si.shippingid
order by shippingid) as e
where shipping_end_fact_datetime is not null),
--временная таблица для is_shipping_finish
finish as (select shippingid, case when status = 'finished' then 1 else 0 end as is_shipping_finish
from (select shippingid, state_datetime, status, ROW_NUMBER() OVER(PARTITION BY shippingid order by state_datetime desc)
from shipping) as q
where row_number = 1),
--временная таблица для delay_day_at_shipping
delay_day as (select s.shippingid,
case when shipping_end_fact_datetime > shipping_plan_datetime then date_part ('day', age(shipping_end_fact_datetime, shipping_plan_datetime)) else 0 end as delay_day_at_shipping
from shipping s
left join (select shippingid, shipping_end_fact_datetime
from shipping_status
where state='recieved') as d
on d.shippingid = s.shippingid),
--временная таблица для vat
vat as (select shippingid, sa.agreement_commission, payment_amount * ( shipping_country_base_rate + agreement_rate ++ shipping_transfer_rate) as vat
from shipping_info si
join shipping_country_rates scr 
on si.shipping_country_rates_id = scr.id 
join shipping_transfer st 
on st.id = si.shipping_transfer_id 
join shipping_agreement sa 
on sa.agreementid = si.agreementid)
--скрипт для заполнения view
select s.shippingid, vendorid, transfer_type, full_day_at_shipping, is_delay, is_shipping_finish, delay_day_at_shipping, s.payment_amount, vat, v.agreement_commission * s.payment_amount as profit
from shipping_info s
left join shipping_transfer st 
on st.id = s.shipping_transfer_id 
join full_day as fd
on fd.shippingid = s.shippingid
left join isdelay isd
on isd.shippingid = s.shippingid
left join finish f
on f.shippingid = s.shippingid
left join delay_day dd
on dd.shippingid = s.shippingid
left join vat v
on v.shippingid = s.shippingid;
