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
CREATE OR REPLACE VIEW shipping_datamart AS 
SELECT 
    si.shippingid,
    si.vendorid,
    st.transfer_type,
    EXTRACT (DAY FROM (ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime)) AS full_day_at_shipping,
    CASE
        WHEN ss.shipping_end_fact_datetime IS NULL THEN NULL 
        WHEN ss.shipping_end_fact_datetime < si.shipping_plan_datetime THEN 0
        ELSE 1
    END AS is_delay,
    CASE
        WHEN ss.status = 'finished' THEN 1
        ELSE 0
    END AS is_shipping_finish,
    CASE
        WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN EXTRACT (DAY FROM (ss.shipping_end_fact_datetime - si.shipping_plan_datetime))
        ELSE 0
    END AS delay_day_at_shipping,
    si.payment_amount AS payment_amount,
    si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat,
    si.payment_amount * sa.agreement_commission AS profit
FROM 
    public.shipping_info AS si
JOIN
    shipping_transfer AS st
        USING (transfer_type_id)
JOIN 
    shipping_status AS ss
        USING (shippingid)
JOIN 
    shipping_country_rates AS scr
        USING (shipping_country_id)
JOIN 
    shipping_agreement AS sa
        USING (agreementid);
