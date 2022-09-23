-- добавьте код сюда
create or replace view analysis.orders as
select p.*, s.status_id as status
from production.orders p
join production.orderstatuslog s
on p.order_id = s.order_id
where p.order_id in (select order_id
from (select order_id, max(dttm)
      from production.orderstatuslog
      group by order_id) as t)

