select entity_id ,count(1) as countt,'1' as pt, '0' as pmod  from order.totalpayinfo group by entity_id order by countt desc limit 10