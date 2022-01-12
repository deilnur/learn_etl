with order_id as (insert into orders(
        customer_id
        ,order_date
        ,ship_date
        ,ship_mode
    )
select
	c.id
	,current_date as order_date
	,date(current_date + trunc(random()  * 65) * '1 day'::interval) as ship_date
	,(array['First Class', 'Second Class', 'Standard Class'])[floor(random() * 3 + 1)] ship_mode
from customers c
order by random()
limit 1
returning id)
insert into order_items(
                        order_id
                        ,product_id
                        ,quantity
                        ,price
                        ,currency_id
                        ,discount)
select
	o_id.id order_id
	,p.id product_id
	,floor(random() * 200 + 1) quantity
	,round( (random()*150)::numeric , 2) price
	,1 currency_id
	,round((random() / 5)::numeric , 2) discount
from products p
inner join order_id o_id
	on 1 = 1
order by random()
limit random() * 10 + 1;