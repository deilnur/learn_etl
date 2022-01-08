create table if not exists dds.products(
	sk serial
	,id int
	,name varchar(255)
	,category varchar(255)
	,sub_category varchar(255)
	,valid_from date
	,valid_to date
	,processed_dttm timestamp
	,execution_date date
);

delete from dds.products
where execution_date = '{{ds}}';

with u as(
	update dds.products dp set valid_to = sp.last_update - interval '1 day'
	from stg.products sp
	where dp.id = sp.id and 
	dp.valid_to is null 
	and sp.execution_date = '{{ds}}'
)
insert into dds.products(sk,id,name,category, sub_category,valid_from,processed_dttm, execution_date)
select
		coalesce(dp.sk, nextval(pg_get_serial_sequence('dds.products','sk'))) sk
		,sp.id
		,sp.name
		,sp.category
		,sp.sub_category
		,sp.last_update
		,now() processed_dttm
		,'{{ds}}' execution_date
	from stg.products sp
	left join dds.products dp
	on sp.id = dp.id
	where sp.execution_date = '{{ds}}';