select
	id 
	,name
	,category 
	,sub_category
from stg.products p
where processed_dttm >= %(ds)s
and processed_dttm < %(data_interval_end)s;