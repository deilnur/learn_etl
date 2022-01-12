delete from {{params.table_name}}
where execution_date = '{{ds}}';