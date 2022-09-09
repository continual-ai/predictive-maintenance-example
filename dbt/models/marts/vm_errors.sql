{{
 config(
   meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_errors",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts",
       "columns": [
          {"name": "error1", "type": "BOOLEAN"},
          {"name": "error2", "type": "BOOLEAN"},
          {"name": "error3", "type": "BOOLEAN"},
          {"name": "error4", "type": "BOOLEAN"},
          {"name": "error5", "type": "BOOLEAN"}
       ]
     }
   }
 ) 
}}

{%- set errors = dbt_utils.get_column_values(table = ref('stg_errors'), column='error_id') -%}

with errors as (

    select * from {{ ref('stg_errors') }}

),

pivoted as (

    select
      ts,
      machine_id,

      {%- for error in errors %}
      count(case when error_id = '{{ error }}' then 1 end) as {{ error }}
      {{- "," if not loop.last -}}
      {% endfor %}
      
    from errors
    group by 1,2
    order by machine_id asc, ts desc

)

select * from pivoted