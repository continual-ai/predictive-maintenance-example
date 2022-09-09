{{
 config(
   meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_failures",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts",
       "columns": [
          {"name": "comp1_failure", "type": "BOOLEAN"},
          {"name": "comp2_failure", "type": "BOOLEAN"},
          {"name": "comp3_failure", "type": "BOOLEAN"},
          {"name": "comp4_failure", "type": "BOOLEAN"}
       ]
     }
   }
 ) 
}}

{%- set components = dbt_utils.get_column_values(table = ref('stg_failures'), column='failure') -%}

with failures as (

    select * from {{ ref('stg_failures') }}

),

pivoted as (

    select
      ts,
      machine_id,

      {%- for component in components %}
      count(case when failure = '{{ component }}' then 1 end) as {{ component }}_failure
      {{- "," if not loop.last -}}
      {% endfor %}
      
    from failures
    group by 1,2

)

select * from pivoted