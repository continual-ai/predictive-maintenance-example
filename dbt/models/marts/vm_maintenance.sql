{{
 config(
   meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_maintenance",
       "entity": "azure_vm",
       "index": "machine_id",
       "time_index": "ts",
       "columns": [
          {"name": "comp1_maint", "type": "BOOLEAN"},
          {"name": "comp2_maint", "type": "BOOLEAN"},
          {"name": "comp3_maint", "type": "BOOLEAN"},
          {"name": "comp4_maint", "type": "BOOLEAN"}
       ]
     }
   }
 ) 
}}

{%- set components = dbt_utils.get_column_values(table = ref('stg_maintenance'), column='comp') -%}

with maintenance as (

    select * from {{ ref('stg_maintenance') }}

),

pivoted as (

    select
      ts,
      machine_id,

      {%- for component in components %}
      count(case when comp = '{{ component }}' then 1 end) as {{ component }}_maint
      {{- "," if not loop.last -}}
      {% endfor %}
      
    from maintenance
    group by 1,2

)

select * from pivoted