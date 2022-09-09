{{
  config(
    meta = {
     "continual": {
       "type": "Model",
       "name": "vm_needs_maintenance_30day",
       "index": "machine_id",
       "time_index": "ts",
       "target": "failure_in_30day",
       "split": "split",
       "train": {'metric': 'f1'},
       "columns": [
          {"name": "machine_id", "entity": "azure_vm"}
       ]
     }
   }
  )
}}

with failures as (

    select * from {{ ref('vm_next_failure_base') }}

),

final as (

    select
      machine_id,
      ts,
      failure_in_30day,
      split

    from failures

)

select * from final