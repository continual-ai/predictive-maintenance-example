{{
 config(
   meta = {
     "continual": {
       "type": "FeatureSet",
       "name": "vm_machine_info",
       "entity": "azure_vm",
       "index": "machine_id"
     }
   }
 ) 
}}

select * from {{ ref('stg_machines') }}