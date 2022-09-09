{{
  config(
    materialized = 'table'
  )
}}

with next_failures as (

    select * from {{ ref('int_next_failure_hours') }}

),

final as (

    select
      machine_id,
      ts,
      
      case when hours_until_next_failure <= 24 then true else false end as failure_in_1day,
      case when hours_until_next_comp1_failure <= 24 then true else false end as comp1_failure_in_1day,
      case when hours_until_next_comp2_failure <= 24 then true else false end as comp2_failure_in_1day,
      case when hours_until_next_comp3_failure <= 24 then true else false end as comp3_failure_in_1day,
      case when hours_until_next_comp4_failure <= 24 then true else false end as comp4_failure_in_1day,

      case when hours_until_next_failure <= (24*7) then true else false end as failure_in_7day,
      case when hours_until_next_comp1_failure <= (24*7) then true else false end as comp1_failure_in_7day,
      case when hours_until_next_comp2_failure <= (24*7) then true else false end as comp2_failure_in_7day,
      case when hours_until_next_comp3_failure <= (24*7) then true else false end as comp3_failure_in_7day,
      case when hours_until_next_comp4_failure <= (24*7) then true else false end as comp4_failure_in_7day,

      case when hours_until_next_failure <= (24*30) then true else false end as failure_in_30day,
      case when hours_until_next_comp1_failure <= (24*30) then true else false end as comp1_failure_in_30day,
      case when hours_until_next_comp2_failure <= (24*30) then true else false end as comp2_failure_in_30day,
      case when hours_until_next_comp3_failure <= (24*30) then true else false end as comp3_failure_in_30day,
      case when hours_until_next_comp4_failure <= (24*30) then true else false end as comp4_failure_in_30day,

      case 
        when ts < '{{ var("train_end") }}' and ts >= '{{ var("train_start") }}'  then 'TRAIN'
			  when ts < '{{ var("vali_end") }}'  and ts >= '{{ var("vali_start") }}'  then 'VALI'
			  when ts < '{{ var("test_end") }} ' and ts >= '{{ var("test_start") }}'  then 'TEST'
			  else 'PREDICT_ME'
      end as split

      from next_failures
      order by machine_id asc, ts desc

)

select * from final