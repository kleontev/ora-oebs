with
args as (
  select
    -- matches either request_id's program or specific program(s)
    246599437 as request_id,
    '' as concurrent_program_name
  from dual
),
programs_of_interest as (
  select cp.application_id, cp.concurrent_program_name
  from apps.fnd_concurrent_requests r
  join apps.fnd_concurrent_programs cp on 1 = 1
    and cp.application_id = r.program_application_id
    and cp.concurrent_program_id = r.concurrent_program_id
  where r.request_id = (select args.request_id from args)
  --
  union all
  --
  select cp.application_id, cp.concurrent_program_name
  from apps.fnd_concurrent_programs cp
  where cp.concurrent_program_name like (select args.concurrent_program_name from args)
)
select
  poi.concurrent_program_name,
  dfcu.last_update_date,
  dfcu.application_Column_name,
  dfcu.column_seq_num,
  dfcu.end_user_column_name,
  dfcul.description,
  dfcu.enabled_Flag,
  dfcu.required_Flag,
  dfcu.display_Flag,
  dfcu.display_Size,
  dfcu.maximum_description_len,
  dfcu.concatenation_description_len,
  dfcu.default_value,
  (
    select meaning
    from apps.fnd_lookup_values_vl v
    where 1 = 1
      and v.lookup_type = 'FLEX_DEFAULT_TYPE'
      and v.lookup_code = dfcu.default_type
  ) as default_value_type,
  dfcu.flex_Value_Set_id,
  fvs.flex_Value_Set_name,
  fvs.description fvs_description,
  fvt.application_table_name,
  fvt.id_column_name,
  fvt.id_column_size,
  fvt.value_column_name,
  fvt.value_column_size,
  (
    select meaning
    from apps.fnd_lookup_values_vl v
    where 1 = 1
      and v.lookup_type = 'FIELD_TYPE'
      and v.lookup_code = fvs.format_type
  ) as value_format_type,
  fvs.maximum_size as max_value_size, -- это максимальный размер fvt.value_column_name на уровне НЗ, не на уровне таблицы валидации
  fvt.meaning_column_name,
  fvt.meaning_column_size,
  fvt.additional_where_clause
from programs_of_interest poi
join apps.fnd_descr_flex_column_usages dfcu on 1 = 1
  and dfcu.application_id = poi.application_id
  and dfcu.descriptive_flexfield_name = '$SRS$.' || poi.concurrent_program_name
join apps.fnd_descr_flex_col_usage_vl dfcul on 1 = 1
  and dfcul.application_id = dfcu.application_id
  and dfcul.descriptive_flexfield_name = dfcu.descriptive_flexfield_name
  and dfcul.application_column_name = dfcu.application_column_name
left join apps.fnd_Flex_value_sets fvs on 1 = 1
  and fvs.flex_value_set_id = dfcu.flex_Value_Set_id
left join apps.fnd_user fvsu on 1 = 1
  and fvsu.user_id = fvs.created_by
left join apps.fnd_flex_Validation_tables fvt on 1 = 1
  and fvs.flex_Value_Set_id = fvt.flex_Value_set_id
order by
  poi.concurrent_program_name,
  dfcu.column_seq_num