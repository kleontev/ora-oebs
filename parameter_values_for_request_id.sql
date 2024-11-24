with 
args as (
  select 
    246978891 as request_id 
  from dual
), 
request_parameter_values as (
  select 
    log_col_name,
    argument_value
  from (
    select * 
    from apps.fnd_concurrent_requests ur 
    left join apps.fnd_conc_request_arguments ura on ura.request_id = ur.request_id
    -- this whole unpivot thing should have been a neat CTE, 
    -- however, I was not able to make JPPD kick in for this condition
    where ur.request_id = (select args.request_id from args)
  )
  unpivot exclude nulls (
    argument_value 
    for log_col_name in (
        argument1
      , argument2
      , argument3
      , argument4
      , argument5
      , argument6
      , argument7
      , argument8
      , argument9
      , argument10
      , argument11
      , argument12
      , argument13
      , argument14
      , argument15
      , argument16
      , argument17
      , argument18
      , argument19
      , argument20
      , argument21
      , argument22
      , argument23
      , argument24
      , argument25
      , argument26
      , argument27
      , argument28
      , argument29
      , argument30
      , argument31
      , argument32
      , argument33
      , argument34
      , argument35
      , argument36
      , argument37
      , argument38
      , argument39
      , argument40
      , argument41
      , argument42
      , argument43
      , argument44
      , argument45
      , argument46
      , argument47
      , argument48
      , argument49
      , argument50
      , argument51
      , argument52
      , argument53
      , argument54
      , argument55
      , argument56
      , argument57
      , argument58
      , argument59
      , argument60
      , argument61
      , argument62
      , argument63
      , argument64
      , argument65
      , argument66
      , argument67
      , argument68
      , argument69
      , argument70
      , argument71
      , argument72
      , argument73
      , argument74
      , argument75
      , argument76
      , argument77
      , argument78
      , argument79
      , argument80
      , argument81
      , argument82
      , argument83
      , argument84
      , argument85
      , argument86
      , argument87
      , argument88
      , argument89
      , argument90
      , argument91
      , argument92
      , argument93
      , argument94
      , argument95
      , argument96
      , argument97
      , argument98
      , argument99
      , argument100
   )
 )
), 
concurrent_program_parameters as (
  select
      cp.application_id as program_application_id,
      cp.concurrent_program_id,
      dfcu.application_column_name,
      dfcu.column_seq_num,
      dfcu.end_user_column_name,
      dfcul.description, 
      'ARGUMENT' || row_number() over(partition by cp.application_id, cp.concurrent_program_id order by dfcu.column_seq_num) as log_col_name 
  from apps.fnd_concurrent_programs cp 
  join apps.fnd_descr_flex_column_usages dfcu on 1 = 1 
    and dfcu.application_id = cp.application_id 
    and dfcu.descriptive_flexfield_name = '$SRS$.' || cp.concurrent_program_name
  join apps.fnd_descr_flex_col_usage_vl dfcul on 1 = 1
      and dfcul.application_id = dfcu.application_id
      and dfcul.descriptive_flexfield_name = dfcu.descriptive_flexfield_name
      and dfcul.application_column_name = dfcu.application_column_name
  where (cp.application_id, cp.concurrent_program_id) = (
    select program_application_id, concurrent_program_id 
    from apps.fnd_concurrent_requests 
    where request_id = (select request_id from args)
  )
)
select
  cpp.column_seq_num as paremeter_order,
  cpp.end_user_column_name as parameter_name,
  cpp.description as parameter_description,
  cpp.log_col_name,
  rpv.argument_value parameter_value
from concurrent_program_parameters cpp
left join request_parameter_values rpv on rpv.log_col_name = cpp.log_col_name
order by cpp.column_seq_num