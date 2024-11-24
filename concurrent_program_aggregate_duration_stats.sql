with
args as (
  select
    -- matches either request_id's program or specific program(s)
    246599437 as request_id,
    'RU_RETROPAY' as concurrent_program_name
  from dual 
),
programs_of_interest as (
  -- aggregate duration for a specific request_id's program
  select r1.program_application_id, r1.concurrent_program_id
  from apps.fnd_concurrent_requests r1
  where r1.request_id = (select args.request_id from args)
  union all 
  -- aggregate duration for specific program(s)
  select cp1.application_id, cp1.concurrent_program_id 
  from apps.fnd_concurrent_programs cp1
  where cp1.concurrent_program_name like (select args.concurrent_program_name from args)
),
requests_of_interest as (
  select --+ index(r(program_application_id, concurrent_program_id))
    r.request_date,
    cp.concurrent_program_name,
    r.request_id,
    r.phase_code,
    r.status_code,
    r.argument_text,
    trunc(r.request_date, 'q') as request_date_q,
    trunc(r.request_date, 'mm') as request_date_mm,
    r.actual_completion_date - r.actual_start_date as duration
  from programs_of_interest poi 
  join apps.fnd_concurrent_programs cp on 1 = 1
    and cp.application_id = poi.program_application_id 
    and cp.concurrent_program_id = poi.concurrent_program_id
  join apps.fnd_concurrent_requests r on 1 = 1 
    and r.program_application_id = cp.application_id
    and r.concurrent_program_id = cp.concurrent_program_id
  where 1 = 1 
    and r.request_date >= add_months(trunc(sysdate, 'mm'), -12)
    and r.phase_code = 'C' -- completed    
)
select 
    r.concurrent_program_name,
    request_date_mm,
    s.meaning as request_completion_status,    
    count(*) as request_count,
    numtodsinterval(min(duration), 'day') as min_duration,
    numtodsinterval(percentile_disc(0.10) within group(order by duration), 'day') dur_10,
    numtodsinterval(percentile_disc(0.50) within group(order by duration), 'day') dur_50,
    numtodsinterval(percentile_disc(0.75) within group(order by duration), 'day') dur_75,
    numtodsinterval(percentile_disc(0.90) within group(order by duration), 'day') dur_90,
    numtodsinterval(percentile_disc(0.95) within group(order by duration), 'day') dur_95,
    numtodsinterval(percentile_disc(0.99) within group(order by duration), 'day') dur_99,
    numtodsinterval(max(duration), 'day') as max_duration,    
    any_value(request_id) keep(dense_rank last order by duration) as request_with_max_duration,
    numtodsinterval(round(avg(duration), 5), 'day') as avg_duration,
    any_value(argument_text) keep(dense_rank last order by duration) as arguments_for_request_with_max_duration    
from requests_of_interest r 
join (
  select 
    lookup_code as status_code, 
    trim(meaning) as meaning
  from apps.fnd_lookup_values
  where lookup_type = 'CP_STATUS_CODE'
    and language = 'US'
) s 
on s.status_code = r.status_code
group by 
  r.concurrent_program_name,
  grouping sets (
    (s.meaning),
    (r.request_date_mm, s.meaning),
    (r.request_date_mm)
  )
order by 
  r.concurrent_program_name,
  decode(s.meaning, 'Normal', 0, 1),
  s.meaning nulls last,
  request_date_mm nulls last