with
args as (
  select
    -- matches either request_id's program or specific program(s)
    246599437 as request_id,
    'RU_RETROPAY' as concurrent_program_name,
    6 as number_of_months_to_show,
    10 as top_n
  from dual 
),
programs_of_interest as (
  select r1.program_application_id as application_id, r1.concurrent_program_id
  from apps.fnd_concurrent_requests r1
  where r1.request_id = (select args.request_id from args)
  union all 
  select cp1.application_id, cp1.concurrent_program_id 
  from apps.fnd_concurrent_programs cp1
  where cp1.concurrent_program_name like (select args.concurrent_program_name from args)
)
select *
from (
  select
    cp.concurrent_program_name,
    to_char(r.request_date, 'Mon-YY') as mon,
    row_number() over(
        partition by cp.concurrent_program_name, trunc(r.request_date, 'MM')
        order by numtodsinterval(r.actual_completion_date - r.actual_start_date, 'day') desc nulls last
    ) as rn,
    r.request_date,
    r.request_id,
    (
      select meaning
      from apps.fnd_lookups
      where lookup_type = 'CP_STATUS_CODE'
      and lookup_code = r.status_code
    ) status,
    numtodsinterval(r.actual_completion_date - r.actual_start_date, 'day') as duration,
    r.parent_request_id,
    r.priority_request_id,
    r.priority,
    (
      select user_name
      from apps.fnd_user
      where user_id = r.requested_by
    ) as requested_by,
    (
      select responsibility_name
      from apps.fnd_responsibility_vl
      where responsibility_id = r.responsibility_id
    ) as responsibility_name,
    r.actual_start_date,
    r.actual_completion_date,
    r.logfile_name,
    r.lfile_size,
    r.outfile_name,
    r.ofile_size,
    r.argument_text,
    r.oracle_session_id
  from programs_of_interest poi 
  join (
    select r.*
    from apps.fnd_concurrent_requests r 
    where 1 = 1 
      and r.phase_code = 'C'
      and r.request_date >= add_months(trunc(sysdate, 'MM'), (select -args.number_of_months_to_show from args))
  ) r on 1 = 1 
    and r.program_application_id = poi.application_id
    and r.concurrent_program_id = poi.concurrent_program_id
  join apps.fnd_concurrent_programs cp on 1 = 1 
    and r.program_application_id = cp.application_id
    and r.concurrent_program_id = cp.concurrent_program_id
) where rn <= (select args.top_n from args)
order by
  concurrent_program_name,
  trunc(request_date, 'MM') desc,
  duration desc