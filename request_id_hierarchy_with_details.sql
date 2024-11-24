with
args as (
  select   
    246599437 as request_id, 
    null as hierarchy_depth
  from dual
)
select
    lpad(' ', 2 * level , ' ') ||  level as level_in_hierarchy,
    p.concurrent_program_name,
    pl.user_concurrent_program_name,
    (
      select execution_file_name           
      from apps.fnd_executables e 
      where e.application_id = p.executable_application_id
        and e.executable_id = p.executable_id    
    ) as execution_file_name,
    (
      select trim(meaning)
      from apps.fnd_lookups 
      where lookup_type = 'CP_PHASE_CODE'
        and lookup_code = r.phase_code
    ) phase,
    (
      select trim(meaning) 
      from apps.fnd_lookups 
      where lookup_type = 'CP_STATUS_CODE' 
        and lookup_code = r.status_code
    ) status,
    numtodsinterval(
      case r.phase_code 
      when 'C' 
      then r.actual_completion_date 
      else sysdate 
      end - r.actual_Start_date, 'day'
    ) duration,
    r.request_id,
    r.parent_request_id,    
    r.root_request_id,
    (
      select rbu.user_name 
      from apps.fnd_user rbu 
      where rbu.user_id = r.requested_by
    ) as requested_by,
    (
      select responsibility_name 
      from apps.fnd_responsibility_tl 
      where responsibility_id = r.responsibility_id 
      and language = 'RU'
    ) as responsibility_name,
    r.requested_start_Date,
    r.actual_start_date,    
    r.completion_text,
    decode(r.phase_code, 'C', r.actual_completion_date) as actual_completion_date,
    r.argument_text,    
    r.description as request_description,
    r.lfile_size, 
    r.logfile_name,    
    r.ofile_size, 
    r.outfile_name
from apps.fnd_concurrent_requests r
join apps.fnd_concurrent_programs p on 1 = 1
    and p.application_id  = r.program_application_id
    and p.concurrent_program_id = r.concurrent_program_id
left join apps.fnd_concurrent_programs_vl pl on 1 = 1 
    and pl.application_id  = p.application_id
    and pl.concurrent_program_id = p.concurrent_program_id
connect by 1 = 1
    and prior r.request_id = r.parent_request_id
    and level <= nvl((select hierarchy_depth from args), level)
start with r.request_id = (select args.request_id from args)
order siblings by
    r.actual_start_date nulls last,
    r.request_id