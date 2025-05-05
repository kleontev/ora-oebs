with
running_requests as (
  select --+ no_merge
    r.request_id,
    r.parent_request_id, 
    cp.concurrent_program_name,
    u.user_name as requested_by,
    r.request_date,
    r.requested_start_date,
    r.actual_start_date,
    r.actual_completion_date,
    r.oracle_session_id
  from apps.fnd_concurrent_Requests r
  join apps.fnd_Concurrent_programs cp on 1 = 1 
    and cp.application_id = r.program_application_id 
    and cp.concurrent_program_id = r.concurrent_program_id
  join apps.fnd_user u on u.user_id = r.requested_by
  where r.phase_code = 'R'
),
request_sessions as (
  select --+ materialize
    rr.*,
    s.inst_id,
    s.sid,
    s.blocking_instance,
    s.blocking_session,
    s.status,
    s.event,
    s.seconds_in_wait
  from running_requests rr 
  -- won't detect non-requests sessions here. scheduler jobs?.. 
  -- TODO right join?
  join gv$session s on s.audsid = rr.oracle_Session_id
),
requests_involved_in_locks as ( 
  select --+ materialize
    * 
  from (
    -- sessions that block somebody
    select *
    from request_sessions s
    where exists (
      select null
      from request_sessions blocked
      where 1 = 1 
        and blocked.blocking_instance = s.inst_id
        and blocked.blocking_session = s.sid 
        and blocked.request_id != s.request_id -- PX Deq:* events make it seem like request is blocking itself. I don't need them
    )
    union
    -- sessions that are blocked by somebody
    select *
    from request_sessions s
    where exists (
      select null
      from request_sessions blocker
      where 1 = 1 
        and blocker.inst_id = s.blocking_instance
        and blocker.sid = s.blocking_session 
        and blocker.request_id != s.request_id -- see above
    )
  )
)
select 
  lpad(' ', (level - 1) * 2, ' ') || to_char(ril.request_id) request_id_tree,
  ril.*
from requests_involved_in_locks ril
start with ril.blocking_session is null
connect by 1 = 1  
  and prior ril.inst_id = ril.blocking_instance
  and prior ril.sid = ril.blocking_session
order siblings by ril.request_id
