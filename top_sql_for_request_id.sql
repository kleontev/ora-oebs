with
args as (
  select
    246978891 as request_id,
    'N' as group_by_event,
    'N' as group_by_plsql_object_id
  from dual
),
request_sessions as (
  -- unfortunately vanilla OEBS doesn't seem to have a way to map request_id to sid/serial, so we have to make do
  select /*+ no_merge inline */ distinct
    request_id,
    inst_id,
    sid,
    serial#
  from system.fnd_concurrent_sessions cs 
  where 1 = 1 
    and request_id = (select args.request_id from args)
    and sid is not null
),
data_awr as (
  select --+ ordered use_nl(s)
    rs.request_id,
    rs.inst_id,
    rs.sid,
    rs.serial#,
    s.dbid,
    xt.sample_id,
    to_timestamp(xt.sample_time, 'yyyy-mm-dd hh24:mi:ss.ff') sample_time,
    xt.sql_id,
    xt.sql_exec_id,
    xt.event,
    xt.module,
    xt.plsql_entry_object_id,
    xt.plsql_entry_subprogram_id,
    xt.plsql_object_id,
    xt.plsql_subprogram_id
  from request_sessions rs 
  join apps.fnd_concurrent_requests r on 1 = 1
    and rs.request_id = r.request_id
  join dba_hist_snapshot s on 1 = 1
    and s.instance_number = rs.inst_id
    and not (
      s.begin_interval_time > r.actual_completion_date
      or
      s.end_interval_time < r.actual_start_date
    )
  cross apply xmltable(
    '/ROWSET/ROW'
    passing dbms_xmlgen.getxmltype('
    select
      sample_id,
      to_char(sample_time, ''yyyy-mm-dd hh24:mi:ss.ff'') sample_time,
      sql_id,
      sql_exec_id,
      event,
      module,
      plsql_entry_object_id,
      plsql_entry_subprogram_id,
      plsql_object_id,
      plsql_subprogram_id
    from dba_hist_Active_sess_history ash
    where 1 = 1
      and ash.instance_number = ' || rs.inst_id || '
      and ash.session_id = ' || rs.sid || '
      and ash.session_serial#= ' || rs.serial# ||'
      and ash.dbid = '|| s.dbid || '
      and ash.snap_id = ' || s.snap_id
    )
    columns
      SAMPLE_ID number,
      SAMPLE_TIME,
      SQL_ID,
      SQL_EXEC_ID number,
      EVENT,
      MODULE,
      PLSQL_ENTRY_OBJECT_ID number,
      PLSQL_ENTRY_SUBPROGRAM_ID number,
      PLSQL_OBJECT_ID number,
      PLSQL_SUBPROGRAM_ID number
  ) xt
),
data_ash as (
  select --+ ordered use_nl(ash)
    rs.request_id,
    rs.inst_id,
    rs.sid,
    rs.serial#,
    (select dbid from v$database) dbid,
    ash.sample_id,
    ash.sample_time,
    ash.sql_id,
    ash.sql_exec_id,
    ash.event,
    ash.module,
    ash.plsql_entry_object_id,
    ash.plsql_entry_subprogram_id,
    ash.plsql_object_id,
    ash.plsql_subprogram_id
  from request_sessions rs
  join gv$active_session_history ash on 1 = 1
    and ash.inst_id = rs.inst_id
    and ash.session_id = rs.sid
    and ash.session_serial# = rs.serial#
),
data_combined as (
  select 
      d.request_id
    , d.inst_id
    , d.sid
    , d.serial#
    , d.dbid
    , d.sample_id
    , d.sample_time
    , d.sql_id
    , d.sql_exec_id
    , d.module
    , decode(args.group_by_event, 'Y', nvl(d.event, 'ON CPU')) as event
    , decode(args.group_by_plsql_object_id, 'Y', d.plsql_entry_object_id    ) as plsql_entry_object_id
    , decode(args.group_by_plsql_object_id, 'Y', d.plsql_entry_subprogram_id) as plsql_entry_subprogram_id
    , decode(args.group_by_plsql_object_id, 'Y', d.plsql_object_id          ) as plsql_object_id
    , decode(args.group_by_plsql_object_id, 'Y', d.plsql_subprogram_id      ) as plsql_subprogram_id
  from args
  cross join (
    select * from data_ash 
    union
    select * from data_awr
  ) d 
),
data_aggregated as (
  select
    dc.request_id,
    any_value(dbid) dbid,
    dc.inst_id,
    dc.sid,
    dc.serial#,
    dc.sql_id,
    dc.plsql_entry_object_id,
    dc.plsql_entry_subprogram_id,
    dc.plsql_object_id,
    dc.plsql_subprogram_id,
    dc.event,
    dc.module,
    min(dc.sample_time) min_sample_time,
    max(dc.sample_time) max_sample_time,
    count(*) sample_count,
    count(distinct dc.sql_exec_id) executions_count
  from data_combined dc
  group by
    dc.request_id,
    dc.inst_id,
    dc.sid,
    dc.serial#,
    dc.sql_id,
    dc.plsql_entry_object_id,
    dc.plsql_entry_subprogram_id,
    dc.plsql_object_id,
    dc.plsql_subprogram_id,
    dc.event,
    dc.module
)
select 
  inst_id,
  sid,
  serial#,     
  module,
  sql_id,
  event,
  min_sample_time,
  max_sample_time,
  sample_count,
  executions_count,
  coalesce(
    (
      select s.sql_text
      from gv$sqlarea s
      where 1 = 1
        and s.inst_id = da.inst_id
        and s.sql_id = da.sql_id
    ),
    (
      select sql_text
      from xmltable(
        '/ROWSET/ROW'
        passing dbms_xmlgen.getxmltype('
         select substr(s.sql_text, 1, 4000) sql_text
         from dba_hist_sqltext s
         where 1 = 1
         and s.sql_id = ' ||  dbms_assert.enquote_literal(da.sql_id) || '
         and s.dbid = ' ||  da.dbid
        )
        columns
            SQL_TEXT
      )
    )
  ) sql_text,
  plsql_entry_object_id,
  plsql_entry_subprogram_id,
  plsql_object_id,
  plsql_subprogram_id,
   (
    -- all/dba_procedures, that accepts *subprogram_id, is unfortunately way too slow
    select p.owner || '.' || p.object_name --|| decode(p.procedure_name, null, null, '.' || p.procedure_name)
    from all_objects p
    where 1 = 1
      and p.object_id = da.plsql_entry_object_id
      -- and p.subprogram_id = da.plsql_entry_subprogram_id
      and rownum = 1
  ) as plsql_entry_object_name,
  (
    select p.owner || '.' || p.object_name
    from all_objects p
    where 1 = 1
      and p.object_id = da.plsql_object_id
      and rownum = 1
  ) as plsql_object_name
from data_aggregated da
order by 
  da.sample_count desc