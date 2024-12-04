set serverout on

declare
  c_root_request constant int := 242606286;

  l_inst_id fnd_table_of_number;
  l_sid fnd_table_of_number;
  l_serial fnd_table_of_number;

  procedure kill_session(
    p_inst_id int,
    p_sid int,
    p_serial int
  ) as
    e_session_marked_for_kill exception;
    pragma exception_init(e_session_marked_for_kill, -31);
    l_sql varchar2(4000) := q'[alter system disconnect session '{sid},{serial#},@{inst_id}' immediate]';
  begin
    l_sql := replace(l_sql, '{inst_id}', p_inst_id);
    l_sql := replace(l_sql, '{sid}', p_sid);
    l_sql := replace(l_sql, '{serial#}', p_serial);

    dbms_output.put_line(l_sql);
    execute immediate l_sql;
  exception when e_session_marked_for_kill then
    null;
  end kill_session;
begin
  <<loop_until_all_killed>>
  loop
    select
      inst_id,
      sid,
      serial#
    bulk collect into
      l_inst_id,
      l_sid,
      l_serial
    from gv$session
    where 1 = 1
      and status = 'ACTIVE'
      and audsid in (
        select r.oracle_session_id
        from fnd_concurrent_Requests r
        connect by r.parent_request_id = prior r.request_id
        start with r.request_id = c_root_request
      );

    exit when l_sid.count() = 0;

    <<active_sessions>>
    for i in 1 .. l_sid.count() loop
      kill_session(l_inst_id(i), l_sid(i), l_serial(i));
    end loop active_sessions;
    
    dbms_session.sleep(5);
  end loop loop_until_all_killed;
end;
/
