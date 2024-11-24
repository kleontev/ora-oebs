with
top_n(n) as (
  select 50 from dual
),
date_range(start_date, end_date) as (
  select
    add_months(trunc(sysdate), -6),
    trunc(sysdate)
  from dual
),
sid_to_request_id as (
  select distinct cs.inst_id, cs.sid, cs.serial#, cs.request_id
  from date_range dr
  join system.fnd_concurrent_sessions cs on cs.v_timestamp between dr.start_date and dr.end_date
),
request_id_to_concurrent_program as (
  select
    r.request_id,
    trunc(r.request_date, 'MM') request_date_mm,
    cp.concurrent_program_name,
    cptl.user_concurrent_program_name,
    a.application_short_name,
    case when cp.concurrent_program_name like 'XXHR%' then 'Y' else 'N' end is_xxhr,
    r.actual_start_date,
    r.actual_completion_date
  from apps.fnd_concurrent_requests r
  join apps.fnd_concurrent_programs cp on 1 = 1
    and cp.application_id = r.program_application_id
    and cp.concurrent_program_id = r.concurrent_program_id
  join apps.fnd_application a on a.application_id = cp.application_id
  left join apps.fnd_concurrent_programs_tl cptl on 1 = 1
    and cptl.application_id = cp.application_id
    and cptl.concurrent_program_id = cp.concurrent_program_id
    and cptl.language = 'RU'
),
sample_count_by_sid as (
  select
    ash.instance_number as inst_id,
    ash.session_id as sid,
    ash.session_serial# as serial#,
    count(*) as sample_count
  from date_range dr
  left join dba_hist_snapshot s on 1 = 1
   and s.begin_interval_time <= dr.end_date
   and dr.start_date <= s.end_interval_time
  left join dba_hist_active_sess_history ash on 1 = 1
    and ash.snap_id = s.snap_id
    and ash.instance_number = s.instance_number
    and ash.dbid = s.dbid
  where 1 = 1
    and ash.sql_id in (
      -- various fnd_profile.value queries
      '9k7gd9pn661pj',
      '05xb5jwcc12ng',
      'gj84g2yx56u2q',
      '0fmdxmf0k3u4z',
      '6f8nnh3kc2syp',
      '7qwsx7rw0s3a5',
      '0mcwzxa8uk2n4'
    )
  group by
    ash.instance_number,
    ash.session_id,
    ash.session_serial#
),
top_n_offenders_per_month as (
  select *
  from (
    select --+ ordered
      rc.request_date_mm,
      rc.application_short_name,
      rc.concurrent_program_name,
      any_value(rc.is_xxhr) as is_xxhr,
      any_value(rc.user_concurrent_program_name) as user_concurrent_program_name,
      max(rc.request_id) keep(dense_rank last order by rc.actual_completion_date - rc.actual_start_date nulls first) request_id_with_max_duration,
      sum(sc.sample_count) as sum_sample_count,
      count(distinct rc.request_id) as request_count,
      -- общий топ
      row_number() over(partition by rc.request_date_mm                        order by sum(sc.sample_count) desc) rank_by_month,
      -- топ xxhr и не-xxhr отдельно
      row_number() over(partition by rc.request_date_mm, any_value(rc.is_xxhr) order by sum(sc.sample_count) desc) rank_by_month_and_xxhr
    from sample_count_by_sid sc
    join sid_to_request_id sr on 1 = 1
      and sr.inst_id = sc.inst_id
      and sr.sid = sc.sid
      and sr.serial# = sc.serial#
    join request_id_to_concurrent_program rc on 1 = 1
      and rc.request_id = sr.request_id
    group by
      rc.request_date_mm,
      rc.application_short_name,
      rc.concurrent_program_name
  ) t
  -- конкаррент попадает в один из двух топов
  -- медленно, ну и бог с ним
  where least(rank_by_month, rank_by_month_and_xxhr) <= (select n from top_n)
)
select
  t.*,
  case when t.rank_by_month          <= (select n from top_n) then 'Y' else 'N' end in_top_by_month,
  case when t.rank_by_month_and_xxhr <= (select n from top_n) then 'Y' else 'N' end in_top_by_month_and_xxhr
from top_n_offenders_per_month t