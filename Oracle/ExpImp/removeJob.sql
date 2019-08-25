set serverout on
--add drop job by zhangl
prompt drop job
declare
  i integer := 0;
begin
  -- job
  i := 0;
  for i_job in (select job from user_jobs) loop
    dbms_job.remove(i_job.job);
	commit;
	i := i+1;
  end loop;
  dbms_output.put_line(i || ' job droped.');
  exception
  when others then
    dbms_output.put_line('drop pkg/p error:ora' || sqlcode);
end;
/
quit;