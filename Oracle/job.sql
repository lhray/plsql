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
-- add end 
variable job number;
declare
  i integer := 0;
  
begin
  -- create job
  dbms_job.submit(job=>:job,
				    what=>'FMP_ClEARTBMID.FMSP_CLEARTBMID(pIn_vFmUser=>null,pIn_nInterval=>24);',
				    next_date=>trunc(sysdate)+1+1/12,
				    interval=>'trunc(sysdate)+1+1/12');
  commit;
  exception
  when others then
    null;
end;
/
