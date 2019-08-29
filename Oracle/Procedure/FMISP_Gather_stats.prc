create or replace procedure FMISP_Gather_stats(pin_isjobrun  number default 0, -- if run job 
                                               pOut_nJobno   out number,
                                               pOut_nSqlCode out number,
                                               pOut_eSqlMsg  out varchar2)
--*****************************************************************
  -- Description: gather schema stats info
  -- Parameters:
  --      
  --     pin_isjobrun  number default 0, -- if run job \
  --     pOut_nJobno   out number,
  --     pOut_nSqlCode out number,
  --     pOut_eSqlMsg  out varchar2
  --
  -- Error Conditions Raised:
  --
  -- Author:     Yi.Zhu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        28-APR-2013     Yi.Zhu       Created
  -- **************************************************************                                               
 as
  v_jobno number;
begin
  pOut_nSqlCode := 0;
  if pin_isjobrun = 0 then
    dbms_stats.gather_schema_stats(ownname          => user,
                                   estimate_percent => 30,
                                   method_opt       => 'for all indexed columns',
                                   cascade          => true);
  else
    dbms_job.submit(v_jobno,
                    '
                    begin 
                       dbms_stats.gather_schema_stats(
                               ownname          => user,
                               estimate_percent => 30,
                               method_opt       => ''for all indexed columns'',
                               cascade          => true);
                    end;',
                    sysdate);
    commit;
    pOut_nJobno := v_jobno;
    --dbms_job.run(v_jobno);
  end if;
exception
  when others then
    pOut_nSqlCode := sqlcode;
    pOut_eSqlMsg  := sqlerrm;
end FMISP_Gather_stats;
/
