-------------------------------------------------------------------------
-- UnInstall.sql (sqlplus script)
-- The script is use to drop all objects in the current schema
-- The objects will be droped includes:table, index, sequence,temporary tables,function,view,materialized view,procedure,package

-- History 
--          JYLiu   11/16/2012   Create

-- Usage:
--      exec the script in SQL*PLUS on WIN.
--      locate your curent location to current directory then exec the script.(use cd command)
--      check the uninstall.log in the current directory 
-------------------------------------------------------------------------

set termout on
set serveroutput on


spool uninstll.log
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
-- drop all object
declare
  type t_cursor is ref cursor;
  c_obj      t_cursor;
  v_sql      varchar2(1000);
  nCount     number := 0;
  nFailCount number := 0;
begin
  loop
    select count(t.OBJECT_TYPE)
      into nCount
      from user_objects t
     where t.object_type <> 'INDEX'
       and t.object_type <> 'LOB'
       and t.object_type <> 'TABLE PARTITION';
    nFailCount := nFailCount + 1;
    if nCount = 0 or nFailCount = 10 then 
	-- there are no row or In ten cycles 
      exit; -- exit here
    end if;
    for c_obj in (select T.OBJECT_TYPE, T.OBJECT_NAME
                    from user_objects t
                   where t.object_type <> 'INDEX'
                     and t.object_type <> 'LOB'
                     and t.object_type <> 'TABLE PARTITION') loop
      begin
        v_sql := 'drop ' || c_obj.object_type || ' ' || c_obj.object_name;
        if c_obj.object_type = 'TABLE' then
          v_sql := v_sql || ' CASCADE CONSTRAINTS PURGE'; 
        end if;
        begin
          execute immediate v_sql;
        exception
          when others then
            null;
        end;
      end;
    end loop; --loop object
  end loop; --loop
end;
/
spool off