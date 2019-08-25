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

prompt drop packages procedures
declare
  i integer := 0;
begin
  --packages
  for i_pkg in (select distinct object_name name
                  from user_procedures
                 where object_type = 'PACKAGE') loop
    execute immediate 'drop package ' || i_pkg.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' package droped.');

  --procedure
  i := 0;
  for i_p in (select distinct object_name name
                from user_procedures
               where object_type = 'PROCEDURE') loop
    execute immediate 'drop procedure ' || i_p.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' procedure droped.'); 
exception
  when others then
    dbms_output.put_line('drop pkg/p error:ora' || sqlcode);
end;
/

prompt drop materialized view 
declare
  i integer := 0;
begin
  --mview
  for i_mv in (select mview_name name from user_mviews) loop
    execute immediate 'drop materialized view ' || i_mv.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' mview droped.');

  --mview log
  i := 0;
  for i_mvlog in (select master name from user_mview_logs) loop
    execute immediate 'drop materialized view log on  ' || i_mvlog.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' mview log  droped.');
exception
  when others then
    dbms_output.put_line('drop mview/log error:ora' || sqlcode);
end;
/

prompt drop view
declare
  i integer := 0;
begin
  --view
  for i_v in (select view_name name from user_views) loop
    execute immediate 'drop view ' || i_v.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' view droped.');
exception
  when others then
    dbms_output.put_line('drop view error:ora' || sqlcode);
end;
/

prompt drop function

declare
  i integer := 0;
begin
  --functions
  for i_f in (select distinct object_name name
                from user_procedures
               where object_type = 'FUNCTION') loop
    execute immediate 'drop function ' || i_f.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' function droped.');
exception
  when others then
    dbms_output.put_line('drop function error:ora' || sqlcode);
end;
/


prompt drop sequence

declare
  i integer := 0;
begin
  --sequence
  for i_seq in (select sequence_name name  from user_sequences) loop
    execute immediate 'drop sequence ' || i_seq.name ;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' sequence droped.');
exception
  when others then
    dbms_output.put_line('drop sequence error:ora' || sqlcode);
end;
/


prompt drop temorary tables and tables 

declare
  i integer := 0;
begin
  --table
  for i_tab in (select table_name name from user_tables) loop
    begin
      i := i + 1;
      execute immediate 'drop table ' || i_tab.name ||
                        '  cascade constraints  purge';
    exception
      when others then
        --if sqlcode = -24005 then
          dbms_output.put_line('table ' || i_tab.name || ' cannot droped');
          i := i - 1;
        --end if;
    end;
  
  end loop;
  dbms_output.put_line(i || ' table droped.');
exception
  when others then
    dbms_output.put_line('drop table error:ora' || sqlcode);
end;
/
spool off