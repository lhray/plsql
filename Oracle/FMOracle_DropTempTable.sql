declare
   v_strsql varchar2(1000):='';
begin
   for v_cur in (select * from user_tables t where t.table_name like 'TBMID%') loop
   v_strsql:= 'drop table '||v_cur.table_name || ' purge';
    execute immediate v_strsql;
   end loop;
end;
/
exit