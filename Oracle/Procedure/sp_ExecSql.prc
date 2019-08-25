create or replace procedure sp_ExecSql(p_Sql in clob) as
begin
  execute immediate p_Sql;
  commit;
exception
  when others then
    rollback;
    Fmp_Log.LOGERROR;
    raise_application_error(-20004, sqlcode || ';' || sqlerrm);
end;
/
