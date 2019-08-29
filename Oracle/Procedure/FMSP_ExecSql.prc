create or replace procedure FMSP_ExecSql(pIn_cSql in clob) as

begin
  fmp_log.LOGDEBUG(pIn_cSqlText => pIn_cSql);
  execute immediate pIn_cSql;
  
  -- if you need test   modify Transaction_COMMIT = 0. 
  if p_constant.Transaction_COMMIT = 0 then
  commit;
  end if;
exception
  when others then
    fmp_log.LOGERROR(pIn_cSqlText => pIn_cSql);
    raise_application_error(-20004, sqlcode || ';' || sqlerrm);
end;
/
