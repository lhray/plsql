create or replace procedure sp_Log(p_type      in number,
                                   p_operation in varchar2,
                                   p_status    in number,
                                   p_logmsg    in varchar2,
                                   p_sqltext   in clob default '',
                                   p_logcode   in number) as
  pragma autonomous_transaction;
  
   pLPARAMETERS TLOG.LPARAMETERS%TYPE := '';
   pLSQLTEXT    TLOG.LSQLTEXT%TYPE;
   
begin
  /*
   *  the p_type parameter: 1,information;2,error;3,warning;4,fatal;5,debug
   *  the p_operation parameter: procedure name or other operation
   *  the p_status parameter: 0 sucess;1 failed
   *  the p_logmsg parameter: log message
   *  the p_logcod parameter: eerror code
   *
   */
  IF (p_type=1) THEN
     PLOG.info(p_logmsg || ';p_logcode is ' || p_logcode,p_operation,p_sqltext);

  ELSIF (p_type=2) THEN
     PLOG.error(p_logmsg || ';p_logcode is ' || p_logcode,p_operation,p_sqltext);

  ELSIF (p_type=3) THEN
     PLOG.warn(p_logmsg || ';p_logcode is ' || p_logcode,p_operation,p_sqltext);
     
  ELSIF (p_type=4) THEN
     PLOG.fatal(p_logmsg || ';p_logcode is ' || p_logcode,p_operation,p_sqltext);   
     
  ELSIF (p_type=5) THEN
     PLOG.debug(p_logmsg || ';p_logcode is ' || p_logcode,p_operation,p_sqltext);      

  END IF;

  exception
     when others then
          PLOG.error;
          commit;
end;
/