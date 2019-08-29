create or replace procedure sp_Log(p_type      in number,
                                   p_operation in varchar2,
                                   p_status    in number,
                                   p_logmsg    in varchar2,
                                   p_sqltext   in clob default '',
                                   p_logcode   in number) as
  pragma autonomous_transaction;
begin
  insert into log_operation
    (type, operation, status, logmsg, logcode, sqltext)
  values
    (p_type, p_operation, p_status, p_logmsg, p_logcode, p_sqltext);
  commit;
exception
  when others then
    commit;
end;
/
