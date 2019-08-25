create or replace function f_GetStr(f_String   in varchar2,
                                    f_BeginStr in varchar2,
                                    f_EndStr   in varchar2) return varchar2 is
  v_Return   varchar2(400);
  v_BeginIdx number;
  v_EndIdx   number;
begin
  v_BeginIdx := instr(f_String, f_BeginStr);
  v_EndIdx   := instr(f_String, f_EndStr);
  v_Return   := substr(f_String,
                       v_BeginIdx + 1,
                       v_EndIdx - (v_BeginIdx + 1));
  return v_Return;
exception
  when others then
    raise_application_error(sqlcode, sqlerrm);
end;
/
