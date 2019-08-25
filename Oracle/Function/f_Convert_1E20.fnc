create or replace function f_Convert_1E20(f_val number) return number is
/********
Created by jyliu on 8/13/2012 1E-20 is NULL.process null as per to config.
********/
begin
  if f_val = 1E-20 then
    return null;
  else
    return f_val;
  end if;
exception
  when others then
    raise_application_error(sqlcode, sqlerrm);
end;
/
