create or replace procedure sp_refresh authid current_user as
begin
  for i in (select mview_name name from user_mviews) loop
    dbms_mview.refresh(i.name, '?');
  end loop;
exception
  when others then
    raise_application_error(-20004,sqlcode||sqlerrm);
end;
/
