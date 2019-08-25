create or replace procedure p_ImpData2IOT as
  v_sqlcode number;
begin
  for i in (select table_name
              from user_tables
             where table_name like '%88'
             order by 1) loop
    begin
      execute immediate 'insert into  ' ||
                        substr(i.table_name, 1, length(i.table_name) - 2) ||
                        ' select * from  ' || i.table_name;
      execute immediate 'drop table ' || i.table_name;
    exception
      when others then
        v_sqlcode := sqlcode;
    end;
  end loop;
exception
  when others then
    v_sqlcode := sqlcode;
end;
/
