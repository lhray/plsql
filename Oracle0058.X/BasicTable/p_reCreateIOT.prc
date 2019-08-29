create or replace procedure p_reCreateIOT as
  v_sqlcode number;
begin
  for i in (select table_name
              from user_tables
             where table_name not like '%88'
               and temporary = 'N'
             order by 1) loop
    for j in (select u.CONSTRAINT_NAME
                from user_constraints u
               where u.TABLE_NAME = i.table_name) loop
      begin
        execute immediate 'alter table ' || i.table_name ||
                          ' drop constraint ' || j.constraint_name;
      exception
        when others then
          v_sqlcode := sqlcode;
      end;
    end loop;
    begin
      execute immediate 'rename ' || i.table_name || ' to ' || i.table_name || '88';
    exception
      when others then
        v_sqlcode := sqlcode;
    end;
  end loop;

  for k in (select u.INDEX_NAME
              from user_indexes u
             where u.index_type <> 'LOB') loop
    begin
      execute immediate 'drop index ' || k.index_name;
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
