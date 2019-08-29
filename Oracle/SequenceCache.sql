/*************
add cache 
modify the start with to the current number 
*************/
declare
  v_sqltext_drop   varchar2(2000);
  v_sqltext_create varchar2(2000);
begin
  for i in (SELECT *
              from user_sequences u
             where u.CACHE_SIZE = 0
               and sequence_name like 'SEQ\_%' ESCAPE '\'
             order by 1) loop
  
    v_sqltext_drop   := 'drop sequence ' || i.sequence_name;
    v_sqltext_create := ' create sequence ' || i.sequence_name ||
                        ' minvalue 100 maxvalue 9999999999999999999999999999 start with ' ||
                        i.last_number ||
                        ' increment by 100 cache 10000 order ';
    execute immediate v_sqltext_drop;
    execute immediate v_sqltext_create;
  end loop;
exception
  when others then
    dbms_output.put_line(v_sqltext_drop);  
    dbms_output.put_line(v_sqltext_create);
    dbms_output.put_line(sqlcode);
end;
/