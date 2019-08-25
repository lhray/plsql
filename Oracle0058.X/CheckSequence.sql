declare
  v_tab_max_value   number;
  v_last_seq_number number;
  v_seq_name        varchar2(128);
  v_sqltext         varchar2(8000);
  v_DDL             boolean := false;
begin
  for i_tablename in (select t.table_name
                        from user_tables t, user_sequences u
                       where t.table_name = substr(u.sequence_name, 5)
                         and (t.TABLE_NAME not like '%M' AND
                             t.TABLE_NAME not like '%W' AND
                             t.TABLE_NAME not like '%D')) loop
    v_sqltext := 'select nvl(max(' || i_tablename.table_name ||
                 '_em_addr),0) from ' || i_tablename.table_name;
    execute immediate v_sqltext
      into v_tab_max_value;
    --dbms_output.put_line(i_tablename.table_name||'''s max value is '||v_tab_max_value );
  
    v_seq_name := upper('SEQ_' || i_tablename.table_name);
    v_sqltext  := ' select u.last_number from user_sequences u where u.sequence_name=:1';
    execute immediate v_sqltext
      into v_last_seq_number
      using v_seq_name;
    --dbms_output.put_line(i_tablename.table_name || '''s Sequence value is ' || v_last_seq_number);
    if v_last_seq_number < v_tab_max_value then
      dbms_output.put_line(i_tablename.table_name ||
                           '''s sequence error.The last number is ' ||
                           v_last_seq_number || ' but the max values is ' ||
                           v_tab_max_value);
      if v_DDL then
        execute immediate 'drop sequence ' || v_seq_name;
        execute immediate 'create sequence ' || v_seq_name ||
                          ' minvalue 1 maxvalue 9999999999999999999999999999 start with ' ||
                          v_tab_max_value ||
                          ' increment by 1 cache 10000 order ';
      end if;
    end if;
  end loop;
  ---time series table 
  for i_tablename in (select t.table_name
                        from user_tables t, user_sequences u
                       where t.table_name = substr(u.sequence_name, 5)
                         and t.table_name in ('DON_M',
                                              'DON_W',
                                              'DON_D',
                                              'PRB_M',
                                              'PRB_w',
                                              'PRB_D',
                                              'BUD_M',
                                              'BUD_W',
                                              'BUD_D')) loop
    v_sqltext := 'select nvl(max(' || i_tablename.table_name ||
                 'id),0) from ' || i_tablename.table_name;
    execute immediate v_sqltext
      into v_tab_max_value;
    --dbms_output.put_line(i_tablename.table_name||'''s max value is '||v_tab_max_value );
  
    v_seq_name := upper('SEQ_' || i_tablename.table_name);
    v_sqltext  := ' select u.last_number from user_sequences u where u.sequence_name=:1';
    execute immediate v_sqltext
      into v_last_seq_number
      using v_seq_name;
    --dbms_output.put_line(i_tablename.table_name || '''s Sequence value is ' || v_last_seq_number);
    if v_last_seq_number < v_tab_max_value then
      dbms_output.put_line(i_tablename.table_name ||
                           '''s sequence error.The last number is ' ||
                           v_last_seq_number || ' but the max values is ' ||
                           v_tab_max_value);
      if v_ddl then
        execute immediate 'drop sequence ' || v_seq_name;
        execute immediate 'create sequence ' || v_seq_name ||
                          ' minvalue 1 maxvalue 9999999999999999999999999999 start with ' ||
                          v_tab_max_value ||
                          ' increment by 1 cache 10000 order ';
      end if;
    end if;
  end loop;

exception
  when others then
    dbms_output.put_line('sqlerror is ' || sqlcode);
end;

