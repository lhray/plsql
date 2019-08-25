create or replace function F_IDtoLevel(f_table varchar2, --fam,geo,dis
                                       f_ID    in number
                                       
                                       ) RETURN number IS
---get the level according to the f_id
---f_id can be the id of product, sales territory and trade channel,
---the corresponding f_table should be fam, geo and dis
  v_return number;
  v_strsql varchar2(200);
  v_level  int;
begin
  if f_ID  is not null then
    v_strsql := 'select nlevel from v_' || f_table || '_tree where ' ||
                f_table || '_em_addr=' || f_ID;
    execute immediate v_strsql
      into v_return;
  end if;
  RETURN v_return;

exception
  when others then
    raise_application_error(-20004, sqlerrm);
end;
/
