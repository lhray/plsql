create or replace function F_IDleveltofield(P_DetailorAgg in varchar2, --1:DetailNode,2:AggNode
                                            f_table       varchar2, --fam,geo,dis
                                            f_ID    in number,
                                            f_level in number, -->=1
                                            f_field in varchar2 --f_cle,f_desc,f_desc_court
                                            ) RETURN varchar2 IS

  v_return varchar2(60);
  v_strsql varchar2(200);
  v_level  int;
  i        int;
begin
  v_level := f_level;
  
  if f_table = 'fam' then
    i := 0;
  elsif f_table = 'geo' then
    i := 1;
  elsif f_table = 'dis' then
    i := 2;
  end if;

  /*  select * from v_fam_tree
  where   nlevel =2
  start with fam_em_addr=22 connect by prior fam0_em_addr=fam_em_addr*/

  if f_ID is not null then
    if P_DetailorAgg = 2 then
      --2:AggNode
      v_strsql := 'select nlevel-1+' || f_level || ' from v_' || f_table ||
                  '_tree where ' || f_table || '_em_addr=' || f_ID;
      execute immediate v_strsql
        into v_level;
    end if;
  
    v_strsql := ' select nvl(max(' || f_field || '),'''') from v_' ||
                f_table || '_tree
          where   nlevel =' || v_level || '
         start with ' || f_table || '_em_addr=' || f_ID ||
                ' connect by prior ' || f_table || i || '_em_addr=' ||
                f_table || '_em_addr';
  
    execute immediate v_strsql
      into v_return;
  end if;

  RETURN v_return;

exception
  when others then
    raise_application_error(-20004, sqlerrm);
end;
/
