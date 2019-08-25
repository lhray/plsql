create or replace function F_leveltoKey(f_table varchar2,   --fam,geo,dis
                                        f_ID    in number,
                                        f_level in number,  -->=1
                                        f_class varchar2    --cle,desc,desc_court
                                        ) RETURN varchar2 IS

  v_return varchar2(60);
  v_field  varchar2(60);
  v_strsql varchar2(200);

begin

  if f_table = 'fam' then
    v_field := 'f_';
  elsif f_table = 'geo' then
    v_field := 'f_';
  elsif f_table = 'dis' then
    v_field := 'f_';
  end if;

  /*  select * from v_fam_tree
  where   nlevel =2
  start with fam_em_addr=22 connect by prior fam0_em_addr=fam_em_addr*/

  v_strsql := ' select ' || v_field || f_class || ' from v_' || f_table ||
              '_tree
          where   nlevel =' || f_level || '
         start with ' || f_table || '_em_addr=' || f_ID ||
              ' connect by prior ' || f_table || '0_em_addr=' || f_table ||
              '_em_addr';

  execute immediate v_strsql
    into v_return;

  RETURN v_return;

exception
  when others then
    raise_application_error(-20004, sqlerrm);
end;
/
