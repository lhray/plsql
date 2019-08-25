create or replace procedure sp_fam_unite(p_crtid   in number,
                                        p_id      in number,
                                        p_result  out sys_refcursor,
                                        p_sqlcode out number) authid current_user as
/******
Created by JyLiu on date 14/06/2012 select unite of product ,convert one row to multi  rows
******/
  v_Sql varchar2(2000);

begin
  p_sqlcode := 0;
  v_sql := 'select a.rfc_em_addr, a.ident_crt,a.numero_crt, vct_em_addr, val,lib_crt,c.unite  Ratio
      from rfc a left join vct b  on a.vct10_em_addr = b.vct_em_addr
      left join (select id, unite  from (
               select 1 id, unite_1 unite  from fam  where fam_em_addr = :1 union all
                  select 2, unite_2        from fam  where fam_em_addr = :1 union all
                  select 3, unite_3        from fam  where fam_em_addr = :1 union all
                  select 4, unite_4        from fam  where fam_em_addr = :1 union all
                  select 5, unite_5        from fam  where fam_em_addr = :1 union all
                  select 6, unite_6        from fam  where fam_em_addr = :1 union all
                  select 7, unite_7        from fam  where fam_em_addr = :1 union all
                  select 8, unite_8        from fam  where fam_em_addr = :1 union all
                  select 9, unite_9        from fam  where fam_em_addr = :1 union all
                  select 10, unite_10      from fam  where fam_em_addr = :1)
                 ) c
      on a.numero_crt - 68 = c.id
   where id_crt = :2
     and (num_crt = 69 or num_crt = 68)
     and fam7_em_addr = :1';

  open p_result for v_sql
    using p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_crtid,p_id;

exception
  when others then
    p_sqlcode :=sqlcode;
end;
/
