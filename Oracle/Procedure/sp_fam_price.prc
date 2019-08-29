create or replace procedure sp_fam_price(p_id      in number,
                                        p_result  out sys_refcursor,
                                        p_sqlcode out number) authid current_user as
/******
Created by JyLiu on date 14/06/2012 select price of product ,convert one row to multi  rows
******/
  v_Sql varchar2(2000);

begin
  p_sqlcode := 0;
  v_sql := 'select a.famtrf_em_addr, a.num_trf, b.trf_cle, b.trf_desc, c.unite ration
    from famtrf a
    left join trf b
      on a.trf34_em_addr = b.trf_em_addr
    left join (select id, unite  from (
               select 1 id, prix_1 unite  from fam  where fam_em_addr = :1 union all
                  select 2, prix_2        from fam  where fam_em_addr = :1 union all
                  select 3, prix_3        from fam  where fam_em_addr = :1 union all
                  select 4, prix_4        from fam  where fam_em_addr = :1 union all
                  select 5, prix_5        from fam  where fam_em_addr = :1 union all
                  select 6, prix_6        from fam  where fam_em_addr = :1 union all
                  select 7, prix_7        from fam  where fam_em_addr = :1 union all
                  select 8, prix_8        from fam  where fam_em_addr = :1 union all
                  select 9, prix_9        from fam  where fam_em_addr = :1 union all
                  select 10, prix_10      from fam  where fam_em_addr = :1)
                ) c
      on a.num_trf + 1 = c.id
   where fam33_em_addr = :1 ';

  open p_result for v_sql
    using p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id,p_id;

exception
  when others then
    p_sqlcode :=sqlcode;
end;
/
