create or replace view v_aggregatenode as
select
p. prv_em_addr
,p.prv_cle
,s.sel_em_addr
,s.sel_cle
,s.sel_desc
from prvsel ps,sel s,prv p
 where ps.sel16_em_addr=s.sel_em_addr
 and ps.prv15_em_addr=p.prv_em_addr
  and s.sel_bud=71
order by prv15_em_addr,sel_em_addr;
