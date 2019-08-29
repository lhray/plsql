create or replace view v_getattributebysel as
select s.sel_em_addr,c.rcd_cdt,c.n0_cdt,v.vct_em_addr,v.num_crt,v.val,v.lib_crt,v.id_crt
from sel s,cdt c,vct v
where s.sel_em_addr=c.sel11_em_addr
and c.adr_cdt=v.vct_em_addr;
