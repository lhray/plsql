create or replace view v_prvtoseltopvt as
select
p.prv15_em_addr prvID,p.sel16_em_addr selID,r.pvt14_em_addr pvtID
from prvsel p,rsp r
where p.sel16_em_addr=r.sel13_em_addr;
