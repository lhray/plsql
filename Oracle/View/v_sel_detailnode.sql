create or replace view v_sel_detailnode as
select sel13_em_addr as sel_em_addr, p.pvt_em_addr, p.pvt_cle, p.pvt_desc, p.fam4_em_addr, p.geo5_em_addr, p.dis6_em_addr
  from pvt p, rsp r
 where p.pvt_em_addr = r.pvt14_em_addr order by sel_em_addr;
