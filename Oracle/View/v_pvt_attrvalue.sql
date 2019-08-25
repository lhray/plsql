create or replace view v_pvt_attrvalue as
select  p.pvt35_em_addr,c.crtserie_em_addr,c.num_crt_serie,c.val_crt_serie,c.lib_crt_serie
          from pvtcrt p, crtserie c
         where p.crtserie36_em_addr=c.crtserie_em_addr;
