create or replace view v_sel_attrvalue as
select  s.sel53_em_addr,c.crtserie_em_addr,c.num_crt_serie,c.val_crt_serie,c.lib_crt_serie
          from selcrt s, crtserie c
         where s.crtserie54_em_addr=c.crtserie_em_addr;
