create or replace view v_aggnodetodimension as
select s.sel_em_addr,
       s.sel_cle,
       f_c.adr_cdt   fam4_em_addr,
       f_g.adr_cdt   geo5_em_addr,
       f_d.adr_cdt   dis6_em_addr
  from sel s
  left join cdt f_c
    on s.sel_em_addr = f_c.sel11_em_addr
   and f_c.rcd_cdt = 10000
   and f_c.n0_val_cdt = 0
  left join cdt f_g
    on s.sel_em_addr = f_g.sel11_em_addr
   and f_g.rcd_cdt = 10001
   and f_g.n0_val_cdt = 0
  left join cdt f_d
    on s.sel_em_addr = f_d.sel11_em_addr
   and f_d.rcd_cdt = 10002
   and f_d.n0_val_cdt = 0
 where sel_bud = 71;
