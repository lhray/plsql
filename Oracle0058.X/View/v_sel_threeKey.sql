create or replace view v_sel_threeKey
 as
select n.sel_em_addr,s.sel_cle,f.f_cle,g.g_cle,d.d_cle
from v_aggnodetodimension n
left outer join  fam f
on n.fam4_em_addr=f.fam_em_addr
left outer join  geo g
on n.geo5_em_addr=g.geo_em_addr
left outer join  dis d
on n.dis6_em_addr=d.dis_em_addr
left outer join sel s
on n.sel_em_addr=s.sel_em_addr; 