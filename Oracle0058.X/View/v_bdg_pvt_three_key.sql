create or replace  view v_bdg_pvt_three_key
as
select n.bdg_em_addr as pvt_em_addr,m.pvt_cle,m.f_cle,m.g_cle,m.d_cle
from v_pvt_three_key m,
     bdg n
where m.pvt_cle=n.B_CLE
and n.id_bdg=80;