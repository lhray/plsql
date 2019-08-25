create or replace  view v_bdg_sel_three_key
as
select n.bdg_em_addr as sel_em_addr,m.sel_cle,m.f_cle,m.g_cle,m.d_cle
from v_sel_threeKey m,
     bdg n
where m.sel_cle=n.B_CLE
and n.id_bdg=71;