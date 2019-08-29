create or replace view v_pvt_three_key
as
select t.pvt_em_addr,t.pvt_cle,t1.f_cle,t2.g_cle,t3.d_cle
from pvt t left outer join fam t1
on t.fam4_em_addr=t1.fam_em_addr
left outer join geo t2
on t.geo5_em_addr=t2.geo_em_addr
left outer join dis t3
on t.dis6_em_addr=t3.dis_em_addr;