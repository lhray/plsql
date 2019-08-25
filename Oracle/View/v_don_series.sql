create or replace view v_don_series as
select d."DON_EM_ADDR",d."ANNEE",f_convert_1E20(d.M_1) M_1
,f_convert_1E20(d.M_2) M_2
,f_convert_1E20(d.M_3) M_3
,f_convert_1E20(d.M_4) M_4
,f_convert_1E20(d.M_5) M_5
,f_convert_1E20(d.M_6) M_6
,f_convert_1E20(d.M_7) M_7
,f_convert_1E20(d.M_8) M_8
,f_convert_1E20(d.M_9) M_9
,f_convert_1E20(d.M_10) M_10
,f_convert_1E20(d.M_11) M_11
,f_convert_1E20(d.M_12) M_12
,f_convert_1E20(d.M_13) M_13
,f_convert_1E20(d.M_14) M_14
,d."RPD18_EM_ADDR",d."RPD18_EM_ADDR_ORD",r.num_serie,r.pvt17_em_addr
from don d,rpd r
where d.rpd18_em_addr=r.rpd_em_addr;
