create or replace view fmv_geodvs as
select geo39_em_addr dimensionID,gv.n0_dvs+1 NO,d.dvs_em_addr coefficientID,d.dvs_cle Key,d.dvs_desc Description
from geodvs gv ,dvs d
where  gv.dvs38_em_addr=d.dvs_em_addr
and gv.dvs38_em_addr>1;


