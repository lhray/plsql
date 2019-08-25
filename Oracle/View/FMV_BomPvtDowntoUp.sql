create or replace view fmv_bompvtdowntoup as
select
s.sel_cle Key
,p1.pvt_em_addr FromID
,p1.pvt_cle FromName
,n.nmc_field
--,f.f_cle
--,f.f_desc
--,n.fils_pro_nmc famID
--,p.geo5_em_addr geoID
--,p.dis6_em_addr disID
,p.pvt_em_addr ToID
,p.pvt_cle     ToName
 ,p1.pvt_desc  ToDesc
,n.qute
from sel s join rsp r on s.sel_bud=0 and s.sel_em_addr=r.sel13_em_addr
join pvt p on r.pvt14_em_addr=p.pvt_em_addr
--join fam f on p.fam4_em_addr=f.fam_em_addr
join NMC n on n.fils_pro_nmc=p.fam4_em_addr and n.nmc_field<>83
left join pvt p1 on p.geo5_em_addr=p1.geo5_em_addr
    and p.dis6_em_addr=p1.dis6_em_addr and n.pere_pro_nmc=p1.fam4_em_addr
--left join fam f1 on f1.fam_em_addr=n.pere_pro_nmc;
