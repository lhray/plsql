create or replace view fmv_bompvtuptodown as
select
s.sel_cle  Key
,p.pvt_em_addr fromID
,p.pvt_cle fromName
--,f.f_cle
--,f.f_desc
,n.nmc_field
,n.fils_pro_nmc famID
,p.geo5_em_addr geoID
,p.dis6_em_addr disID
,p1.pvt_em_addr ToID
,case when p1.pvt_cle is null then
 REPLACE(p.pvt_cle,f.f_cle,f1.f_cle)
else
 p1.pvt_cle
end
 ToName
 ,case when p1.pvt_cle is null then
 REPLACE(p.pvt_desc,f.f_desc,f1.f_desc)
else
 p1.pvt_desc
end
 ToDesc
,n.qute
from sel s join rsp r on s.sel_bud=0 and s.sel_em_addr=r.sel13_em_addr
join pvt p on r.pvt14_em_addr=p.pvt_em_addr
join fam f on p.fam4_em_addr=f.fam_em_addr
join NMC n on n.pere_pro_nmc=p.fam4_em_addr and n.nmc_field<>83
left join pvt p1 on nvl(p.geo5_em_addr,0)=nvl(p1.geo5_em_addr,0)
    and nvl(p.dis6_em_addr,0)=nvl(p1.dis6_em_addr,0)
     and n.fils_pro_nmc=p1.fam4_em_addr
left join fam f1 on f1.fam_em_addr=n.fils_pro_nmc;
