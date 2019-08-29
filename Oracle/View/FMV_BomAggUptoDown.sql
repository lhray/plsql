create or replace view fmv_bomagguptodown as
select
vn.prv_cle key
,d.sel_em_addr FromID
,vn.sel_cle fromname
,n.nmc_field
--,vn.sel_desc
--,n.fils_pro_nmc
,n.qute
,NL.aggnodeid ToID
,n.fils_pro_nmc PID
,d.geo5_em_addr STID
,d.dis6_em_addr TCID
--,NL.name
--,NL.description
--,f1.f_cle
,f2.f_cle
,f2.f_desc
/*,case when NL.name is null then
 REPLACE(vn.sel_cle,f1.f_cle,f2.f_cle)
else
 NL.name
end
  ToName
 ,case when NL.name is null then
 REPLACE(vn.sel_desc,f1.f_desc,f2.f_desc)
else
 NL.description
end
 ToDesc*/
from v_aggregatenode VN join v_aggnodetodimension D on vn.sel_em_addr=d.sel_em_addr
join NMC n on n.pere_pro_nmc=d.fam4_em_addr and n.nmc_field<>83
left join v_aggnodewithlevel NL on NL.PID=N.fils_pro_nmc
and nvl(NL.STID,0)=nvl(D.geo5_em_addr,0)
 and nvl(NL.TCID,0)=nvl(D.dis6_em_addr,0)
--left join fam f1 on d.fam4_em_addr=f1.fam_em_addr
left join fam f2 on N.fils_pro_nmc=f2.fam_em_addr;
