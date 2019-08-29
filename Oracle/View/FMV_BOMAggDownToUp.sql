create or replace view fmv_bomaggdowntoup as
select
vn.prv_cle Key
,NL.aggnodeid fromID
,Nl.name fromName
,n.nmc_field
,d.sel_em_addr ToID
,vn.sel_cle ToName
,n.qute
from v_aggregatenode VN join v_aggnodetodimension D on vn.sel_em_addr=d.sel_em_addr
join NMC n on n.fils_pro_nmc=d.fam4_em_addr and n.nmc_field<>83
join v_aggnodewithlevel NL on NL.PID=N.pere_pro_nmc
    and nvl(NL.STID,0)=nvl(D.geo5_em_addr,0)
    and nvl(NL.TCID,0)=nvl(D.dis6_em_addr,0);
