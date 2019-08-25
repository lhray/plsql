create or replace view fmv_uomvct as
select 
tt.dimensionID,tt.No,Tt.Unite 
,v.vct_em_addr coefficientID 
,v.val Key 
,v.val Description
,v.num_crt
from 
( select 
  r.fam7_em_addr dimensionID 
  ,r.numero_crt-68 NO 
  ,f.unite 
  from rfc r left join fmv_productUnite_inrow f 
  on r.fam7_em_addr=f.fam_em_addr 
  AND r.numero_crt-68=f.NO 
  where r.ident_crt=70 and r.numero_crt>=69 
  union 
  select fam_em_addr,NO,f.unite 
  from fmv_productUnite_inrow f 
 ) tt
left outer join rfc r
on r.fam7_em_addr=tt.dimensionid
and r.numero_crt-68=tt.NO 
and r.ident_crt=70 and r.numero_crt>=69 
left outer join vct v 
on r.vct10_em_addr=v.vct_em_addr 
and v.id_crt=80 and v.num_crt=69;
