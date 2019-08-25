create or replace view fmv_famtrf as
select dimensionID,NO,price
,f.trf_em_addr coefficientID
,f.trf_cle Key
,f.trf_desc Description
from (select 
  ft.fam33_em_addr dimensionID
  ,ft.num_trf+1 NO
  ,f.price
  from famtrf ft left join fmv_productprice_inrow f
  on ft.fam33_em_addr=f.fam_em_addr and ft.num_trf+1=f.NO
  union
  select fam_em_addr,NO, f.price
  from fmv_productprice_inrow f) t
  left join famtrf a on t.dimensionid=a.fam33_em_addr
  and t.NO=a.num_trf+1
  left join trf f on a.trf34_em_addr=f.trf_em_addr 
;

