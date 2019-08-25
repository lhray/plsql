create or replace view v_cdt as
select n0_cdt as attrordno,
       rcd_cdt as tabID,
       operant ope,
       n0_val_cdt val_idx,
       adr_cdt addr
  from cdt;
