create or replace view v_attrvalue as
select  v.id_crt,r.fam7_em_addr,r.geo8_em_addr,r.dis9_em_addr, v.num_crt, v.vct_em_addr,val,lib_crt
          from rfc r, vct v
         where r.vct10_em_addr = v.vct_em_addr;
         
         
    
