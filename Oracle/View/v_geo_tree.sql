create or replace view v_geo_tree as
select f."GEO_EM_ADDR",f."G_CLE",f."G_DESC",f."G_DESC_COURT",f."SUP_GEO",f."USER_CREATE_GEO",f."DATE_CREATE_GEO",f."USER_MODIFY_GEO",f."DATE_MODIFY_GEO",f."GEO1_EM_ADDR",f."GEO1_EM_ADDR_ORD" ,connect_by_root geo_em_addr grp ,connect_by_isleaf isleaf,max(level) over (partition by connect_by_root geo_em_addr )+1-level nlevel
  from geo f
 start with f.geo1_em_addr = 0
connect by   prior f.geo_em_addr =f.geo1_em_addr;
