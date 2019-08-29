create or replace view v_dis_tree as
select f."DIS_EM_ADDR",f."D_CLE",f."D_DESC",f."D_DESC_COURT",f."SUP_DIS",f."USER_CREATE_DIS",f."DATE_CREATE_DIS",f."USER_MODIFY_DIS",f."DATE_MODIFY_DIS",f."STATUS_DIS",f."DIS2_EM_ADDR",f."RMS40_EM_ADDR",f."DIS2_EM_ADDR_ORD",f."RMS40_EM_ADDR_ORD" ,
connect_by_root dis_em_addr grp ,connect_by_isleaf isleaf,max(level) over (partition by connect_by_root dis_em_addr )+1-level nlevel
  from dis f
 start with f.dis2_em_addr = 0
connect by   prior f.dis_em_addr =f.dis2_em_addr;
