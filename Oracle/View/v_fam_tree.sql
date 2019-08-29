create or replace view v_fam_tree as
select f."FAM_EM_ADDR",f."ID_FAM",f."F_CLE",f."F_DESC",f."F_DESC_COURT",f."PRIX_1",f."PRIX_2",f."PRIX_3",f."PRIX_4",f."PRIX_5",f."PRIX_6",f."PRIX_7",f."PRIX_8",f."PRIX_9",f."PRIX_10",f."UNITE_1",f."UNITE_2",f."UNITE_3",f."UNITE_4",f."UNITE_5",f."UNITE_6",f."UNITE_7",f."UNITE_8",f."UNITE_9",f."UNITE_10",f."SUP_FAM",f."USER_CREATE_FAM",f."DATE_CREATE_FAM",f."USER_MODIFY_FAM",f."DATE_MODIFY_FAM",f."FAM0_EM_ADDR",f."FAM37_EM_ADDR",f."NMC1_EM_ADDR",f."FAM0_EM_ADDR_ORD",f."FAM37_EM_ADDR_ORD",f."NMC1_EM_ADDR_ORD" ,connect_by_root fam_em_addr grp ,connect_by_isleaf isleaf,max(level) over (partition by connect_by_root fam_em_addr )+1-level nlevel
  from fam f
 start with f.fam0_em_addr = 0
connect by   prior f.fam_em_addr =f.fam0_em_addr;
