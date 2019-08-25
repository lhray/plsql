create unique index UIDX_BDG_BDG_CLE_KEY on BDG (ID_BDG, B_CLE) ;
create index idx_cdt_sel11_em_addr on cdt (sel11_em_addr) ;
create index idx_cdt_prv12_em_addr on cdt (prv12_em_addr) ;
create unique index UIDX_CRTSERIE_KEY on CRTSERIE (ID_CRT_SERIE, NUM_CRT_SERIE, VAL_CRT_SERIE) ;
create unique index UIDX_DIS_G_CLE_KEY on DIS (D_CLE) ;
create unique index UIDX_DVS_DVS_CLE_KEY on DVS (DVS_CLE) ;
create unique index UIDX_EXO_EXO_CLE_KEY on EXO (EXO_CLE) ;
create unique index UIDX_FAM_F_CLE_KEY on FAM (F_CLE);
create unique index UIDX_FORM_ETAT_CLE_KEY on FORM (ID_ETAT, CLE_ETAT);
create unique index UIDX_GEO_G_CLE_KEY on GEO (G_CLE);
create unique index UIDX_MAIL_INFO_KEY on MAIL_INFO (ID_TEMPLATE, CLE_TEMPLATE);
create unique index UIDX_NCT_NOM_CT_KEY on NCT (ID_CT, NUM_CT, NOM);
create unique index UIDX_OPTION_PARAM_KEY on OPTION_PARAM (ID_OPTION, CLE_OPTION);
create unique index UIDX_PROFIL_PROFIL_CLE_KEY on PROFIL (PROFIL_CLE);
create unique index UIDX_PRV_PRV_CLE_KEY on PRV (PRV_CLE);
create index idx_prvsel_prv_sel on prvsel (prv15_em_addr,sel16_em_addr);
create unique index UIDX_PVT_PVT_CLE_KEY on PVT (PVT_CLE);
create index idx_pvt_fam4_em_addr on pvt (fam4_em_addr);
create index idx_pvt_geo5_em_addr on pvt (geo5_em_addr);
create index idx_pvt_dis6_em_addr on pvt (dis6_em_addr);
create unique index UIDX_RMS_RMS_CLE_KEY on RMS (RMS_CLE);
alter table  rsp add constraint uidx_rsp_sel_pvt unique (SEL13_EM_ADDR,PVT14_EM_ADDR);
create unique index UIDX_SCL_SCL_CLE_KEY on SCL (SCL_CLE);
create unique index UIDX_PVT_SEL_DBG_KEY on SEL (SEL_BUD, SEL_CLE);
create unique index UIDX_SEL_SEL_CLE_KEY on SEL (SEL_CLE);
create unique index UIDX_SERIE_SUPPLIER_KEY on SERIE_SUPPLIER (PRO_CLE, GEO_CLE, DIS_CLE);
create unique index UIDX_SETUPTYPE_PVT_CLE_KEY on SETUPTYPE (SETUP_CLE);
create unique index UIDX_SYSMESSAGE_KEY on SYSMESSAGE (CODE_LANGUE, MESSAGE_ID);
create unique index UIDX_TRF_TRF_CLE_KEY on TRF (TRF_CLE);
create unique index UIDX_USER_ID_USER_KEY on USER_ (ID_USER);
create unique index UIDX_USER_USER_CLE_KEY on USER_ (USER_CLE);
create unique index UIDX_VCT_VAL_CT_KEY on VCT (ID_CRT, NUM_CRT, VAL);
create UNIQUE index uidx_pvt_pro_geo_dis_key on pvt (adr_pro ,adr_geo ,adr_dis );
create UNIQUE index uidx_supplier_adr_fils_bdg_key on supplier (id_supplier ,pere_bdg,fils_bdg );

create index idx_prvsel_prv on prvsel (prv15_em_addr);
create index idx_prvsel_sel on prvsel (sel16_em_addr);

create index idx_prb_rbp22_annee on prb(rbp22_em_addr,annee_prv);
create index idx_rbp_sel21_numprv on rbp(sel21_em_addr,num_prv);

create  index idx_BDGK_MOD on MOD (NUM_MOD, BDG30_EM_ADDR);


create index idx_aggruleid_ids_nodeid on aggregatenode_fullid(aggregationid,aggregatefullid,aggregatenodeid);
create index idx_rpd_pvt17_em_addr on rpd(pvt17_em_addr);
create index idx_rpd_pvtserisno on rpd(pvt17_em_addr,num_serie);
create index idx_rbp_sel21_en_addr on rbp(sel21_em_addr);
create index IDX_DON_RPD18_EM_ADDR on DON (RPD18_EM_ADDR);
create index idx_RFC_DIS on RFC (DIS9_EM_ADDR);
create index idx_RFC_FAM on RFC (FAM7_EM_ADDR);
create index idx_RFC_GEO on RFC (GEO8_EM_ADDR);

create index idx_prvselpvt_prvid on prvselpvt(prvid);
