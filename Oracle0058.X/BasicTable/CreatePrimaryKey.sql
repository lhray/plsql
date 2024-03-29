alter table ADR_EMAIL add constraint PK_ADR_EMAIL primary key (ADR_EMAIL_EM_ADDR);
alter table BDG   add constraint PK_BDG primary key (BDG_EM_ADDR);  
alter table BDG_TASK add constraint PK_BDG_TASK primary key (BDG_TASK_EM_ADDR);
alter table BGC add constraint PK_BGC primary key (BGC_EM_ADDR);
alter table BUD add constraint PK_BUD primary key (BUD_EM_ADDR);
alter table CDT   add constraint PK_CDT primary key (CDT_EM_ADDR);
alter table CRTSERIE add constraint PK_CRTSERIE primary key (CRTSERIE_EM_ADDR);
alter table DETAILREMARK add constraint PK_DETAILREMARK primary key (DETAILREMARK_EM_ADDR);
alter table DETAILSERIE add constraint PK_DETAILSERIE primary key (DETAILSERIE_EM_ADDR);  
alter table DETAILTYPE add constraint PK_DETAILTYPE primary key (DETAILTYPE_EM_ADDR);
alter table DETAIL_SEQUENCE add constraint PK_DETAIL_SEQUENCE primary key (DETAIL_SEQUENCE_EM_ADDR);
alter table DIS add constraint PK_DIS primary key (DIS_EM_ADDR);
alter table DON add constraint PK_DON primary key (DON_EM_ADDR);
alter table DON_BUDGET add constraint PK_DON_BUDGET primary key (DON_BUDGET_EM_ADDR);
alter table DON_NMC add constraint PK_DON_NMC primary key (DON_NMC_EM_ADDR);
alter table DON_PVT add constraint PK_DON_PVT primary key (DON_PVT_EM_ADDR);
alter table DON_SEL add constraint PK_DON_SEL primary key (DON_SEL_EM_ADDR);
alter table DON_SUPPLIER add constraint PK_DON_SUPPLIER primary key (DON_SUPPLIER_EM_ADDR);
alter table DVS add constraint PK_DVS primary key (DVS_EM_ADDR);
alter table EXO add constraint PK_EXO primary key (EXO_EM_ADDR);
alter table FAM add constraint PK_FAM primary key (FAM_EM_ADDR);
alter table FAMTRF add constraint PK_FAMTRF primary key (FAMTRF_EM_ADDR);
alter table FORM add constraint PK_FORM primary key (FORM_EM_ADDR);
alter table FORM_CRTSERIE add constraint PK_FORM_CRTSERIE primary key (FORM_CRTSERIE_EM_ADDR);
alter table FORM_VCT add constraint PK_FORM_VCT primary key (FORM_VCT_EM_ADDR);
alter table GEO add constraint PK_GEO primary key (GEO_EM_ADDR);
alter table GEODVS add constraint PK_GEODVS primary key (GEODVS_EM_ADDR);
alter table IST add constraint PK_IST primary key (IST_EM_ADDR);
alter table IST_CRTSERIE add constraint PK_IST_CRTSERIE primary key (IST_CRTSERIE_EM_ADDR);
alter table IST_DIS add constraint PK_IST_DIS primary key (IST_DIS_EM_ADDR);
alter table IST_FAM add constraint PK_IST_FAM primary key (IST_FAM_EM_ADDR);
alter table IST_GEO add constraint PK_IST_GEO primary key (IST_GEO_EM_ADDR);
alter table IST_PARAM add constraint PK_IST_PARAM primary key (IST_PARAM_EM_ADDR);
alter table IST_PRV add constraint PK_IST_PRV primary key (IST_PRV_EM_ADDR);
alter table IST_SCL add constraint PK_IST_SCL primary key (IST_SCL_EM_ADDR);
alter table IST_SEL add constraint PK_IST_SEL primary key (IST_SEL_EM_ADDR);
alter table IST_VCT add constraint PK_IST_VCT primary key (IST_VCT_EM_ADDR);
alter table LANGUE add constraint PK_LANGUE primary key (LANGUE_EM_ADDR);
alter table LIB_FAM_LANGUE add constraint PK_LIB_FAM_LANGUE primary key (LIB_FAM_LANGUE_EM_ADDR);
alter table LICENSE add constraint PK_LICENSE primary key (LICENSE_EM_ADDR);
alter table LIEN_ETAT_MAIL add constraint PK_LIEN_ETAT_MAIL primary key (LIEN_ETAT_MAIL_EM_ADDR);
alter table LOGSESSION add constraint PK_LOGSESSION primary key (LOGSESSION_EM_ADDR);
alter table MAIL_INFO add constraint PK_MAIL_INFO primary key (MAIL_INFO_EM_ADDR);
alter table MENU_DATA add constraint PK_MENU_DATA primary key (MENU_DATA_EM_ADDR);
alter table MENU_ITEM add constraint PK_MENU_ITEM primary key (MENU_ITEM_EM_ADDR);
alter table MOD add constraint PK_MOD primary key (MOD_EM_ADDR);
alter table MODSCL add constraint PK_MODSCL primary key (MODSCL_EM_ADDR);
alter table NCT add constraint PK_NCT primary key (NCT_EM_ADDR);
alter table NMC   add constraint PK_NMC primary key (NMC_EM_ADDR);
alter table OFDETAIL add constraint PK_OFDETAIL primary key (OFDETAIL_EM_ADDR);
alter table OFTASK add constraint PK_OFTASK primary key (OFTASK_EM_ADDR);
alter table OFTYPE add constraint PK_OFTYPE primary key (OFTYPE_EM_ADDR);
alter table OPTION_PARAM add constraint PK_OPTION_PARAM primary key (OPTION_PARAM_EM_ADDR);
alter table OPTION_PARAM_LNK add constraint PK_OPTION_PARAM_LNK primary key (OPTION_PARAM_LNK_EM_ADDR);
alter table OPTION_PRV add constraint PK_OPTION_PRV primary key (OPTION_PRV_EM_ADDR);
alter table OPTION_SEL add constraint PK_OPTION_SEL primary key (OPTION_SEL_EM_ADDR);
alter table OPT_SETUPTYPE_LNK add constraint PK_OPT_SETUPTYPE_LNK primary key (OPT_SETUPTYPE_LNK_EM_ADDR);
alter table PLAN_PVT add constraint PK_PLAN_PVT primary key (PLAN_PVT_EM_ADDR);
alter table PRB add constraint PK_PRB primary key (PRB_EM_ADDR);
alter table PRM add constraint PK_PRM primary key (PRM_EM_ADDR);
alter table PROFIL add constraint PK_PROFIL primary key (PROFIL_EM_ADDR);
alter table PROFIL_DATA add constraint PK_PROFIL_DATA primary key (PROFIL_DATA_EM_ADDR);
alter table PROFIL_LICENSE add constraint PK_PROFIL_LICENSE primary key (PROFIL_LICENSE_EM_ADDR);
alter table PROFIL_LNK add constraint PK_PROFIL_LNK primary key (PROFIL_LNK_EM_ADDR);
alter table PROFIL_PRO_LNK add constraint PK_PROFIL_PRO_LNK primary key (PROFIL_PRO_LNK_EM_ADDR);
alter table PROMO add constraint PK_PROMO primary key (PROMO_EM_ADDR);
alter table PROMO_GEO_LNK add constraint PK_PROMO_GEO_LNK primary key (PROMO_GEO_LNK_EM_ADDR);
alter table PROMO_PRO add constraint PK_PROMO_PRO primary key (PROMO_PRO_EM_ADDR);
alter table PROMO_PROFIL add constraint PK_PROMO_PROFIL primary key (PROMO_PROFIL_EM_ADDR);
alter table PROMO_PROFIL_QTY add constraint PK_PROMO_PROFIL_QTY primary key (PROMO_PROFIL_QTY_EM_ADDR);
alter table PROMO_PROMO_PRO_LNK add constraint PK_PROMO_PROMO_PRO_LNK primary key (PROMO_PROMO_PRO_LNK_EM_ADDR);
alter table PROMO_PRO_CRTSERIE add constraint PK_PROMO_PRO_CRTSERIE primary key (PROMO_PRO_CRTSERIE_EM_ADDR);
alter table PROMO_PRO_VCT add constraint PK_PROMO_PRO_VCT primary key (PROMO_PRO_VCT_EM_ADDR);
alter table PRV add constraint PK_PRV primary key (PRV_EM_ADDR);
alter table PRVSEL add constraint PK_PRVSEL primary key (PRVSEL_EM_ADDR);
alter table pvt  add constraint PK_pvt primary key (PVT_EM_ADDR);
alter table PVTCRT add constraint PK_PVTCRT primary key (PVTCRT_EM_ADDR);
alter table RBP add constraint PK_RBP primary key (RBP_EM_ADDR);
alter table RFC add constraint PK_RFC primary key (RFC_EM_ADDR);
alter table RMS add constraint PK_RMS primary key (RMS_EM_ADDR);
alter table RPD add constraint PK_RPD primary key (RPD_EM_ADDR);
alter table RPE add constraint PK_RPE primary key (RPE_EM_ADDR);
alter table RSP add constraint PK_RSP primary key (RSP_EM_ADDR);

alter table  rsp add constraint uidx_rsp_sel_pvt unique (SEL13_EM_ADDR,PVT14_EM_ADDR);

alter table SCL add constraint PK_SCL primary key (SCL_EM_ADDR);
alter table SEL   add constraint PK_SEL primary key (SEL_EM_ADDR);
alter table SELCRT add constraint PK_SELCRT primary key (SELCRT_EM_ADDR);
alter table SELECT_SEL   add constraint PK_SELECT_SEL primary key (SELECT_SEL_EM_ADDR);
alter table SERIE_BUDGET add constraint PK_SERIE_BUDGET primary key (SERIE_BUDGET_EM_ADDR);
alter table SERIE_NMC add constraint PK_SERIE_NMC primary key (SERIE_NMC_EM_ADDR);
alter table SERIE_PVT add constraint PK_SERIE_PVT primary key (SERIE_PVT_EM_ADDR);
alter table SERIE_SEL add constraint PK_SERIE_SEL primary key (SERIE_SEL_EM_ADDR);
alter table SERIE_SUPPLIER add constraint PK_SERIE_SUPPLIER primary key (SERIE_SUPPLIER_EM_ADDR);
alter table SERIE_SUPPLIER2 add constraint PK_SERIE_SUPPLIER2 primary key (SERIE_SUPPLIER2_EM_ADDR);
alter table SERINOTE add constraint PK_SERINOTE primary key (SERINOTE_EM_ADDR);
alter table SETUPTYPE add constraint PK_SETUPTYPE primary key (SETUPTYPE_EM_ADDR);
alter table SUPPLIER   add constraint PK_SUPPLIER primary key (SUPPLIER_EM_ADDR);
alter table SYSMESSAGE add constraint PK_SYSMESSAGE primary key (SYSMESSAGE_EM_ADDR);
alter table TB_VERSION add constraint PK_RM_TB_VERSION primary key (ID);
alter table TIMENOTE add constraint PK_TIMENOTE primary key (TIMENOTE_EM_ADDR);
alter table TRF add constraint PK_TRF primary key (TRF_EM_ADDR);
alter table TYPENOTE add constraint PK_TYPENOTE primary key (TYPENOTE_EM_ADDR);
alter table TYPE_SEQUENCE add constraint PK_TYPE_SEQUENCE primary key (TYPE_SEQUENCE_EM_ADDR);
alter table TYPE_STOCK add constraint PK_TYPE_STOCK primary key (TYPE_STOCK_EM_ADDR);
alter table USER_ add constraint PK_USER_ primary key (USER__EM_ADDR);
alter table USER_DATA add constraint PK_USER_DATA primary key (USER_DATA_EM_ADDR);
alter table USER_LICENSE add constraint PK_USER_LICENSE primary key (USER_LICENSE_EM_ADDR);
alter table USER_SEL add constraint PK_USER_SEL primary key (USER_SEL_EM_ADDR);
alter table VCT add constraint PK_VCT primary key (VCT_EM_ADDR);
alter table continuation_node add constraint pk_continuation_node primary key (nodeid,type);
alter table DON_W add constraint PK_DON_w primary key (DON_WID);
alter table prb_W add constraint PK_prb_w primary key (prb_WID);
alter table bud_W add constraint PK_bud_w primary key (bud_WID);
alter table DON_M add constraint PK_DON_M primary key (DON_MID);
alter table PRB_M add constraint PK_PRB_M primary key (PRB_MID);
alter table BUD_M add constraint PK_BUD_M primary key (BUD_MID);

alter table TRF_M add constraint PK_TRF_M primary key (ID);
alter table TRF_W add constraint PK_TRF_W primary key (ID);
alter table DVS_M
  add constraint PK_DVS_M primary key (ID);
alter table DVS_W
  add constraint PK_DVS_W primary key (ID);
alter table RMS_M
  add constraint PK_RMS_M primary key (ID);
alter table RMS_W
  add constraint PK_RMS_W primary key (ID);
alter table SCL_M
  add constraint PK_SCL_M primary key (ID);
alter table SCL_W
  add constraint PK_SCL_W primary key (ID);

alter table mod_drp add constraint pk_mod_drp primary key (mod_em_addr);
alter table mod_Forecast add constraint mod_Forecast primary key (mod_em_addr);
alter table LOG_OPERATION_LEVEL  add constraint pk_log_operation_level primary key (LLEVEL);