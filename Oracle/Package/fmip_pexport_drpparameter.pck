create or replace package fmip_pexport_drpparameter is

  -- Author  : XFZHANG
  -- Created : 4/17/2013 11:16:35 AM
  -- Purpose :

  -- Public type declarations
  ANNEE_MIN           NUMBER DEFAULT 1980;
  nCommand201         NUMBER DEFAULT 201;
  nCommand211         NUMBER DEFAULT 211;
  vTmpAggTableName    varchar2(40) DEFAULT 'TB_TS_AggregateNodeCon';
  vTmpDetailTableName varchar2(40) DEFAULT 'TB_TS_DetailNodeSelCdt';
  -- Public function and procedure declarations

  PROCEDURE FMISP_ProcessExpDRPParameter(pIn_nCommandNumber in number,
                                         pIn_nChronology    in integer, --1 Month ,2 Week,3 Day
                                         pIn_vstrFMUSER     in varchar2,
                                         pIn_vStrOption     in varchar2,
                                         pIn_nDecimals      in number, --Decimals config
                                         pOut_vstrTableName out varchar2,
                                         pIn_nb_day_by_week in number,
                                         pIn_vNumMod        in varchar2,
                                         pIn_vSeperator     in varchar2 default ',',
                                         pIn_chSdlt         in char,
                                         pOut_nSqlCode      out integer);

end fmip_pexport_drpparameter;
/
create or replace package body fmip_pexport_drpparameter is

  -- Private type declarations

  -- Function and procedure implementations

  PROCEDURE FMISP_ProcessExpDRPParameter(pIn_nCommandNumber in number,
                                         pIn_nChronology    in integer,
                                         pIn_vstrFMUSER     in varchar2,
                                         pIn_vStrOption     in varchar2,
                                         pIn_nDecimals      in number, --Decimals config
                                         pOut_vstrTableName out varchar2,
                                         pIn_nb_day_by_week in number,
                                         pIn_vNumMod        in varchar2,
                                         pIn_vSeperator     in varchar2 default ',',
                                         pIn_chSdlt         in char,
                                         pOut_nSqlCode      out integer) IS
    --*****************************************************************
    -- Description:  Export drp parameter's data
    -- Parameters:
    --pIn_nChronology:1 monthly ;2 weekly; 4 daily
    -- Error Conditions Raised:
    --
    -- Author:      xf zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        18-APR-2013     xf zhang     Created.
    --  v7.0        31-May-2013     xf zhang     Modified. add switch a_m,aa_mm;
    --                                           Can export null drp parameters
    -- **************************************************************
    v_oOptions        P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType; --   switches of command line
    v_nSelOrAggRuleID integer := 0; --prv.prv_em_addr
    vStrSql           clob;
    nBdg              NUMBER;
    vTmpStr           VARCHAR2(2000);
    vSeperator        varchar2(30) := 'chr(44)||'; --default ,
    vSdlt             varchar2(10);
    chDataType        char(1);
    nASCII            number;
    vFormant          varchar2(120) := 'fm9999999999999999999990.00';
    sNodeList         sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
    vTabName          varchar2(30);
    vStrDateFormat    VARCHAR2(1000);
    vStrDateDebut     VARCHAR2(1000);
    nNb_DayByWeek     NUMBER;
  BEGIN
    IF pIn_nCommandNumber NOT IN (nCommand201, nCommand211) THEN
      RETURN;
    END IF;
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nCommandNumber);
    Fmp_log.FMP_SetValue(pIn_nChronology);
    Fmp_log.FMP_SetValue(pIn_vstrFMUSER);
    Fmp_log.FMP_SetValue(pIn_vStrOption);
    Fmp_Log.FMP_SetValue(pIn_nDecimals);
    Fmp_log.FMP_SetValue(pOut_vstrTableName);
    Fmp_log.FMP_SetValue(pIn_nb_day_by_week);
    Fmp_log.FMP_SetValue(pIn_vNumMod);
    Fmp_log.FMP_SetValue(pIn_vSeperator);
    Fmp_log.FMP_SetValue(pIn_chSdlt);
    Fmp_log.LOGBEGIN;
    if upper(pIn_chSdlt) = '"' then
      --if sdlt specified ,the text delimiter is '"'
      vSdlt := 'chr(34)';
    else
      -- otherwise  ''
      vSdlt := 'chr(null)';
    end if;
  
    case upper(pIn_vSeperator)
      when 'ESP' then
        --space
        vSeperator := 'chr(32)||';
      when 'SBS' then
        --the same as ESP
        vSeperator := 'chr(32)||';
      when 'STAB' then
        --tab
        vSeperator := 'chr(9)||';
      else
        --ascii to decimal ,then to ascii
        nASCII     := ascii(nvl(pIn_vSeperator, ','));
        vSeperator := 'chr(' || nASCII || ')||';
    end case;
    --Parse options
    P_BATCHCOMMAND_COMMON.sp_ParseOptions(pIn_vStrOption,
                                          v_oOptions,
                                          pOut_nSqlCode);
    nNb_DayByWeek := pIn_nb_day_by_week;
    --if specified sel switch,export the specified selection's mod_drp,
    --if not specified sel switch,export all mod_drp
    IF v_oOptions.bSel THEN
      IF pIn_nCommandNumber = nCommand201 THEN
        select nvl(max(sel.sel_em_addr), 0)
          into v_nSelOrAggRuleID
          from sel
         where sel.sel_cle = v_oOptions.strSel;
      
        --detail node
        vTmpStr := 'with tab as(
select e.f_cle, e.g_cle, e.d_cle, m.*
from sel s join rsp r on s.sel_em_addr=r.sel13_em_addr  join pvt p on r.pvt14_em_addr=p.pvt_em_addr
     join bdg b on p.pvt_cle=b.b_cle left  join (select * from mod_drp where num_mod=' ||
                   pIn_vNumMod ||
                   ') m on b.bdg_em_addr=m.bdg_em_addr
     left join v_bdg_pvt_three_key  e on e.pvt_em_addr=b.bdg_em_addr
     where s.sel_bud=0 and b.id_bdg=80 and s.sel_em_addr=' ||
                   v_nSelOrAggRuleID || ')
     select * from tab where num_mod=' || pIn_vNumMod ||
                   ' or num_mod is null';
      
      ELSE
        select nvl(max(prv.prv_em_addr), 0)
          into v_nSelOrAggRuleID
          from prv
         where prv.prv_cle = v_oOptions.strSel;
      
        vTmpStr := 'with tab as (
select  b.b_cle, e.f_cle,e.g_cle,e.d_cle,m.*
from  prv p join prvsel ps on p.prv_em_addr=ps.prv15_em_addr left join sel s on ps.sel16_em_addr=s.sel_em_addr
     left join bdg b on s.sel_cle=b.b_cle
     left join (SELECT * FROM mod_drp WHERE NUM_MOD=' ||
                   pIn_vNumMod || ') m on b.bdg_em_addr=m.bdg_em_addr
     left join v_bdg_sel_three_key e on b.bdg_em_addr=e.sel_em_addr where s.sel_bud=71 and b.id_bdg=71
     and p.prv_em_addr=' || v_nSelOrAggRuleID || ' )
     select * from tab where  num_mod=' || pIn_vNumMod ||
                   ' or  num_mod is null';
      END IF;
    ELSE
      IF pIn_nCommandNumber = nCommand201 THEN
        vTmpStr := 'with tab as(
        SELECT   e.f_cle,e.g_cle,e.d_cle, d.*
        FROM (select * from MOD_DRP where num_mod=' ||
                   pIn_vNumMod ||
                   ')  d,v_bdg_pvt_three_key e
        where d.bdg_em_addr(+)=e.pvt_em_addr)
              select * from tab where num_mod=' ||
                   pIn_vNumMod || ' or  num_mod is null';
      
      ELSE
        vTmpStr := 'with tab as ( SELECT  E.SEL_CLE b_cle, e.f_cle,e.g_cle,e.d_cle, d.*
        FROM (select * from MOD_DRP where num_mod=' ||
                   pIn_vNumMod ||
                   ')  d,v_bdg_sel_three_key e
        where d.bdg_em_addr(+)=e.sel_em_addr)
             select * from tab where num_mod=' ||
                   pIn_vNumMod || ' or num_mod is null';
      END IF;
    END IF;
  
    --a_m aa_mm switches
    IF v_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j THEN
      vStrDateFormat := 'sss.date_deb_appro_annee||lpad(sss.date_deb_appro_periode,2,0) ||''' ||
                        pIn_vSeperator ||
                        '''||
                         sss.date_fin_appro_annee||lpad(sss.date_fin_appro_periode,2,0) ||''' ||
                        pIn_vSeperator || '''||';
    
      vStrDateDebut := 'sss.date_debut_calcul_ss_annee||lpad(sss.date_debut_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator ||
                       '''||
                        sss.date_fin_calcul_ss_annee || lpad(sss.date_fin_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator || '''||';
    
    ELSIF v_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m THEN
      vStrDateFormat := 'sss.date_deb_appro_annee||nvl2(sss.date_deb_appro_annee,''' ||
                        pIn_vSeperator ||
                        ''','''')||lpad(sss.date_deb_appro_periode,2,0) ||''' ||
                        pIn_vSeperator ||
                        '''||
                         sss.date_fin_appro_annee||nvl2(sss.date_fin_appro_annee,''' ||
                        pIn_vSeperator ||
                        ''','''')||lpad(sss.date_fin_appro_periode,2,0) ||''' ||
                        pIn_vSeperator || '''||';
    
      vStrDateDebut := 'sss.date_debut_calcul_ss_annee||nvl2(sss.date_debut_calcul_ss_annee,''' ||
                       pIn_vSeperator ||
                       ''','''')||lpad(sss.date_debut_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator ||
                       '''||
                         sss.date_fin_calcul_ss_annee||nvl2(sss.date_fin_calcul_ss_annee,''' ||
                       pIn_vSeperator ||
                       ''','''')||lpad(sss.date_fin_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator || '''||';
    ELSIF v_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm THEN
      vStrDateFormat := 'lpad(substr(sss.date_deb_appro_annee,3,2),4,'' '')||lpad(sss.date_deb_appro_periode,2,0) ||''' ||
                        pIn_vSeperator ||
                        '''||
                         lpad(substr(sss.date_fin_appro_annee,3,2),4,'' '')||lpad(sss.date_fin_appro_periode,2,0) ||''' ||
                        pIn_vSeperator || '''||';
    
      vStrDateDebut := 'lpad(substr(sss.date_debut_calcul_ss_annee,3,2),4,'' '')||lpad(sss.date_debut_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator ||
                       '''||
                         lpad(substr(sss.date_fin_calcul_ss_annee,3,2),4,'' '')||lpad(sss.date_fin_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator || '''||';
    ELSE
      vStrDateFormat := 'sss.date_deb_appro_annee||lpad(sss.date_deb_appro_periode,2,0) ||''' ||
                        pIn_vSeperator ||
                        '''||
                         sss.date_fin_appro_annee||lpad(sss.date_fin_appro_periode,2,0) ||''' ||
                        pIn_vSeperator || '''||';
    
      vStrDateDebut := 'sss.date_debut_calcul_ss_annee||lpad(sss.date_debut_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator ||
                       '''||
                        sss.date_fin_calcul_ss_annee || lpad(sss.date_fin_calcul_ss_periode,2,0) ||''' ||
                       pIn_vSeperator || '''||';
    END IF;
  
    IF pIn_nCommandNumber = nCommand211 THEN
      nBdg := 71;
    ELSE
      nBdg := 80;
    END IF;
  
    --create temp talbe
  
    pOut_vstrTableName := fmf_gettmptablename();
    vStrSql            := 'CREATE /*global temporary*/ TABLE ' ||
                          pOut_vstrTableName ||
                          '(name clob) nologging/*on commit preserve rows*/ ';
    fmsp_execsql(vStrSql);
  
    vStrSql := 'INSERT INTO  ' || pOut_vstrTableName;
    vStrSql := vStrSql || ' with tmp as (
select r.numero_crt-69+1 numcrt,r.numero_crt-69 umlotsize,f.f_cle , nvl(max(v.val), -1) val
  from vct v , rfc r,fam f
 where v.id_crt = 80
   and v.num_crt = 69
   and v.vct_em_addr = r.vct10_em_addr and
   r.fam7_em_addr = f.fam_em_addr
   group by numero_crt,f_cle),
   tab as(
 select b.bdg_em_addr,c.num_modscl, NVL(MAX(s.scl_cle), NULL) val
      from mod_drp m, bdg b, modscl c, scl s
     where m.bdg_em_addr = b.bdg_em_addr
       and m.mod_em_addr = c.mod42_em_addr
       and c.scl41_em_addr = s.scl_em_addr
       and m.num_mod = ' || pIn_vNumMod || '
       and b.id_bdg = ' || nBdg || '
       group by b.bdg_em_addr,c.num_modscl)  select ';
    IF pIn_nCommandNumber = nCommand211 THEN
      vStrSql := vStrSql || '''' || pIn_chSdlt || '''|| b_cle ||''' ||
                 pIn_chSdlt || pIn_vSeperator;
    ELSE
      vStrSql := vStrSql || '''';
    END IF;
    vStrSql := vStrSql || pIn_chSdlt || '''|| sss.f_cle ||''' || pIn_chSdlt ||
               pIn_vSeperator || pIn_chSdlt || '''||
g_cle||''' || pIn_chSdlt || pIn_vSeperator || pIn_chSdlt ||
               '''||
d_cle||''' || pIn_chSdlt || pIn_vSeperator ||
               '''||
NVL(DELAI_TRANSIT,0)||''' || pIn_vSeperator ||
               '''||
ROUND(ALERTE)||''' || pIn_vSeperator || '''||
TO_CHAR(TIME_PARAM,''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
to_char(SERVICE,''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(DELAI_APPRO,0)||''' || pIn_vSeperator ||
               '''||
ROUND(min_appro)||''' || pIn_vSeperator ||
               '''||
ROUND(multi_appro)||''' || pIn_vSeperator || '''||
null ||''' || pIn_vSeperator || '''||
null ||''' || pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
ROUND(max_appro)||''' || pIn_vSeperator ||
               '''||
NVL(nb_order_periode,0)||''' || pIn_vSeperator ||
               '''||
NVL(first_order_periode,0)||''' || pIn_vSeperator ||
               '''||
NVL(order_if_out_of_stock,0)||''' || pIn_vSeperator || '''||';
    vStrSql := vStrSql || vStrDateFormat || '

NVL(calendor_supply_period_0,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_1,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_2,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_3,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_4,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_5,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_period_6,0)||''' || pIn_vSeperator ||
               '''||
NVL(calendor_supply_s_o_l_f,0)||''' || pIn_vSeperator ||
               '''||
NVL(jour_date_deb_appro,0)||''' || pIn_vSeperator ||
               '''||
NVL(jour_date_fin_appro,' || nNb_DayByWeek || ')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
ROUND(max_stock)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(max_stock_time,''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(nb_periode_a_lancer,0)||''' || pIn_vSeperator ||
               '''||
NVL(jour_nb_periode_a_lancer,0)||''' || pIn_vSeperator ||
               '''||
DECODE(SIGN(pourcent_lot_size),-1,0,NULL,0,pourcent_lot_size)||''' ||
               pIn_vSeperator || '''||
NVL(type_min_stock,0)||''' || pIn_vSeperator ||
               '''||
NVL(min_stock_pourcent,0)||''' || pIn_vSeperator ||
               '''||
ROUND(min_stock)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(min_stock_time,''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(type_max_stock,0)||''' || pIn_vSeperator ||
               '''||
NVL(max_stock_pourcent,0)||''' || pIn_vSeperator ||
               '''||
NVL(jour_appro_fix,0)||''' || pIn_vSeperator ||
               '''||
NVL(reset_stock_every,0)||''' || pIn_vSeperator ||
               '''||
NVL(reset_stock_from,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_variable,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_0,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_1,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_2,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_3,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_4,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_5,0)||''' || pIn_vSeperator ||
               '''||
NVL(lead_time_6,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_lead_time_variable,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_0,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_1,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_2,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_3,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_4,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_5,0)||''' || pIn_vSeperator ||
               '''||
NVL(transit_time_6,0)||''' || pIn_vSeperator ||
               '''||
NVL(safety_time_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(SAFETY_TIME_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(SAFETY_TIME_6,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(safety_stock_variable,0)||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_0,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_1,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_2,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_3,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_4,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_5,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(safety_stock_6,0))||''' || pIn_vSeperator ||
               '''||
NVL(service_level_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(service_level_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(service_level_6,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(type_safety_stock,0)||''' || pIn_vSeperator ||
               '''||
NVL(mode_periode_lissage,0)||''' || pIn_vSeperator ||
               '''||
NVL(nb_periode_lissage,0)||''' || pIn_vSeperator ||
               '''||
NVL(mode_lissage,0)||''' || pIn_vSeperator ||
               '''||
NVL(nb_per_max_avance,0)||''' || pIn_vSeperator || pIn_chSdlt ||
               '''||
/*CLE_SCL_DRP,*/
tab1.val ||''' || pIn_chSdlt || pIn_vSeperator ||
               '''||
NVL(mode_stock_dlc,0)||''' || pIn_vSeperator ||
               '''||
NVL(priority,0)||''' || pIn_vSeperator || '''||
NVL(suite_de,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(cadence_prod,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(dlc_customer,0)||''' || pIn_vSeperator ||
               '''||
NVL(delai_affinage,0)||''' || pIn_vSeperator ||
               '''||
NVL(dlc_consumer,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_0,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_1,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_2,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_3,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_4,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_5,0)||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_ORDER_PERIOD_6,0)||''' || pIn_vSeperator ||
               pIn_chSdlt || '''||
/*CLE_SCL_P12P,*/
tab2.val ||''' || pIn_chSdlt || pIn_vSeperator ||
               '''||
NVL(type_min_appro,0)||''' || pIn_vSeperator ||
               '''||
NVL(type_max_appro,0)||''' || pIn_vSeperator ||
               '''||
NVL(min_stock_pourcent_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_0,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_1,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_2,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_3,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_4,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_5,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_pourcent_6,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator || '''||
NVL(min_stock_variable,0)||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_0,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_1,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_2,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_3,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_4,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_5,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(min_stock_6,0))||''' || pIn_vSeperator ||
               '''||
NVL(min_stock_time_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(min_stock_time_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(min_stock_time_6,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(max_stock_pourcent_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_0,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_1,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_2,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_3,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_4,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_5,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_pourcent_6,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator || '''||
NVL(max_stock_variable,0)||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_0,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_1,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_2,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_3,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_4,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_5,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(max_stock_6,0))||''' || pIn_vSeperator ||
               '''||
NVL(max_stock_time_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(max_stock_time_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(max_stock_time_6,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(regle_ajuste_current_periode,0)||''' || pIn_vSeperator ||
               '''||
NVL(carnet_ajuste_periode,0)||''' || pIn_vSeperator ||
               '''||
NVL(is_safety_time_2_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_param,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_0,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_1,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_2,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_3,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_4,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_5,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(safety_time_2_variable_6,0),''' || vFormant ||
               ''')||''' || pIn_vSeperator ||
               '''||
NVL(CALENDOR_SUPPLY_NB_WORKING_DAY,0)||''' || pIn_vSeperator ||
               '''||
NVL(rules_safety_time,0)||''' || pIn_vSeperator ||
               '''||
NVL(nb_util_rules_safety_time,0)||''' || pIn_vSeperator ||
               '''||
NVL(PlanningRule,0)||''' || pIn_vSeperator ||
               '''||
NVL(qt_min_variable,0)||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_0,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_1,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_2,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_3,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_4,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_5,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_min_var_6,0))||''' || pIn_vSeperator ||
               '''||
NVL(qt_min_time_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(qt_min_time_var_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_min_time_var_6,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
NVL(mode_gelage,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_0,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_1,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_2,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_3,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_4,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_5,0)||''' || pIn_vSeperator ||
               '''||
NVL(gelage_periode_6,0)||''' || pIn_vSeperator ||
               '''||
NVL(IsZeroOnStockMaxAccepted,0)||''' || pIn_vSeperator ||
               '''||
NVL(mode_gestion_lot,0)||''' || pIn_vSeperator ||
               '''||
NVL(mode_gestion_gamme,0)||''' || pIn_vSeperator ||
               '''||
NVL(qt_max_variable,0)||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_0,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_1,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_2,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_3,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_4,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_5,0))||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_max_var_6,0))||''' || pIn_vSeperator ||
               '''||
NVL(qt_max_time_variable,0)||''' || pIn_vSeperator ||
               '''||
TO_CHAR(NVL(qt_max_time_var_0,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_1,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_2,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_3,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_4,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_5,0),''' || vFormant || ''')||''' ||
               pIn_vSeperator || '''||
TO_CHAR(NVL(qt_max_time_var_6,0),''' || vFormant ||
               ''') ||''' || pIn_vSeperator || pIn_chSdlt ||
               '''||
    tmp1.val  ||''' || pIn_chSdlt || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_5,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(DelaiLivraisonClient_6,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(ReleaseLeadTime,0)  ||''' || pIn_vSeperator ||
               '''||
DECODE(ReleaseLeadTimeNotToApply,1,0,1)  ||''' ||
               pIn_vSeperator || '''||
NVL(LEVELFORDETAILEDF_P_O,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(NegativeStock,0) || ''' || pIn_vSeperator ||
               '''||
NVL(um_qte_min,-1)  ||''' || pIn_vSeperator ||
               '''||
NVL(PropagationModeJIT,0)  ||''' || pIn_vSeperator ||
               pIn_chSdlt || '''||
/*AdvUmLotSize_0  ,
AdvUmLotSize_1  ,
AdvUmLotSize_2  ,
AdvUmLotSize_3  ,
AdvUmLotSize_4  ,*/
tmp2.val ||''' || pIn_chSdlt || pIn_vSeperator || pIn_chSdlt ||
               '''||
tmp3.val ||''' || pIn_chSdlt || pIn_vSeperator || pIn_chSdlt ||
               '''||
tmp4.val ||''' || pIn_chSdlt || pIn_vSeperator || pIn_chSdlt ||
               '''||
tmp5.val ||''' || pIn_chSdlt || pIn_vSeperator || pIn_chSdlt ||
               '''||
tmp6.val ||''' || pIn_chSdlt || pIn_vSeperator ||
               '''||
ROUND(NVL(AdvInitialQty_0,0))  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(AdvInitialQty_1,0))  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(AdvInitialQty_2,0))  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(AdvInitialQty_3,0))  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(AdvInitialQty_4,0))  ||''' || pIn_vSeperator ||
               '''||
NVL(AdvPourcentThreshold_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(AdvPourcentThreshold_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(AdvPourcentThreshold_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(AdvPourcentThreshold_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(AdvPourcentThreshold_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_applyminorderqty,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(modefirmplannedorder,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_stockintransit,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_reorderqty,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_scheduledreceipt,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(first_order_annee,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(max_periode_anti_date,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(dlc_consumer_ideal,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(min_dlc_consumer,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(ratio_risque_besoin_brut_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(ratio_risque_besoin_brut_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(ratio_risque_besoin_brut_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(ratio_risque_besoin_brut_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(n0rule_split_order_dlc,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(nb_max_dlc_par_order,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(avec_lot_size_dlc,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_qte_min_par_dlc,0)  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qte_min_par_dlc,0))  ||''' || pIn_vSeperator ||
               '''||
/*date_debut_calcul  ,
date_fin_calcul  ,*/';
    vStrSql := vStrSql || vStrDateDebut || '
NVL(nb_utile_calcul_ss,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_5,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_6,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_5,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_6,0) ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_from,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_2_until,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_5,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_6,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_from,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(coef_sais_3_until,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_0,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_1,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_2,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_3,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_4,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_5,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(period_ouvert_si_ferie_6,0)  ||''' || pIn_vSeperator ||
               '''||
NVL(mode_gestion_deploiement,0)  ||''' || pIn_vSeperator ||
               '''||
ROUND(NVL(qt_perdu_par_ordre,0))  ||''' || pIn_vSeperator ||
               '''||
NVL(dlc_from,0)
from (';
  
    vStrSql := vStrSql || vTmpStr ||
               '  ) sss
       left join tmp tmp1 on tmp1.umlotsize = sss.um_lot_size and tmp1.f_cle= sss.f_cle
       left join tmp tmp2 on tmp2.numcrt = sss.AdvUmLotSize_0 and tmp2.f_cle= sss.f_cle
       left join tmp tmp3 on tmp3.numcrt = sss.AdvUmLotSize_1 and tmp3.f_cle= sss.f_cle
       left join tmp tmp4 on tmp4.numcrt = sss.AdvUmLotSize_2 and tmp4.f_cle= sss.f_cle
       left join tmp tmp5 on tmp5.numcrt = sss.AdvUmLotSize_3 and tmp5.f_cle= sss.f_cle
       left join tmp tmp6 on tmp6.numcrt = sss.AdvUmLotSize_4 and tmp6.f_cle= sss.f_cle
       left join tab tab1 on tab1.bdg_em_addr=sss.bdg_em_addr and tab1.num_modscl=0
       left join tab tab2 on tab2.bdg_em_addr=sss.bdg_em_addr and tab2.num_modscl=4  ';
  
    fmsp_execsql(vStrSql);
  
    Fmp_log.LOGEND;
  
  EXCEPTION
    WHEN OTHERS THEN
      FMP_LOG.LOGERROR;
      pOut_nSqlCode := SQLCODE;
    
  END FMISP_ProcessExpDRPParameter;

end fmip_pexport_drpparameter;
/
