CREATE OR REPLACE PACKAGE FMP_CalcPrice IS
  v_strSql clob;

  vTmpAggTableName    varchar2(40) DEFAULT 'TB_TS_AggregateNodeCon';
  vTmpDetailTableName varchar2(40) DEFAULT 'TB_TS_DetailNodeSelCdt';
  vOldValDetailTable  varchar2(16) default 'tmp_don_m_old';
  vOldValAggTable     varchar2(16) default 'tmp_prb_m_old';
  TYPE T_cur IS REF CURSOR;
  TYPE MonthType IS RECORD(
    JANUARY   INTEGER,
    FEBRARY   INTEGER,
    MARCH     INTEGER,
    APRIL     INTEGER,
    MAY       INTEGER,
    JUNE      INTEGER,
    JULY      INTEGER,
    AUGUST    INTEGER,
    SEPTEMPER INTEGER,
    OCTOBER   INTEGER,
    NOVERBER  INTEGER,
    DECEMBER  INTEGER);

  PROCEDURE FMSP_CalcPrice(pIn_nChronology         IN NUMBER,
                           pIn_nHistValDBID        IN NUMBER,
                           pIn_nHistQtyDBID        IN NUMBER,
                           pIn_nPriceDBID          IN NUMBER,
                           pIn_nTimeSeriesNodeType IN NUMBER,
                           pIn_vFUser              IN VARCHAR2,
                           pIn_vOptions            IN VARCHAR2,
                           pIn_nPeriodIndex        IN NUMBER,
                           pIn_nBeginDate          IN NUMBER,
                           pIn_nEndCalcOfDate      IN NUMBER,
                           pIn_nEndDate            IN NUMBER,
                           pIn_nlsUsedInFuture     IN NUMBER,
                           pIn_Precision           IN NUMBER DEFAULT 2,
                           pOut_nSqlCode           OUT NUMBER);
  FUNCTION FMF_GetColumnName(pIn_vYear IN VARCHAR2) RETURN VARCHAR2;

END;
/
CREATE OR REPLACE PACKAGE BODY FMP_CalcPrice IS
  --*****************************************************************
  -- Description: Calculate specified time serials's price
  --
  -- Author:      XF.ZHANG
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        28-JAN-2013     XF.ZHANG     Created.
  -- **************************************************************

  procedure FMSP_DropTSTable(P_TableName in varchar2, p_SqlCode out number) as
    v_strsql varchar2(2000);
  
  begin
  
    p_SqlCode := 0;
  
    v_strsql := 'Drop table ' || p_TableName || ' purge ';
  
    fmsp_execsql(v_strsql);
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end FMSP_DropTSTable;

  FUNCTION FMF_GetColumnName(pIn_vYear IN VARCHAR2) RETURN VARCHAR2 AS
    nMonth INTEGER;
  BEGIN
    nMonth := SUBSTR(pIn_vYear, 5, 2);
  
    FOR i in 1 .. 12 LOOP
      IF (i = nMonth) THEN
        RETURN 'T' || i;
      END IF;
    END LOOP;
  END FMF_GetColumnName;

  PROCEDURE FMSP_CreateDetailNodeTB(pIn_vTableName   IN VARCHAR2,
                                    pIn_nHistValDBID IN NUMBER,
                                    pIn_nHistQtyDBID IN NUMBER,
                                    pIn_vOptions     IN VARCHAR2,
                                    pIn_nPeriodIndex IN NUMBER,
                                    pIn_nBeginDate   IN NUMBER,
                                    pIn_nEndDate     IN NUMBER,
                                    pOut_nSqlCode    OUT NUMBER) AS
    dtDate     DATE;
    nBeginYear NUMBER;
    nEndYear   NUMBER;
    nSelEmAddr NUMBER DEFAULT 0;
    sNodeList  sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
    --ts not in don_m
    nInterval  NUMBER;
    nStartWith NUMBER;
    vTabName   varchar2(30);
  BEGIN
    /* P_BATCHCOMMAND_COMMON.sp_ParseOptions(pIn_vOptions,
    v_oOptions,
    pOut_nSqlCode);*/
    IF pIn_nPeriodIndex = 1 OR pIn_nPeriodIndex = 13 THEN
      dtDate := TO_DATE(pIn_nBeginDate, 'YYYYMM');
    ELSE
      dtDate := ADD_MONTHS(TO_DATE(pIn_nBeginDate, 'YYYYMM'),
                           
                           -pIn_nPeriodIndex);
    END IF;
    nBeginYear := TO_CHAR(dtDate, 'YYYY');
    nEndYear   := TO_CHAR(TO_DATE(pIn_nEndDate, 'YYYYMM'), 'YYYY');
    nStartWith := SUBSTR(pIn_nBeginDate, 1, 4) - 1;
    nInterval  := SUBSTR(pIn_nEndDate, 1, 4) - SUBSTR(pIn_nBeginDate, 1, 4) + 1;
    BEGIN
      IF pIn_vOptions IS NOT NULL THEN
        SELECT sel_em_addr
          INTO nSelEmAddr
          FROM sel
         WHERE sel_cle = pIn_vOptions
           and sel_bud = 0;
      END IF;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pOut_nSqlCode := -100;
        RETURN;
    END;
    v_strSql := 'truncate table ' || vTmpDetailTableName;
    fmsp_execsql(v_strSql);
  
    p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => nSelEmAddr,
                                         P_Conditions  => '',
                                         P_Sequence    => null, --Sort sequence
                                         p_DetailNode  => sNodeList,
                                         pOut_vTabName => vTabName,
                                         p_SqlCode     => pOut_nSqlCode);
  
    BEGIN
      v_strSql := 'truncate table ' || pIn_vTableName;
      fmsp_execsql(v_strSql);
    EXCEPTION
      WHEN OTHERS THEN
        v_strSql := ' create global temporary table ' || pIn_vTableName;
        v_strSql := v_strSql ||
                    '(SELID NUMBER,TSID  NUMBER,VERSION INTEGER,YYMM INTEGER,';
        v_strSql := v_strSql ||
                    ' VAL   NUMBER,calcval NUMBER,QTY   NUMBER,PRICE   NUMBER)  on commit preserve rows  ';
        fmsp_execsql(v_strSql);
    END;
  
    --insert the suitable data into tmp table
    v_strSql := '
    insert  all
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''01'',t1)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''02'',t2)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''03'',t3)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''04'',t4)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''05'',t5)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''06'',t6)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''07'',t7)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''08'',t8)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''09'',t9)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''10'',t10)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''11'',t11)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(pvtid,tsid,version,yy||''12'',t12)

    select /*+ parallel */ a.pvtid, a.tsid, a.version,b.yy,a.t1,a.t2,a.t3,a.t4,a.t5,a.t6,
    a.t7,a.t8,a.t9,a.t10,a.t11,a.t12
from (  select  pvtid,tsid,version,yy,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12
     FROM ' || vTmpDetailTableName ||
                ' R, don_m D
     WHERE R.ID = D.PVTID   ';
    /*   IF pIn_nlsUsedInFuture = 0 THEN
      v_strSql := v_strSql || ' AND D.YY BETWEEN ' || nBeginYear || ' AND ' ||
                  nEndYear || '
       AND D.TSID IN(' || pIn_nHistValDBID || ',' ||
                  pIn_nHistQtyDBID || ',' || pIn_nPriceDBID || ')';
    ELSE*/
    v_strSql := v_strSql || ' AND D.YY BETWEEN ' || nBeginYear || ' AND ' ||
                nEndYear || '
       AND D.TSID IN(' || pIn_nHistValDBID || ',' ||
                pIn_nHistQtyDBID || ')';
    /*END IF;*/
    v_strSql := v_strSql || ') a partition by(a.pvtid, a.tsid, a.version)
right outer join (select level + ' || nStartWith ||
                ' yy from dual connect by level <= ' || nInterval || ') b
on a.yy = b.yy';
  
    fmsp_execsql(v_strSql);
  
    pOut_nSqlCode := 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := -1;
      RAISE;
  END FMSP_CreateDetailNodeTB;

  --Aggregation Node Section
  PROCEDURE FMSP_CreateAggregateNodeTB(pIn_vTableName   IN VARCHAR2,
                                       pIn_nHistValDBID IN NUMBER,
                                       pIn_nHistQtyDBID IN NUMBER,
                                       pIn_vOptions     IN VARCHAR2,
                                       pIn_vConditions  IN VARCHAR2,
                                       pIn_nPeriodIndex IN NUMBER,
                                       pIn_nBeginDate   IN NUMBER,
                                       pIn_nEndDate     IN NUMBER,
                                       pOut_nSqlCode    OUT NUMBER) AS
    dtDate       DATE;
    nBeginYear   NUMBER;
    nEndYear     NUMBER;
    nPRV_EM_ADDR NUMBER DEFAULT 0;
    vConditions  VARCHAR2(400);
    sNodeList    sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
    --ts not in prb_m
    nInterval  NUMBER;
    nStartWith NUMBER;
  BEGIN
  
    pOut_nSqlCode := 0;
    IF pIn_nPeriodIndex = 1 OR pIn_nPeriodIndex = 13 THEN
      dtDate := TO_DATE(pIn_nBeginDate, 'YYYYMM');
    ELSE
      dtDate := ADD_MONTHS(TO_DATE(pIn_nBeginDate, 'YYYYMM'),
                           
                           -pIn_nPeriodIndex);
    END IF;
    nBeginYear := TO_CHAR(dtDate, 'YYYY');
    nEndYear   := TO_CHAR(TO_DATE(pIn_nEndDate, 'YYYYMM'), 'YYYY');
    nStartWith := SUBSTR(pIn_nBeginDate, 1, 4) - 1;
    nInterval  := SUBSTR(pIn_nEndDate, 1, 4) - SUBSTR(pIn_nBeginDate, 1, 4) + 1;
  
    IF pIn_vOptions IS NOT NULL THEN
      BEGIN
        select PRV_EM_ADDR
          INTO nPRV_EM_ADDR
          from prv
         where prv_cle = pIn_vOptions;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pOut_nSqlCode := -100;
          return;
      END;
      SELECT replace(wmsys.wm_concat(str), ';,', ';')
        INTO vConditions
        FROM (select c.n0_cdt || ',' || c.rcd_cdt || ',' || c.operant || ',' ||
                     c.n0_val_cdt || ',' || c.adr_cdt || ';' STR
                from cdt c
               where c.sel11_em_addr =
                     (SELECT SEL_EM_ADDR
                        FROM SEL
                       WHERE SEL_CLE = pIn_vConditions));
    
    END IF;
    v_strSql := 'truncate table ' || vTmpAggTableName;
    fmsp_execsql(v_strSql);
  
    p_aggregation.FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => nPRV_EM_ADDR,
                                            pIn_vConditions => vConditions,
                                            pOut_Nodes      => sNodeList,
                                            pOut_nSqlCode   => pOut_nSqlCode);
  
    BEGIN
      v_strSql := 'truncate table ' || pIn_vTableName;
      fmsp_execsql(v_strSql);
    EXCEPTION
      WHEN OTHERS THEN
        v_strSql := ' create global temporary  table ' || pIn_vTableName;
        v_strSql := v_strSql ||
                    '(SELID NUMBER,TSID  NUMBER,VERSION INTEGER,YYMM INTEGER,';
        v_strSql := v_strSql ||
                    ' VAL   NUMBER,calcval NUMBER,QTY   NUMBER,PRICE   NUMBER)  on commit preserve rows ';
        fmsp_execsql(v_strSql);
    END;
    --insert the suitable data into tmp table
    v_strSql := '
    insert  all
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''01'',t1)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''02'',t2)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''03'',t3)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''04'',t4)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''05'',t5)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''06'',t6)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''07'',t7)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''08'',t8)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''09'',t9)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''10'',t10)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''11'',t11)
    into ' || pIn_vTableName || '(selid,tsid,version,yymm,val) values(selid,tsid,version,yy||''12'',t12)

    select /*+ parallel */ a.selid, a.tsid, a.version,  b.yy,a.t1,a.t2,a.t3,a.t4,a.t5,
    a.t6,a.t7,a.t8,a.t9,a.t10,a.t11,a.t12
/* case b.yy when a.yy then a.t1 else null end t1,
case  when a.yy=b.yy then a.t2 else null end t2
,case  when a.yy=b.yy then a.t3 else null end t3
,case  when a.yy=b.yy then a.t4 else null end t4
,case  when a.yy=b.yy then a.t5 else null end t5
,case  when a.yy=b.yy then a.t6 else null end t6
,case when a.yy=b.yy then a.t7 else null end t7
,case when a.yy=b.yy then a.t8 else null end t8
,case  when a.yy=b.yy then a.t9 else null end t9
,case  when a.yy=b.yy then a.t10 else null end t10
,case  when a.yy=b.yy then a.t11 else null end t11
,case  when a.yy=b.yy then a.t12 else null end t12*/
from (  select selid,tsid,version,yy,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12
     FROM  ' || vTmpAggTableName ||
                ' R, prb_m D
     WHERE r.id = d.selid  ';
    /*    IF pIn_nlsUsedInFuture = 0 THEN
      v_strSql := v_strSql || ' AND d.YY BETWEEN ' || nBeginYear || ' AND ' ||
                  nEndYear || '
       AND d.TSID IN(' || pIn_nHistValDBID || ',' ||
                  pIn_nHistQtyDBID || ',' || pIn_nPriceDBID || ')';
    ELSE*/
    v_strSql := v_strSql || ' AND d.YY BETWEEN ' || nBeginYear || ' AND ' ||
                nEndYear || '
       AND d.TSID IN(' || pIn_nHistValDBID || ',' ||
                pIn_nHistQtyDBID || ')';
    /* END IF;*/
    v_strSql := v_strSql || ') a partition by(a.selid, a.tsid, a.version)
right outer join (select level + ' || nStartWith ||
                ' yy from dual connect by level <= ' || nInterval || ') b
on a.yy = b.yy
     ';
    fmsp_execsql(v_strSql);
  
    pOut_nSqlCode := 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := -1;
      RAISE;
  END FMSP_CreateAggregateNodeTB;

  PROCEDURE FMSP_UpdateDetail(pIn_vInnerTableName IN VARCHAR2,
                              pIn_nBeginDate      IN NUMBER,
                              pIn_nEndDate        IN NUMBER,
                              pOut_nSqlCode       OUT NUMBER) IS
    vBeginDate    number;
    vEndDate      number;
    vBeginMonth   number;
    vEndmonth     number;
    vYearInterval number;
    vSetColumn    varchar2(200);
    vColumn       varchar2(200);
    vTmpColumn    varchar2(200);
    vSql          varchar2(4000);
  BEGIN
    vBeginDate    := pIn_nBeginDate;
    vEndDate      := pIn_nEndDate;
    vBeginMonth   := substr(vBeginDate, 5, 2);
    vEndmonth     := substr(vEndDate, 5, 2);
    vYearInterval := substr(vEndDate, 1, 4) - substr(vBeginDate, 1, 4);
  
    IF vYearInterval = 0 THEN
      for i in vBeginMonth .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || ' where m.yy=' ||
                    substr(vBeginDate, 1, 4) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
    
      fmsp_execsql(vSql);
    
    ELSIF vYearInterval = 1 THEN
      for i in vBeginMonth .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vBeginDate, 1, 4) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
      -- PROCESS
      --
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vEndDate, 1, 4) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
    ELSE
      for i in vBeginMonth .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vBeginDate, 1, 4) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
      -- PROCESS
      --
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vEndDate, 1, 4) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into don_m m
using ' || pIn_vInnerTableName || ' tmp on (m.pvtid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy BETWEEN ' ||
                    TO_CHAR(TO_NUMBER(substr(vBeginDate, 1, 4)) + 1) ||
                    ' AND ' ||
                    TO_CHAR(TO_NUMBER(substr(vEndDate, 1, 4)) - 1) || '
when not matched then
  insert(don_mid,pvtid,tsid,version,yy' || vColumn || ')
  values(seq_don_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
    END IF;
    pOut_nSqlCode := 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_UpdateDetail;

  PROCEDURE FMSP_UpdateDetailFutrue(vTempTableInner    IN VARCHAR2,
                                    pIn_nPriceDBID     IN NUMBER,
                                    pIn_nBeginDate     IN NUMBER,
                                    pIn_nEndCalcOfDate IN NUMBER,
                                    pIn_nEndDate       IN NUMBER,
                                    pOut_nSqlCode      OUT NUMBER) AS
  
    vFuturDate       VARCHAR2(6);
    vMonth           NUMBER;
    vFurutrYear      NUMBER;
    vCurrentYear     NUMBER;
    vEndYear         number;
    vCondition       clob;
    vEndMonth        number;
    vAfterCondition  varchar2(4000);
    vValuesCondition varchar2(4000);
    vTmpStr          VARCHAR2(4000);
  
    nInterval  NUMBER;
    nStartWith NUMBER;
  BEGIN
  
    vFuturDate   := TO_CHAR(ADD_MONTHS(to_DATE(pIn_nEndCalcOfDate, 'YYYYMM'),
                                       1),
                            'YYYYMM');
    vMonth       := SUBSTR(pIn_nEndCalcOfDate, 5, 2);
    vFurutrYear  := SUBSTR(vFuturDate, 1, 4);
    vCurrentYear := SUBSTR(pIn_nEndCalcOfDate, 1, 4);
    vEndYear     := SUBSTR(pIn_nEndDate, 1, 4);
    vEndMonth    := SUBSTR(pIn_nEndDate, 5, 2);
    nStartWith   := SUBSTR(pIn_nBeginDate, 1, 4) - 1;
    nInterval    := SUBSTR(pIn_nEndDate, 1, 4) -
                    SUBSTR(pIn_nBeginDate, 1, 4) + 1;
    if vMonth <> 12 then
      v_strSql := ' merge /*+ parallel */ into don_m f
      using (select o.PVTID,o.TSID,o.VERSION,o.YY, o.t2 told,n.t' ||
                  to_number(vMonth) ||
                  ' tnew from (SELECT PVTID,TSID,VERSION,b.YY, last_value(T2 ignore nulls) over(partition by pvtid order by pvtid,b.yy) t2 FROM(
SELECT A.PVTID,A.YY,A.VERSION,A.TSID,a.t2 FROM ' ||
                  vOldValDetailTable || ' A WHERE A.YY BETWEEN ' ||
                  vCurrentYear || ' AND ' || vEndYear || ' AND A.TSID =' ||
                  pIn_nPriceDBID || '
) A PARTITION BY(a.pvtid, a.tsid, a.version ) RIGHT OUTER JOIN (select level + ' ||
                  nStartWith || ' yy from dual connect by level <= ' ||
                  nInterval || ')B
ON A.YY=B.YY) o,don_m n where
   o.pvtid=n.pvtid and o.tsid=n.tsid and o.version=o.version and o.yy=n.yy and o.tsid=' ||
                  pIn_nPriceDBID || ' and o.yy=' || vCurrentYear || ') d
  on(d.pvtid=f.pvtid and d.tsid=f.tsid and d.version=f.version and d.yy=f.yy)
      when matched then update set ';
      FOR i IN vMonth + 1 .. 12 LOOP
        vCondition := vCondition || ' f.t' || i || '=case when ';
        IF i = vMonth + 1 THEN
          vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
        ELSE
          vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
        END IF;
        vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                            ' is null then d.tnew else f.t' || i || ' end,';
        vAfterCondition  := vAfterCondition || 't' || i || ',';
        vValuesCondition := vValuesCondition || 'd.tnew,';
      END LOOP;
      vCondition       := substr(vCondition, 1, length(vCondition) - 1);
      vAfterCondition  := substr(vAfterCondition,
                                 1,
                                 length(vAfterCondition) - 1);
      vValuesCondition := substr(vValuesCondition,
                                 1,
                                 length(vValuesCondition) - 1);
      vCondition       := vCondition ||
                          ' when not matched then  insert(don_mid,pvtid,tsid,version,yy,' ||
                          vAfterCondition || ')';
      vCondition       := vCondition ||
                          ' values(seq_don_m.nextval,d.pvtid,d.tsid,d.version,d.yy,' ||
                          vValuesCondition || ')';
      v_strSql         := v_strSql || vCondition;
    
      fmsp_execsql(v_strSql);
    end if;
  
    for cyear in vCurrentYear + 1 .. vEndYear loop
      v_strSql         := ' merge /*+ parallel */ into don_m f
      using (select o.PVTID,o.TSID,o.VERSION,o.YY, o.t2 told,n.t' ||
                          to_number(vMonth) ||
                          ' tnew  from (SELECT PVTID,TSID,VERSION,b.YY, last_value(T2 ignore nulls) over(partition by pvtid order by pvtid,b.yy) t2 FROM(
SELECT A.PVTID,A.YY,A.VERSION,A.TSID,a.t2 FROM ' ||
                          vOldValDetailTable || ' A WHERE A.YY BETWEEN ' ||
                          vCurrentYear || ' AND ' || vEndYear ||
                          ' AND A.TSID =' || pIn_nPriceDBID || '
) A PARTITION BY(a.pvtid, a.tsid, a.version ) RIGHT OUTER JOIN (select level + ' ||
                          nStartWith ||
                          ' yy from dual connect by level <= ' || nInterval || ')B
ON A.YY=B.YY) o,(select pvtid,tsid,version,' ||
                          cyear || ' yy,t' || to_number(vMonth) ||
                          ' from don_m where yy=' || vCurrentYear ||
                          ' ) n where
   o.pvtid=n.pvtid(+) and o.tsid=n.tsid(+) and o.version=n.version(+) and o.yy=n.yy(+) and o.tsid=64 and o.yy=' ||
                          cyear || ') d
  on(d.pvtid=f.pvtid and d.tsid=f.tsid and d.version=f.version and d.yy=f.yy)
      when matched then update set ';
      vCondition       := '';
      vTmpStr          := '';
      vAfterCondition  := '';
      vValuesCondition := '';
      if cyear <> vEndYear then
        FOR i IN 1 .. 12 LOOP
          vCondition := vCondition || ' f.t' || i || '=case when ';
          IF i = 1 THEN
            vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
          ELSE
            vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
          END IF;
          vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                              ' is null then d.tnew else f.t' || i ||
                              ' end,';
          vAfterCondition  := vAfterCondition || 't' || i || ',';
          vValuesCondition := vValuesCondition || 'd.tnew ,';
        END LOOP;
      else
      
        FOR i IN 1 .. vEndMonth LOOP
          vCondition := vCondition || ' f.t' || i || '=case when ';
          IF i = 1 THEN
            vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
          ELSE
            vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
          END IF;
          vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                              ' is null then d.tnew else f.t' || i ||
                              ' end,';
          vAfterCondition  := vAfterCondition || 't' || i || ',';
          vValuesCondition := vValuesCondition || 'd.tnew ,';
        END LOOP;
      end if;
      vCondition       := substr(vCondition, 1, length(vCondition) - 1);
      vAfterCondition  := substr(vAfterCondition,
                                 1,
                                 length(vAfterCondition) - 1);
      vValuesCondition := substr(vValuesCondition,
                                 1,
                                 length(vValuesCondition) - 1);
      --vCondition       := vCondition || ' where f.yy=' || cyear;
      vCondition := vCondition ||
                    ' when not matched then  insert(don_mid,pvtid,tsid,version,yy,' ||
                    vAfterCondition || ')';
      vCondition := vCondition ||
                    ' values(seq_don_m.nextval,d.pvtid,d.tsid,d.version,d.yy,' ||
                    vValuesCondition || ')';
      v_strSql   := v_strSql || vCondition;
    
      fmsp_execsql(v_strSql);
    
    end loop;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_UpdateDetailFutrue;

  PROCEDURE FMSP_UpdateAggreate(pIn_vInnerTableName IN VARCHAR2,
                                pIn_nBeginDate      IN NUMBER,
                                pIn_nEndDate        IN NUMBER,
                                pOut_nSqlCode       OUT NUMBER) IS
    vBeginDate    number;
    vEndDate      number;
    vBeginMonth   number;
    vEndmonth     number;
    vYearInterval number;
    vSetColumn    varchar2(200);
    vColumn       varchar2(200);
    vTmpColumn    varchar2(200);
    vSql          varchar2(4000);
  BEGIN
    vBeginDate    := pIn_nBeginDate;
    vEndDate      := pIn_nEndDate;
    vBeginMonth   := substr(vBeginDate, 5, 2);
    vEndmonth     := substr(vEndDate, 5, 2);
    vYearInterval := substr(vEndDate, 1, 4) - substr(vBeginDate, 1, 4);
  
    IF vYearInterval = 0 THEN
      for i in vBeginMonth .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into PRB_M m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || ' where m.yy=' ||
                    substr(vBeginDate, 1, 4) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
    
      fmsp_execsql(vSql);
    
    ELSIF vYearInterval = 1 THEN
      for i in vBeginMonth .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into prb_m m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vBeginDate, 1, 4) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
      -- PROCESS
      --
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into prb_m m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vEndDate, 1, 4) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
    ELSE
      for i in vBeginMonth .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into prb_m m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vBeginDate, 1, 4) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
      -- PROCESS
      --
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. vEndmonth loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into prb_m m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy=' || substr(vEndDate, 1, 4) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
      vSetColumn := '';
      vColumn    := '';
      vTmpColumn := '';
      for i in 1 .. 12 loop
        vSetColumn := vSetColumn || ',m.t' || i || '=tmp.t' || i;
        vColumn    := vColumn || ',t' || i;
        vTmpColumn := vTmpColumn || ',tmp.t' || i;
      end loop;
      vSetColumn := substr(vSetColumn, 2);
      vSql       := 'merge /*+ parallel */ into prb_m m
using ' || pIn_vInnerTableName || ' tmp on (m.selid=tmp.selid and m.tsid=tmp.tsid and m.version=tmp.version and m.yy=tmp.yy)
when matched then
  update set ' || vSetColumn || '
  where m.yy BETWEEN ' ||
                    TO_CHAR(TO_NUMBER(substr(vBeginDate, 1, 4)) + 1) ||
                    ' AND ' ||
                    TO_CHAR(TO_NUMBER(substr(vEndDate, 1, 4)) - 1) || '
when not matched then
  insert(prb_mid,selid,tsid,version,yy' || vColumn || ')
  values(seq_prb_m.nextval,tmp.selid,tmp.tsid,tmp.version,tmp.yy' ||
                    vTmpColumn || ')';
      fmsp_execsql(vSql);
    
    END IF;
    pOut_nSqlCode := 0;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_UpdateAggreate;

  PROCEDURE FMSP_UpdateAggreateFuture(vTempTableInner    IN VARCHAR2,
                                      pIn_nPriceDBID     IN NUMBER,
                                      pIn_nBeginDate     IN NUMBER,
                                      pIn_nEndCalcOfDate IN NUMBER,
                                      pIn_nEndDate       IN NUMBER,
                                      pOut_nSqlCode      OUT NUMBER) AS
    vFuturDate       VARCHAR2(6);
    vMonth           NUMBER;
    vFurutrYear      NUMBER;
    vCurrentYear     NUMBER;
    vEndYear         number;
    vCondition       clob;
    vEndMonth        number;
    vAfterCondition  varchar2(4000);
    vValuesCondition varchar2(4000);
    vTmpStr          VARCHAR2(4000);
  
    nInterval  NUMBER;
    nStartWith NUMBER;
  BEGIN
  
    vFuturDate   := TO_CHAR(ADD_MONTHS(to_DATE(pIn_nEndCalcOfDate, 'YYYYMM'),
                                       1),
                            'YYYYMM');
    vMonth       := SUBSTR(pIn_nEndCalcOfDate, 5, 2);
    vFurutrYear  := SUBSTR(vFuturDate, 1, 4);
    vCurrentYear := SUBSTR(pIn_nEndCalcOfDate, 1, 4);
    vEndYear     := SUBSTR(pIn_nEndDate, 1, 4);
    vEndMonth    := SUBSTR(pIn_nEndDate, 5, 2);
    nStartWith   := SUBSTR(pIn_nBeginDate, 1, 4) - 1;
    nInterval    := SUBSTR(pIn_nEndDate, 1, 4) -
                    SUBSTR(pIn_nBeginDate, 1, 4) + 1;
    if vMonth <> 12 then
      v_strSql := ' merge /*+ parallel */ into prb_m f
      using (select o.selid,o.TSID,o.VERSION,o.YY, o.t2 told,n.t' ||
                  to_number(vMonth) ||
                  ' tnew from (SELECT selid,TSID,VERSION,b.YY, last_value(T2 ignore nulls) over(partition by selid order by selid,b.yy) t2 FROM(
SELECT A.selid,A.YY,A.VERSION,A.TSID,a.t2 FROM ' ||
                  vOldValAggTable || ' A WHERE A.YY BETWEEN ' ||
                  vCurrentYear || ' AND ' || vEndYear || ' AND A.TSID =' ||
                  pIn_nPriceDBID || '
) A PARTITION BY(a.selid, a.tsid, a.version ) RIGHT OUTER JOIN (select level + ' ||
                  nStartWith || ' yy from dual connect by level <= ' ||
                  nInterval || ')B
ON A.YY=B.YY) o,prb_m n where
   o.selid=n.selid and o.tsid=n.tsid and o.version=o.version and o.yy=n.yy and o.tsid=' ||
                  pIn_nPriceDBID || ' and o.yy=' || vCurrentYear || ') d
  on(d.selid=f.selid and d.tsid=f.tsid and d.version=f.version and d.yy=f.yy)
      when matched then update set ';
      FOR i IN vMonth + 1 .. 12 LOOP
        vCondition := vCondition || ' f.t' || i || '=case when ';
        IF i = vMonth + 1 THEN
          vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
        ELSE
          vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
        END IF;
        vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                            ' is null then d.tnew else f.t' || i || ' end,';
        vAfterCondition  := vAfterCondition || 't' || i || ',';
        vValuesCondition := vValuesCondition || 'd.tnew,';
      END LOOP;
      vCondition       := substr(vCondition, 1, length(vCondition) - 1);
      vAfterCondition  := substr(vAfterCondition,
                                 1,
                                 length(vAfterCondition) - 1);
      vValuesCondition := substr(vValuesCondition,
                                 1,
                                 length(vValuesCondition) - 1);
      vCondition       := vCondition ||
                          ' when not matched then  insert(prb_mid,selid,tsid,version,yy,' ||
                          vAfterCondition || ')';
      vCondition       := vCondition ||
                          ' values(seq_prb_m.nextval,d.selid,d.tsid,d.version,d.yy,' ||
                          vValuesCondition || ')';
      v_strSql         := v_strSql || vCondition;
    
      fmsp_execsql(v_strSql);
    end if;
  
    for cyear in vCurrentYear + 1 .. vEndYear loop
      v_strSql         := ' merge /*+ parallel */ into prb_m f
      using (select o.selid,o.TSID,o.VERSION,o.YY, o.t2 told,n.t' ||
                          to_number(vMonth) ||
                          ' tnew  from (SELECT selid,TSID,VERSION,b.YY, last_value(T2 ignore nulls) over(partition by selid order by selid,b.yy) t2 FROM(
SELECT A.selid,A.YY,A.VERSION,A.TSID,a.t2 FROM ' ||
                          vOldValAggTable || ' A WHERE A.YY BETWEEN ' ||
                          vCurrentYear || ' AND ' || vEndYear ||
                          ' AND A.TSID =' || pIn_nPriceDBID || '
) A PARTITION BY(a.selid, a.tsid, a.version ) RIGHT OUTER JOIN (select level + ' ||
                          nStartWith ||
                          ' yy from dual connect by level <= ' || nInterval || ')B
ON A.YY=B.YY) o,(select selid,tsid,version,' ||
                          cyear || ' yy,t' || to_number(vMonth) ||
                          ' from prb_m where yy=' || vCurrentYear ||
                          ' )n where
   o.selid=n.selid(+) and o.tsid=n.tsid(+) and o.version=n.version(+) and o.yy=n.yy(+) and o.tsid=64 and o.yy=' ||
                          cyear || ') d
  on(d.selid=f.selid and d.tsid=f.tsid and d.version=f.version and d.yy=f.yy)
      when matched then update set ';
      vCondition       := '';
      vTmpStr          := '';
      vAfterCondition  := '';
      vValuesCondition := '';
      if cyear <> vEndYear then
        FOR i IN 1 .. 12 LOOP
          vCondition := vCondition || ' f.t' || i || '=case when ';
          IF i = 1 THEN
            vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
          ELSE
            vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
          END IF;
          vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                              ' is null then d.tnew else f.t' || i ||
                              ' end,';
          vAfterCondition  := vAfterCondition || 't' || i || ',';
          vValuesCondition := vValuesCondition || 'd.tnew ,';
        END LOOP;
      else
      
        FOR i IN 1 .. vEndMonth LOOP
          vCondition := vCondition || ' f.t' || i || '=case when ';
          IF i = 1 THEN
            vTmpStr := vTmpStr || 'f.t' || i || '=d.told';
          ELSE
            vTmpStr := vTmpStr || ' AND ' || 'f.t' || i || '=d.told';
          END IF;
          vCondition       := vCondition || vTmpStr || ' or f.t' || i ||
                              ' is null then d.tnew else f.t' || i ||
                              ' end,';
          vAfterCondition  := vAfterCondition || 't' || i || ',';
          vValuesCondition := vValuesCondition || 'd.tnew ,';
        END LOOP;
      end if;
      vCondition       := substr(vCondition, 1, length(vCondition) - 1);
      vAfterCondition  := substr(vAfterCondition,
                                 1,
                                 length(vAfterCondition) - 1);
      vValuesCondition := substr(vValuesCondition,
                                 1,
                                 length(vValuesCondition) - 1);
      --vCondition       := vCondition || ' where f.yy=' || cyear;
      vCondition := vCondition ||
                    ' when not matched then  insert(prb_mid,selid,tsid,version,yy,' ||
                    vAfterCondition || ')';
      vCondition := vCondition ||
                    ' values(seq_prb_m.nextval,d.selid,d.tsid,d.version,d.yy,' ||
                    vValuesCondition || ')';
      v_strSql   := v_strSql || vCondition;
    
      fmsp_execsql(v_strSql);
    
    end loop;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_UpdateAggreateFuture;

  PROCEDURE FMSP_GenNode(pIn_vTableName          IN VARCHAR2,
                         pIn_nHistValDBID        IN NUMBER,
                         pIn_nHistQtyDBID        IN NUMBER,
                         pIn_nPriceDBID          IN NUMBER,
                         pIn_nTimeSeriesNodeType IN NUMBER,
                         pIn_nPeriodIndex        IN NUMBER,
                         pIn_nBeginDate          IN NUMBER,
                         pIn_nEndDate            IN NUMBER,
                         pIn_nlsUsedInFuture     IN NUMBER, --0 : true, 1:false
                         pIn_Precision           IN NUMBER,
                         pIn_isEndDate           IN NUMBER, --1 NO,2 YES
                         pOut_nSqlCode           OUT NUMBER) AS
  
    vSQL            VARCHAR2(4000);
    nPeriod         NUMBER;
    vTempTableInner VARCHAR2(30) DEFAULT 'tmp_timeserialsdata';
  BEGIN
    --Create second temporary table ,including the  conversion data,
    --the table structure as the don_m
  
    /*FOR i IN 1 .. pIn_nPeriodIndex - 1 LOOP
      vConditions := vConditions || '+' || 'presentnnv(val[cv(yymm)-' || i ||
                     '],val[cv(yymm)-' || i || '],0)';
    END LOOP;*/
    nPeriod := pIn_nPeriodIndex - 1;
    --vTempTableInner := fmf_gettmptablename();
    BEGIN
      v_strSql := 'truncate table ' || vTempTableInner;
      fmsp_execsql(v_strSql);
    EXCEPTION
      WHEN OTHERS THEN
        v_strSql := 'create global temporary  table ' || vTempTableInner;
        v_strSql := v_strSql ||
                    '(SELID NUMBER,TSID  NUMBER,VERSION INTEGER,YY INTEGER,';
        v_strSql := v_strSql ||
                    ' T1   NUMBER,T2 NUMBER,T3   NUMBER,T4   NUMBER,T5   NUMBER,T6 NUMBER,T7   NUMBER,T8   NUMBER,';
        v_strSql := v_strSql ||
                    ' T9   NUMBER,T10 NUMBER,T11   NUMBER,T12   NUMBER) on commit preserve rows ';
        fmsp_execsql(v_strSql);
    END;
  
    vSQL := 'INSERT  INTO ' || vTempTableInner ||
            ' with tab as(

select/*+ parallel */ selid,TSID,version,MM,YY,yymm,val,
last_value(price ignore nulls) over(partition by selid order by selid,yymm) price  from (

select  a.selid,64 tsid,a.version,mod(a.yymm,100) mm, floor(a.yymm/100) yy,a.yymm,a.val,/*a.calcval, */
 round((case when a.calcval<=0 then null else a.calcval end )/(case when b.calcval<=0 then null else b.calcval end),' ||
            pIn_Precision ||
            ') price
 from (
select /*+ parallel */ selid,version,yymm,val,
sum(val) over(partition by selid,tsid order by selid,tsid,yymm rows between ' ||
            nPeriod || ' preceding and current row)  calcval
from ' || pIn_vTableName || '
where tsid=' || pIn_nHistValDBID || '
 )a ,
(select * from(
select /*+ parallel */ selid,version,yymm,val,
sum(val) over(partition by selid,tsid order by selid,tsid,yymm rows between ' ||
            nPeriod || ' preceding and current row)  calcval
from ' || pIn_vTableName || '
where tsid=' || pIn_nHistQtyDBID || ')
 ) b
where a.selid=b.selid  and a.version=b.version and a.yymm=b.yymm
)

  )
  select *
  from
         (select selid,tsid,version, yy,mm,price from  tab   where  YYMM between ' ||
            pIn_nBeginDate || ' and ' || pIn_nEndDate || ' and tsid = ' ||
            pIn_nPriceDBID || ')
       pivot (max(price) for mm in (1 as t1, 2 as t2 ,3 as t3 ,4 as t4, 5 as t5 ,6 as t6 , 7 as t7 , 8 as t8
              ,9 as t9,10 as t10 ,11 as t11 ,12 as t12))';
  
    fmsp_execsql(vSQL);
  
    IF pIn_nTimeSeriesNodeType = 1 THEN
    
      --UPDATE DETAIL NODES
      if pIn_isEndDate = 1 then
      
        FMSP_UpdateDetail(vTempTableInner,
                          pIn_nBeginDate,
                          pIn_nEndDate,
                          pOut_nSqlCode);
      
      end if;
    
      --Update aggregate nodes
    ELSIF pIn_nTimeSeriesNodeType = 2 THEN
    
      FMSP_UpdateAggreate(vTempTableInner,
                          pIn_nBeginDate,
                          pIn_nEndDate,
                          pOut_nSqlCode);
    
    END IF;
  
    --Drop inner temprory table
    --sp_DropTSTable(vTempTableInner, pOut_nSqlCode);
  
    --
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_GenNode;

  PROCEDURE FMSP_GenNode13(pIn_vTableName          IN VARCHAR2,
                           pIn_nHistValDBID        IN NUMBER,
                           pIn_nHistQtyDBID        IN NUMBER,
                           pIn_nPriceDBID          IN NUMBER,
                           pIn_nTimeSeriesNodeType IN NUMBER,
                           pIn_nBeginDate          IN NUMBER,
                           pIn_nEndDate            IN NUMBER,
                           pIn_nlsUsedInFuture     IN NUMBER, --0 : true, 1:false
                           pIn_Precision           IN NUMBER,
                           pOut_nSqlCode           OUT NUMBER) AS
    /* Used in pIn_nPeriodIndex =13*/
    vTempTableInner VARCHAR2(30) DEFAULT 'tmp_timeserialsdata';
  BEGIN
  
    --vTempTableInner := fmf_gettmptablename();
    BEGIN
      v_strSql := 'truncate table ' || vTempTableInner;
      fmsp_execsql(v_strSql);
    EXCEPTION
      WHEN OTHERS THEN
        v_strSql := ' create global temporary  table ' || vTempTableInner;
        v_strSql := v_strSql ||
                    '(SELID NUMBER,TSID  NUMBER,VERSION INTEGER,YY INTEGER,';
        v_strSql := v_strSql ||
                    ' T1   NUMBER,T2 NUMBER,T3   NUMBER,T4   NUMBER,T5   NUMBER,T6 NUMBER,T7   NUMBER,T8   NUMBER,';
        v_strSql := v_strSql ||
                    ' T9   NUMBER,T10 NUMBER,T11   NUMBER,T12   NUMBER) on commit preserve rows ';
        fmsp_execsql(v_strSql);
    END;
  
    v_strSql := 'INSERT  INTO ' || vTempTableInner || '
 with tab as (
select /*+ parallel(t,4) parallel(s,4) */ t.selid, t.tsid,t.version,t.yymm,mod(t.yymm,100) mm, floor(t.yymm/100) yy,round((case when t.a<=0 then null else t.a end)/(case when s.b<=0 then null else s.b end),' ||
                pIn_Precision || ') price from (
select selid,' || pIn_nPriceDBID || ' tsid,version,yymm,val,sum(val) over(partition by selid ,tsid,version order  by tsid,selid ,version,yymm rows between unbounded preceding and current row )a
        from ' || pIn_vTableName || '
       where tsid = ' || pIn_nHistValDBID || '
         AND YYMM BETWEEN ' || pIn_nBeginDate || ' and ' ||
                pIn_nEndDate || '
         ) t,
         ( SELECT selid ,' || pIn_nPriceDBID || ' tsid,version ,yymm,val,sum(val) over(partition by selid ,tsid,version order  by tsid,selid ,version,yymm rows between unbounded preceding and current row )b
  FROM ' || pIn_vTableName || '
 where tsid=' || pIn_nHistQtyDBID || '
 and yymm between ' || pIn_nBeginDate || ' and ' ||
                pIn_nEndDate || '
 ) s
where t.selid = s.selid and t.tsid=s.tsid and t.yymm=s.yymm and t.version =s.version
)
select * from (select selid,tsid,version, yy,mm,price from  tab   where  YYMM between ' ||
                pIn_nBeginDate || ' and ' || pIn_nEndDate || '
and tsid=' || pIn_nPriceDBID || '
)
pivot (max(price) for mm in (1 as t1, 2 as t2 ,3 as t3 ,4 as t4, 5 as t5 ,6 as t6 , 7 as t7 , 8 as t8
              ,9 as t9,10 as t10 ,11 as t11 ,12 as t12))';
  
    fmsp_execsql(v_strSql);
    IF pIn_nTimeSeriesNodeType = 1 THEN
      --update in future is checked
      FMSP_UpdateDetail(vTempTableInner,
                        pIn_nBeginDate,
                        pIn_nEndDate,
                        pOut_nSqlCode);
    
      --UPDATE AGGREGATE NODES
    ELSIF pIn_nTimeSeriesNodeType = 2 THEN
      --update in future is checked
      FMSP_UpdateAggreate(vTempTableInner,
                          pIn_nBeginDate,
                          pIn_nEndDate,
                          pOut_nSqlCode);
    
    END IF;
  
    --Drop inner temprory table
    --FMSP_DropTSTable(vTempTableInner, pOut_nSqlCode);
  
    pOut_nSqlCode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_GenNode13;

  PROCEDURE FMSP_CalcPrice(pIn_nChronology         IN NUMBER, --1 Month ,2 Week,4 Day
                           pIn_nHistValDBID        IN NUMBER,
                           pIn_nHistQtyDBID        IN NUMBER,
                           pIn_nPriceDBID          IN NUMBER,
                           pIn_nTimeSeriesNodeType IN NUMBER, --1  Detail Node  2  Aggregate Node
                           pIn_vFUser              IN VARCHAR2,
                           pIn_vOptions            IN VARCHAR2,
                           pIn_nPeriodIndex        IN NUMBER,
                           pIn_nBeginDate          IN NUMBER,
                           pIn_nEndCalcOfDate      IN NUMBER,
                           pIn_nEndDate            IN NUMBER,
                           pIn_nlsUsedInFuture     IN NUMBER, --0 : false, 1:true
                           pIn_Precision           IN NUMBER DEFAULT 2,
                           pOut_nSqlCode           OUT NUMBER) AS
  
    --2.update section
    --3.Update the detailnodes
    --pragma autonomous_transaction;
    nPrecision     NUMBER;
    v_oOptions     P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    v_vConditions  VARCHAR2(600) DEFAULT '';
    v_strTableName VARCHAR2(30) DEFAULT 'tmp_newstructtimeserials';
    vEndDATE       NUMBER;
    vBeginDate     number;
    type v_arr is varray(2) of number;
    date_arr v_arr := v_arr(pIn_nEndCalcOfDate, pIn_nEndDate);
  BEGIN
    --TSET
    --v_strTableName := 'TBMID30022';
    --
    --log begin
    FMP_LOG.FMP_SETVALUE(pIn_nChronology);
    FMP_LOG.FMP_SETVALUE(pIn_nHistValDBID);
    FMP_LOG.FMP_SETVALUE(pIn_nHistQtyDBID);
    FMP_LOG.FMP_SETVALUE(pIn_nPriceDBID);
    FMP_LOG.FMP_SETVALUE(pIn_nTimeSeriesNodeType);
    FMP_LOG.FMP_SETVALUE(pIn_vFUser);
    FMP_LOG.FMP_SETVALUE(pIn_vOptions);
    FMP_LOG.FMP_SETVALUE(pIn_nPeriodIndex);
    FMP_LOG.FMP_SETVALUE(pIn_nBeginDate);
    fmp_log.FMP_SetValue(pIn_nEndCalcOfDate);
    FMP_LOG.FMP_SETVALUE(pIn_nEndDate);
    FMP_LOG.FMP_SETVALUE(pIn_nlsUsedInFuture);
    FMP_LOG.FMP_SETVALUE(pIn_Precision);
    FMP_log.logBegin;
  
    IF pIn_Precision IS NULL THEN
      nPrecision := 2;
    ELSE
      nPrecision := pIn_Precision;
    END IF;
    vEndDATE := pIn_nEndDate;
    P_BATCHCOMMAND_COMMON.sp_ParseOptions(pIn_vOptions,
                                          v_oOptions,
                                          pOut_nSqlCode);
    /*v_oOptions.bSel*/
    IF v_oOptions.bSel OR pIn_vOptions IS NULL THEN
    
      IF pIn_nChronology = p_constant.Monthly THEN
      
        IF pIn_nlsUsedInFuture = 0 THEN
          vEndDATE := pIn_nEndCalcOfDate;
          --v_strTableName := fmf_gettmptablename();
          --Create Aggregation Node temp table
          IF pIn_nTimeSeriesNodeType = 1 THEN
          
            FMSP_CreateDetailNodeTB(v_strTableName,
                                    pIn_nHistValDBID,
                                    pIn_nHistQtyDBID,
                                    v_oOptions.strSel,
                                    pIn_nPeriodIndex,
                                    
                                    pIn_nBeginDate,
                                    vEndDATE,
                                    pOut_nSqlCode);
          
            --Create Aggregation Node temp table
          ELSIF pIn_nTimeSeriesNodeType = 2 THEN
            IF v_oOptions.bSelCondit THEN
              v_vConditions := v_oOptions.strSelCondit;
            END IF;
            FMSP_CreateAggregateNodeTB(pIn_vTableName   => v_strTableName,
                                       pIn_nHistValDBID => pIn_nHistValDBID,
                                       pIn_nHistQtyDBID => pIn_nHistQtyDBID,
                                       pIn_vOptions     => v_oOptions.strSel,
                                       pIn_vConditions  => v_vConditions,
                                       pIn_nPeriodIndex => pIn_nPeriodIndex,
                                       pIn_nBeginDate   => pIn_nBeginDate,
                                       pIn_nEndDate     => vEndDATE,
                                       pOut_nSqlCode    => pOut_nSqlCode);
            --v_strTableName := 'TBMID30070';
          END IF;
          IF pOut_nSqlCode = -100 THEN
            RETURN;
          END IF;
        
          --pIn_nPeriodIndex=13 process accumulation
          IF pIn_nPeriodIndex = 13 THEN
            FMSP_GenNode13(pIn_vTableName          => v_strTableName,
                           pIn_nHistValDBID        => pIn_nHistValDBID,
                           pIn_nHistQtyDBID        => pIn_nHistQtyDBID,
                           pIn_nPriceDBID          => pIn_nPriceDBID,
                           pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                           pIn_nBeginDate          => pIn_nBeginDate,
                           pIn_nEndDate            => vEndDATE,
                           pIn_nlsUsedInFuture     => pIn_nlsUsedInFuture,
                           pIn_Precision           => nPrecision,
                           pOut_nSqlCode           => pOut_nSqlCode);
          ELSE
            --update detail node
            FMSP_GenNode(pIn_vTableName          => v_strTableName,
                         pIn_nHistValDBID        => pIn_nHistValDBID,
                         pIn_nHistQtyDBID        => pIn_nHistQtyDBID,
                         pIn_nPriceDBID          => pIn_nPriceDBID,
                         pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                         pIn_nPeriodIndex        => pIn_nPeriodIndex,
                         pIn_nBeginDate          => pIn_nBeginDate,
                         pIn_nEndDate            => vEndDATE,
                         pIn_nlsUsedInFuture     => pIn_nlsUsedInFuture,
                         pIn_Precision           => nPrecision,
                         pIn_isEndDate           => 1,
                         pOut_nSqlCode           => pOut_nSqlCode);
          
          END IF;
          --SELECTED IN FEATURE
        ELSIF pIn_nlsUsedInFuture = 1 THEN
          FOR i IN 1 .. date_arr.COUNT LOOP
            if i = 1 then
              vBeginDate := pIn_nBeginDate;
              vEndDATE   := date_arr(i); --to_number(to_char(add_months(to_date(date_arr(i),'yyyymm'),-1),'yyyymm'));
              --Create Aggregation Node temp table
              IF pIn_nTimeSeriesNodeType = 1 THEN
                --
                FMSP_CreateDetailNodeTB(v_strTableName,
                                        pIn_nHistValDBID,
                                        pIn_nHistQtyDBID,
                                        v_oOptions.strSel,
                                        pIn_nPeriodIndex,
                                        vBeginDate,
                                        vEndDATE,
                                        pOut_nSqlCode);
                --save old value to table
                BEGIN
                  v_strSql := 'TRUNCATE TABLE ' || vOldValDetailTable;
                  fmsp_execsql(v_strSql);
                  v_strSql := 'insert into ' || vOldValDetailTable ||
                              ' select pvtid,tsid,version,yy,t' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) ||
                              ' from ' || vTmpDetailTableName ||
                              ' t1,don_m t2 where t1.id=t2.pvtid and t2.tsid=' ||
                              pIn_nPriceDBID || '
                              and t2.yy= ' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4));
                  fmsp_execsql(v_strSql);
                EXCEPTION
                  when others then
                    v_strSql := 'create global temporary table ' ||
                                vOldValDetailTable ||
                                '(pvtid integer,tsid integer,version integer,yy integer,t2 number) on commit preserve rows';
                    fmsp_execsql(v_strSql);
                    v_strSql := 'insert into ' || vOldValDetailTable ||
                                ' select pvtid,tsid,version,yy,t' ||
                                to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) ||
                                ' from ' || vTmpDetailTableName ||
                                ' t1,don_m t2 where t1.id=t2.pvtid and t2.tsid=' ||
                                pIn_nPriceDBID || '
                              and t2.yy= ' ||
                                to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4));
                    fmsp_execsql(v_strSql);
                end;
                --Create Aggregation Node temp table
              ELSIF pIn_nTimeSeriesNodeType = 2 THEN
              
                IF v_oOptions.bSelCondit THEN
                  v_vConditions := v_oOptions.strSelCondit;
                END IF;
              
                FMSP_CreateAggregateNodeTB(pIn_vTableName   => v_strTableName,
                                           pIn_nHistValDBID => pIn_nHistValDBID,
                                           pIn_nHistQtyDBID => pIn_nHistQtyDBID,
                                           pIn_vOptions     => v_oOptions.strSel,
                                           pIn_vConditions  => v_vConditions,
                                           pIn_nPeriodIndex => pIn_nPeriodIndex,
                                           pIn_nBeginDate   => vBeginDate,
                                           pIn_nEndDate     => vEndDATE,
                                           pOut_nSqlCode    => pOut_nSqlCode);
                --save old value to table
                BEGIN
                  v_strSql := 'TRUNCATE TABLE ' || vOldValAggTable;
                  fmsp_execsql(v_strSql);
                  v_strSql := 'insert into ' || vOldValAggTable ||
                              ' select selid,tsid,version,yy,t' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) ||
                              ' from ' || vTmpAggTableName ||
                              ' t1,prb_m t2 where t1.id=t2.selid and t2.tsid=' ||
                              pIn_nPriceDBID || '
                              and t2.yy= ' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4));
                  fmsp_execsql(v_strSql);
                EXCEPTION
                  when others then
                    v_strSql := 'create global temporary table ' ||
                                vOldValAggTable ||
                                '(selid integer,tsid integer,version integer,yy integer,t2 number) on commit preserve rows';
                    fmsp_execsql(v_strSql);
                    v_strSql := 'insert into ' || vOldValAggTable ||
                                ' select selid,tsid,version,yy,t' ||
                                to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) ||
                                ' from ' || vTmpAggTableName ||
                                ' t1,prb_m t2 where t1.id=t2.selid and t2.tsid=' ||
                                pIn_nPriceDBID || '
                              and t2.yy= ' ||
                                to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4));
                    fmsp_execsql(v_strSql);
                end;
              END IF;
            
              IF pOut_nSqlCode = -100 THEN
                RETURN;
              END IF;
            
              --pIn_nPeriodIndex=13 process accumulation
              IF pIn_nPeriodIndex = 13 THEN
                FMSP_GenNode13(pIn_vTableName          => v_strTableName,
                               pIn_nHistValDBID        => pIn_nHistValDBID,
                               pIn_nHistQtyDBID        => pIn_nHistQtyDBID,
                               pIn_nPriceDBID          => pIn_nPriceDBID,
                               pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                               pIn_nBeginDate          => vBeginDate,
                               pIn_nEndDate            => vEndDATE,
                               pIn_nlsUsedInFuture     => pIn_nlsUsedInFuture,
                               pIn_Precision           => nPrecision,
                               pOut_nSqlCode           => pOut_nSqlCode);
              ELSE
                --update  node
                FMSP_GenNode(pIn_vTableName          => v_strTableName,
                             pIn_nHistValDBID        => pIn_nHistValDBID,
                             pIn_nHistQtyDBID        => pIn_nHistQtyDBID,
                             pIn_nPriceDBID          => pIn_nPriceDBID,
                             pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                             pIn_nPeriodIndex        => pIn_nPeriodIndex,
                             pIn_nBeginDate          => vBeginDate,
                             pIn_nEndDate            => vEndDATE,
                             pIn_nlsUsedInFuture     => pIn_nlsUsedInFuture,
                             pIn_Precision           => nPrecision,
                             pIn_isEndDate           => i,
                             pOut_nSqlCode           => pOut_nSqlCode);
              
              END IF;
            ELSE
            
              IF pIn_nTimeSeriesNodeType = 1 THEN
                --save old value to table
                BEGIN
                  --v_strSql := 'TRUNCATE TABLE don_m_old';
                  --fmsp_execsql(v_strSql);
                  v_strSql := 'merge into ' || vOldValDetailTable || '  d
        using (select * from don_m t where t.tsid=' ||
                              pIn_nPriceDBID || ' and t.yy=' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4)) ||
                              ') t2
        on(d.pvtid = t2.pvtid and d.tsid=t2.tsid and d.version=t2.version and d.yy=t2.yy)
        when not matched then
          insert(pvtid,tsid,version,yy,t2)
          values(t2.pvtid,t2.tsid,t2.version,t2.yy,t2.t' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) || ')';
                
                  fmsp_execsql(v_strSql);
                
                  --added begin 20130322 when qty ,value is null ,update the specifyed price's month data to null
                  v_strSql := 'merge into don_m d
      using (select m.pvtid
               from don_m m,' ||
                              vTmpDetailTableName ||
                              ' tmp
              where m.pvtid=tmp.id and m.tsid = ' ||
                              pIn_nPriceDBID || '
                and m.yy between ' ||
                              substr(pIn_nBeginDate, 1, 4) ||
                              ' and
                    ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4) || '
             minus
             select m.pvtid
               from don_m m,' ||
                              vTmpDetailTableName ||
                              ' tmp
              where m.pvtid=tmp.id and m.tsid in (' ||
                              pIn_nHistValDBID || ', ' || pIn_nHistQtyDBID || ')
                and m.yy between ' ||
                              substr(pIn_nBeginDate, 1, 4) ||
                              ' and
                    ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4) || ') e
      on (d.pvtid = e.pvtid)
      when matched then
        update
           set d.t' ||
                              to_number(substr(pIn_nEndCalcOfDate, 5, 2)) ||
                              ' = null
         where d.tsid = ' || pIn_nPriceDBID || '
           and d.yy = ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4);

                  fmsp_execsql(v_strSql);
                  --end
                EXCEPTION
                  when others then
                    raise;
                end;
                --update in future is checked
                FMSP_UpdateDetailFutrue('',
                                        pIn_nPriceDBID,
                                        pIn_nBeginDate,
                                        pIn_nEndCalcOfDate,
                                        pIn_nEndDate,
                                        pOut_nSqlCode);
                --UPDATE AGGREGATE NODES
              ELSIF pIn_nTimeSeriesNodeType = 2 THEN
                --save old value to table
                BEGIN
                  v_strSql := 'merge into ' || vOldValAggTable || '  d
        using (select * from prb_m t where t.tsid=' ||
                              pIn_nPriceDBID || ' and t.yy=' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 1, 4)) ||
                              ') t2
        on(d.selid = t2.selid and d.tsid=t2.tsid and d.version=t2.version and d.yy=t2.yy)
        when not matched then
          insert(selid,tsid,version,yy,t2)
          values(t2.selid,t2.tsid,t2.version,t2.yy,t2.t' ||
                              to_number(SUBSTR(pIn_nEndCalcOfDate, 5, 2)) || ')';
                
                  fmsp_execsql(v_strSql);
                
                  --added begin 20130322 when qty ,value is null ,update the specifyed month's  data to null
                  v_strSql := 'merge into prb_m d
      using (select m.selid
               from prb_m m,' ||
                              vTmpAggTableName ||
                              ' tmp
              where m.selid=tmp.id and m.tsid = ' ||
                              pIn_nPriceDBID || '
                and m.yy between ' ||
                              substr(pIn_nBeginDate, 1, 4) ||
                              ' and
                    ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4) || '
             minus
             select m.selid
               from prb_m m,' ||
                              vTmpAggTableName ||
                              ' tmp
              where m.selid=tmp.id and m.tsid in (' ||
                              pIn_nHistValDBID || ', ' || pIn_nHistQtyDBID || ')
                and m.yy between ' ||
                              substr(pIn_nBeginDate, 1, 4) ||
                              ' and
                    ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4) || ') e
      on (d.selid = e.selid)
      when matched then
        update
           set d.t' ||
                              to_number(substr(pIn_nEndCalcOfDate, 5, 2)) ||
                              ' = null
         where d.tsid = ' || pIn_nPriceDBID || '
           and d.yy = ' ||
                              substr(pIn_nEndCalcOfDate, 1, 4);

                  fmsp_execsql(v_strSql);
                  --end
                
                EXCEPTION
                  when others then
                    raise;
                end;
                --update in future is checked
                FMSP_UpdateAggreateFuture('',
                                          pIn_nPriceDBID,
                                          pIn_nBeginDate,
                                          pIn_nEndCalcOfDate,
                                          pIn_nEndDate,
                                          pOut_nSqlCode);
              END IF;
            end if;
          
          END LOOP;
          --IF PRICE SERIALS HAS NOT CORESPONDING VALUE AND QTY SERIALS
          /*IF pIn_nTimeSeriesNodeType = 1 THEN
            v_strSql := 'merge into don_m p
                using( select distinct a.pvtid from (
                         select * from don_m  ,' ||
                        vTmpDetailTableName ||
                        ' A where A.ID=don_m.pvtid AND tsid = ' ||
                        pIn_nPriceDBID ||
                        ' ) a,
                (        select * from don_m where tsid in(' ||
                        pIn_nHistValDBID || ',' || pIn_nHistQtyDBID ||
                        '))  b
                where a.pvtid = b.pvtid(+)  and b.pvtid is null
                ) d
                on(p.pvtid=d.pvtid)
                when matched then
                  update set p.t1=null,p.t2=null,p.t3=null,p.t4=null,p.t5=null,p.t6=null,p.t7=null,
                  p.t8=null,p.t9=null,p.t10=null,p.t11=null,p.t12=null
                  where p.tsid=' || pIn_nPriceDBID;
            fmsp_execsql(v_strSql);
          ELSE
            v_strSql := 'merge into prb_m p
                using( select distinct a.selid from (
                         select * from prb_m ,' ||
                        vTmpAggTableName ||
                        ' A where A.ID=PRB_M.SELID AND tsid = ' ||
                        pIn_nPriceDBID ||
                        ' ) a,
                (        select * from prb_m where tsid in(' ||
                        pIn_nHistValDBID || ',' || pIn_nHistQtyDBID ||
                        '))  b
                where a.selid = b.selid(+)  and b.selid is null
                ) d
                on(p.selid=d.selid)
                when matched then
                  update set p.t1=null,p.t2=null,p.t3=null,p.t4=null,p.t5=null,p.t6=null,p.t7=null,
                  p.t8=null,p.t9=null,p.t10=null,p.t11=null,p.t12=null
                  where p.tsid=' || pIn_nPriceDBID;
            fmsp_execsql(v_strSql);
          END IF;*/
        END IF;
      
      ELSIF pIn_nChronology = p_constant.Weekly THEN
        NULL;
      ELSIF pIn_nChronology = p_constant.Daily THEN
        NULL;
      END IF;
    
    END IF;
    pOut_nSqlCode := 0;
    --log end
    FMP_LOG.LOGEND;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := -1;
      --error
      FMP_LOG.LOGERROR;
      Fmp_Log.LOGERROR;
      RAISE;
  END FMSP_CalcPrice;

END FMP_CalcPrice;
/
