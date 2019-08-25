create or replace package FMIP_CombinKeyBySortSequenc is

  -- Author  : LZHANG
  -- Created : 3/3/2013 3:32:35 PM
  -- Purpose :

  -- Public type declarations
  TYPE aggregationRules IS TABLE OF varchar2(4000) INDEX BY BINARY_INTEGER;
  procedure FMISP_CombineKeyBySortSequenc(pIn_vTableName in varchar2,
                                          pOut_sDataSet  out sys_refcursor,
                                          pOut_nSqlCode  out number);
end FMIP_CombinKeyBySortSequenc;
/
create or replace package body FMIP_CombinKeyBySortSequenc is

  procedure FMSP_GetAggregationRules(pIn_nPLevel   in number,
                                     pIn_nSLevel   in number,
                                     pIn_nTLevel   in number,
                                     pOut_sDataSet out aggregationRules) IS
  begin
    declare
      type aggregationData is record(
        prv_em_addr prv.prv_em_addr%TYPE,
        regroup_pro prv.regroup_pro%TYPE,
        regroup_geo prv.regroup_geo%TYPE,
        regroup_dis prv.regroup_dis%TYPE,
        n0_cdt      cdt.n0_cdt%TYPE,
        rcd_cdt     cdt.rcd_cdt%TYPE,
        prv_cle     prv.prv_cle%TYPE);
      aAggregationData  aggregationData;
      aAggregationRules aggregationRules;
      cSql              clob;
      vAggregationRule  varchar2(4000) := '';
      vPAttributes      varchar2(4000) := '';
      vSAttributes      varchar2(4000) := '';
      vTAttributes      varchar2(4000) := '';
      vNAttributes      varchar2(4000) := ' ';
      vTmp              varchar2(4000) := ' ';
      last_prv_en_addr  prv.prv_em_addr%TYPE := 0;
      nIndex            number := 1;
      cursor cSelectAggregationRules is
        SELECT prv.prv_em_addr,
               prv.regroup_pro,
               prv.regroup_geo,
               prv.regroup_dis,
               0,
               0,
               prv.prv_cle
          FROM PRV
        MINUS
        SELECT prv.prv_em_addr,
               prv.regroup_pro,
               prv.regroup_geo,
               prv.regroup_dis,
               0,
               0,
               prv.prv_cle
          FROM prv, cdt
         WHERE prv.prv_em_addr = cdt.prv12_em_addr
        UNION
        SELECT prv.prv_em_addr,
               prv.regroup_pro,
               prv.regroup_geo,
               prv.regroup_dis,
               cdt.n0_cdt,
               cdt.rcd_cdt,
               prv.prv_cle
          FROM prv, cdt
         WHERE prv.prv_em_addr = cdt.prv12_em_addr
         ORDER BY 7, 6, 5;
    
    begin
      open cSelectAggregationRules;
      loop
        fetch cSelectAggregationRules
          into aAggregationData;
        if cSelectAggregationRules%notfound then
          -- data over
          -- operation the last data
          if length(vPAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vPAttributes;
          end if;
          if length(vSAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vSAttributes;
          end if;
          if length(vTAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vTAttributes;
          end if;
          if length(vNAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vNAttributes;
          end if;
          IF length(vAggregationRule) > 5 THEN
            aAggregationRules(nIndex) := vAggregationRule;
            --DEBUG
            --insert into aggRules (contents) values (vAggregationRule);
            --commit;
            --DEBUG
            nIndex := nIndex + 1;
          END IF;
          exit; -- exit
        end if;
        if aAggregationData.prv_em_addr <> last_prv_en_addr then
          -- operation the last data
          if length(vPAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vPAttributes;
          end if;
          if length(vSAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vSAttributes;
          end if;
          if length(vTAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vTAttributes;
          end if;
          if length(vNAttributes) > 5 then
            vAggregationRule := vAggregationRule || '||''-''||' ||
                                vNAttributes;
          end if;
          IF length(vAggregationRule) > 5 THEN
            -- split joint an aggregation rule
            aAggregationRules(nIndex) := vAggregationRule;
            --DEBUG
            --insert into aggRules (contents) values (vAggregationRule);
            --commit;
            --DEBUG
            nIndex := nIndex + 1;
          END IF;
          vAggregationRule := '';
          vPAttributes     := '';
          vSAttributes     := '';
          vTAttributes     := '';
          vNAttributes     := '';
          -- init last_prv_en_addr
          last_prv_en_addr := aAggregationData.prv_em_addr;
          if aAggregationData.regroup_pro <> 0 AND
             aAggregationData.regroup_pro is not null then
            -- get product level
            vAggregationRule := 'C_9999_' ||
                                to_char(aAggregationData.regroup_pro -
                                        pIn_nPLevel) || '_K';
          end if;
          if aAggregationData.regroup_geo <> 0 AND
             aAggregationData.regroup_geo is not null then
            -- get s level
            if length(vAggregationRule) > 5 then
              vAggregationRule := vAggregationRule || '||''-''||';
            end if;
            vAggregationRule := vAggregationRule || 'C_10001_' ||
                                to_char(aAggregationData.regroup_geo -
                                        pIn_nSLevel) || '_K';
          
          end if;
          if aAggregationData.regroup_dis <> 0 AND
             aAggregationData.regroup_dis is not null then
            -- get t level
            if length(vAggregationRule) > 5 then
              vAggregationRule := vAggregationRule || '||''-''||';
            end if;
            vAggregationRule := vAggregationRule || 'C_10002_' ||
                                to_char(aAggregationData.regroup_dis -
                                        pIn_nTLevel) || '_K';
          end if;
        end if;
        if aAggregationData.rcd_cdt = 20007 then
          -- product attribute
          if length(vPAttributes) > 5 then
            vPAttributes := vPAttributes || '||''-''||';
          end if;
          vPAttributes := vPAttributes || 'C_' ||
                          to_char(30050 + aAggregationData.n0_cdt) || '_1' || '_K';
        elsif aAggregationData.rcd_cdt = 20008 then
          -- sales territory attribute
          if length(vSAttributes) > 5 then
            vSAttributes := vSAttributes || '||''-''||';
          end if;
          vSAttributes := vSAttributes || 'C_' ||
                          to_char(30100 + aAggregationData.n0_cdt) || '_1' || '_K';
        elsif aAggregationData.rcd_cdt = 20009 then
          -- trade channel attribute
          if length(vTAttributes) > 5 then
            vTAttributes := vTAttributes || '||''-''||';
          end if;
          vAggregationRule := vAggregationRule || 'C_' ||
                              to_char(30150 + aAggregationData.n0_cdt) || '_1' || '_K';
        elsif aAggregationData.rcd_cdt = 10055 then
          -- node attribute
          if length(vNAttributes) > 5 then
            vNAttributes := vNAttributes || '||''-''||';
          end if;
          vNAttributes := vNAttributes || 'C_' ||
                          to_char(27000 + aAggregationData.n0_cdt) ||
                          '_1_K';
        end if;
      
      end loop;
      pOut_sDataSet := aAggregationRules;
      close cSelectAggregationRules;
    end;
  end FMSP_GetAggregationRules;

  procedure FMISP_CombineKeyBySortSequenc(pIn_vTableName in varchar2,
                                          pOut_sDataSet  out sys_refcursor,
                                          pOut_nSqlCode  out number) IS
    --*****************************************************************
    -- Description:  it support for CombineKeyBySortSequenc
    --
    -- Parameters:
    -- pIn_vTableName the table's name which C++ developer store up data
    -- pOut_sDataSet  the result columns  is BDG.BDG_EM_ADDR,SEL.SEL_EM_ADDR,KEY
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        3-Mar-2013     lei zhang     Created.
    -- **************************************************************
  begin
    declare
      aAggregationRules aggregationRules;
      nIndex            number;
      cSql              clob;
      vAggregationRule  varchar2(4000);
      vSql              varchar2(4000);
      nPLevel           number;
      nSLevel           number;
      nTLevel           number;
      sCursor           sys_refcursor;
      --sDataSet          sys_refcursor;
      bDetailNode         boolean := false;
    begin
      FMP_LOG.FMP_SetValue(pIn_vTableName);
      FMP_LOG.LOGBEGIN;
      --DEBUG
      --delete from aggRules;
      --commit;
      --DEBUG
      
      cSql := 'select prv.regroup_pro, prv.regroup_geo, prv.regroup_dis
                               from prv, sel, prvsel
                        where sel.sel_cle = (select sel_cle from ' || pIn_vTableName || ' where rownum < 2)
                              and prv.prv_em_addr = prvsel.prv15_em_addr
                              and sel.sel_em_addr = prvsel.sel16_em_addr';
      open sCursor for cSql;
      if sCursor%notfound then
        bDetailNode := true;
      else 
        fetch sCursor
        into nPLevel, nSlevel, nTLevel;
      end if;
      close sCursor;
      cSql:=null;
      if nPLevel > 0 then
        nPlevel := nPlevel - 1;
      end if;
      if nSlevel > 0 then
        nSlevel := nSlevel - 1;
      end if;
      if nTlevel > 0 then
        nTlevel := nTlevel - 1;
      end if;
      FMSP_GetAggregationRules(nPLevel,
                               nSLevel,
                               nTLevel,
                               aAggregationRules);
      if bDetailNode then
        aAggregationRules(aAggregationRules.count+1):='C_9999_1_K-C_10001_1_K-C_10002_1_K';
        aAggregationRules(aAggregationRules.count+1):='C_9999_1_K-C_10001_1_K';
        aAggregationRules(aAggregationRules.count+1):='C_9999_1_K-C_10002_1_K';
        aAggregationRules(aAggregationRules.count+1):='C_9999_1_K';
      end if;
      FOR nIndex in 1 .. aAggregationRules.count LOOP
        begin
          begin
            vSql := 'SELECT ' || aAggregationRules(nIndex) || '  FROM ' ||
                    pIn_vTableName;
            execute immediate vSql;
            vSql := 'SELECT BDG.BDG_EM_ADDR,SEL.SEL_EM_ADDR,' ||
                    aAggregationRules(nIndex) || '  FROM ' ||
                    pIn_vTableName || ',SEL,BDG WHERE BDG.B_CLE=' ||
                    aAggregationRules(nIndex) || ' AND SEL.SEL_CLE=' ||
                    aAggregationRules(nIndex) || ' AND BDG.ID_BDG=71';
            if length(cSql) > 5 then
              cSql := cSql || ' UNION ALL ';
            end if;
            cSql := cSql || vSql;
          end;
        exception
          WHEN OTHERS THEN
            null;
        end;
      END LOOP;
      vSql := cSql;
      if length(cSql) > 5 then
        open pOut_sDataSet for cSql;
      end if;
    end;
    FMP_LOG.LOGEND;
    pOut_nSqlCode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := sqlcode;
      FMP_LOG.LOGERROR;
  end FMISP_CombineKeyBySortSequenc;
end FMIP_CombinKeyBySortSequenc;
/
