create or replace package FMP_ValidateFCST is

  -- Author  : LZHANG
  -- Created : 1/15/2013 1:05:51 PM
  -- Purpose :
  procedure FMSP_validateFCST(pIn_nSelectionID          in number,
                              pIn_nCalculationType      in number,
                              pIn_vFirstPeriodTime      in varchar2,
                              pIn_vLastPeriodTime       in varchar2,
                              pIn_vConditions           in varchar2,
                              pIn_vSourceTimeSeriesIDs  in varchar2,
                              pIn_nValidateTimeSeriesId in number,
                              pIn_nIsPlusTimeSeries5    in number,
                              pIn_nNodeLevel            in number,
                              pIn_nChronology           in number,
                              pIn_nPrecision            in number,
                              pOut_nSqlCode             out number);
  procedure FMSP_validateFCSTByNodeList(pIn_cNodeList             in clob,
                                        pIn_nCalculationType      in number,
                                        pIn_vFirstPeriodTime      in varchar2,
                                        pIn_vLastPeriodTime       in varchar2,
                                        pIn_vSourceTimeSeriesIDs  in varchar2,
                                        pIn_nValidateTimeSeriesId in number,
                                        pIn_nIsPlusTimeSeries5    in number,
                                        pIn_nNodeLevel            in number,
                                        pIn_nChronology           in number,
                                        pIn_nPrecision            in number,
                                        pOut_nSqlCode             out number);
end FMP_ValidateFCST;
/
create or replace package body FMP_ValidateFCST is
  --*****************************************************************
  -- Description: Describe the purpose of the object. If necessary,
  -- describe the design of the object at a very high level.
  --
  -- Author:      lei zhang
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        15-JAN-2013     lei zhang     Created.
  -- **************************************************************
  type rTsidPair is record(
    nTsidLeft  prb_m.tsid%TYPE := -1,
    nTsidRight prb_m.tsid%TYPE := -1); -- it a record Type for store tsid (left  AND  right)
  TYPE aTsidList is TABLE OF rTsidPair INDEX BY BINARY_INTEGER; -- it an array Type for rTsidPair Type
  MARK_LEFT             constant number := 1;
  MARK_RIGHT            constant number := 2;
  MARK_TARGET_1         constant number := 52;
  MARK_TARGET_REPLACE_1 constant number := 51;
  MARK_TARGET_2         constant number := 21;
  MARK_TARGET_REPLACE_2 constant number := 18;
  MARK_TARGET_3         constant number := 22;
  MARK_TARGET_REPLACE_3 constant number := 19;
  MARK_TARGET_4         constant number := 23;
  MARK_TARGET_REPLACE_4 constant number := 20;
  MARK_TARGET         number :=52;
  MARK_TARGET_REPLACE number :=51;
  aTsidListResult     aTsidList;

  type countDataType is record(
    nCountT1  number := 0,
    nCountT2  number := 0,
    nCountT3  number := 0,
    nCountT4  number := 0,
    nCountT5  number := 0,
    nCountT6  number := 0,
    nCountT7  number := 0,
    nCountT8  number := 0,
    nCountT9  number := 0,
    nCountT10 number := 0,
    nCountT11 number := 0,
    nCountT12 number := 0,
    vNodeId   varchar2(2000) := '');

  procedure FMSP_IsTargetData(pIn_nMarkTarget in number,
                              pOut_bFlag      out boolean) IS
  Begin
    begin
      if aTsidListResult(1).nTsidLeft = pIn_nMarkTarget or aTsidListResult(1)
         .nTsidRight = pIn_nMarkTarget or aTsidListResult(2)
         .nTsidLeft = pIn_nMarkTarget or aTsidListResult(2)
         .nTsidRight = pIn_nMarkTarget or aTsidListResult(3)
         .nTsidLeft = pIn_nMarkTarget or aTsidListResult(3)
         .nTsidRight = pIn_nMarkTarget or aTsidListResult(4)
         .nTsidLeft = pIn_nMarkTarget or aTsidListResult(4)
         .nTsidRight = pIn_nMarkTarget or aTsidListResult(5)
         .nTsidLeft = pIn_nMarkTarget or aTsidListResult(5)
         .nTsidRight = pIn_nMarkTarget or aTsidListResult(6)
         .nTsidLeft = pIn_nMarkTarget or aTsidListResult(6)
         .nTsidRight = pIn_nMarkTarget then
        pOut_BFlag := true;
      end if;
    end;
  End FMSP_IsTargetData;

  procedure FMSP_GetSecondTSIDSql(pIn_cSql         in clob,
                                  pIn_nIndex       in number,
                                  pIn_nTSID        in number,
                                  pIn_nType        in number,
                                  pIn_vColumnsName in varchar2,
                                  pOut_cSql        out clob) IS
  Begin
    declare
      nTsid number;
      cSql  clob;

    begin
      if pIn_nType = MARK_LEFT then
        nTsid := aTsidListResult(pIn_nIndex * 10 + pIn_nIndex).nTsidLeft;
      elsif pIn_nType = MARK_RIGHT then
        nTsid := aTsidListResult(pIn_nIndex * 10 + pIn_nIndex).nTsidRight;
      end if;
      if nTsid = -1 then
        pOut_cSql := pIn_cSql;
        return;
      end if;
      if pIn_nTSID <> MARK_TARGET then
        cSql := replace(pIn_cSql,
                        'TSID=' || to_char(pIn_nTSID),
                        'TSID=' || to_char(nTsid));
      elsif pIn_nTSID = MARK_TARGET then
        cSql := replace(pIn_cSql, 'TSID='||MARK_TARGET_REPLACE, 'TSID=000000');
        cSql := replace(cSql,
                        'TSID=' || to_char(pIn_nTSID),
                        'TSID=' || to_char(nTsid));
        cSql := replace(cSql, 'TSID=000000', 'TSID=' || to_char(nTsid));
      end if;
      pOut_cSql := 'select  sum(T1) AS T1, sum(T2) AS T2,
                            sum(T3) AS T3, sum(T4) AS T4,
                            sum(T5) AS T5, sum(T6) AS T6,
                            sum(T7) AS T7, sum(T8) AS T8,
                            sum(T9) AS T9, sum(T10) AS T10,
                            sum(T11) AS T11, sum(T12) AS T12,
                            YY,' || pIn_vColumnsName || '
                            FROM(' || pIn_cSql ||
                   ' UNION ALL ' || cSql || ') group by YY,' ||
                   pIn_vColumnsName;
    end;
  End FMSP_GetSecondTSIDSql;

  procedure FMSP_InitTSIDList(pIn_vSourceTimeseriesIDS in varchar2,
                              pOut_bFlag               out boolean) IS
  Begin
    declare
      vTmp  varchar2(2000);
      vSql  varchar2(200);
      vTSID varchar2(200);
    begin
      pOut_bFlag := false;
      vTSID      := substr(pIn_vSourceTimeSeriesIDs,
                           0,
                           instr(pIn_vSourceTimeSeriesIDs, ',') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(11).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(1).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(11).nTsidLeft := -1;
        aTsidListResult(1).nTsidLeft := to_number(vTSID);
      end if;
      vTmp := substr(pIn_vSourceTimeSeriesIDs,
                     instr(pIn_vSourceTimeSeriesIDs, ',') + 1);

      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);

      if instr(vTSID, '_') > 0 then
        aTsidListResult(11).nTsidRight := to_number(substr(vTSID,
                                                           instr(vTSID, '_') + 1));
        aTsidListResult(1).nTsidRight := to_number(substr(vTSID,
                                                          0,
                                                          instr(vTSID, '_') - 1));
      else
        aTsidListResult(11).nTsidRight := -1;
        aTsidListResult(1).nTsidRight := to_number(vTSID);
      end if;
      vTmp := substr(pIn_vSourceTimeSeriesIDs,
                     instr(pIn_vSourceTimeSeriesIDs, ';') + 1);
      -- the second left  AND  right
      vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);

      if instr(vTSID, '_') > 0 then
        aTsidListResult(22).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(2).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(22).nTsidLeft := -1;
        aTsidListResult(2).nTsidLeft := to_number(vTSID);
      end if;
      vTmp  := substr(vTmp, instr(vTmp, ',') + 1);
      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);

      if instr(vTSID, '_') > 0 then
        aTsidListResult(22).nTsidRight := to_number(substr(vTSID,
                                                           instr(vTSID, '_') + 1));
        aTsidListResult(2).nTsidRight := to_number(substr(vTSID,
                                                          0,
                                                          instr(vTSID, '_') - 1));

      else
        aTsidListResult(22).nTsidRight := -1;
        aTsidListResult(2).nTsidRight := to_number(vTSID);
      end if;

      vTmp := substr(vTmp, instr(vTmp, ';') + 1);
      -- the third left  AND  right
      vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);

      if instr(vTSID, '_') > 0 then
        aTsidListResult(33).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(3).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(33).nTsidLeft := -1;
        aTsidListResult(3).nTsidLeft := to_number(vTSID);
      end if;
      vTmp  := substr(vTmp, instr(vTmp, ',') + 1);
      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(33).nTsidRight := to_number(substr(vTSID,
                                                           instr(vTSID, '_') + 1));
        aTsidListResult(3).nTsidRight := to_number(substr(vTSID,
                                                          0,
                                                          instr(vTSID, '_') - 1));

      else
        aTsidListResult(33).nTsidRight := -1;
        aTsidListResult(3).nTsidRight := to_number(vTSID);
      end if;
      vTmp := substr(vTmp, instr(vTmp, ';') + 1);
      -- the fourth left  AND  right
      vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(44).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(4).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(44).nTsidLeft := -1;
        aTsidListResult(4).nTsidLeft := to_number(vTSID);
      end if;
      vTmp  := substr(vTmp, instr(vTmp, ',') + 1);
      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(44).nTsidRight := to_number(substr(vTSID,
                                                           instr(vTSID, '_') + 1));
        aTsidListResult(4).nTsidRight := to_number(substr(vTSID,
                                                          0,
                                                          instr(vTSID, '_') - 1));

      else
        aTsidListResult(44).nTsidRight := -1;
        aTsidListResult(4).nTsidRight := to_number(vTSID);
      end if;
      vTmp := substr(vTmp, instr(vTmp, ';') + 1);
      -- the fifth left  AND  right
      vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(55).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(5).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(55).nTsidLeft := -1;
        aTsidListResult(5).nTsidLeft := to_number(vTSID);
      end if;
      vTmp  := substr(vTmp, instr(vTmp, ',') + 1);
      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(5).nTsidRight := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(55).nTsidRight := to_number(substr(vTSID,
                                                           0,
                                                           instr(vTSID, '_') - 1));

      else
        aTsidListResult(55).nTsidRight := -1;
        aTsidListResult(5).nTsidRight := to_number(vTSID);
      end if;
      vTmp := substr(vTmp, instr(vTmp, ';') + 1);
      -- the sixth left  AND  right
      vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);
      if instr(vTSID, '_') > 0 then
        aTsidListResult(66).nTsidLeft := to_number(substr(vTSID,
                                                          instr(vTSID, '_') + 1));
        aTsidListResult(6).nTsidLeft := to_number(substr(vTSID,
                                                         0,
                                                         instr(vTSID, '_') - 1));

      else
        aTsidListResult(66).nTsidLeft := -1;
        aTsidListResult(6).nTsidLeft := to_number(vTsid);
      end if;
      vTmp  := substr(vTmp, instr(vTmp, ',') + 1);
      vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);
      if instr(to_char(aTsidListResult(6).nTsidRight), '_') > 0 then
        aTsidListResult(66).nTsidRight := to_number(substr(vTSID,
                                                           instr(vTSID, '_') + 1));
        aTsidListResult(6).nTsidRight := to_number(substr(vTSID,
                                                          0,
                                                          instr(vTSID, '_') - 1));

      else
        aTsidListResult(66).nTsidRight := -1;
        aTsidListResult(6).nTsidRight := to_number(vTSID);
      end if;

      FMSP_IsTargetData(pIn_nMarkTarget => MARK_TARGET_1,
                        pOut_bFlag      => pOut_bFlag);
      if pOut_bFlag then
        Mark_Target         := Mark_Target_1;
        MARK_TARGET_REPLACE := MARK_TARGET_REPLACE_1;
        return;
      end if;

      FMSP_IsTargetData(pIn_nMarkTarget => MARK_TARGET_2,
                        pOut_bFlag      => pOut_bFlag);
      if pOut_bFlag then
        Mark_Target         := Mark_Target_2;
        MARK_TARGET_REPLACE := MARK_TARGET_REPLACE_2;
        return;
      end if;

      FMSP_IsTargetData(pIn_nMarkTarget => MARK_TARGET_3,
                        pOut_bFlag      => pOut_bFlag);
      if pOut_bFlag then
        Mark_Target         := Mark_Target_3;
        MARK_TARGET_REPLACE := MARK_TARGET_REPLACE_3;
        return;
      end if;

      FMSP_IsTargetData(pIn_nMarkTarget => MARK_TARGET_4,
                        pOut_bFlag      => pOut_bFlag);
      if pOut_bFlag then
        Mark_Target         := Mark_Target_4;
        MARK_TARGET_REPLACE := MARK_TARGET_REPLACE_4;
        return;
      end if;
    end;
  End FMSP_InitTSIDList;

  procedure FMP_GetTmpTableSQL(pIn_cSql            in CLOB,
                               pIn_vNodeColumnName in varchar2,
                               pOut_cSql           out CLOB) AS
  BEGIN
    --*****************************************************************
    -- Description: it support for month validateFCST
    --
    -- Parameters:
    -- pIn_cSql  in SQL
    -- pOut_cSql  out SQL
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
    declare
      cSqlTmp6        clob;
      cSqlTmp66       clob;
      cSqlTmp55       clob;
      vNodeColumnName varchar2(200);
    begin
      cSqlTmp55       := pIn_cSql;
      vNodeColumnName := pIn_vNodeColumnName;
      cSqlTmp55       := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName ||
                         ' FROM ( select first_value(T1 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T1,' ||
                         'first_value(T2 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T2,' ||
                         'first_value(T3 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T3,' ||
                         'first_value(T4 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T4,' ||
                         'first_value(T5 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T5,' ||
                         'first_value(T6 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T6,' ||
                         'first_value(T7 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T7,' ||
                         'first_value(T8 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T8,' ||
                         'first_value(T9 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T9,' ||
                         'first_value(T10 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T10,' ||
                         'first_value(T11 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T11,' ||
                         'first_value(T12 ignore nulls) over(partition by YY,' ||
                         vNodeColumnName || ' order by orderid) T12,' ||
                         'row_number() over(partition by YY,' ||
                         vNodeColumnName || '  order by orderid desc) r,' ||
                         'YY,' || vNodeColumnName || ' FROM ' || cSqlTmp55 ||
                         ') where r=1';
      pOut_cSql       := cSqlTmp55;
    end;
  END FMP_GetTmpTableSQL;
  procedure FMSP_IsExitData(pIn_cCountData in countDataType,
                            pOut_bFlag     out boolean) IS
  Begin

    begin
      pOut_bFlag := true;
      if pIn_cCountData.ncountT1 <> 0 or pIn_cCountData.ncountT2 <> 0 or
         pIn_cCountData.ncountT3 <> 0 or pIn_cCountData.ncountT4 <> 0 or
         pIn_cCountData.ncountT5 <> 0 or pIn_cCountData.ncountT6 <> 0 or
         pIn_cCountData.ncountT7 <> 0 or pIn_cCountData.ncountT8 <> 0 or
         pIn_cCountData.ncountT9 <> 0 or pIn_cCountData.ncountT10 <> 0 or
         pIn_cCountData.ncountT11 <> 0 or pIn_cCountData.ncountT12 <> 0 then
        pOut_bFlag := false;
      end if;
    end;
  End FMSP_IsExitData;

  procedure FMSP_OperateTargetResult(pIn_cSql              in clob,
                                     pIn_vTmpTableNodeList in varchar2) IS
  Begin
    declare
      sCursor   sys_refcursor;
      countData countDataType;
      bFlag     boolean;
      cSql      clob;
      TYPE aColumnListType IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER; -- all node list
      aColumnList aColumnListType;
      TYPE aExitDBColumnListType IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER; -- node exit in database
      aExitDBColumnList aExitDBColumnListType;
      nIndex            number := 1;
      cSqlIn            clob;
      cSqlMinus         clob;
      vNodeId           varchar2(200);
    begin
      delete from tb_ts_validatefcstm;
      commit;
      --fmp_log.LOGERROR(pIn_cSql);
      open sCursor for pIn_cSql;
      loop
        fetch sCursor
          into countData;
        exit when sCursor%notfound;
        bFlag := false;
        FMSP_IsExitData(countData, bFlag);
        cSqlIn := 'insert into tb_ts_validatefcstm(nodeid) values(' ||
                  countData.vNodeId || ')';
        execute immediate cSqlIn;
        nIndex := nIndex + 1;
        if bFlag then
          -- insert one node into TB_Node
          cSql := 'merge into TB_Node
          using (select ' || countData.vNodeId ||
                  ' as nodeid from dual) t
          on (TB_Node.id = t.nodeid)
          when not MATCHED then
            insert (id) values (' || countdata.vnodeid || ')';
          execute immediate cSql;
          cSql := '';
          -- delete one node into nodelistTable
          cSql := 'delete from ' || 'tb_ts_aggregatenode' || ' where id=' ||
                  countData.vNodeId;
          execute immediate cSql;
          commit;
        end if;
      end loop;
      close sCursor;
      cSqlMinus := ' select id from ' || pIn_vTmpTableNodeList ||
                   ' where id !=all(select nodeid from tb_ts_validatefcstm)';
      --fmp_log.LOGERROR(cSqlMinus);
      open sCursor for cSqlMinus;
      loop
        fetch sCursor
          into vNodeId;
        exit when sCursor%notfound;
        -- insert one node into TB_Node
        cSql := 'merge into TB_Node
          using (select ' || vNodeId ||
                ' as nodeid from dual) t
          on (TB_Node.id = t.nodeid)
          when not MATCHED then
            insert (id) values (' || vNodeId || ')';
        execute immediate cSql;
        cSql := '';
        -- delete one node into nodelistTable
        cSql := 'delete from ' || 'tb_ts_aggregatenode' || ' where id=' ||
                vNodeId;
        execute immediate cSql;
        commit;
      end loop;
      close sCursor;
    end;
  End FMSP_OperateTargetResult;

  procedure FMSP_OperateTarget(pIn_nFirstYear        in number,
                               pIn_nLastYear         in number,
                               pIn_nFirstMonth       in number,
                               pIn_nLastMonth        in number,
                               pIn_vTmpTableNodeList in varchar2,
                               pIn_vTableNodeLevel   in varchar2,
                               pIn_vColumnNodeLevel  in varchar2,
                               pIn_vTargetTSID       in varchar2,
                               pIn_vReplaceTSID      in varchar2) IS
  Begin
    declare
      countData   countDataType;
      cSql        clob;
      sCursor     sys_refcursor;
      nMonth      number;
      cSqlColumns clob;
      nIndex      number;
      bFlag       boolean;
    begin
      -- delete
      delete from TB_Node;
      commit;

      if pIn_nFirstMonth = 1 and pIn_nLastMonth = 12 then
        -- fm=1 lm=12
        cSql := 'select count(T1) AS countT1,
           count(t2) AS countT2,
           count(t3) AS countT3,
           count(t4) AS countT4,
           count(t5) AS countT5,
           count(t6) AS countT6,
           count(t7) AS countT7,
           count(t8) AS countT8,
           count(t9) AS countT9,
           count(t10) AS countT10,
           count(t11) AS countT11,
           count(t12) AS countT12,
           id
      from ' || pIn_vTableNodeLevel || ',' ||
                pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and tsid='||to_char(MARK_TARGET)||' and yy between ' ||
                to_char(pIn_nFirstYear) || ' AND ' ||
                to_char(pIn_nLastYear) || ' group by id';
        --fmp_log.LOGERROR(cSql);
        -- init tmp table

      elsif pIn_nFirstMonth <> 1 and pIn_nLastMonth = 12 then
        -- fm<>1 lm=12
        nMonth      := 12;
        cSqlColumns := '';
        for nIndex in 1 .. pIn_nFirstMonth - 1 loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || '0 AS countT' || to_char(nIndex);
        end loop;
        for nIndex in pIn_nFirstMonth .. nMonth loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || 'count(t' || to_char(nIndex) ||
                         ')  AS countT' || to_char(nIndex);
        end loop;
        cSql := 'select ' || cSqlColumns || ' ,id from ' ||
                pIn_vTableNodeLevel || ',' || pIn_vTmpTableNodeList ||
                ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and  tsid='||to_char(MARK_TARGET)||' and yy = ' || to_char(pIn_nFirstYear) ||
                ' group by id';
        --fmp_log.LOGERROR(cSql);

        -- year
        if pIn_nFirstYear <> pIn_nLastYear then
          cSql := cSql || '  UNION ALL select count(T1) AS countT1,
           count(t2) AS countT2,
           count(t3) AS countT3,
           count(t4) AS countT4,
           count(t5) AS countT5,
           count(t6) AS countT6,
           count(t7) AS countT7,
           count(t8) AS countT8,
           count(t9) AS countT9,
           count(t10) AS countT10,
           count(t11) AS countT11,
           count(t12) AS countT12,
           id
      from ' || pIn_vTableNodeLevel || ',' ||
                  pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                  '.id and  tsid='||to_char(MARK_TARGET) ||' and  yy between ' ||
                  to_char(pIn_nFirstYear + 1) || ' AND ' ||
                  to_char(pIn_nLastYear) || ' group by id';
          -- init tmp table

        end if;
      elsif pIn_nFirstMonth = 1 and pIn_nLastMonth <> 12 then
        -- fm=1 lm<>12
        nMonth      := 12;
        cSqlColumns := '';
        for nIndex in 1 .. pIn_nLastMonth loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || 'count(t' || to_char(nIndex) ||
                         ') AS countT' || to_char(nIndex);
        end loop;
        for nIndex in pIn_nLastMonth + 1 .. nMonth loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || '0 AS countT' || to_char(nIndex);
        end loop;

        cSql := 'select ' || cSqlColumns || ' ,id from ' ||
                pIn_vTableNodeLevel || ',' || pIn_vTmpTableNodeList ||
                ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and   tsid='||to_char(MARK_TARGET) ||' and yy = ' || to_char(pIn_nLastYear) ||
                ' group by id';
        -- init tmp table

        -- year
        if pIn_nFirstYear <> pIn_nLastYear then
          cSql := cSql || '  UNION ALL select count(T1) AS countT1,
           count(t2) AS countT2,
           count(t3) AS countT3,
           count(t4) AS countT4,
           count(t5) AS countT5,
           count(t6) AS countT6,
           count(t7) AS countT7,
           count(t8) AS countT8,
           count(t9) AS countT9,
           count(t10) AS countT10,
           count(t11) AS countT11,
           count(t12) AS countT12,
           id
      from ' || pIn_vTableNodeLevel || ',' ||
                  pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                  '.id and  tsid='||to_char(MARK_TARGET) ||'  and  yy between ' ||
                  to_char(pIn_nFirstYear) || ' AND ' ||
                  to_char(pIn_nLastYear - 1) || ' group by id';
          -- init tmp table

        end if;
      elsif pIn_nFirstMonth <> 1 and pIn_nLastMonth <> 12 then
        -- fm<>1 lm<>12
        -- nFirst
        if pIn_nFirstYear = pIn_nLastYear then
          nMonth := pIn_nLastMonth;
        else
          nMonth := 12;
        end if;
        cSqlColumns := '';
        for nIndex in 1 .. pIn_nFirstMonth - 1 loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || '0 AS countT' || to_char(nIndex);
        end loop;
        for nIndex in pIn_nFirstMonth .. nMonth loop
          if length(cSqlColumns) > 0 then
            cSqlColumns := cSqlColumns || ',';
          end if;
          cSqlColumns := cSqlColumns || 'count(t' || to_char(nIndex) ||
                         ')  AS countT' || to_char(nIndex);
        end loop;
        cSql := 'select ' || cSqlColumns || ' ,id from ' ||
                pIn_vTableNodeLevel || ',' || pIn_vTmpTableNodeList ||
                ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and   tsid='||to_char(MARK_TARGET) ||'  and yy = ' || to_char(pIn_nFirstYear) ||
                ' group by id';
        -- init tmp table
        --nLast
        if pIn_nFirstYear <> pIn_nLastYear then
          nMonth      := 12;
          cSqlColumns := '';
          for nIndex in 1 .. pIn_nLastMonth loop
            if length(cSqlColumns) > 0 then
              cSqlColumns := cSqlColumns || ',';
            end if;
            cSqlColumns := cSqlColumns || 'count(t' || to_char(nIndex) ||
                           ')  AS countT' || to_char(nIndex);
          end loop;
          for nIndex in pIn_nLastMonth + 1 .. nMonth loop
            if length(cSqlColumns) > 0 then
              cSqlColumns := cSqlColumns || ',';
            end if;
            cSqlColumns := cSqlColumns || '0 AS countT' || to_char(nIndex);
          end loop;

          cSql := cSql || ' UNION ALL select ' || cSqlColumns ||
                  ' ,id from ' || pIn_vTableNodeLevel || ',' ||
                  pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                  '.id and  tsid='||to_char(MARK_TARGET) ||'  and  yy = ' || to_char(pIn_nLastYear) ||
                  ' group by id';
          -- init tmp table
        end if;
        -- year
        if pIn_nFirstYear + 1 <= pIn_nLastYear - 1 then
          cSql := cSql || ' UNION ALL select count(T1)  AS countT1,
           count(t2) AS countT2,
           count(t3) AS countT3,
           count(t4) AS countT4,
           count(t5) AS countT5,
           count(t6) AS countT6,
           count(t7) AS countT7,
           count(t8) AS countT8,
           count(t9) AS countT9,
           count(t10) AS countT10,
           count(t11) AS countT11,
           count(t12) AS countT12,
           id
      from ' || pIn_vTableNodeLevel || ',' ||
                  pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                  '.id and   tsid='||to_char(MARK_TARGET) ||'  and yy between ' ||
                  to_char(pIn_nFirstYear + 1) || ' AND ' ||
                  to_char(pIn_nLastYear - 1) || ' group by id';
          -- init tmp table
        end if;
      end if;
      cSql := 'select
              sum(countT1),
              sum(countT2),
              sum(countT3),
              sum(countT4),
              sum(countT5),
              sum(countT6),
              sum(countT7),
              sum(countT8),
              sum(countT9),
              sum(countT10),
              sum(countT11),
              sum(countT12),
              id  FROM (' || cSql || ') group by id';

      FMSP_OperateTargetResult(pIn_cSql              => cSql,
                               pIn_vTmpTableNodeList => pIn_vTmpTableNodeList);

    end;
  End FMSP_OperateTarget;

  procedure FMSP_GetNullData(pIn_nFirstYear        in number,
                             pIn_nLastYear         in number,
                             pIn_vTSID             in varchar2,
                             pIn_vTmpTableNodeList in varchar2,
                             pIn_vTableNodeLevel   in varchar2,
                             pIn_vColumnNodeLevel  in varchar2,
                             pOut_cSql             out clob) IS
    --*****************************************************************
    -- Description: it support for data not exit in database
    --
    -- Parameters:
    --
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        22-MAR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nIndex number;
      cSql   clob;
    begin
      null;
      for nIndex in pIn_nFirstYear .. pIn_nLastYear loop
        if length(cSql) > 5 then
          cSql := cSql || ' UNION  ';
        end if;
        cSql := cSql || '(select NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTmpTableNodeList || '
        minus
        SELECT NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTableNodeLevel || ',' ||
                pIn_vTmpTableNodeList || '
         WHERE YY = ' || to_char(nIndex) || '
           AND ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList || '.id
           AND TSID=' || pIn_vTSID || ')';
      end loop;
      pOut_cSql := cSql;
    end;
  End FMSP_GetNullData;
  procedure FMSP_GetNULLDataTarget(pIn_nFirstYear        in number,
                                   pIn_nLastYear         in number,
                                   pIn_vTSID             in varchar2,
                                   pIn_vTmpTableNodeList in varchar2,
                                   pIn_vTableNodeLevel   in varchar2,
                                   pIn_vColumnNodeLevel  in varchar2,
                                   pOut_cSql             out clob) IS
    --*****************************************************************
    -- Description: it support for data not exit in database for target nodeid
    --
    -- Parameters:
    --
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        22-MAR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nIndex number;
      cSql   clob;
    begin
      -- vTmpTableNodeList
      for nIndex in pIn_nFirstYear .. pIn_nLastYear loop
        if length(cSql) > 5 then
          cSql := cSql || ' UNION  ';
        end if;
        cSql := cSql || '(select NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTmpTableNodeList || '
        minus
        SELECT NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTableNodeLevel || ',' ||
                pIn_vTmpTableNodeList || '
         WHERE YY = ' || to_char(nIndex) || '
           AND ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList || '.id
           AND TSID=' || pIn_vTSID || ')';
      end loop;

      -- TB_Node
      for nIndex in pIn_nFirstYear .. pIn_nLastYear loop
        if length(cSql) > 5 then
          cSql := cSql || ' UNION  ';
        end if;
        cSql := cSql || '(select NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || 'TB_Node' || '
        minus
        SELECT NULL AS T1,
               NULL AS T2,
               NULL AS T3,
               NULL AS T4,
               NULL AS T5,
               NULL AS T6,
               NULL AS T7,
               NULL AS T8,
               NULL AS T9,
               NULL AS T10,
               NULL AS T11,
               NULL AS T12,' || to_char(nIndex) ||
                ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTableNodeLevel || ',' || 'TB_Node' || '
         WHERE YY = ' || to_char(nIndex) || '
           AND ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || 'TB_Node' || '.id
           AND TSID=' ||to_char(MARK_TARGET_REPLACE)|| ')';
      end loop;
      pOut_cSql := cSql;
    end;
  End FMSP_GetNULLDataTarget;

  procedure FMSP_validateFCSTMTarget(pIn_nSelectionID          in number,
                                     pIn_nCalculationType      in number,
                                     pIn_vFirstPeriodTime      in varchar2,
                                     pIn_vLastPeriodTime       in varchar2,
                                     pIn_vConditions           in varchar2,
                                     pIn_vSourceTimeSeriesIDs  in varchar2,
                                     pIn_nValidateTimeSeriesId in number,
                                     pIn_nIsPlusTimeSeries5    in number,
                                     pIn_nNodeLevel            in number,
                                     pIn_nPrecision            in number,
                                     pIn_nMultiplex            in number) IS
  Begin
    declare
      /*      aTsidListResult aTsidList; -- it an array for store rTsidPair*/
      vNodeLevel varchar2(5) := 'prb_m'; -- table_name default prb_m;
      sNodeList  sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
      nSqlCode   number; -- this variable is the parameter for analyze
      --vSqlNodeList varchar2(100); -- this variable is a sql for get the result of TB_TS_DetailNodeSelCdt(store analyze pIn_vConditions)
      cSqlFirst  CLOB := ' ';
      cSqlSecond CLOB := ' ';
      cSqlThird  CLOB := ' ';
      cSqlFourth CLOB := ' ';
      cSqlFifth  CLOB := ' ';
      cSqlSixth  CLOB := ' ';
      cSqlTmp1   CLOB := ' ';
      cSqlTmp2   CLOB := ' ';
      cSqlTmp3   CLOB := ' ';
      cSqlTmp4   CLOB := ' ';
      cSqlTmp11  CLOB := ' ';
      cSqlTmp22  CLOB := ' ';
      cSqlTmp33  CLOB := ' ';
      cSqlTmp44  CLOB := ' ';
      cSqlTmp5   CLOB := ' ';
      cSqlTmp55  CLOB := ' ';
      cSqlTmp6   CLOB := ' ';
      cSqlTmp66  CLOB := ' ';
      nMark      number := 0;
      TYPE aNodeList IS TABLE OF NUMBER INDEX BY BINARY_INTEGER; -- an array Type stored number
      aNodeListResult  aNodeList; -- an arrary for store the result of TB_TS_DetailNodeSelCdt
      vTmp             varchar2(100); -- for store temp data
      bAND             boolean := false; -- for mark temp boolean
      bTmp             boolean := false; -- for mark temp boolean too
      vNodeColumnName  varchar2(100) := 'SELID';
      vFirstPeriodTime varchar2(10);
      vLastPeriodTime  varchar2(10);
      nFirstMonth      number; -- firstMonth
      nLastMonth       number; -- lastMonth
      nFirstYear       number; -- firstYear
      nLastYear        number; -- lastYear
      TYPE aColumnList IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER;
      aColumnListResult aColumnList;
      vSeq              varchar2(10) := 'seq_prb_m';
      vTmpTableName     varchar2(40) := 'TB_TS_AggregateNodeCon';
      nNumTmp           number;
      vTabName          varchar2(30);
      cSqlNullData      clob;
      cSqlColumns       clob;
      i                 number;
      w                 number;
      n                 number;
      m                 number;
      j                 number;
    BEGIN
      -- init column list
      aColumnListResult(1) := 'T1';
      aColumnListResult(2) := 'T2';
      aColumnListResult(3) := 'T3';
      aColumnListResult(4) := 'T4';
      aColumnListResult(5) := 'T5';
      aColumnListResult(6) := 'T6';
      aColumnListResult(7) := 'T7';
      aColumnListResult(8) := 'T8';
      aColumnListResult(9) := 'T9';
      aColumnListResult(10) := 'T10';
      aColumnListResult(11) := 'T11';
      aColumnListResult(12) := 'T12';
      -- deal with date
      nFirstMonth := to_number(substr(pIn_vFirstPeriodTime, 5));
      nLastMonth  := to_number(substr(pIn_vLastPeriodTime, 5));
      nFirstYear  := to_number(substr(pIn_vFirstPeriodTime, 0, 4));
      nLastYear   := to_number(substr(pIn_vLastPeriodTime, 0, 4));
      -- judge prb_m or don_m  analyze p_conditions for nodelist
      if pIn_nNodeLevel = 1 then
        vNodeLevel      := 'DON_M';
        vNodeColumnName := 'PVTID';
        vSeq            := 'seq_don_m';
        vTmpTableName   := 'TB_TS_DetailNodeSelCdt';
        /*        if pIn_nMultiplex = 0 then
          p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => pIn_nSelectionID,
                                               P_Conditions  => pIn_vConditions,
                                               P_Sequence    => null, --Sort sequence
                                               p_DetailNode  => sNodeList,
                                               pOut_vTabName => vTabName,
                                               p_SqlCode     => nSqlCode);
        end if;*/
      else
        /*        if pIn_nMultiplex = 0 then
          p_aggregation.FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => pIn_nSelectionID,
                                                  pIn_vConditions => pIn_vConditions,
                                                  pOut_Nodes      => sNodeList,
                                                  pOut_nSqlCode   => nSqlCode);
        end if;*/
        null;
      end if;
      -- get the result of TB_TS_DetailNodeSelCdt (NODELIST)
      /*      open sNodeList for 'select id FROM ' || vTmpTableName;
      fetch sNodeList bulk collect
        into aNodeListresult;
      if aNodeListResult.count = 0 then
        return;
      end if;*/
      --debug
      /*      delete from tb_ts;
      commit;
      insert into tb_ts
        select id from TB_TS_DetailNodeSelCdt;
      delete from tb_node_tmp;
      insert into tb_node_tmp
        select id from tb_node;
      delete from tb_node_agg;
      insert into tb_node_agg
        select id from tb_ts_aggregatenode;
      commit;*/
      --debug
      --return;
      -- get the result of TSIDLIST.  there can also use for get the result. i will optimize it later
      -- the first left  AND  right
      -- deal with the  lines
      -- for with the TSIDLIST
      -- i for TSIDLIST
      for i in 1 .. 6 loop
        -- if the left is zero then continue
        if aTsidListResult(i).nTsidLeft = -1 then
          continue;
        end if;
        vTmp := aTsidListResult(i).nTsidLeft;
        vTmp := aTsidListResult(i).nTsidRight;
        -- for with the NODELIST
        -- j for NODELIST
        -- right TSID is 0
        if aTsidListResult(i).nTsidRight = -1 then
          if nFirstMonth = 1 AND nLastMonth = 12 then
            -- no first  AND  no left
            if aTsidListResult(i).nTsidLeft <> MARK_TARGET  then
              -- <> 52
              cSqlTmp1 := ' ';
              nMark    := 1;
              cSqlTmp1 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          vTmpTableName || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add  null data
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET  then
              -- = 52
              cSqlTmp1 := ' ';
              nMark    := 1;
              cSqlTmp1 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear);
              cSqlTmp1 := cSqlTmp1 || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION  ' ||
                          'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'TB_Node' || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear);

              cSqlTmp1 := cSqlTmp1 || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' ||
                          ' TB_Node.id  AND  TSID='||to_char(MARK_TARGET_REPLACE);
              -- add  null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
            end if;
          elsif nFirstMonth != 1 AND nLastMonth = 12 then
            -- <>1  =12
            --if 1 = j then
            if aTsidListResult(i).nTsidLeft <> MARK_TARGET  then
              -- <>52
              nMark    := 2;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              -- nFirst left
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET  then
              -- = 52
              nMark    := 2;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              -- nFirst left
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ';
              cSqlTmp1 := cSqlTmp1 || ' select ' || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_node' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char(MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := cSqlTmp2 || ' UNION ' ||
                            'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;

          elsif nFirstMonth = 1 AND nLastMonth != 12 then
            -- =1 <>12
            --if 1 = j then
            if aTsidListResult(i).nTsidLeft <> MARK_TARGET then
              --<>52
              nMark    := 3;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              -- nLast left
              cSqlTmp1 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET  then
              -- =52
              nMark    := 3;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              -- nLast left
              cSqlTmp1    := 'select ';
              cSqlColumns := '';
              for n in 1 .. nLastMonth loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ' || 'select ' || cSqlColumns ||
                          ' YY,' || vNodeColumnName || '  FROM   ' ||
                          vNodeLevel || ',' || 'tb_node' || ' where  YY = ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char(MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char(MARK_TARGET_REPLACE);
              end if;
            end if;

          elsif nFirstMonth != 1 AND nLastMonth != 12 then
            --<>1 <>12
            --if 1 = j then
            if aTsidListResult(i).nTsidLeft <> MARK_TARGET  then
              --<>52
              nMark    := 4;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              cSqlTmp3 := ' ';
              -- nFirstYear = nLastYear
              if nFirstYear = nLastYear then
                nNumTmp := nLastMonth;
              else
                nNumTmp := 12;
              end if;
              -- nFirst
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. nNumTmp loop
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
              end loop;
              if nNumTmp <> 12 then
                for w in nNumTmp .. 12 loop
                  cSqlTmp1 := cSqlTmp1 || ' null as ' ||
                              aColumnListResult(w) || ',';
                end loop;
              end if;
              cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- nLast
              if nFirstYear <> nLastYear then
                cSqlTmp3 := 'select ';
                for n in 1 .. nLastMonth loop
                  cSqlTmp3 := cSqlTmp3 || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. 12 loop
                  cSqlTmp3 := cSqlTmp3 || ' null as ' ||
                              aColumnListResult(m) || ',';
                end loop;
                cSqlTmp3 := cSqlTmp3 || ' YY,' || vNodeColumnName ||
                            '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || ' where  YY = ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
              -- judge nFirst+1 < nLastYear-1
              if nFirstYear + 1 <= nLastYear - 1 then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET then
              --=52
              nMark    := 4;
              cSqlTmp1 := ' ';
              cSqlTmp2 := ' ';
              cSqlTmp3 := ' ';
              -- nFirstYear = nLastYear
              if nFirstYear = nLastYear then
                nNumTmp := nLastMonth;
              else
                nNumTmp := 12;
              end if;
              -- nFirst
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ';
              cSqlTmp1 := cSqlTmp1 || ' select ' || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_node' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;

              -- nLast
              if nFirstYear <> nLastYear then
                cSqlTmp3    := 'select ';
                cSqlColumns := '';
                for n in 1 .. nLastMonth loop
                  cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. 12 loop
                  cSqlColumns := cSqlColumns || ' null as ' ||
                                 aColumnListResult(m) || ',';
                end loop;
                cSqlTmp3 := cSqlTmp3 || cSqlColumns || ' YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || ' where  YY = ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp3 := cSqlTmp3 || ' UNION ' || 'select ' ||
                            cSqlColumns || ' YY,' || vNodeColumnName ||
                            '  FROM   ' || vNodeLevel || ',' || 'tb_node' ||
                            ' where  YY = ' || to_char(nLastYear) ||
                            ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                            'tb_node' || '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);

              end if;

              -- judge nFirst+1 < nLastYear-1
              if nFirstYear + 1 <= nLastYear - 1 then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := cSqlTmp2 ||
                            ' UNION  select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;

            end if;

          end if;
          -- right TSID is not 0
        else
          if nFirstMonth = 1 AND nLastMonth = 12 then
            -- =1 =12
            --if 1 = j then
            -- no nFirst  AND  no nLast
            nMark     := 11;
            cSqlTmp1  := ' ';
            cSqlTmp11 := ' ';
            if aTsidListResult(i).nTsidLeft <> MARK_TARGET  then
              -- left <>52
              cSqlTmp1 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          vTmpTableName || '  where  YY between ' ||
                          nFirstYear || '  AND  ' || to_char(nLastYear) ||
                          ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data left
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET  then
              -- left =52
              cSqlTmp1 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear);
              cSqlTmp1 := cSqlTmp1 || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION  ' ||
                          'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'TB_Node' || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear);

              cSqlTmp1 := cSqlTmp1 || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' ||
                          ' TB_Node.id  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add  null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
            end if;
            if aTsidListResult(i).nTsidRight <> MARK_TARGET  then
              -- right <>52
              cSqlTmp11 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           nFirstYear || '  AND  ' || to_char(nLastYear) ||
                           ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                           vTmpTableName || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);

              -- add null data right
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidRight),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
            elsif aTsidListResult(i).nTsidRight = MARK_TARGET  then
              -- right =52
              cSqlTmp11 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_ts_aggregatenode' || '  where  YY between ' ||
                           to_char(nFirstYear) || '  AND  ' ||
                           to_char(nLastYear);
              cSqlTmp11 := cSqlTmp11 || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                           '.id  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);
              cSqlTmp11 := cSqlTmp11 || ' UNION  ' ||
                           'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'TB_Node' || '  where  YY between ' ||
                           to_char(nFirstYear) || '  AND  ' ||
                           to_char(nLastYear);

              cSqlTmp11 := cSqlTmp11 || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' ||
                           ' TB_Node.id  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add  null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidRight),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
            end if;

          elsif nFirstMonth != 1 AND nLastMonth = 12 then
            -- <>1 =12
            --if 1 = j then
            nMark     := 22;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            if aTsidListResult(i).nTsidleft <> MARK_TARGET  then
              --<>52
              -- nFirst left
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                if n != nFirstMonth then
                  cSqlTmp1 := cSqlTmp1 || ',';
                end if;
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n);
              end loop;
              cSqlTmp1 := cSqlTmp1 || ', YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || nFirstYear || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data left
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET then
              -- =52
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ';
              cSqlTmp1 := cSqlTmp1 || ' select ' || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_node' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := cSqlTmp2 || ' UNION ' ||
                            'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;
            if aTsidListResult(i).nTsidRight <> MARK_TARGET  then
              --<>52
              -- nFirst right
              cSqlTmp11 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp11 := cSqlTmp11 || ' null as ' ||
                             aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                if n != nFirstMonth then
                  cSqlTmp11 := cSqlTmp11 || ',';
                end if;
                cSqlTmp11 := cSqlTmp11 || aColumnListResult(n);
              end loop;
              cSqlTmp11 := cSqlTmp11 || ',YY,' || vNodeColumnName ||
                           '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);

              -- add null data right
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidRight),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             vTmpTableName || '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' || vTmpTableName ||
                             '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
              end if;
            elsif aTsidListResult(i).nTsidRight = MARK_TARGET  then
              --=52
              cSqlTmp11 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp11 := cSqlTmp11 || cSqlColumns || ' YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_ts_aggregatenode' || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                           '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);
              cSqlTmp11 := cSqlTmp11 || ' UNION ';
              cSqlTmp11 := cSqlTmp11 || ' select ' || cSqlColumns || ' YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_node' || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidRight),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_ts_aggregatenode' ||
                             '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' ||
                             'tb_ts_aggregatenode' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
                cSqlTmp22 := cSqlTmp22 || ' UNION ' ||
                             'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_node' || '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;

          elsif nFirstMonth = 1 AND nLastMonth != 12 then
            -- =1 <>12
            --if 1 = j then
            nMark     := 33;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            if aTsidListResult(i).nTsidleft <> MARK_TARGET  then
              --<>52
              --nLast left
              cSqlTmp1 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || 'YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data left
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET  then
              -- left =52
              -- nLast left
              cSqlTmp1    := 'select ';
              cSqlColumns := '';
              for n in 1 .. nLastMonth loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ' || 'select ' || cSqlColumns ||
                          ' YY,' || vNodeColumnName || '  FROM   ' ||
                          vNodeLevel || ',' || 'tb_node' || ' where  YY = ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;
            if aTsidListResult(i).nTsidRight <> MARK_TARGET  then
              -- right <>52
              -- judge nFirst < nLastYear
              --nLast right
              cSqlTmp11 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp11 := cSqlTmp11 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlTmp11 := cSqlTmp11 || ' null as ' ||
                             aColumnListResult(m) || ',';
              end loop;
              cSqlTmp11 := cSqlTmp11 || ' YY,' || vNodeColumnName ||
                           '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || ' where  YY = ' ||
                           to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);

              -- add null data right
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidRight),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
              if nFirstYear < nLastYear then

                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             vTmpTableName || '  where  YY between ' ||
                             to_char(nFirstYear) || '  AND  ' ||
                             to_char(nLastYear - 1) || ' AND ' ||
                             vNodeLevel || '.' || vNodeColumnName || '=' ||
                             vTmpTableName || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
              end if;
            elsif aTsidListResult(i).nTsidRight = MARK_TARGET  then
              -- right =52
              cSqlTmp11   := 'select ';
              cSqlColumns := '';
              for n in 1 .. nLastMonth loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              cSqlTmp11 := cSqlTmp11 || cSqlColumns || ' YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_ts_aggregatenode' || ' where  YY = ' ||
                           to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                           '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);
              cSqlTmp11 := cSqlTmp11 || ' UNION ' || 'select ' ||
                           cSqlColumns || ' YY,' || vNodeColumnName ||
                           '  FROM   ' || vNodeLevel || ',' || 'tb_node' ||
                           ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                           vNodeLevel || '.' || vNodeColumnName || '=' ||
                           'tb_node' || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidRight),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- judge nFirst < nLastYear
              if nFirstYear < nLastYear then
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_ts_aggregatenode' ||
                             '  where  YY between ' || to_char(nFirstYear) ||
                             '  AND  ' || to_char(nLastYear - 1) || ' AND ' ||
                             vNodeLevel || '.' || vNodeColumnName || '=' ||
                             'tb_ts_aggregatenode' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_node' || '  where  YY between ' ||
                             to_char(nFirstYear) || '  AND  ' ||
                             to_char(nLastYear - 1) || ' AND ' ||
                             vNodeLevel || '.' || vNodeColumnName || '=' ||
                             'tb_node' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;
          elsif nFirstMonth != 1 AND nLastMonth != 12 then
            --if 1 = j then
            nMark     := 44;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp3  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            cSqlTmp33 := ' ';
            -- nFirstYear = nLastYear
            if nFirstYear = nLastYear then
              nNumTmp := nLastMonth;
            else
              nNumTmp := 12;
            end if;
            if aTsidListResult(i).nTsidleft <> MARK_TARGET  then
              -- left  <>52
              -- nFirst left
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. nNumTmp loop
                cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
              end loop;
              if nNumTmp <> 12 then
                for w in nNumTmp + 1 .. 12 loop
                  cSqlTmp1 := cSqlTmp1 || ' null as ' ||
                              aColumnListResult(w) || ',';
                end loop;
              end if;
              cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              -- add null data left
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidLeft),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;
              -- nLast left
              if nFirstYear <> nLastYear then
                cSqlTmp3 := 'select ';
                for n in 1 .. nLastMonth loop
                  cSqlTmp3 := cSqlTmp3 || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. nNumTmp loop
                  cSqlTmp3 := cSqlTmp3 || ' null AS ' ||
                              aColumnListResult(m) || ',';
                end loop;
                if nNumTmp <> 12 then
                  for w in nNumTmp + 1 .. 12 loop
                    cSqlTmp3 := cSqlTmp3 || ' null as ' ||
                                aColumnListResult(w) || ',';
                  end loop;
                end if;
                cSqlTmp3 := cSqlTmp3 || ' YY,' || vNodeColumnName ||
                            '  FROM   ' || vNodeLevel || ',' ||
                            vTmpTableName || ' where  YY = ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || vTmpTableName ||
                            '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                -- judge nFirst+1 < nLastYear-1
                if nFirstYear + 1 <= nLastYear - 1 then
                  cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                              vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                              vTmpTableName || '  where  YY between ' ||
                              to_char(nFirstYear + 1) || '  AND  ' ||
                              to_char(nLastYear - 1) || ' AND ' ||
                              vNodeLevel || '.' || vNodeColumnName || '=' ||
                              vTmpTableName || '.id';
                  cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                              to_char(aTsidListResult(i).nTsidLeft);
                end if;
              end if;
            elsif aTsidListResult(i).nTsidLeft = MARK_TARGET then
              --left =52
              -- nFirst
              cSqlTmp1 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp1 := cSqlTmp1 || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_ts_aggregatenode' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                          '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' ||
                          to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp1 := cSqlTmp1 || ' UNION ';
              cSqlTmp1 := cSqlTmp1 || ' select ' || cSqlColumns || ' YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          'tb_node' || ' where  YY = ' ||
                          to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp1 := cSqlTmp1 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidLeft),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
              end if;

              -- nLast
              if nFirstYear <> nLastYear then
                cSqlTmp3    := 'select ';
                cSqlColumns := '';
                for n in 1 .. nLastMonth loop
                  cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. 12 loop
                  cSqlColumns := cSqlColumns || ' null as ' ||
                                 aColumnListResult(m) || ',';
                end loop;
                cSqlTmp3 := cSqlTmp3 || cSqlColumns || ' YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || ' where  YY = ' ||
                            to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp3 := cSqlTmp3 || ' UNION ' || 'select ' ||
                            cSqlColumns || ' YY,' || vNodeColumnName ||
                            '  FROM   ' || vNodeLevel || ',' || 'tb_node' ||
                            ' where  YY = ' || to_char(nLastYear) ||
                            ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                            'tb_node' || '.id';
                cSqlTmp3 := cSqlTmp3 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);

              end if;

              -- judge nFirst+1 < nLastYear-1
              if nFirstYear + 1 <= nLastYear - 1 then
                cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_ts_aggregatenode' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                            '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                            to_char(aTsidListResult(i).nTsidLeft);
                cSqlTmp2 := cSqlTmp2 ||
                            ' UNION  select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                            vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                            'tb_node' || '  where  YY between ' ||
                            to_char(nFirstYear + 1) || '  AND  ' ||
                            to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                            vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp2 := cSqlTmp2 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;
            if aTsidListResult(i).nTsidRight <> MARK_TARGET  then
              --left <>52
              -- nFirst right
              --if nFirstYear <> nLastYear then
              cSqlTmp11 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlTmp11 := cSqlTmp11 || ' null as ' ||
                             aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlTmp11 := cSqlTmp11 || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp11 := cSqlTmp11 || ' YY,' || vNodeColumnName ||
                           '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);
              --end if;

              -- add null data right
              cSqlNullData := '';
              FMSP_GetNULLData(pIn_nFirstYear        => nFirstYear,
                               pIn_nLastYear         => nLastYear,
                               pIn_vTSID             => to_char(aTsidListResult(i)
                                                                .nTsidRight),
                               pIn_vTmpTableNodeList => vTmpTableName,
                               pIn_vTableNodeLevel   => vNodeLevel,
                               pIn_vColumnNodeLevel  => vNodeColumnName,
                               pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;

              -- nLast right
              if nFirstYear <> nLastYear then
                cSqlTmp33 := 'select ';
                for n in 1 .. nLastMonth loop
                  cSqlTmp33 := cSqlTmp33 || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. 12 loop
                  cSqlTmp33 := cSqlTmp33 || ' null AS ' ||
                               aColumnListResult(m) || ',';
                end loop;
                cSqlTmp33 := cSqlTmp33 || ' YY,' || vNodeColumnName ||
                             '  FROM   ' || vNodeLevel || ',' ||
                             vTmpTableName || ' where  YY = ' ||
                             to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' || vTmpTableName ||
                             '.id';
                cSqlTmp33 := cSqlTmp33 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
              end if;
              -- judge nFirst+1 < nLastYear-1
              if nFirstYear + 1 <= nLastYear - 1 then
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             vTmpTableName || '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear - 1);
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
              end if;
            elsif aTsidListResult(i).nTsidRight = MARK_TARGET  then
              -- left =52
              -- nFirst
              cSqlTmp11 := 'select ';
              for m in 1 .. nFirstMonth - 1 loop
                cSqlColumns := cSqlColumns || ' null as ' ||
                               aColumnListResult(m) || ',';
              end loop;
              for n in nFirstMonth .. 12 loop
                cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
              end loop;
              cSqlTmp11 := cSqlTmp11 || cSqlColumns || ' YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_ts_aggregatenode' || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_ts_aggregatenode' ||
                           '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID=' ||
                           to_char(aTsidListResult(i).nTsidRight);
              cSqlTmp11 := cSqlTmp11 || ' UNION ';
              cSqlTmp11 := cSqlTmp11 || ' select ' || cSqlColumns || ' YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           'tb_node' || ' where  YY = ' ||
                           to_char(nFirstYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || 'tb_node' || '.id';
              cSqlTmp11 := cSqlTmp11 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              -- add null data
              cSqlNullData := '';
              FMSP_GetNULLDataTarget(pIn_nFirstYear        => nFirstYear,
                                     pIn_nLastYear         => nLastYear,
                                     pIn_vTSID             => to_char(aTsidListResult(i)
                                                                      .nTsidRight),
                                     pIn_vTmpTableNodeList => 'tb_ts_aggregatenode',
                                     pIn_vTableNodeLevel   => vNodeLevel,
                                     pIn_vColumnNodeLevel  => vNodeColumnName,
                                     pOut_cSql             => cSqlNullData);
              if length(cSqlNullData) > 5 then
                cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
              end if;

              -- nLast
              if nFirstYear <> nLastYear then
                cSqlTmp33   := 'select ';
                cSqlColumns := '';
                for n in 1 .. nLastMonth loop
                  cSqlColumns := cSqlColumns || aColumnListResult(n) || ',';
                end loop;
                for m in nLastMonth + 1 .. 12 loop
                  cSqlColumns := cSqlColumns || ' null as ' ||
                                 aColumnListResult(m) || ',';
                end loop;
                cSqlTmp33 := cSqlTmp33 || cSqlColumns || ' YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_ts_aggregatenode' || ' where  YY = ' ||
                             to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' ||
                             'tb_ts_aggregatenode' || '.id';
                cSqlTmp33 := cSqlTmp33 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
                cSqlTmp33 := cSqlTmp33 || ' UNION ' || 'select ' ||
                             cSqlColumns || ' YY,' || vNodeColumnName ||
                             '  FROM   ' || vNodeLevel || ',' || 'tb_node' ||
                             ' where  YY = ' || to_char(nLastYear) ||
                             ' AND ' || vNodeLevel || '.' ||
                             vNodeColumnName || '=' || 'tb_node' || '.id';
                cSqlTmp33 := cSqlTmp33 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);

              end if;

              -- judge nFirst+1 < nLastYear-1
              if nFirstYear + 1 <= nLastYear - 1 then
                cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_ts_aggregatenode' ||
                             '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear - 1) || ' AND ' ||
                             vNodeLevel || '.' || vNodeColumnName || '=' ||
                             'tb_ts_aggregatenode' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID=' ||
                             to_char(aTsidListResult(i).nTsidRight);
                cSqlTmp22 := cSqlTmp22 ||
                             ' UNION  select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                             vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                             'tb_node' || '  where  YY between ' ||
                             to_char(nFirstYear + 1) || '  AND  ' ||
                             to_char(nLastYear - 1) || ' AND ' ||
                             vNodeLevel || '.' || vNodeColumnName || '=' ||
                             'tb_node' || '.id';
                cSqlTmp22 := cSqlTmp22 || '  AND  TSID='||to_char( MARK_TARGET_REPLACE);
              end if;
            end if;
          end if;
        end if;
        -- end loop;
        -- no right no nLast no nFirst
        if 1 = nMark then
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp1,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp1);
          if 1 = i then
            cSqlFirst := cSqlTmp1;
          elsif 2 = i then
            cSqlSecond := cSqlTmp1;
          elsif 3 = i then
            cSqlThird := cSqlTmp1;
          elsif 4 = i then
            cSqlFourth := cSqlTmp1;
          elsif 5 = i then
            cSqlFifth := cSqlTmp1;
          elsif 6 = i then
            cSqlSixth := cSqlTmp1;
          end if;
          --debug
          ---- (csqlTmp1);
          --commit;
          --debug
          -- no right nFirst no nLast
        elsif 2 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          --debug
          --  --insert into clobtest(sqlcontent) values(csqlTmp3);
          --  commit;
          --debug
          -- i for TSID
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          if 1 = i then
            cSqlFirst := cSqlTmp3;
          elsif 2 = i then
            cSqlSecond := cSqlTmp3;
          elsif 3 = i then
            cSqlThird := cSqlTmp3;
          elsif 4 = i then
            cSqlFourth := cSqlTmp3;
          elsif 5 = i then
            cSqlFifth := cSqlTmp3;
          elsif 6 = i then
            cSqlSixth := cSqlTmp3;
          end if;
          -- no right nLast no nFirst
        elsif 3 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          -- i for TSID
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          if 1 = i then
            cSqlFirst := cSqlTmp3;
          elsif 2 = i then
            cSqlSecond := cSqlTmp3;
          elsif 3 = i then
            cSqlThird := cSqlTmp3;
          elsif 4 = i then
            cSqlFourth := cSqlTmp3;
          elsif 5 = i then
            cSqlFifth := cSqlTmp3;
          elsif 6 = i then
            cSqlSixth := cSqlTmp3;
          end if;
          -- no right nLast nFirst
        elsif 4 = nMark then
          if length(cSqlTmp2) > 5 and length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || '  UNION ALL ' ||
                        cSqlTmp3 || ' )';
          elsif length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp3 || ')';
          else
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ')';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp4,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp4);
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right no nLast no nFirst
        elsif 11 = nMark then
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp1,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp1);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp11,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_RIGHT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp11);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp1 || ') a left join (' ||
                      cSqlTmp11 || ') b on a.YY=b.YY  AND  ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right no nLast nFirst
        elsif 22 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ')';
          end if;
          if length(cSqlTmp22) > 5 then
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || ' )';
          else
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp33,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp33);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp3 || ') a left join (' ||
                      cSqlTmp33 || ') b on a.YY=b.YY  AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          --debug
          ---- (cSqlTmp4);
          --commit;
          --debug
          -- right nLast no nFirst
        elsif 33 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          if length(cSqlTmp22) > 5 then
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || ' )';
          else
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp33,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp33);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp3 || ') a left join (' ||
                      cSqlTmp33 || ') b on a.YY=b.YY AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right nLast nFirst
        elsif 44 = nMark then
          if length(cSqlTmp2) > 5 and length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || '  UNION ALL ' ||
                        cSqlTmp3 || ' )';
          elsif length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL ' || cSqlTmp3 || ' )';
          else
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          if length(cSqlTmp22) > 5 and length(cSqlTmp33) > 5 then
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || '  UNION ALL ' ||
                         cSqlTmp33 || ' )';
          elsif length(cSqlTmp33) > 5 then
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp33 || ' )';
          else
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          --debug
          --  -- (cSqlTmp4);
          --  commit;
          --debug
          --debug
          --  -- (cSqlTmp44);
          --  commit;
          --debug
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp4,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp4);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp44,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp44);
          cSqlTmp5 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp4 || ') a left join (' ||
                      cSqlTmp44 || ') b on a.YY=b.YY AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp5;
          elsif 2 = i then
            cSqlSecond := cSqlTmp5;
          elsif 3 = i then
            cSqlThird := cSqlTmp5;
          elsif 4 = i then
            cSqlFourth := cSqlTmp5;
          elsif 5 = i then
            cSqlFifth := cSqlTmp5;
          elsif 6 = i then
            cSqlSixth := cSqlTmp5;
          end if;

        end if;
        if 1 = i then
          cSqlFirst := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',1 AS orderID FROM (' ||
                       cSqlFirst || ')';
        elsif 2 = i then
          cSqlSecond := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                        vNodeColumnName || ',2 AS orderID FROM (' ||
                        cSqlSecond || ')';
        elsif 3 = i then
          cSqlThird := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',3 AS orderID FROM (' ||
                       cSqlThird || ')';
        elsif 4 = i then
          cSqlFourth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                        vNodeColumnName || ',4 AS orderID FROM (' ||
                        cSqlFourth || ')';
        elsif 5 = i then
          cSqlFifth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',5 AS orderID FROM (' ||
                       cSqlFifth || ')';
          /*        elsif 6 = i then
          cSqlSixth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',6 AS orderID FROM (' ||
                       cSqlSixth || ')';*/
        end if;
      end loop;

      -- first value
      if pIn_nCalculationType = 2 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := ' ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')';
            -- the top five lines firstValue
            -- change HERE
            if length(cSqlSixth) > 5 then
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);

              cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              -- change HERE
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := ' ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')';
            -- the top five lines firstValue
            -- change HERE
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || ' ,orderid FROM   (' ||
                         cSqlTmp55 || ')  ';

            if length(cSqlSixth) > 5 then
              -- the sixth line add
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else

              -- change HERE
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth line
          cSqlTmp55 := ' ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ')';
          -- the top five lines firstValue
          -- change HERE
          -- the sixth line add
          if length(cSqlSixth) > 5 then
            FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM  (' || cSqlTmp55 || ')';
          end if;
        end if;
        -- max value
      elsif pIn_nCalculationType = 1 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                         'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                         'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                         'MAX(T9) T9,' || 'MAX(T10) T10,' ||
                         'MAX(T11) T11,' || 'MAX(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ') group by YY,' || vNodeColumnName;
            -- the top five lines firstValue
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                         'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                         'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                         'MAX(T9) T9,' || 'MAX(T10) T10,' ||
                         'MAX(T11) T11,' || 'MAX(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            -- the top five lines firstValue
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth line
          cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                       'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                       'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                       'MAX(T9) T9,' || 'MAX(T10) T10,' || 'MAX(T11) T11,' ||
                       'MAX(T12) T12,' || 'YY,' || vNodeColumnName ||
                       ' FROM  ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ') group by YY,' || vNodeColumnName;
          cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
          -- the top five lines firstValue
          if length(cSqlSixth) > 5 then
            -- the sixth line add
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
          end if;
        end if;
        -- min value
      elsif pIn_nCalculationType = 3 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                         'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                         'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                         'MIN(T9) T9,' || 'MIN(T10) T10,' ||
                         'MIN(T11) T11,' || 'MIN(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            -- the top five lines firstValue
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID , sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                         'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                         'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                         'MIN(T9) T9,' || 'MIN(T10) T10,' ||
                         'MIN(T11) T11,' || 'MIN(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            -- the top five lines firstValue
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth
          cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                       'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                       'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                       'MIN(T9) T9,' || 'MIN(T10) T10,' || 'MIN(T11) T11,' ||
                       'MIN(T12) T12,' || 'YY,' || vNodeColumnName ||
                       ' FROM  ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
          -- the top five lines firstValue
          if length(cSqlSixth) > 5 then
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            -- the sixth line add
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
          end if;
        end if;
      end if;
      -- debug
      --Fmp_Log.LOGERROR(cSqlTmp55);
      -- debug
      -- update or insert
      if nFirstMonth = 1 AND nLastMonth = 12 then
        cSqlTmp55 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                     cSqlTmp55 ||
                     ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                     vNodeColumnName || '=' || 'T.' || vNodeColumnName ||
                     ') WHEN MATCHED THEN  UPDATE ' ||
                     'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                     '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                     '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                     '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                     '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                     '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                     'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                     '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                     '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                     '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                     '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                     '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                     'WHEN NOT MATCHED THEN ' || 'INSERT ' || ' VALUES(' || vSeq ||
                     '.nextval,T.' || vNodeColumnName ||
                     ',T.TSID,0,T.YY,round(T.T1,' ||
                     to_char(pIn_nPrecision) || '),round(T.T2,' ||
                     to_char(pIn_nPrecision) || '),round(T.T3,' ||
                     to_char(pIn_nPrecision) || '),round(T.T4,' ||
                     to_char(pIn_nPrecision) || '),round(T.T5,' ||
                     to_char(pIn_nPrecision) || '),round(T.T6,' ||
                     to_char(pIn_nPrecision) || '),round(T.T7,' ||
                     to_char(pIn_nPrecision) || '),round(T.T8,' ||
                     to_char(pIn_nPrecision) || '),round(T.T9,' ||
                     to_char(pIn_nPrecision) || '),round(T.T10,' ||
                     to_char(pIn_nPrecision) || '),round(T.T11,' ||
                     to_char(pIn_nPrecision) || '),round(T.T12,' ||
                     to_char(pIn_nPrecision) || ')' || ')';
        FMSP_ExecSql(cSqlTmp55);
        --FMP_log.LOGERROR(cSqlTmp55);
        -- nFirstMonth no nLastMonth
      elsif nFirstMonth <> 1 AND nLastMonth = 12 then
        cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                    vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                    ' )where  YY = ' || to_char(nFirstYear);
        cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' || cSqlTmp5 ||
                    ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                    vNodeColumnName || '= T.' || vNodeColumnName ||
                    ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT ';
        cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                    vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                    to_char(pIn_nPrecision) || '),round(T.T2,' ||
                    to_char(pIn_nPrecision) || '),round(T.T3,' ||
                    to_char(pIn_nPrecision) || '),round(T.T4,' ||
                    to_char(pIn_nPrecision) || '),round(T.T5,' ||
                    to_char(pIn_nPrecision) || '),round(T.T6,' ||
                    to_char(pIn_nPrecision) || '),round(T.T7,' ||
                    to_char(pIn_nPrecision) || '),round(T.T8,' ||
                    to_char(pIn_nPrecision) || '),round(T.T9,' ||
                    to_char(pIn_nPrecision) || '),round(T.T10,' ||
                    to_char(pIn_nPrecision) || '),round(T.T11,' ||
                    to_char(pIn_nPrecision) || '),round(T.T12,' ||
                    to_char(pIn_nPrecision) || ')' || ')';
        for m in nFirstMonth .. 12 loop
          if m != nFirstMonth then
            cSqlTmp1 := cSqlTmp1 || ',';
          end if;
          cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                      ' = round(T.' || aColumnListResult(m) || ',' ||
                      to_char(pIn_nPrecision) || ')';
        end loop;
        cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
        FMSP_ExecSql(cSqlTmp3);
        --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp3);
        -- normal
        if nFirstYear < nLastYear then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY between ' || to_char(nFirstYear + 1) ||
                      '  AND  ' || to_char(nLastYear);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ' ) WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      'WHEN NOT MATCHED THEN  ' || 'INSERT ' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp4);
        end if;
        -- no nFirstMonth nLastMonth
      elsif nFirstMonth = 1 AND nLastMonth <> 12 then
        -- nLast
        cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                    vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                    ')  where  YY = ' || to_char(nLastYear);
        cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' || cSqlTmp5 ||
                    ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                    vNodeColumnName || '= T.' || vNodeColumnName ||
                    ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT';
        cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                    vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                    to_char(pIn_nPrecision) || '),round(T.T2,' ||
                    to_char(pIn_nPrecision) || '),round(T.T3,' ||
                    to_char(pIn_nPrecision) || '),round(T.T4,' ||
                    to_char(pIn_nPrecision) || '),round(T.T5,' ||
                    to_char(pIn_nPrecision) || '),round(T.T6,' ||
                    to_char(pIn_nPrecision) || '),round(T.T7,' ||
                    to_char(pIn_nPrecision) || '),round(T.T8,' ||
                    to_char(pIn_nPrecision) || '),round(T.T9,' ||
                    to_char(pIn_nPrecision) || '),round(T.T10,' ||
                    to_char(pIn_nPrecision) || '),round(T.T11,' ||
                    to_char(pIn_nPrecision) || '),round(T.T12,' ||
                    to_char(pIn_nPrecision) || ')' || ')';
        for m in 1 .. nLastMonth loop
          if m <> nFirstMonth then
            cSqlTmp1 := cSqlTmp1 || ',';
          end if;
          cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                      ' = round(T.' || aColumnListResult(m) || ',' ||
                      to_char(pIn_nPrecision) || ')';
        end loop;
        cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
        FMSP_ExecSql(cSqlTmp3);
        --FMP_log.LOGERROR(cSqlTmp3);
        -- normal
        if nFirstYear < nLastYear then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ')  where  YY between ' || to_char(nFirstYear) ||
                      '  AND  ' || to_char(nLastYear - 1);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      'WHEN NOT MATCHED THEN ' || 'INSERT' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --FMP_log.LOGERROR(cSqlTmp4);
        end if;
      elsif nFirstMonth <> 1 AND nLastMonth <> 12 then
        if nFirstYear <> nLastYear then
          -- nLast
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY = ' || to_char(nLastYear);
          cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE set ';
          cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT';
          cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                      vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          for m in 1 .. nLastMonth loop
            if m != 1 then
              cSqlTmp1 := cSqlTmp1 || ',';
            end if;
            cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                        ' = round(T.' || aColumnListResult(m) || ',' ||
                        to_char(pIn_nPrecision) || ')';
          end loop;
          cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
          FMSP_ExecSql(cSqlTmp3);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp3);
        end if;

        if nFirstYear = nLastYear then
          nNumTmp := nLastMonth;
        else
          nNumTmp := 12;
        end if;

        --nFirst
        cSqlTmp5  := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                     vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                     ')  where  YY = ' || to_char(nFirstYear);
        cSqlTmp11 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                     cSqlTmp5 ||
                     ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                     vNodeColumnName || '=T.' || vNodeColumnName ||
                     ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp22 := ' WHEN NOT MATCHED THEN  INSERT ';
        cSqlTmp22 := cSqlTmp22 || ' VALUES(' || vSeq || '.nextval,T.' ||
                     vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                     to_char(pIn_nPrecision) || '),round(T.T2,' ||
                     to_char(pIn_nPrecision) || '),round(T.T3,' ||
                     to_char(pIn_nPrecision) || '),round(T.T4,' ||
                     to_char(pIn_nPrecision) || '),round(T.T5,' ||
                     to_char(pIn_nPrecision) || '),round(T.T6,' ||
                     to_char(pIn_nPrecision) || '),round(T.T7,' ||
                     to_char(pIn_nPrecision) || '),round(T.T8,' ||
                     to_char(pIn_nPrecision) || '),round(T.T9,' ||
                     to_char(pIn_nPrecision) || '),round(T.T10,' ||
                     to_char(pIn_nPrecision) || '),round(T.T11,' ||
                     to_char(pIn_nPrecision) || '),round(T.T12,' ||
                     to_char(pIn_nPrecision) || ')' || ')';
        for m in nFirstMonth .. nNumTmp loop
          if m != nFirstMonth then
            cSqlTmp11 := cSqlTmp11 || ',';
          end if;
          cSqlTmp11 := cSqlTmp11 || 'V.' || aColumnListResult(m) ||
                       ' = round(T.' || aColumnListResult(m) || ',' ||
                       to_char(pIn_nPrecision) || ')';
        end loop;
        cSqlTmp33 := cSqlTmp11 || cSqlTmp22;
        FMSP_ExecSql(cSqlTmp33);
        --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp33);
        -- normal
        if nFirstYear + 1 <= nLastYear - 1 then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY between ' || to_char(nFirstYear + 1) ||
                      '  AND  ' || to_char(nLastYear - 1);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      ' WHEN NOT MATCHED THEN ' || 'INSERT ' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp4);
        end if;
      end if;
    END;
  End FMSP_validateFCSTMTarget;

  procedure FMSP_validateFCSTM(pIn_nSelectionID          in number,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vConditions           in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number,
                               pIn_nMultiplex            in number) AS
    --*****************************************************************
    -- Description: it support for month validateFCST
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- pIn_nMultiplex  mark multiplex
    -- 0 means no multiplex
    -- 1 means multiplex in nodelist
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    declare
      /*      type rTsidPair is record(
        nTsidLeft  prb_m.tsid%TYPE := -1,
        nTsidRight prb_m.tsid%TYPE := -1); -- it a record Type for store tsid (left  AND  right)
      TYPE aTsidList is TABLE OF rTsidPair INDEX BY BINARY_INTEGER; -- it an array Type for rTsidPair Type
      aTsidListResult aTsidList; -- it an array for store rTsidPair*/
      vNodeLevel varchar2(5) := 'prb_m'; -- table_name default prb_m;
      sNodeList  sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
      nSqlCode   number; -- this variable is the parameter for analyze
      --vSqlNodeList varchar2(100); -- this variable is a sql for get the result of TB_TS_DetailNodeSelCdt(store analyze pIn_vConditions)
      cSqlFirst  CLOB := ' ';
      cSqlSecond CLOB := ' ';
      cSqlThird  CLOB := ' ';
      cSqlFourth CLOB := ' ';
      cSqlFifth  CLOB := ' ';
      cSqlSixth  CLOB := ' ';
      cSqlTmp1   CLOB := ' ';
      cSqlTmp2   CLOB := ' ';
      cSqlTmp3   CLOB := ' ';
      cSqlTmp4   CLOB := ' ';
      cSqlTmp11  CLOB := ' ';
      cSqlTmp22  CLOB := ' ';
      cSqlTmp33  CLOB := ' ';
      cSqlTmp44  CLOB := ' ';
      cSqlTmp5   CLOB := ' ';
      cSqlTmp55  CLOB := ' ';
      cSqlTmp6   CLOB := ' ';
      cSqlTmp66  CLOB := ' ';
      nMark      number := 0;
      TYPE aNodeList IS TABLE OF NUMBER INDEX BY BINARY_INTEGER; -- an array Type stored number
      aNodeListResult  aNodeList; -- an arrary for store the result of TB_TS_DetailNodeSelCdt
      vTmp             varchar2(100); -- for store temp data
      bAND             boolean := false; -- for mark temp boolean
      bTmp             boolean := false; -- for mark temp boolean too
      vNodeColumnName  varchar2(100) := 'SELID';
      vFirstPeriodTime varchar2(10);
      vLastPeriodTime  varchar2(10);
      nFirstMonth      number; -- firstMonth
      nLastMonth       number; -- lastMonth
      nFirstYear       number; -- firstYear
      nLastYear        number; -- lastYear
      TYPE aColumnList IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER;
      aColumnListResult aColumnList;
      vSeq              varchar2(10) := 'seq_prb_m';
      vTmpTableName     varchar2(40) := 'TB_TS_AggregateNodeCon';
      nNumTmp           number;
      vTabName          varchar2(30);
      cSqlNullData      clob;
      bTargetFlag       boolean;
      i                 number;
      w                 number;
      n                 number;
      m                 number;
      j                 number;
    BEGIN

      -- init column list
      aColumnListResult(1) := 'T1';
      aColumnListResult(2) := 'T2';
      aColumnListResult(3) := 'T3';
      aColumnListResult(4) := 'T4';
      aColumnListResult(5) := 'T5';
      aColumnListResult(6) := 'T6';
      aColumnListResult(7) := 'T7';
      aColumnListResult(8) := 'T8';
      aColumnListResult(9) := 'T9';
      aColumnListResult(10) := 'T10';
      aColumnListResult(11) := 'T11';
      aColumnListResult(12) := 'T12';
      -- deal with date
      nFirstMonth := to_number(substr(pIn_vFirstPeriodTime, 5));
      nLastMonth  := to_number(substr(pIn_vLastPeriodTime, 5));
      nFirstYear  := to_number(substr(pIn_vFirstPeriodTime, 0, 4));
      nLastYear   := to_number(substr(pIn_vLastPeriodTime, 0, 4));
      -- judge prb_m or don_m  analyze p_conditions for nodelist
      if pIn_nNodeLevel = 1 then
        vNodeLevel      := 'DON_M';
        vNodeColumnName := 'PVTID';
        vSeq            := 'seq_don_m';
        vTmpTableName   := 'TB_TS_DetailNodeSelCdt';
        if pIn_nMultiplex = 0 then
          p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => pIn_nSelectionID,
                                               P_Conditions  => pIn_vConditions,
                                               P_Sequence    => null, --Sort sequence
                                               p_DetailNode  => sNodeList,
                                               pOut_vTabName => vTabName,
                                               p_SqlCode     => nSqlCode);
        end if;
      else
        if pIn_nMultiplex = 0 then
          p_aggregation.FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => pIn_nSelectionID,
                                                  pIn_vConditions => pIn_vConditions,
                                                  pOut_Nodes      => sNodeList,
                                                  pOut_nSqlCode   => nSqlCode);
        end if;
      end if;
      -- get the result of TB_TS_DetailNodeSelCdt (NODELIST)
      open sNodeList for 'select id FROM ' || vTmpTableName;
      fetch sNodeList bulk collect
        into aNodeListresult;
      if aNodeListResult.count = 0 then
        return;
      end if;
      --debug
      /*      delete from tb_ts;
      commit;
      insert into tb_ts
        select id from TB_TS_AggregateNodeCon;
      commit;*/
      --debug
      -- if target or not
      bTargetFlag := false;
      FMSP_InitTSIDList(pIn_vSourceTimeSeriesIDs => pIn_vSourceTimeSeriesIDs,
                        pOut_bFlag               => bTargetFlag);
      if aTsidListResult(1).nTsidLeft = -1 AND aTsidListResult(1)
         .nTsidRight = -1 AND aTsidListResult(2)
         .nTsidLeft = -1 AND aTsidListResult(2)
         .nTsidRight = -1 AND aTsidListResult(3)
         .nTsidLeft = -1 AND aTsidListResult(3)
         .nTsidRight = -1 AND aTsidListResult(4)
         .nTsidLeft = -1 AND aTsidListResult(4)
         .nTsidRight = -1 AND aTsidListResult(5)
         .nTsidLeft = -1 AND aTsidListResult(5)
         .nTsidRight = -1 AND aTsidListResult(6)
         .nTsidLeft = -1 AND aTsidListResult(6)
         .nTsidRight = -1 then
        return ;
      end if;
      if bTargetFlag then
        delete from tb_ts_aggregatenode;
        if pIn_nNodeLevel = 2 then
          insert into tb_ts_aggregatenode
            select id from tb_ts_aggregatenodecon;
        elsif pIn_nNodeLevel = 1 then
          insert into tb_ts_aggregatenode
            select id from tb_ts_detailnodeselcdt;
        end if;
        commit;
        FMSP_OperateTarget(pIn_nFirstYear        => nFirstYear,
                           pIn_nLastYear         => nLastYear,
                           pIn_nFirstMonth       => nFirstMonth,
                           pIn_nLastMonth        => nLastMonth,
                           pIn_vTmpTableNodeList => vTmpTableName,
                           pIn_vTableNodeLevel   => vNodeLevel,
                           pIn_vColumnNodeLevel  => vNodeColumnName,
                           pIn_vTargetTSID       => to_char(MARK_TARGET) ,
                           pIn_vReplaceTSID      => to_char(MARK_TARGET_REPLACE));

        FMSP_validateFCSTMTarget(pIn_nSelectionID,
                                 pIn_nCalculationType,
                                 pIn_vFirstPeriodTime,
                                 pIn_vLastPeriodTime,
                                 pIn_vConditions,
                                 pIn_vSourceTimeSeriesIDs,
                                 pIn_nValidateTimeSeriesId,
                                 pIn_nIsPlusTimeSeries5,
                                 pIn_nNodeLevel,
                                 to_char(pIn_nPrecision),
                                 pIn_nMultiplex => 0);
        return;
      end if;

      -- deal with the  lines*/
      -- for with the TSIDLIST
      -- i for TSIDLIST
      for i in 1 .. 6 loop
        -- if the left is zero then continue
        if aTsidListResult(i).nTsidLeft = -1 then
          continue;
        end if;
        vTmp := aTsidListResult(i).nTsidLeft;
        vTmp := aTsidListResult(i).nTsidRight;
        -- for with the NODELIST
        -- j for NODELIST
        -- right TSID is 0
        if aTsidListResult(i).nTsidRight = -1 then
          if nFirstMonth = 1 AND nLastMonth = 12 then
            -- no first  AND  no left
            cSqlTmp1 := ' ';
            nMark    := 1;
            cSqlTmp1 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                        vTmpTableName || '  where  YY between ' ||
                        to_char(nFirstYear) || '  AND  ' ||
                        to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                        vNodeColumnName || '=' || vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            -- add  null data
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
          elsif nFirstMonth != 1 AND nLastMonth = 12 then
            --if 1 = j then
            nMark    := 2;
            cSqlTmp1 := ' ';
            cSqlTmp2 := ' ';
            -- nFirst left
            cSqlTmp1 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. 12 loop
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
            end loop;
            cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                        '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            -- add null data
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- judge nFirst < nLastYear
            if nFirstYear < nLastYear then
              cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          vTmpTableName || '  where  YY between ' ||
                          to_char(nFirstYear + 1) || '  AND  ' ||
                          to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2 := cSqlTmp2 || '  AND  TSID = ' ||
                          to_char(aTsidListResult(i).nTsidLeft);
            end if;
          elsif nFirstMonth = 1 AND nLastMonth != 12 then
            --if 1 = j then
            nMark    := 3;
            cSqlTmp1 := ' ';
            cSqlTmp2 := ' ';
            -- nLast left
            cSqlTmp1 := 'select ';
            for n in 1 .. nLastMonth loop
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
            end loop;
            for m in nLastMonth + 1 .. 12 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                        '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            -- add null data
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- judge nFirst < nLastYear
            if nFirstYear < nLastYear then
              cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          vTmpTableName || '  where  YY between ' ||
                          to_char(nFirstYear) || '  AND  ' ||
                          to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2 := cSqlTmp2 || '  AND  TSID = ' ||
                          to_char(aTsidListResult(i).nTsidLeft);
            end if;
          elsif nFirstMonth != 1 AND nLastMonth != 12 then
            --if 1 = j then
            nMark    := 4;
            cSqlTmp1 := ' ';
            cSqlTmp2 := ' ';
            cSqlTmp3 := ' ';
            -- nFirstYear = nLastYear
            if nFirstYear = nLastYear then
              nNumTmp := nLastMonth;
            else
              nNumTmp := 12;
            end if;
            -- nFirst
            cSqlTmp1 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. nNumTmp loop
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
            end loop;
            if nNumTmp <> 12 then
              for w in nNumTmp + 1 .. 12 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(w) || ',';
              end loop;
            end if;
            cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                        '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            --add data
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- nLast
            if nFirstYear <> nLastYear then
              cSqlTmp3 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp3 := cSqlTmp3 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlTmp3 := cSqlTmp3 || ' null as ' || aColumnListResult(m) || ',';
              end loop;
              cSqlTmp3 := cSqlTmp3 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp3 := cSqlTmp3 || '  AND  TSID = ' ||
                          to_char(aTsidListResult(i).nTsidLeft);
            end if;
            -- judge nFirst+1 < nLastYear-1
            if nFirstYear + 1 <= nLastYear - 1 then
              cSqlTmp2 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                          vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                          vTmpTableName || '  where  YY between ' ||
                          to_char(nFirstYear + 1) || '  AND  ' ||
                          to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                          vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2 := cSqlTmp2 || '  AND  TSID = ' ||
                          to_char(aTsidListResult(i).nTsidLeft);
            end if;
          end if;
          -- right TSID is not 0
        else
          if nFirstMonth = 1 AND nLastMonth = 12 then
            --if 1 = j then
            -- no nFirst  AND  no nLast
            nMark     := 11;
            cSqlTmp1  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp1  := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                         vTmpTableName || '  where  YY between ' ||
                         nFirstYear || '  AND  ' || to_char(nLastYear) ||
                         ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                         vTmpTableName || '.id';
            cSqlTmp1  := cSqlTmp1 || '  AND  TSID = ' ||
                         to_char(aTsidListResult(i).nTsidLeft);
            cSqlTmp11 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                         vTmpTableName || '  where  YY between ' ||
                         nFirstYear || '  AND  ' || to_char(nLastYear) ||
                         ' AND ' || vNodeLevel || '.' || vNodeColumnName || '=' ||
                         vTmpTableName || '.id';
            cSqlTmp11 := cSqlTmp11 || '  AND  TSID = ' ||
                         to_char(aTsidListResult(i).nTsidRight);
            -- add null data left
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- add null data right
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidRight),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
            end if;
          elsif nFirstMonth != 1 AND nLastMonth = 12 then
            --if 1 = j then
            nMark     := 22;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            -- nFirst left
            cSqlTmp1 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. 12 loop
              if n != nFirstMonth then
                cSqlTmp1 := cSqlTmp1 || ',';
              end if;
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n);
            end loop;
            cSqlTmp1 := cSqlTmp1 || ', YY,' || vNodeColumnName ||
                        '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || nFirstYear || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            -- nFirst right
            cSqlTmp11 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp11 := cSqlTmp11 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. 12 loop
              if n != nFirstMonth then
                cSqlTmp11 := cSqlTmp11 || ',';
              end if;
              cSqlTmp11 := cSqlTmp11 || aColumnListResult(n);
            end loop;
            cSqlTmp11 := cSqlTmp11 || ',YY,' || vNodeColumnName ||
                         '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                         ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                         vNodeLevel || '.' || vNodeColumnName || '=' ||
                         vTmpTableName || '.id';
            cSqlTmp11 := cSqlTmp11 || '  AND  TSID = ' ||
                         to_char(aTsidListResult(i).nTsidRight);
            -- add null data left
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- add null data right
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidRight),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- judge nFirst < nLastYear
            if nFirstYear < nLastYear then
              cSqlTmp2  := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear + 1) || '  AND  ' ||
                           to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2  := cSqlTmp2 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear + 1) || '  AND  ' ||
                           to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp22 := cSqlTmp22 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidRight);
            end if;
          elsif nFirstMonth = 1 AND nLastMonth != 12 then
            --if 1 = j then
            nMark     := 33;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            --nLast left
            cSqlTmp1 := 'select ';
            for n in 1 .. nLastMonth loop
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
            end loop;
            for m in nLastMonth + 1 .. 12 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            cSqlTmp1 := cSqlTmp1 || 'YY,' || vNodeColumnName || '  FROM   ' ||
                        vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            --nLast right
            cSqlTmp11 := 'select ';
            for n in 1 .. nLastMonth loop
              cSqlTmp11 := cSqlTmp11 || aColumnListResult(n) || ',';
            end loop;
            for m in nLastMonth + 1 .. 12 loop
              cSqlTmp11 := cSqlTmp11 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            cSqlTmp11 := cSqlTmp11 || ' YY,' || vNodeColumnName ||
                         '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                         ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                         vNodeLevel || '.' || vNodeColumnName || '=' ||
                         vTmpTableName || '.id';
            cSqlTmp11 := cSqlTmp11 || '  AND  TSID = ' ||
                         to_char(aTsidListResult(i).nTsidRight);

            -- add null data left
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- add null data right
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidRight),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- judge nFirst < nLastYear
            if nFirstYear < nLastYear then
              cSqlTmp2  := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear) || '  AND  ' ||
                           to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2  := cSqlTmp2 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear) || '  AND  ' ||
                           to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp22 := cSqlTmp22 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidRight);
            end if;
          elsif nFirstMonth != 1 AND nLastMonth != 12 then
            --if 1 = j then
            nMark     := 44;
            cSqlTmp1  := ' ';
            cSqlTmp2  := ' ';
            cSqlTmp3  := ' ';
            cSqlTmp11 := ' ';
            cSqlTmp22 := ' ';
            cSqlTmp33 := ' ';
            -- nFirstYear = nLastYear
            if nFirstYear = nLastYear then
              nNumTmp := nLastMonth;
            else
              nNumTmp := 12;
            end if;
            -- nFirst left
            cSqlTmp1 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. nNumTmp loop
              cSqlTmp1 := cSqlTmp1 || aColumnListResult(n) || ',';
            end loop;
            if nNumTmp <> 12 then
              for w in nNumTmp + 1 .. 12 loop
                cSqlTmp1 := cSqlTmp1 || ' null as ' || aColumnListResult(w) || ',';
              end loop;
            end if;
            cSqlTmp1 := cSqlTmp1 || ' YY,' || vNodeColumnName ||
                        '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                        ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                        vNodeLevel || '.' || vNodeColumnName || '=' ||
                        vTmpTableName || '.id';
            cSqlTmp1 := cSqlTmp1 || '  AND  TSID = ' ||
                        to_char(aTsidListResult(i).nTsidLeft);
            -- nFirst right
            --if nFirstYear <> nLastYear then
            cSqlTmp11 := 'select ';
            for m in 1 .. nFirstMonth - 1 loop
              cSqlTmp11 := cSqlTmp11 || ' null as ' || aColumnListResult(m) || ',';
            end loop;
            for n in nFirstMonth .. 12 loop
              cSqlTmp11 := cSqlTmp11 || aColumnListResult(n) || ',';
            end loop;
            cSqlTmp11 := cSqlTmp11 || ' YY,' || vNodeColumnName ||
                         '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                         ' where  YY = ' || to_char(nFirstYear) || ' AND ' ||
                         vNodeLevel || '.' || vNodeColumnName || '=' ||
                         vTmpTableName || '.id';
            cSqlTmp11 := cSqlTmp11 || '  AND  TSID = ' ||
                         to_char(aTsidListResult(i).nTsidRight);
            --end if;
            -- add null data left
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidLeft),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- add null data right
            cSqlNullData := '';
            FMSP_GetNullData(pIn_nFirstYear        => nFirstYear,
                             pIn_nLastYear         => nLastYear,
                             pIn_vTSID             => to_char(aTsidListResult(i)
                                                              .nTsidRight),
                             pIn_vTmpTableNodeList => vTmpTableName,
                             pIn_vTableNodeLevel   => vNodeLevel,
                             pIn_vColumnNodeLevel  => vNodeColumnName,
                             pOut_cSql             => cSqlNullData);
            if length(cSqlNullData) > 5 then
              cSqlTmp11 := cSqlTmp11 || '  UNION ALL ' || cSqlNullData;
            end if;
            -- nLast left
            if nFirstYear <> nLastYear then
              cSqlTmp3 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp3 := cSqlTmp3 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. nNumTmp loop
                cSqlTmp3 := cSqlTmp3 || ' null AS ' || aColumnListResult(m) || ',';
              end loop;
              if nNumTmp <> 12 then
                for w in nNumTmp + 1 .. 12 loop
                  cSqlTmp3 := cSqlTmp3 || ' null as ' ||
                              aColumnListResult(w) || ',';
                end loop;
              end if;
              cSqlTmp3 := cSqlTmp3 || ' YY,' || vNodeColumnName ||
                          '  FROM   ' || vNodeLevel || ',' || vTmpTableName ||
                          ' where  YY = ' || to_char(nLastYear) || ' AND ' ||
                          vNodeLevel || '.' || vNodeColumnName || '=' ||
                          vTmpTableName || '.id';
              cSqlTmp3 := cSqlTmp3 || '  AND  TSID = ' ||
                          to_char(aTsidListResult(i).nTsidLeft);
            end if;
            -- nLast right
            if nFirstYear <> nLastYear then
              cSqlTmp33 := 'select ';
              for n in 1 .. nLastMonth loop
                cSqlTmp33 := cSqlTmp33 || aColumnListResult(n) || ',';
              end loop;
              for m in nLastMonth + 1 .. 12 loop
                cSqlTmp33 := cSqlTmp33 || ' null AS ' ||
                             aColumnListResult(m) || ',';
              end loop;
              cSqlTmp33 := cSqlTmp33 || ' YY,' || vNodeColumnName ||
                           '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || ' where  YY = ' ||
                           to_char(nLastYear) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp33 := cSqlTmp33 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidRight);
            end if;
            -- judge nFirst+1 < nLastYear-1
            if nFirstYear + 1 <= nLastYear - 1 then
              cSqlTmp2  := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear + 1) || '  AND  ' ||
                           to_char(nLastYear - 1) || ' AND ' || vNodeLevel || '.' ||
                           vNodeColumnName || '=' || vTmpTableName || '.id';
              cSqlTmp2  := cSqlTmp2 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidLeft);
              cSqlTmp22 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                           vNodeColumnName || '  FROM   ' || vNodeLevel || ',' ||
                           vTmpTableName || '  where  YY between ' ||
                           to_char(nFirstYear + 1) || '  AND  ' ||
                           to_char(nLastYear - 1);
              cSqlTmp22 := cSqlTmp22 || '  AND  TSID = ' ||
                           to_char(aTsidListResult(i).nTsidRight);
            end if;
          end if;
        end if;
        -- end loop;
        -- no right no nLast no nFirst
        if 1 = nMark then
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp1,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp1);
          if 1 = i then
            cSqlFirst := cSqlTmp1;
          elsif 2 = i then
            cSqlSecond := cSqlTmp1;
          elsif 3 = i then
            cSqlThird := cSqlTmp1;
          elsif 4 = i then
            cSqlFourth := cSqlTmp1;
          elsif 5 = i then
            cSqlFifth := cSqlTmp1;
          elsif 6 = i then
            cSqlSixth := cSqlTmp1;
          end if;
          --debug
          ---- (csqlTmp1);
          --commit;
          --debug
          -- no right nFirst no nLast
        elsif 2 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          --debug
          --  --insert into clobtest(sqlcontent) values(csqlTmp3);
          --  commit;
          --debug
          -- i for TSID
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          if 1 = i then
            cSqlFirst := cSqlTmp3;
          elsif 2 = i then
            cSqlSecond := cSqlTmp3;
          elsif 3 = i then
            cSqlThird := cSqlTmp3;
          elsif 4 = i then
            cSqlFourth := cSqlTmp3;
          elsif 5 = i then
            cSqlFifth := cSqlTmp3;
          elsif 6 = i then
            cSqlSixth := cSqlTmp3;
          end if;
          -- no right nLast no nFirst
        elsif 3 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          -- i for TSID
          if 1 = i then
            cSqlFirst := cSqlTmp3;
          elsif 2 = i then
            cSqlSecond := cSqlTmp3;
          elsif 3 = i then
            cSqlThird := cSqlTmp3;
          elsif 4 = i then
            cSqlFourth := cSqlTmp3;
          elsif 5 = i then
            cSqlFifth := cSqlTmp3;
          elsif 6 = i then
            cSqlSixth := cSqlTmp3;
          end if;
          -- no right nLast nFirst
        elsif 4 = nMark then
          if length(cSqlTmp2) > 5 and length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || '  UNION ALL ' ||
                        cSqlTmp3 || ' )';
          elsif length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp3 || ')';
          else
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ')';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp4,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp4);
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right no nLast no nFirst
        elsif 11 = nMark then
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp1,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp1);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp11,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp11);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp1 || ') a left join (' ||
                      cSqlTmp11 || ') b on a.YY=b.YY  AND  ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right no nLast nFirst
        elsif 22 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ')';
          end if;
          if length(cSqlTmp22) > 5 then
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || ' )';
          else
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp33,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp33);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp3 || ') a left join (' ||
                      cSqlTmp33 || ') b on a.YY=b.YY  AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          --debug
          ---- (cSqlTmp4);
          --commit;
          --debug
          -- right nLast no nFirst
        elsif 33 = nMark then
          if length(cSqlTmp2) > 5 then
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || ' )';
          else
            cSqlTmp3 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          if length(cSqlTmp22) > 5 then
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || ' )';
          else
            cSqlTmp33 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp3,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp3);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp33,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp33);
          cSqlTmp4 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp3 || ') a left join (' ||
                      cSqlTmp33 || ') b on a.YY=b.YY AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp4;
          elsif 2 = i then
            cSqlSecond := cSqlTmp4;
          elsif 3 = i then
            cSqlThird := cSqlTmp4;
          elsif 4 = i then
            cSqlFourth := cSqlTmp4;
          elsif 5 = i then
            cSqlFifth := cSqlTmp4;
          elsif 6 = i then
            cSqlSixth := cSqlTmp4;
          end if;
          -- right nLast nFirst
        elsif 44 = nMark then
          if length(cSqlTmp2) > 5 and length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL  ' || cSqlTmp2 || '  UNION ALL ' ||
                        cSqlTmp3 || ' )';
          elsif length(cSqlTmp3) > 5 then
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 ||
                        '  UNION ALL ' || cSqlTmp3 || ' )';
          else
            cSqlTmp4 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                        vNodeColumnName || '  FROM   (' || cSqlTmp1 || ' )';
          end if;
          if length(cSqlTmp22) > 5 and length(cSqlTmp33) > 5 then
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp22 || '  UNION ALL ' ||
                         cSqlTmp33 || ' )';
          elsif length(cSqlTmp33) > 5 then
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 ||
                         '  UNION ALL  ' || cSqlTmp33 || ' )';
          else
            cSqlTmp44 := 'select t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp11 || ' )';
          end if;
          --debug
          --  -- (cSqlTmp4);
          --  commit;
          --debug
          --debug
          --  -- (cSqlTmp44);
          --  commit;
          --debug
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp4,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidLeft,
                                pIn_nType        => MARK_LEFT,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp4);
          FMSP_GetSecondTSIDSql(pIn_cSql         => cSqlTmp44,
                                pIn_nIndex       => i,
                                pIn_nTSID        => aTsidListResult(i)
                                                    .nTsidRight,
                                pIn_nType        => MARK_Right,
                                pIn_vColumnsName => vNodeColumnName,
                                pOut_cSql        => cSqlTmp44);
          cSqlTmp5 := 'select a.t1*b.t1 as T1,a.t2*b.t2 as T2,a.t3*b.t3 as T3,a.t4*b.t4 as T4,a.t5*b.t5 as T5,' ||
                      'a.t6*b.t6 as T6,a.t7*b.t7 T7,a.t8*b.t8 T8,a.t9*b.t9 T9,a.t10*b.t10 T10,a.t11*b.t11 T11,' ||
                      'a.t12*b.t12 T12,a.YY,a.' || vNodeColumnName ||
                      '  FROM   (' || cSqlTmp4 || ') a left join (' ||
                      cSqlTmp44 || ') b on a.YY=b.YY AND ' || 'a.' ||
                      vNodeColumnName || '=' || 'b.' || vNodeColumnName;
          -- i for NODELIST
          if 1 = i then
            cSqlFirst := cSqlTmp5;
          elsif 2 = i then
            cSqlSecond := cSqlTmp5;
          elsif 3 = i then
            cSqlThird := cSqlTmp5;
          elsif 4 = i then
            cSqlFourth := cSqlTmp5;
          elsif 5 = i then
            cSqlFifth := cSqlTmp5;
          elsif 6 = i then
            cSqlSixth := cSqlTmp5;
          end if;

        end if;
        if 1 = i then
          cSqlFirst := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',1 AS orderID FROM (' ||
                       cSqlFirst || ')';
        elsif 2 = i then
          cSqlSecond := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                        vNodeColumnName || ',2 AS orderID FROM (' ||
                        cSqlSecond || ')';
        elsif 3 = i then
          cSqlThird := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',3 AS orderID FROM (' ||
                       cSqlThird || ')';
        elsif 4 = i then
          cSqlFourth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                        vNodeColumnName || ',4 AS orderID FROM (' ||
                        cSqlFourth || ')';
        elsif 5 = i then
          cSqlFifth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',5 AS orderID FROM (' ||
                       cSqlFifth || ')';
          /*        elsif 6 = i then
          cSqlSixth := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || ',6 AS orderID FROM (' ||
                       cSqlSixth || ')';*/
        end if;
      end loop;

      -- first value
      if pIn_nCalculationType = 2 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := ' ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')';
            -- the top five lines firstValue
            -- change HERE
            if length(cSqlSixth) > 5 then
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);

              cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              -- change HERE
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := ' ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')';
            -- the top five lines firstValue
            -- change HERE
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || ' ,orderid FROM   (' ||
                         cSqlTmp55 || ')  ';

            if length(cSqlSixth) > 5 then
              -- the sixth line add
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else

              -- change HERE
              FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth line
          cSqlTmp55 := ' ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ')';
          -- the top five lines firstValue
          -- change HERE
          -- the sixth line add
          if length(cSqlSixth) > 5 then
            FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            FMP_GetTmpTableSQL(cSqlTmp55, vNodeColumnName, cSqlTmp55);
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM  (' || cSqlTmp55 || ')';
          end if;
        end if;
        -- max value
      elsif pIn_nCalculationType = 1 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                         'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                         'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                         'MAX(T9) T9,' || 'MAX(T10) T10,' ||
                         'MAX(T11) T11,' || 'MAX(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ') group by YY,' || vNodeColumnName;
            -- the top five lines firstValue
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                         'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                         'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                         'MAX(T9) T9,' || 'MAX(T10) T10,' ||
                         'MAX(T11) T11,' || 'MAX(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            -- the top five lines firstValue
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth line
          cSqlTmp55 := 'select MAX(T1) T1,' || 'MAX(T2) T2,' ||
                       'MAX(T3) T3,' || 'MAX(T4) T4,' || 'MAX(T5) T5,' ||
                       'MAX(T6) T6,' || 'MAX(T7) T7,' || 'MAX(T8) T8,' ||
                       'MAX(T9) T9,' || 'MAX(T10) T10,' || 'MAX(T11) T11,' ||
                       'MAX(T12) T12,' || 'YY,' || vNodeColumnName ||
                       ' FROM  ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ') group by YY,' || vNodeColumnName;
          cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                       vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
          -- the top five lines firstValue
          if length(cSqlSixth) > 5 then
            -- the sixth line add
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
          end if;
        end if;
        -- min value
      elsif pIn_nCalculationType = 3 then
        cSqlTmp55 := ' ';
        if length(cSqlFifth) > 5 then
          -- line 5 no add
          if pIn_nIsPlusTimeSeries5 = 0 then
            cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                         'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                         'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                         'MIN(T9) T9,' || 'MIN(T10) T10,' ||
                         'MIN(T11) T11,' || 'MIN(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFifth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFifth;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            -- the top five lines firstValue
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID , sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           ' UNION ALL ' || cSqlSixth || ') group by YY,' ||
                           vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            end if;
            -- line 5 add
          else
            cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                         'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                         'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                         'MIN(T9) T9,' || 'MIN(T10) T10,' ||
                         'MIN(T11) T11,' || 'MIN(T12) T12,' || 'YY,' ||
                         vNodeColumnName || ' FROM  ';
            bTmp      := false;
            if length(cSqlFirst) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
              end if;
            end if;
            if length(cSqlSecond) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
              end if;
            end if;
            if length(cSqlThird) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
              end if;
            end if;
            if length(cSqlFourth) > 5 then
              if bTmp = false then
                cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
                bTmp      := true;
              else
                cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
              end if;
            end if;
            cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ')';
            -- the top five lines firstValue
            if length(cSqlSixth) > 5 then
              -- the sixth line add
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || '  UNION ALL  ' ||
                           cSqlSixth || ') group by YY,' || vNodeColumnName;
            else
              cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                           ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                           'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                           vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                           '  UNION ALL  ' || cSqlFifth || ') group by YY,' ||
                           vNodeColumnName;
            end if;
          end if;
        else
          -- no fifth
          cSqlTmp55 := 'select MIN(T1) T1,' || 'MIN(T2) T2,' ||
                       'MIN(T3) T3,' || 'MIN(T4) T4,' || 'MIN(T5) T5,' ||
                       'MIN(T6) T6,' || 'MIN(T7) T7,' || 'MIN(T8) T8,' ||
                       'MIN(T9) T9,' || 'MIN(T10) T10,' || 'MIN(T11) T11,' ||
                       'MIN(T12) T12,' || 'YY,' || vNodeColumnName ||
                       ' FROM  ';
          bTmp      := false;
          if length(cSqlFirst) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFirst;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFirst;
            end if;
          end if;
          if length(cSqlSecond) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlSecond;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlSecond;
            end if;
          end if;
          if length(cSqlThird) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlThird;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlThird;
            end if;
          end if;
          if length(cSqlFourth) > 5 then
            if bTmp = false then
              cSqlTmp55 := cSqlTmp55 || '(' || cSqlFourth;
              bTmp      := true;
            else
              cSqlTmp55 := cSqlTmp55 || '  UNION ALL  ' || cSqlFourth;
            end if;
          end if;
          cSqlTmp55 := cSqlTmp55 || ')group by YY,' || vNodeColumnName;
          -- the top five lines firstValue
          if length(cSqlSixth) > 5 then
            cSqlTmp55 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
            -- the sixth line add
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID, sum(T1) T1,sum(T2) T2,sum(T3) T3,sum(T4) T4,sum(T5) T5,sum(T6) T6,sum(T7) T7,sum(T8) T8,' ||
                         'sum(T9) T9,sum(T10) T10,sum(T11) T11,sum(T12) T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 ||
                         '  UNION ALL  ' || cSqlSixth || ') group by YY,' ||
                         vNodeColumnName;
          else
            cSqlTmp55 := 'select ' || to_char(pIn_nValidateTimeSeriesId) ||
                         ' AS TSID,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,YY,' ||
                         vNodeColumnName || '  FROM   (' || cSqlTmp55 || ') ';
          end if;
        end if;
      end if;
      -- debug
      --Fmp_Log.LOGERROR(cSqlTmp55);
      -- debug
      -- update or insert
      if nFirstMonth = 1 AND nLastMonth = 12 then
        cSqlTmp55 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                     cSqlTmp55 ||
                     ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                     vNodeColumnName || '=' || 'T.' || vNodeColumnName ||
                     ') WHEN MATCHED THEN  UPDATE ' ||
                     'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                     '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                     '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                     '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                     '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                     '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                     'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                     '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                     '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                     '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                     '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                     '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                     'WHEN NOT MATCHED THEN ' || 'INSERT ' || ' VALUES(' || vSeq ||
                     '.nextval,T.' || vNodeColumnName ||
                     ',T.TSID,0,T.YY,round(T.T1,' ||
                     to_char(pIn_nPrecision) || '),round(T.T2,' ||
                     to_char(pIn_nPrecision) || '),round(T.T3,' ||
                     to_char(pIn_nPrecision) || '),round(T.T4,' ||
                     to_char(pIn_nPrecision) || '),round(T.T5,' ||
                     to_char(pIn_nPrecision) || '),round(T.T6,' ||
                     to_char(pIn_nPrecision) || '),round(T.T7,' ||
                     to_char(pIn_nPrecision) || '),round(T.T8,' ||
                     to_char(pIn_nPrecision) || '),round(T.T9,' ||
                     to_char(pIn_nPrecision) || '),round(T.T10,' ||
                     to_char(pIn_nPrecision) || '),round(T.T11,' ||
                     to_char(pIn_nPrecision) || '),round(T.T12,' ||
                     to_char(pIn_nPrecision) || ')' || ')';
        FMSP_ExecSql(cSqlTmp55);
        --FMP_log.LOGERROR(cSqlTmp55);
        -- nFirstMonth no nLastMonth
      elsif nFirstMonth <> 1 AND nLastMonth = 12 then
        cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                    vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                    ' )where  YY = ' || to_char(nFirstYear);
        cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' || cSqlTmp5 ||
                    ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                    vNodeColumnName || '= T.' || vNodeColumnName ||
                    ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT ';
        cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                    vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                    to_char(pIn_nPrecision) || '),round(T.T2,' ||
                    to_char(pIn_nPrecision) || '),round(T.T3,' ||
                    to_char(pIn_nPrecision) || '),round(T.T4,' ||
                    to_char(pIn_nPrecision) || '),round(T.T5,' ||
                    to_char(pIn_nPrecision) || '),round(T.T6,' ||
                    to_char(pIn_nPrecision) || '),round(T.T7,' ||
                    to_char(pIn_nPrecision) || '),round(T.T8,' ||
                    to_char(pIn_nPrecision) || '),round(T.T9,' ||
                    to_char(pIn_nPrecision) || '),round(T.T10,' ||
                    to_char(pIn_nPrecision) || '),round(T.T11,' ||
                    to_char(pIn_nPrecision) || '),round(T.T12,' ||
                    to_char(pIn_nPrecision) || ')' || ')';
        for m in nFirstMonth .. 12 loop
          if m != nFirstMonth then
            cSqlTmp1 := cSqlTmp1 || ',';
          end if;
          cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                      ' = round(T.' || aColumnListResult(m) || ',' ||
                      to_char(pIn_nPrecision) || ')';
        end loop;
        cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
        FMSP_ExecSql(cSqlTmp3);
        --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp3);
        -- normal
        if nFirstYear < nLastYear then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY between ' || to_char(nFirstYear + 1) ||
                      '  AND  ' || to_char(nLastYear);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ' ) WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      'WHEN NOT MATCHED THEN  ' || 'INSERT ' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp4);
        end if;
        -- no nFirstMonth nLastMonth
      elsif nFirstMonth = 1 AND nLastMonth <> 12 then
        -- nLast
        cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                    vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                    ')  where  YY = ' || to_char(nLastYear);
        cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' || cSqlTmp5 ||
                    ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                    vNodeColumnName || '= T.' || vNodeColumnName ||
                    ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT';
        cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                    vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                    to_char(pIn_nPrecision) || '),round(T.T2,' ||
                    to_char(pIn_nPrecision) || '),round(T.T3,' ||
                    to_char(pIn_nPrecision) || '),round(T.T4,' ||
                    to_char(pIn_nPrecision) || '),round(T.T5,' ||
                    to_char(pIn_nPrecision) || '),round(T.T6,' ||
                    to_char(pIn_nPrecision) || '),round(T.T7,' ||
                    to_char(pIn_nPrecision) || '),round(T.T8,' ||
                    to_char(pIn_nPrecision) || '),round(T.T9,' ||
                    to_char(pIn_nPrecision) || '),round(T.T10,' ||
                    to_char(pIn_nPrecision) || '),round(T.T11,' ||
                    to_char(pIn_nPrecision) || '),round(T.T12,' ||
                    to_char(pIn_nPrecision) || ')' || ')';
        for m in 1 .. nLastMonth loop
          if m <> nFirstMonth then
            cSqlTmp1 := cSqlTmp1 || ',';
          end if;
          cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                      ' = round(T.' || aColumnListResult(m) || ',' ||
                      to_char(pIn_nPrecision) || ')';
        end loop;
        cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
        FMSP_ExecSql(cSqlTmp3);
        --FMP_log.LOGERROR(cSqlTmp3);
        -- normal
        if nFirstYear < nLastYear then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ')  where  YY between ' || to_char(nFirstYear) ||
                      '  AND  ' || to_char(nLastYear - 1);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      'WHEN NOT MATCHED THEN ' || 'INSERT' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --FMP_log.LOGERROR(cSqlTmp4);
        end if;
      elsif nFirstMonth <> 1 AND nLastMonth <> 12 then
        if nFirstYear <> nLastYear then
          -- nLast
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY = ' || to_char(nLastYear);
          cSqlTmp1 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE set ';
          cSqlTmp2 := ' WHEN NOT MATCHED THEN  INSERT';
          cSqlTmp2 := cSqlTmp2 || ' VALUES(' || vSeq || '.nextval,T.' ||
                      vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          for m in 1 .. nLastMonth loop
            if m != 1 then
              cSqlTmp1 := cSqlTmp1 || ',';
            end if;
            cSqlTmp1 := cSqlTmp1 || 'V.' || aColumnListResult(m) ||
                        ' = round(T.' || aColumnListResult(m) || ',' ||
                        to_char(to_char(pIn_nPrecision)) || ')';
          end loop;
          cSqlTmp3 := cSqlTmp1 || cSqlTmp2;
          FMSP_ExecSql(cSqlTmp3);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp3);
        end if;

        if nFirstYear = nLastYear then
          nNumTmp := nLastMonth;
        else
          nNumTmp := 12;
        end if;

        --nFirst
        cSqlTmp5  := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                     vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                     ')  where  YY = ' || to_char(nFirstYear);
        cSqlTmp11 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                     cSqlTmp5 ||
                     ') T on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                     vNodeColumnName || '=T.' || vNodeColumnName ||
                     ') WHEN MATCHED THEN  UPDATE set ';
        cSqlTmp22 := ' WHEN NOT MATCHED THEN  INSERT ';
        cSqlTmp22 := cSqlTmp22 || ' VALUES(' || vSeq || '.nextval,T.' ||
                     vNodeColumnName || ',T.TSID,0,T.YY,round(T.T1,' ||
                     to_char(pIn_nPrecision) || '),round(T.T2,' ||
                     to_char(pIn_nPrecision) || '),round(T.T3,' ||
                     to_char(pIn_nPrecision) || '),round(T.T4,' ||
                     to_char(pIn_nPrecision) || '),round(T.T5,' ||
                     to_char(pIn_nPrecision) || '),round(T.T6,' ||
                     to_char(pIn_nPrecision) || '),round(T.T7,' ||
                     to_char(pIn_nPrecision) || '),round(T.T8,' ||
                     to_char(pIn_nPrecision) || '),round(T.T9,' ||
                     to_char(pIn_nPrecision) || '),round(T.T10,' ||
                     to_char(pIn_nPrecision) || '),round(T.T11,' ||
                     to_char(pIn_nPrecision) || '),round(T.T12,' ||
                     to_char(pIn_nPrecision) || ')' || ')';
        for m in nFirstMonth .. nNumTmp loop
          if m != nFirstMonth then
            cSqlTmp11 := cSqlTmp11 || ',';
          end if;
          cSqlTmp11 := cSqlTmp11 || 'V.' || aColumnListResult(m) ||
                       ' = round(T.' || aColumnListResult(m) || ',' ||
                       to_char(to_char(pIn_nPrecision)) || ')';
        end loop;
        cSqlTmp33 := cSqlTmp11 || cSqlTmp22;
        FMSP_ExecSql(cSqlTmp33);
        --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp33);
        -- normal
        if nFirstYear + 1 <= nLastYear - 1 then
          cSqlTmp5 := 'select T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,TSID,YY,' ||
                      vNodeColumnName || '  FROM  (' || cSqlTmp55 ||
                      ') where  YY between ' || to_char(nFirstYear + 1) ||
                      '  AND  ' || to_char(nLastYear - 1);
          cSqlTmp4 := 'MERGE INTO ' || vNodeLevel || ' V USING (' ||
                      cSqlTmp5 ||
                      ')  T  on (V.TSID=T.TSID  AND  V.YY=T.YY  AND  V.' ||
                      vNodeColumnName || '= T.' || vNodeColumnName ||
                      ') WHEN MATCHED THEN  UPDATE ' ||
                      'set V.T1=round(T.T1,' || to_char(pIn_nPrecision) ||
                      '),V.T2=round(T.T2,' || to_char(pIn_nPrecision) ||
                      '), V.T3=round(T.T3,' || to_char(pIn_nPrecision) ||
                      '),V.T4=round(T.T4,' || to_char(pIn_nPrecision) ||
                      '), V.T5=round(T.T5,' || to_char(pIn_nPrecision) ||
                      '),V.T6=round(T.T6,' || to_char(pIn_nPrecision) || '),' ||
                      'V.T7=round(T.T7,' || to_char(pIn_nPrecision) ||
                      '),V.T8=round(T.T8,' || to_char(pIn_nPrecision) ||
                      '),V.T9=round(T.T9,' || to_char(pIn_nPrecision) ||
                      '),V.T10=round(T.T10,' || to_char(pIn_nPrecision) ||
                      '),V.T11=round(T.T11,' || to_char(pIn_nPrecision) ||
                      '),V.T12=round(T.T12 ,' || to_char(pIn_nPrecision) || ')' ||
                      ' WHEN NOT MATCHED THEN ' || 'INSERT ' || ' VALUES(' || vSeq ||
                      '.nextval,T.' || vNodeColumnName ||
                      ',T.TSID,0,T.YY,round(T.T1,' ||
                      to_char(pIn_nPrecision) || '),round(T.T2,' ||
                      to_char(pIn_nPrecision) || '),round(T.T3,' ||
                      to_char(pIn_nPrecision) || '),round(T.T4,' ||
                      to_char(pIn_nPrecision) || '),round(T.T5,' ||
                      to_char(pIn_nPrecision) || '),round(T.T6,' ||
                      to_char(pIn_nPrecision) || '),round(T.T7,' ||
                      to_char(pIn_nPrecision) || '),round(T.T8,' ||
                      to_char(pIn_nPrecision) || '),round(T.T9,' ||
                      to_char(pIn_nPrecision) || '),round(T.T10,' ||
                      to_char(pIn_nPrecision) || '),round(T.T11,' ||
                      to_char(pIn_nPrecision) || '),round(T.T12,' ||
                      to_char(pIn_nPrecision) || ')' || ')';
          FMSP_ExecSql(cSqlTmp4);
          --Fmp_Log.LOGERROR(pIn_cSqlText => cSqlTmp4);
        end if;
      end if;
    END;
  END FMSP_validateFCSTM;

  procedure FMSP_validateFCSTW(pIn_nSelectionID          in number,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vConditions           in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number,
                               pIn_nMultiplex            in number) AS
    --*****************************************************************
    -- Description:  it support for week validateFCST
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYWW weekly example 201221

    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- pIn_nMultiplex  mark multiplex
    -- 0 means no multiplex
    -- 1 means multiplex in nodelist
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_validateFCSTW;

  procedure FMSP_validateFCSTD(pIn_nSelectionID          in number,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vConditions           in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number,
                               pIn_nMultiplex            in number) AS
    --*****************************************************************
    -- Description: it support for  day validateFCST
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- pIn_nMultiplex  mark multiplex
    -- 0 means no multiplex
    -- 1 means multiplex in nodelist
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_validateFCSTD;
  procedure FMSP_validateFCSTM(pIn_cNodeList             in clob,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number) AS
    --*****************************************************************
    -- Description: it support for month validateFCST
    --
    -- Parameters:
    -- pIn_cNodeList ---- nodeList
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- pIn_nMultiplex  mark multiplex
    -- 0 means no multiplex
    -- 1 means multiplex in nodelist
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    declare
      --vTmpTableName varchar2(40) := 'TB_TS_AggregateNodeCon';
      fNodeList fmt_nest_tab_nodeid;
      nSqlCode  number;
    BEGIN
      FMSP_ClobToNestedTable(pIn_cClob     => pIn_cNodeList,
                             pOut_tNestTab => fNodeList,
                             pOut_nSqlCode => nSqlCode);
      if nSqlCode <> 0 then
        return;
      end if;
      if pIn_nNodeLevel = 1 then
        delete from TB_TS_DetailNodeSelCdt;
        commit;
        insert into TB_TS_DetailNodeSelCdt
          select * from table(fNodeList);
        commit;
      else
        delete from TB_TS_AggregateNodeCon;
        commit;
        insert into TB_TS_AggregateNodeCon
          select * from table(fNodeList);
        commit;
      end if;
      FMSP_validateFCSTM(pIn_nSelectionID          => 1,
                         pIn_nCalculationType      => pIn_nCalculationType,
                         pIn_vFirstPeriodTime      => pIn_vFirstPeriodTime,
                         pIn_vLastPeriodTime       => pIn_vLastPeriodTime,
                         pIn_vConditions           => '0',
                         pIn_vSourceTimeSeriesIDs  => pIn_vSourceTimeSeriesIDs,
                         pIn_nValidateTimeSeriesId => pIn_nValidateTimeSeriesId,
                         pIn_nIsPlusTimeSeries5    => pIn_nIsPlusTimeSeries5,
                         pIn_nNodeLevel            => pIn_nNodeLevel,
                         pIn_nPrecision            => pIn_nPrecision,
                         pIn_nMultiplex            => 1);
    END;
  END FMSP_validateFCSTM;
  procedure FMSP_validateFCSTW(pIn_cNodeList             in clob,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number) AS
    --*****************************************************************
    -- Description:  it support for week validateFCST
    --
    -- Parameters:
    -- pIn_cNodeList ---- nodeList
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYWW weekly example 201221

    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime

    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_validateFCSTW;
  procedure FMSP_validateFCSTD(pIn_cNodeList             in clob,
                               pIn_nCalculationType      in number,
                               pIn_vFirstPeriodTime      in varchar2,
                               pIn_vLastPeriodTime       in varchar2,
                               pIn_vSourceTimeSeriesIDs  in varchar2,
                               pIn_nValidateTimeSeriesId in number,
                               pIn_nIsPlusTimeSeries5    in number,
                               pIn_nNodeLevel            in number,
                               pIn_nPrecision            in number) AS
    --*****************************************************************
    -- Description: it support for  day validateFCST
    --
    -- Parameters:
    -- pIn_cNodeList ---- nodeList
    -- pIn_nCalculationType ---- operation Type -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime

    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ---- -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_validateFCSTD;
  procedure FMSP_validateFCST(pIn_nSelectionID          in number,
                              pIn_nCalculationType      in number,
                              pIn_vFirstPeriodTime      in varchar2,
                              pIn_vLastPeriodTime       in varchar2,
                              pIn_vConditions           in varchar2,
                              pIn_vSourceTimeSeriesIDs  in varchar2,
                              pIn_nValidateTimeSeriesId in number,
                              pIn_nIsPlusTimeSeries5    in number,
                              pIn_nNodeLevel            in number,
                              pIn_nChronology           in number,
                              pIn_nPrecision            in number,
                              pOut_nSqlCode             out number) AS
    --*****************************************************************
    -- Description: this  is the interface of  validateFCST. it support for month validateFCST  AND  week validateFCST  AND  day validateFCST
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nCalculationType ---- operation Type
    -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ----
    -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- pIn_nChronology ---------- it mark month or week or day
    --- 1 means month
    --- 2 means week
    --- 4 means day
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    begin
      FMP_LOG.FMP_SETVALUE(pIn_nSelectionID);
      FMP_LOG.FMP_SETVALUE(pIn_nCalculationType);
      FMP_LOG.FMP_SETVALUE(pIn_vFirstPeriodTime);
      FMP_LOG.FMP_SETVALUE(pIn_vLastPeriodTime);
      FMP_LOG.FMP_SETVALUE(pIn_vConditions);
      FMP_LOG.FMP_SETVALUE(pIn_vSourceTimeSeriesIDs);
      FMP_LOG.FMP_SETVALUE(pIn_nValidateTimeSeriesId);
      FMP_LOG.FMP_SETVALUE(pIn_nIsPlusTimeSeries5);
      FMP_LOG.FMP_SETVALUE(pIn_nNodeLevel);
      FMP_LOG.FMP_SETVALUE(pIn_nChronology);
      FMP_LOG.FMP_SETVALUE(to_char(pIn_nPrecision));
      FMP_log.logBegin;
      pOut_nSqlCode := 0;

      if pIn_nChronology = 1 then
        FMSP_validateFCSTM(pIn_nSelectionID,
                           pIn_nCalculationType,
                           pIn_vFirstPeriodTime,
                           pIn_vLastPeriodTime,
                           pIn_vConditions,
                           pIn_vSourceTimeSeriesIDs,
                           pIn_nValidateTimeSeriesId,
                           pIn_nIsPlusTimeSeries5,
                           pIn_nNodeLevel,
                           to_char(pIn_nPrecision),
                           pIn_nMultiplex => 0);
      elsif pIn_nChronology = 2 then
        FMSP_validateFCSTW(pIn_nSelectionID,
                           pIn_nCalculationType,
                           pIn_vFirstPeriodTime,
                           pIn_vLastPeriodTime,
                           pIn_vConditions,
                           pIn_vSourceTimeSeriesIDs,
                           pIn_nValidateTimeSeriesId,
                           pIn_nIsPlusTimeSeries5,
                           pIn_nNodeLevel,
                           to_char(pIn_nPrecision),
                           pIn_nMultiplex => 0);
      elsif pIn_nChronology = 4 then
        FMSP_validateFCSTD(pIn_nSelectionID,
                           pIn_nCalculationType,
                           pIn_vFirstPeriodTime,
                           pIn_vLastPeriodTime,
                           pIn_vConditions,
                           pIn_vSourceTimeSeriesIDs,
                           pIn_nValidateTimeSeriesId,
                           pIn_nIsPlusTimeSeries5,
                           pIn_nNodeLevel,
                           to_char(pIn_nPrecision),
                           pIn_nMultiplex => 0);
      end if;
      FMP_LOG.LOGEND;
    end;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  END FMSP_validateFCST;

  procedure FMSP_validateFCSTByNodeList(pIn_cNodeList             in clob,
                                        pIn_nCalculationType      in number,
                                        pIn_vFirstPeriodTime      in varchar2,
                                        pIn_vLastPeriodTime       in varchar2,
                                        pIn_vSourceTimeSeriesIDs  in varchar2,
                                        pIn_nValidateTimeSeriesId in number,
                                        pIn_nIsPlusTimeSeries5    in number,
                                        pIn_nNodeLevel            in number,
                                        pIn_nChronology           in number,
                                        pIn_nPrecision            in number,
                                        pOut_nSqlCode             out number) AS
    --*****************************************************************
    -- Description: this  is the interface of  validateFCST. it support for month validateFCST  AND  week validateFCST  AND  day validateFCST
    --
    -- Parameters:
    -- pIn_cNodeList ---- node list
    -- pIn_nCalculationType ---- operation Type
    -- 1- the largest value of
    -- 2- the first significant value of
    -- 3- the smallest value of
    -- pIn_vFirstPeriodTime ---- the begin of operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vLastPeriodTime ---- the end of operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime

    -- pIn_vSourceTimeSeriesIDs ---- Time series IDs which is involved in validation.
    -- the format of this parameter like this
    -- (TimeSeriesBasicID1, TimeSeriesCoeffID1;
    --  TimeSeriesBasicID2, TimeSeriesCoeffID2;
    --  TimeSeriesBasicID3, TimeSeriesCoeffID3;
    --  TimeSeriesBasicID4, TimeSeriesCoeffID4;
    --  TimeSeriesBasicID5, TimeSeriesCoeffID5;
    --  TimeSeriesBasicID6, TimeSeriesCoeffID6;)
    --  NOTE THIS: -1 means there is no time series to be specified.
    -- pIn_nValidateTimeSeriesId ---- Time series ID which stored the validation result.
    -- pIn_nIsPlusTimeSeries5 ----
    -- 0 - No
    -- 1 - Yes
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- pIn_nChronology ---------- it mark month or week or day
    --- 1 means month
    --- 2 means week
    --- 4 means day
    -- to_char(pIn_nPrecision) ----- it mark the precision of the result
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    FMP_LOG.FMP_SETVALUE(pIn_cNodeList);
    FMP_LOG.FMP_SETVALUE(pIn_nCalculationType);
    FMP_LOG.FMP_SETVALUE(pIn_vFirstPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vLastPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vSourceTimeSeriesIDs);
    FMP_LOG.FMP_SETVALUE(pIn_nValidateTimeSeriesId);
    FMP_LOG.FMP_SETVALUE(pIn_nIsPlusTimeSeries5);
    FMP_LOG.FMP_SETVALUE(pIn_nNodeLevel);
    FMP_LOG.FMP_SETVALUE(pIn_nChronology);
    FMP_LOG.FMP_SETVALUE(to_char(pIn_nPrecision));
    FMP_log.logBegin;
    pOut_nSqlCode := 0;
    if pIn_nChronology = 1 then
      FMSP_validateFCSTM(pIn_cNodeList,
                         pIn_nCalculationType,
                         pIn_vFirstPeriodTime,
                         pIn_vLastPeriodTime,
                         pIn_vSourceTimeSeriesIDs,
                         pIn_nValidateTimeSeriesId,
                         pIn_nIsPlusTimeSeries5,
                         pIn_nNodeLevel,
                         to_char(pIn_nPrecision));
    elsif pIn_nChronology = 2 then
      FMSP_validateFCSTW(pIn_cNodeList,
                         pIn_nCalculationType,
                         pIn_vFirstPeriodTime,
                         pIn_vLastPeriodTime,
                         pIn_vSourceTimeSeriesIDs,
                         pIn_nValidateTimeSeriesId,
                         pIn_nIsPlusTimeSeries5,
                         pIn_nNodeLevel,
                         to_char(pIn_nPrecision));
      null;
    elsif pIn_nChronology = 4 then
      FMSP_validateFCSTD(pIn_cNodeList,
                         pIn_nCalculationType,
                         pIn_vFirstPeriodTime,
                         pIn_vLastPeriodTime,
                         pIn_vSourceTimeSeriesIDs,
                         pIn_nValidateTimeSeriesId,
                         pIn_nIsPlusTimeSeries5,
                         pIn_nNodeLevel,
                         to_char(pIn_nPrecision));
      null;
    end if;
    FMP_LOG.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  END FMSP_validateFCSTByNodeList;

END FMP_ValidateFCST;
/
