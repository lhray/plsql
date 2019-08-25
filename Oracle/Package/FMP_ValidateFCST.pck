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
  procedure FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint in number default 0,
                                     pIn_nLastTimePoint  in number default 0,
                                     pIn_nMaxTimePoint   in number,
                                     pIn_nMinTimePoint   in number,
                                     pIn_vColumnName     in varchar2,
                                     pOut_cSql           out clob);
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
  type rTargetPair is record(
    targetTSID        number,
    targetTSIDREPLACE number);
  TYPE targetListType is TABLE OF rTargetPair INDEX BY BINARY_INTEGER;
  G_TargetList targetListType;
  GC_LEFT             constant number := 1;
  GC_RIGHT            constant number := 2;
  GC_TARGET_1         constant number := 52;
  GC_TARGET_REPLACE_1 constant number := 51;
  GC_TARGET_2         constant number := 21;
  GC_TARGET_REPLACE_2 constant number := 18;
  GC_TARGET_3         constant number := 22;
  GC_TARGET_REPLACE_3 constant number := 19;
  GC_TARGET_4         constant number := 23;
  GC_TARGET_REPLACE_4 constant number := 20;
  GC_MONTHLY          constant number := 1;
  GC_WEEKLY           constant number := 2;
  GC_DAILY            constant number := 4;
  GC_AGGLEVEL         constant number := 2;
  GC_DETAILLEVEL      constant number := 1;
  GC_MAXVALUE         constant number := 1;
  GC_FIRSTVALUE       constant number := 2;
  GC_MINVALUE         constant number := 3;
  G_TARGET          number := 52;
  G_TARGET_REPLACE  number := 51;
  G_aTsidListResult aTsidList;
  GC_OPERATIONNODELIST constant number := 1;
  GC_OPERATIONSEL      constant number := 0;
  type nodeType is record(
    vTmpTable       varchar2(200),
    vNodeLevelTable varchar2(200),
    vSeq            varchar2(200),
    vColumnsName    varchar2(200),
    nPeriod         number);
  G_NodeType nodeType;

  procedure FMSP_InitTargetList IS
    --*****************************************************************
    -- Description:   Init TargetList
    --
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        8-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    begin
      G_TargetList(1).targetTSID := 52;
      G_TargetList(1).targetTSIDREPLACE := 51;
      G_TargetList(2).targetTSID := 21;
      G_TargetList(2).targetTSIDREPLACE := 18;
      G_TargetList(3).targetTSID := 22;
      G_TargetList(3).targetTSIDREPLACE := 19;
      G_TargetList(4).targetTSID := 23;
      G_TargetList(4).targetTSIDREPLACE := 20;
    end;
  End FMSP_InitTargetList;

  procedure FMSP_GetTimePoint(pIn_nNodeType      in nodeType,
                              pOut_nMinTimePoint out number,
                              pOut_nMaxTimePoint out number) IS
    --*****************************************************************
    -- Description:   get time point monthly 1-12 weekly 1-53 daily 1-366
    --
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        1-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    begin
      if pIn_nNodeType.nPeriod = GC_WEEKLY then
        pOut_nMinTimePoint := 1;
        pOut_nMaxTimePoint := 53;
      elsif pIn_nNodeType.nPeriod = GC_DAILY then
        pOut_nMinTimePoint := 1;
        pOut_nMaxTimePoint := 366;
      elsif pIn_nNodeType.nPeriod = GC_MONTHLY then
        pOut_nMinTimePoint := 1;
        pOut_nMaxTimePoint := 12;
      else
        pOut_nMinTimePoint := 0;
        pOut_nMaxTimePoint := 0;
      end if;
    end;
  End FMSP_GetTimePoint;

  procedure FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint in number default 0,
                                     pIn_nLastTimePoint  in number default 0,
                                     pIn_nMaxTimePoint   in number,
                                     pIn_nMinTimePoint   in number,
                                     pIn_vColumnName     in varchar2,
                                     pOut_cSql           out clob) IS
  Begin
    declare
      cSql          clob;
      nMaxTimePoint number;
      nMinTimePoint number;
    begin
      cSql := 'SELECT ';
      if pIn_nLastTimePoint <> pIn_nMaxTimePoint AND
         pIn_nLastTimePoint <> 0 then
        nMaxTimePoint := pIn_nLastTimePoint;
      else
        nMaxTimePoint := pIn_nMaxTimePoint;
      end if;

      if pIn_nFirstTimePoint <> 0 AND
         pIn_nFirstTimePoint <> pIn_nMinTimePoint then
        nMinTimePoint := pIn_nFirstTimePoint;
      else
        nMinTimePoint := pIn_nMinTimePoint;
      end if;

      for i in pIn_nMinTimePoint .. nMinTimePoint - 1 loop
        if length(cSql) > 7 then
          cSql := cSql || ',';
        end if;
        cSql := cSql || 'NULL AS T' || to_char(i);
      end loop;

      for i in nMinTimePoint .. nMaxTimePoint loop
        if length(cSql) > 7 then
          cSql := cSql || ',';
        end if;
        cSql := cSql || 'T' || to_char(i);
      end loop;

      for i in nMaxTimePoint + 1 .. pIn_nMaxTimePoint loop
        if length(cSql) > 7 then
          cSql := cSql || ',';
        end if;
        cSql := cSql || 'NULL AS T' || to_char(i);
      end loop;

      cSql      := cSql || ',YY,' || pIn_vColumnName || ' FROM ';
      pOut_cSql := cSql;
    end;
  End FMSP_GetSelectColumnsSQL;

  procedure FMSP_InitNodeType(pIn_nChronology  in number,
                              pIn_nNodeLevel   in number,
                              pIn_nSelectionID in number default null,
                              pIn_vConditions  in varchar2 default null,
                              pIn_cNodeList    in clob default null,
                              pOut_nSqlCode    out number,
                              pOut_nNodeType   out nodeType) IS
    --*****************************************************************
    -- Description: Get Node_list Temp_Node_List_Table_Name . Get  table_name  column_name sequence_name .
    --
    -- Parameters:
    -- pIn_cNodeList ---- node list
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- pIn_nChronology ---------- it mark month or week or day
    --- 1 means month
    --- 2 means week
    --- 4 means day
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        1-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      vTabName  varchar2(200);
      sNodeList sys_refcursor;
      fNodeList fmt_nest_tab_nodeid;
      nSqlCode  number;
      vPeriod   varchar2(10);
    begin
      if pIn_nChronology = GC_DAILY then
        vPeriod                := '_D';
        pOut_nNodeType.nPeriod := GC_DAILY;
      elsif pIn_nChronology = GC_WEEKLY then
        vPeriod                := '_W';
        pOut_nNodeType.nPeriod := GC_WEEKLY;
      elsif pIn_nChronology = GC_MONTHLY then
        vPeriod                := '_M';
        pOut_nNodeType.nPeriod := GC_MONTHLY;
      end if;
      if pIn_nNodeLevel = GC_DETAILLEVEL then
        pOut_nNodeType.vTmpTable       := 'Tb_Ts_Detailnodeselcdt';
        pOut_nNodeType.vNodeLevelTable := 'DON' || vPeriod;
        pOut_nNodeType.vSeq            := 'SEQ_DON' || vPeriod;
        pOut_nNodeType.vColumnsName    := 'PVTID';
      elsif pIn_nNodeLevel = GC_AGGLEVEL then
        pOut_nNodeType.vTmpTable       := 'TB_TS_AGGREGATENODECON';
        pOut_nNodeType.vNodeLevelTable := 'PRB' || vPeriod;
        pOut_nNodeType.vSeq            := 'SEQ_PRB' || vPeriod;
        pOut_nNodeType.vColumnsName    := 'SELID';
      end if;
      if pIn_cNodeList is null then
        -- deal with get node list by selection,condition
        if pIn_nNodeLevel = GC_AGGLEVEL then
          -- get aggregation node list
          p_aggregation.FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => pIn_nSelectionID,
                                                  pIn_vConditions => pIn_vConditions,
                                                  pOut_Nodes      => sNodeList,
                                                  pOut_nSqlCode   => nSqlCode);
        elsif pIn_nNodeLevel = GC_DETAILLEVEL then
          -- get detail node list
          p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => pIn_nSelectionID,
                                               P_Conditions  => pIn_vConditions,
                                               P_Sequence    => null, --Sort sequence
                                               p_DetailNode  => sNodeList,
                                               pOut_vTabName => vTabName,
                                               p_SqlCode     => nSqlCode);
        end if;
      elsif pIn_cNodeList is not null then
        -- deal with get node list by pIn_cNodeList parameter
        FMSP_ClobToNestedTable(pIn_cClob     => pIn_cNodeList,
                               pOut_tNestTab => fNodeList,
                               pOut_nSqlCode => nSqlCode);
        if nSqlCode <> 0 then
          -- error
          pOut_nSqlCode := nSqlCode;
          return;
        end if;
        if pIn_nNodeLevel = GC_DETAILLEVEL then
          -- insert detail nodelist table
          delete from TB_TS_DetailNodeSelCdt;
          insert into TB_TS_DetailNodeSelCdt
            select * from table(fNodeList);

        elsif pIn_nNodeLevel = GC_AGGLEVEL then
          -- insert aggregation nodelist table
          delete from TB_TS_AggregateNodeCon;
          insert into TB_TS_AggregateNodeCon
            select * from table(fNodeList);

        end if;
      end if;
    end;
  End FMSP_InitNodeType;

  procedure FMSP_IsTargetData(pIn_nMarkTarget     in number,
                              pIn_aTsidListResult in aTsidList,
                              pOut_bFlag          out boolean) IS
  Begin
    begin
      if pIn_aTsidListResult(1).nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(1)
         .nTsidRight = pIn_nMarkTarget or pIn_aTsidListResult(2)
         .nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(2)
         .nTsidRight = pIn_nMarkTarget or pIn_aTsidListResult(3)
         .nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(3)
         .nTsidRight = pIn_nMarkTarget or pIn_aTsidListResult(4)
         .nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(4)
         .nTsidRight = pIn_nMarkTarget or pIn_aTsidListResult(5)
         .nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(5)
         .nTsidRight = pIn_nMarkTarget or pIn_aTsidListResult(6)
         .nTsidLeft = pIn_nMarkTarget or pIn_aTsidListResult(6)
         .nTsidRight = pIn_nMarkTarget then
        pOut_BFlag := true;
      end if;
    end;
  End FMSP_IsTargetData;

  procedure FMSP_InitTSIDList(pIn_vSourceTimeseriesIDS in varchar2,
                              pOut_nTargetTSID         out number,
                              pOut_nTargetTSIDREPLACE  out number,
                              pOut_bFlag               out boolean, -- is target timeseries
                              pOut_aTsidListResult     out aTsidList) IS
  Begin
    declare
      vTmp  varchar2(2000);
      vSql  varchar2(200);
      vTSID varchar2(200);

    begin
      pOut_bFlag := false;
      vTmp       := pIn_vSourceTimeseriesIDS;
      for i in 1 .. 6 loop
        vTSID := substr(vTmp, 0, instr(vTmp, ',') - 1);
        if instr(vTSID, '_') > 0 then
          pOut_aTsidListResult(i * 10 + 1).nTsidLeft := to_number(substr(vTSID,
                                                                         instr(vTSID,
                                                                               '_') + 1));
          pOut_aTsidListResult(i).nTsidLeft := to_number(substr(vTSID,
                                                                0,
                                                                instr(vTSID,
                                                                      '_') - 1));

        else
          pOut_aTsidListResult(i * 10 + 1).nTsidLeft := -1;
          pOut_aTsidListResult(i).nTsidLeft := to_number(vTSID);
        end if;
        vTmp := substr(vTmp, instr(vTmp, ',') + 1);

        vTSID := substr(vTmp, 0, instr(vTmp, ';') - 1);

        vTmp := substr(vTmp, instr(vTmp, ';') + 1);
        if instr(vTSID, '_') > 0 then
          pOut_aTsidListResult(i * 10 + 1).nTsidRight := to_number(substr(vTSID,
                                                                          instr(vTSID,
                                                                                '_') + 1));
          pOut_aTsidListResult(i).nTsidRight := to_number(substr(vTSID,
                                                                 0,
                                                                 instr(vTSID,
                                                                       '_') - 1));
        else
          pOut_aTsidListResult(i * 10 + 1).nTsidRight := -1;
          pOut_aTsidListResult(i).nTsidRight := to_number(vTSID);
        end if;
      end loop;

      for i in 1 .. G_TargetList.count loop
        FMSP_IsTargetData(pIn_nMarkTarget     => G_TargetList(i).targettsid,
                          pIn_aTsidListResult => pOut_aTsidListResult,
                          pOut_bFlag          => pOut_bFlag);
        if pOut_bFlag then
          pOut_nTargetTSID        := G_TargetList(i).targettsid;
          pOut_nTargetTSIDREPLACE := G_TargetList(i).targettsidreplace;
          G_TARGET                := G_TargetList(i).targettsid;
          G_TARGET_REPLACE        := G_TargetList(i).targettsidreplace;
          return;
        end if;
      end loop;
    end;
  End FMSP_InitTSIDList;

  procedure FMSP_OperateTargetResult(pIn_cSql              in clob,
                                     pIn_vTmpTableNodeList in varchar2) IS
    --*****************************************************************
    -- Description:        init tb_ts_aggregatenode and init tb_node
    -- Target Qty nodelist table is tb_ts_aggregatenode
    -- Forecast Qty nodelist table is tb_node
    -- describe the design of the object at a very high level.
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     lei zhang     Created.
    -- *************************************************************
  Begin
    declare
      nId  number;
      cSql clob;
    begin
      cSql := 'insert into tb_ts_aggregatenode(id) ' || pIn_cSql;
      --fmsp_execsql(pIn_cSql => cSql);
      execute immediate cSql;
      cSql := 'insert into tb_node(id) select id from ' ||
              pIn_vTmpTableNodeList ||
              ' where id !=all(select id from tb_ts_aggregateNode)';
      --fmsp_execsql(pIn_cSql => cSql);
      execute immediate cSql;

      -- Target Qty nodelist table is tb_ts_aggregatenode
      -- Forecast Qty nodelist table is tb_node
    end;
  End FMSP_OperateTargetResult;

  procedure FMSP_OperateTarget(pIn_nFirstYear        in number,
                               pIn_nLastYear         in number,
                               pIn_nFirstTimePoint   in number,
                               pIn_nLastTimePoint    in number,
                               pIn_nMaxTimePoint     in number,
                               pIn_nMinTimePoint     in number,
                               pIn_vTmpTableNodeList in varchar2,
                               pIn_vTableNodeLevel   in varchar2,
                               pIn_vColumnNodeLevel  in varchar2) IS
  Begin
    declare
      cSql             clob;
      sCursor          sys_refcursor;
      nMonth           number;
      cSqlColumns      clob;
      nIndex           number;
      bFlag            boolean;
      cSqlHavingALL    clob;
      cSqlSelectSumALL clob;
      nMinTimePoint    number;
      nMaxTimePoint    number;
    begin
      -- delete TB_NODE
      delete from TB_Node;
      delete from Tb_Ts_Aggregatenode;
      delete from tb_ts_validatefcstm;

      -- select all count columns and all sum columns
      cSqlSelectsumALL := 'SELECT ID ';
      cSqlHavingALL    := 'HAVING ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        if length(cSqlHavingALL) > 7 then
          cSqlHavingALL := cSqlHavingALL || ' OR ';
        end if;
        cSqlHavingALL := cSqlHavingALL || 'SUM(T' || to_char(i) || ')<>0';
      end loop;

      cSql := 'SELECT ';
      if pIn_nFirstYear = pIn_nLastYear then
        nMaxTimePoint := pIn_nLastTimePoint;
      else
        nMaxTimePoint := pIn_nMaxTimePoint;
      end if;

      for i in pIn_nMinTimePoint .. pIn_nFirstTimePoint - 1 loop
        cSql := cSql || '0 AS T' || to_char(i);
        --if i <> pIn_nMinTimePoint then
        cSql := cSql || ',';
        --end if;
      end loop;
      for i in pIn_nFirstTimePoint .. nMaxTimePoint loop
        cSql := cSql || 'COUNT(T' || to_char(i) || ') AS T' || to_char(i);
        --if i <> nMaxTimePoint then
        cSql := cSql || ',';
        --end if;
      end loop;
      for i in nMaxTimePoint + 1 .. pIn_nMaxTimePoint loop
        cSql := cSql || '0 AS T' || to_char(i);
        --if i <> nMaxTimePoint then
        cSql := cSql || ',';
        --end if;
      end loop;
      cSql := cSql || 'id FROM ' || pIn_vTableNodeLevel || ',' ||
              pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
              pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
              '.id and  tsid=' || to_char(G_TARGET) || ' and yy = ' ||
              to_char(pIn_nFirstYear) || ' group by id';

      if pIn_nFirstYear <> pIn_nLastYear then
        cSql := cSql || ' UNION ALL SELECT ';
        for i in pIn_nMinTimePoint .. pIn_nLastTimePoint loop
          cSql := cSql || 'COUNT(T' || to_char(i) || ') AS T' || to_char(i);
          if i <> pIn_nLastTimePoint then
            cSql := cSql || ',';
          end if;
        end loop;
        for i in pIn_nLastTimePoint + 1 .. pIn_nMaxTimePoint loop
          cSql := cSql || ',0 AS T' || to_char(i);
        end loop;
        cSql := cSql || ',id FROM ' || pIn_vTableNodeLevel || ',' ||
                pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and  tsid=' || to_char(G_TARGET) || ' and yy = ' ||
                to_char(pIn_nLastYear) || ' group by id';
      end if;

      if pIn_nFirstYear + 1 <= pIn_nLastYear + 1 then
        cSql := cSql || ' UNION ALL SELECT ';
        for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
          cSql := cSql || 'COUNT(T' || to_char(i) || ') AS T' || to_char(i);
          if i <> pIn_nMaxTimePoint then
            cSql := cSql || ',';
          end if;
        end loop;
        cSql := cSql || ',id FROM ' || pIn_vTableNodeLevel || ',' ||
                pIn_vTmpTableNodeList || ' where ' || pIn_vTableNodeLevel || '.' ||
                pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList ||
                '.id and  tsid=' || to_char(G_TARGET) || ' and yy >= ' ||
                to_char(pIn_nFirstYear + 1) || ' and yy<=' ||
                to_char(pIn_nLastYear - 1) || ' group by id';
      end if;

      cSql := 'select
              id  FROM (' || cSql || ') group by id ' ||
              cSqlHavingALL;
      FMSP_OperateTargetResult(pIn_cSql              => cSql,
                               pIn_vTmpTableNodeList => pIn_vTmpTableNodeList);

    end;
  End FMSP_OperateTarget;

  procedure FMSP_GetNullData(pIn_nFirstYear        in number,
                             pIn_nLastYear         in number,
                             pIn_nMaxTimePoint     in number,
                             pIn_nMinTimePoint     in number,
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
      nIndex        number;
      cSql          clob;
      cSqlSelectALL clob;
    begin
      cSqlSelectALL := 'SELECT ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        if length(cSqlSelectALL) > 7 then
          cSqlSelectALL := cSqlSelectALL || ',';
        end if;
        cSqlSelectALL := cSqlSelectALL || ' NULL AS T' || to_char(i);
      end loop;
      if pIn_vTSID <> G_TARGET then
        for nIndex in pIn_nFirstYear .. pIn_nLastYear loop
          if length(cSql) > 5 then
            cSql := cSql || ' UNION  ';
          end if;
          cSql := cSql || '(' || cSqlSelectALL || ',' || to_char(nIndex) ||
                  ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTmpTableNodeList || '
        minus
        ' || cSqlSelectALL || ',' || to_char(nIndex) ||
                  ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTableNodeLevel || ',' ||
                  pIn_vTmpTableNodeList || '
         WHERE YY = ' || to_char(nIndex) || '
           AND ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || pIn_vTmpTableNodeList || '.id
           AND TSID=' || pIn_vTSID || ')';
        end loop;
      elsif pIn_vTSID = G_TARGET then
        for nIndex in pIn_nFirstYear .. pIn_nLastYear loop
          if length(cSql) > 5 then
            cSql := cSql || ' UNION  ';
          end if;
          cSql := cSql || '(' || cSqlSelectALL || ',' || to_char(nIndex) ||
                  ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTmpTableNodeList || '
        minus
        ' || cSqlSelectALL || ',' || to_char(nIndex) ||
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
          cSql := cSql || '(' || cSqlSelectALL || ',' || to_char(nIndex) ||
                  ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || 'TB_Node' || '
        minus
        ' || cSqlSelectALL || ',' || to_char(nIndex) ||
                  ' AS YY,
               ID   AS ' || pIn_vColumnNodeLevel || '
          FROM ' || pIn_vTableNodeLevel || ',' ||
                  'TB_Node' || '
         WHERE YY = ' || to_char(nIndex) || '
           AND ' || pIn_vTableNodeLevel || '.' ||
                  pIn_vColumnNodeLevel || '=' || 'TB_Node' || '.id
           AND TSID=' || to_char(G_TARGET_REPLACE) || ')';
        end loop;
      end if;
      pOut_cSql := cSql;
    end;
  End FMSP_GetNullData;

  procedure FMSP_GetMinOrMaxValue(pIn_cSqlLineFirst    in clob default null,
                                  pIn_cSqlLineSecond   in clob default null,
                                  pIn_cSqlLineThird    in clob default null,
                                  pIn_cSqlLineFourth   in clob default null,
                                  pIn_cSqlLineFifth    in clob default null,
                                  pIn_nMaxTimePoint    in number,
                                  pIn_nMinTimePoint    in number,
                                  pIn_vColumnsName     in varchar2,
                                  pIn_nCalculationType in number,
                                  pOut_cSql            out clob) IS
    --*****************************************************************
    -- Description: it support for get minValue or maxValue
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
      cSqlColumns clob;
      vSqlM       varchar2(4);
      bFlag       boolean := false;
    begin
      if pIn_nCalculationType = GC_MAXVALUE then
        vSqlM := 'MAX';
      elsif pIn_nCalculationType = GC_MINVALUE then
        vSqlM := 'MIN';
      end if;

      cSqlColumns := 'SELECT ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        if length(cSqlColumns) > 7 then
          cSqlColumns := cSqlColumns || ',';
        end if;
        cSqlColumns := cSqlColumns || vSqlM || '( T' || to_char(i) || ') T' ||
                       to_char(i);
      end loop;
      cSqlColumns := cSqlColumns || ',YY' || pIn_vColumnsName;

      if pIn_cSqlLineFirst is not NULL then
        pOut_cSql := pOut_cSql || pIn_cSqlLineFirst;
        bFlag     := true;
      end if;
      if pIn_cSqlLineSecond is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || pIn_cSqlLineSecond;
        bFlag     := true;
      end if;
      if pIn_cSqlLineThird is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || pIn_cSqlLineThird;
        bFlag     := true;
      end if;
      if pIn_cSqlLineFourth is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || pIn_cSqlLineFourth;
        bFlag     := true;
      end if;
      if pIn_cSqlLineFifth is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || pIn_cSqlLineFifth;
        bFlag     := true;
      end if;
      pOut_cSql := cSqlColumns || ' FROM (' || pOut_cSql ||
                   ') GROUP BY YY,' || pIn_vColumnsName;
    end;
  End FMSP_GetMinOrMaxValue;

  procedure FMSP_GetFirstValue(pIn_cSqlLineFirst  in clob default null,
                               pIn_cSqlLineSecond in clob default null,
                               pIn_cSqlLineThird  in clob default null,
                               pIn_cSqlLineFourth in clob default null,
                               pIn_cSqlLineFifth  in clob default null,
                               pIn_nMaxTimePoint  in number,
                               pIn_nMinTimePoint  in number,
                               pIn_vColumnsName   in varchar2,
                               pOut_cSql          out clob) IS
    --*****************************************************************
    -- Description: it support for get firstValue
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
      cSqlColumns      clob;
      cSqlFirstColumns clob;
      bFlag            boolean := false;
    begin
      cSqlColumns := 'SELECT ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        if length(cSqlColumns) > 7 then
          cSqlColumns := cSqlColumns || ',';
        end if;
        cSqlColumns := cSqlColumns || ' T' || to_char(i);
      end loop;
      cSqlColumns := cSqlColumns || ',YY,' || pIn_vColumnsName;

      cSqlFirstColumns := cSqlColumns || ' FROM ( SELECT ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        cSqlFirstColumns := cSqlFirstColumns || ' FIRST_VALUE(T' ||
                            to_char(i) ||
                            ' ignore nulls) OVER(PARTITION BY YY,' ||
                            pIn_vColumnsName || ' order by orderid) T' ||
                            to_char(i) || ',';
      end loop;
      cSqlFirstColumns := cSqlFirstColumns ||
                          ' row_number() over(partition by YY,' ||
                          pIn_vColumnsName ||
                          ' order by orderid desc) r,YY,' ||
                          pIn_vColumnsName || ' FROM ';

      if pIn_cSqlLineFirst is not NULL then
        pOut_cSql := cSqlColumns || ',1 as orderid FROM (' ||
                     pIn_cSqlLineFirst || ')';
        bFlag     := true;
      end if;
      if pIn_cSqlLineSecond is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || cSqlColumns || ',2 as orderid FROM (' ||
                     pIn_cSqlLineSecond || ')';
        bFlag     := true;
      end if;
      if pIn_cSqlLineThird is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || cSqlColumns || ',3 as orderid FROM (' ||
                     pIn_cSqlLineThird || ')';
        bFlag     := true;
      end if;
      if pIn_cSqlLineFourth is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || cSqlColumns || ',4 as orderid FROM (' ||
                     pIn_cSqlLineFourth || ')';
        bFlag     := true;
      end if;
      if pIn_cSqlLineFifth is not null then
        if bFlag then
          pOut_cSql := pOut_cSql || ' UNION ALL ';
        end if;
        pOut_cSql := pOut_cSql || cSqlColumns || ',5 as orderid FROM (' ||
                     pIn_cSqlLineFifth || ')';
        bFlag     := true;
      end if;

      pOut_cSql := cSqlFirstColumns || '(' || pOut_cSql || ')) WHERE r=1';

    end;
  End FMSP_GetFirstValue;
  procedure FMSP_GetLineSQL(pIn_nNodeType       in nodeType,
                            pIn_nTimeSeries     in number,
                            pIn_nFirstYear      in number,
                            pIn_nLastYear       in number,
                            pIn_nFirstTimePoint in number,
                            pIn_nLastTimePoint  in number,
                            pOut_cSql           out clob) IS
    --*****************************************************************
    -- Description:   get every line SQL
    --
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        1-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nMaxTimePoint    number;
      nMinTimePoint    number;
      cSqlSelectALL    clob;
      cSqlSelectFPart  clob;
      cSqlSelectLPart  clob;
      cSqlSelectFLPart clob;
      cSqlNullData     clob;
      cSqlTmp1         clob;
      cSqlTmp2         clob;
      cSqlTmp3         clob;
      nId              number;
    begin
      cSqlNullData := '';
      FMSP_GetTimePoint(pIn_nNodeType      => pIn_nNodeType,
                        pOut_nMinTimePoint => nMinTimePoint,
                        pOut_nMaxTimePoint => nMaxTimePoint);
      FMSP_GetNULLData(pIn_nFirstYear        => pIn_nFirstYear,
                       pIn_nLastYear         => pIn_nLastYear,
                       pIn_nMaxTimePoint     => nMaxTimePoint,
                       pIn_nMinTimePoint     => nMinTimePoint,
                       pIn_vTSID             => pIn_nTimeSeries,
                       pIn_vTmpTableNodeList => pIn_nNodeType.vTmpTable,
                       pIn_vTableNodeLevel   => pIn_nNodeType.vNodeLevelTable,
                       pIn_vColumnNodeLevel  => pIn_nNodeType.vColumnsName,
                       pOut_cSql             => cSqlNullData);
      FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint => nMinTimePoint,
                               pIn_nLastTimePoint  => nMaxTimePoint,
                               pIn_nMaxTimePoint   => nMaxTimePoint,
                               pIn_nMinTimePoint   => nMinTimePoint,
                               pIn_vColumnName     => pIn_nNodeType.vColumnsName,
                               pOut_cSql           => cSqlSelectALL);
      FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                               pIn_nLastTimePoint  => pIn_nLastTimePoint,
                               pIn_nMaxTimePoint   => nMaxTimePoint,
                               pIn_nMinTimePoint   => nMinTimePoint,
                               pIn_vColumnName     => pIn_nNodeType.vColumnsName,
                               pOut_cSql           => cSqlSelectFLPart);

      FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                               pIn_nLastTimePoint  => 0,
                               pIn_nMaxTimePoint   => nMaxTimePoint,
                               pIn_nMinTimePoint   => nMinTimePoint,
                               pIn_vColumnName     => pIn_nNodeType.vColumnsName,
                               pOut_cSql           => cSqlSelectFPart);
      FMSP_GetSelectColumnsSQL(pIn_nFirstTimePoint => 0,
                               pIn_nLastTimePoint  => pIn_nLastTimePoint,
                               pIn_nMaxTimePoint   => nMaxTimePoint,
                               pIn_nMinTimePoint   => nMinTimePoint,
                               pIn_vColumnName     => pIn_nNodeType.vColumnsName,
                               pOut_cSql           => cSqlSelectLPart);
      nId := 0;
      select count(id) into nId from tb_node; -- ensure all node with target timeserise have value.
      --if 1 = j then
      if pIn_nTimeSeries <> G_TARGET OR nId = 0 then
        --<>52
        cSqlTmp1 := ' ';
        cSqlTmp2 := ' ';
        cSqlTmp3 := ' ';
        -- nFirstYear = nLastYear
        if pIn_nFirstYear = pIn_nLastYear then
          cSqlSelectFPart := cSqlSelectFLPart;
        end if;
        -- nFirst
        cSqlTmp1 := cSqlSelectFPart || pIn_nNodeType.vNodeLevelTable || ',' ||
                    pIn_nNodeType.vTmpTable || ' where  YY = ' ||
                    to_char(pIn_nFirstYear) || ' AND ' ||
                    pIn_nNodeType.vNodeLevelTable || '.' ||
                    pIn_nNodeType.vColumnsName || '=' ||
                    pIn_nNodeType.vTmpTable || '.id';
        cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' || to_char(pIn_nTimeSeries);
        -- add null data
        if length(cSqlNullData) > 5 then
          cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
        end if;
        -- nLast
        if pIn_nFirstYear <> pIn_nLastYear then
          cSqlTmp3 := cSqlSelectLPart || pIn_nNodeType.vNodeLevelTable || ',' ||
                      pIn_nNodeType.vTmpTable || ' where  YY = ' ||
                      to_char(pIn_nLastYear) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' ||
                      pIn_nNodeType.vTmpTable || '.id';
          cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                      to_char(pIn_nTimeSeries);
        end if;
        -- judge nFirst+1 < nLastYear-1
        if pIn_nFirstYear + 1 <= pIn_nLastYear - 1 then
          cSqlTmp2 := cSqlSelectALL || pIn_nNodeType.vNodeLevelTable || ',' ||
                      pIn_nNodeType.vTmpTable || '  where  YY between ' ||
                      to_char(pIn_nFirstYear + 1) || '  AND  ' ||
                      to_char(pIn_nLastYear - 1) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' ||
                      pIn_nNodeType.vTmpTable || '.id';
          cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                      to_char(pIn_nTimeSeries);
        end if;
      elsif pIn_nTimeSeries = G_TARGET then
        --=52
        cSqlTmp1 := ' ';
        cSqlTmp2 := ' ';
        cSqlTmp3 := ' ';
        -- nFirstYear = nLastYear
        if pIn_nFirstYear = pIn_nLastYear then
          cSqlSelectFPart := cSqlSelectFLPart;
        end if;
        -- nFirst
        cSqlTmp1 := cSqlSelectFPart || pIn_nNodeType.vNodeLevelTable || ',' ||
                    'tb_ts_aggregatenode' || ' where  YY = ' ||
                    to_char(pIn_nFirstYear) || ' AND ' ||
                    pIn_nNodeType.vNodeLevelTable || '.' ||
                    pIn_nNodeType.vColumnsName || '=' ||
                    'tb_ts_aggregatenode' || '.id';
        cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' || to_char(pIn_nTimeSeries);
        cSqlTmp1 := cSqlTmp1 || ' UNION ';
        cSqlTmp1 := cSqlTmp1 || cSqlSelectFPart ||
                    pIn_nNodeType.vNodeLevelTable || ',' || 'tb_node' ||
                    ' where  YY = ' || to_char(pIn_nFirstYear) || ' AND ' ||
                    pIn_nNodeType.vNodeLevelTable || '.' ||
                    pIn_nNodeType.vColumnsName || '=' || 'tb_node' || '.id';
        cSqlTmp1 := cSqlTmp1 || '  AND  TSID=' || to_char(G_TARGET_REPLACE);
        -- add null data
        if length(cSqlNullData) > 5 then
          cSqlTmp1 := cSqlTmp1 || '  UNION ALL ' || cSqlNullData;
        end if;

        -- nLast
        if pIn_nFirstYear <> pIn_nLastYear then
          cSqlTmp3 := cSqlSelectLPart || pIn_nNodeType.vNodeLevelTable || ',' ||
                      'tb_ts_aggregatenode' || ' where  YY = ' ||
                      to_char(pIn_nLastYear) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' ||
                      'tb_ts_aggregatenode' || '.id';
          cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                      to_char(pIn_nTimeSeries);
          cSqlTmp3 := cSqlTmp3 || ' UNION ' || cSqlSelectLPart ||
                      pIn_nNodeType.vNodeLevelTable || ',' || 'tb_node' ||
                      ' where  YY = ' || to_char(pIn_nLastYear) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' || 'tb_node' ||
                      '.id';
          cSqlTmp3 := cSqlTmp3 || '  AND  TSID=' ||
                      to_char(G_TARGET_REPLACE);

        end if;

        -- judge nFirst+1 < nLastYear-1
        if pIn_nFirstYear + 1 <= pIn_nLastYear - 1 then
          cSqlTmp2 := cSqlSelectALL || pIn_nNodeType.vNodeLevelTable || ',' ||
                      'tb_ts_aggregatenode' || '  where  YY between ' ||
                      to_char(pIn_nFirstYear + 1) || '  AND  ' ||
                      to_char(pIn_nLastYear - 1) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' ||
                      'tb_ts_aggregatenode' || '.id';
          cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                      to_char(pIn_nTimeSeries);
          cSqlTmp2 := cSqlTmp2 || ' UNION ' || cSqlSelectALL ||
                      pIn_nNodeType.vNodeLevelTable || ',' || 'tb_node' ||
                      '  where  YY between ' || to_char(pIn_nFirstYear + 1) ||
                      '  AND  ' || to_char(pIn_nLastYear - 1) || ' AND ' ||
                      pIn_nNodeType.vNodeLevelTable || '.' ||
                      pIn_nNodeType.vColumnsName || '=' || 'tb_node' ||
                      '.id';
          cSqlTmp2 := cSqlTmp2 || '  AND  TSID=' ||
                      to_char(G_TARGET_REPLACE);
        end if;
      end if;

      pOut_cSql := cSqlTmp1;
      if length(cSqlTmp2) > 1 then
        pOut_cSql := pOut_cSql || ' UNION ALL ' || cSqlTmp2;
      end if;
      if length(cSqlTmp3) > 1 then
        pOut_cSql := pOut_cSql || ' UNION ALL ' || cSqlTmp3;
      end if;
    end;
  End FMSP_GetLineSQL;
  procedure FMSP_GetFinallyLineSQL(pIn_nNodeType       in nodeType,
                                   pIn_nFirstYear      in number,
                                   pIn_nLastYear       in number,
                                   pIn_nFirstTimePoint in number,
                                   pIn_nLastTimePoint  in number,
                                   pIn_aTsidListResult in aTsidList,
                                   pIn_nLineNum        in number,
                                   pIn_nMinTimePoint   in number,
                                   pIn_nMaxTimePoint   in number,
                                   pOut_cSql           out clob) IS
  Begin
    declare
      cSqlSelectALLColumns  clob;
      cSqlSelectALLSUM      clob;
      cSqlSelectALLMultiply clob;
      nTSIDLeft             number;
      nTSIDRight            number;
      cSqlTmp               clob;
      cSqlTmpRight          clob;
    begin
      cSqlSelectALLColumns  := 'SELECT ';
      cSqlSelectALLSUM      := 'SELECT ';
      cSqlSelectALLMultiply := 'SELECT ';
      for i in pIn_nMinTimePoint .. pIn_nMaxTimePoint loop
        if length(cSqlSelectALLColumns) > 7 then
          cSqlSelectALLColumns  := cSqlSelectALLColumns || ',';
          cSqlSelectALLSUM      := cSqlSelectALLSUM || ',';
          cSqlSelectALLMultiply := cSqlSelectALLMultiply || ',';
        end if;
        cSqlSelectALLColumns  := cSqlSelectALLColumns || ' T' || to_char(i);
        cSqlSelectALLSUM      := cSqlSelectALLSUM || ' SUM(T' || to_char(i) ||
                                 ') AS T' || to_char(i);
        cSqlSelectALLMultiply := cSqlSelectALLMultiply || '(T.T' ||
                                 to_char(i) || '*NVL(S.T' || to_char(i) ||
                                 ',1)) AS T' || to_char(i);
      end loop;
      nTSIDLeft := pIn_aTsidListResult(pIn_nLineNum).nTSIDLeft;
      if nTSIDLeft = -1 then
        -- when left tsis = -1 then return.
        return;
      end if;
      FMSP_GetLineSQL(pIn_nNodeType       => pIn_nNodeType,
                      pIn_nTimeSeries     => nTSIDLeft,
                      pIn_nFirstYear      => pIn_nFirstYear,
                      pIn_nLastYear       => pIn_nLastYear,
                      pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                      pIn_nLastTimePoint  => pIn_nLastTimePoint,
                      pOut_cSql           => cSqlTmp);
      pOut_cSql := cSqlTmp;
      nTSIDLeft := pIn_aTsidListResult(pIn_nLineNum * 10 + 1).nTSIDLeft;
      if nTSIDLeft <> -1 then
        -- when there are two tsids in one column then sum their values
        FMSP_GetLineSQL(pIn_nNodeType       => pIn_nNodeType,
                        pIn_nTimeSeries     => nTSIDLeft,
                        pIn_nFirstYear      => pIn_nFirstYear,
                        pIn_nLastYear       => pIn_nLastYear,
                        pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                        pIn_nLastTimePoint  => pIn_nLastTimePoint,
                        pOut_cSql           => cSqlTmp);
        pOut_cSql := cSqlSelectALLSUM || ',YY,' ||
                     pIn_nNodeType.vColumnsName || ' FROM (' || pOut_cSql ||
                     ' UNION ALL ' || cSqlTmp || ') GROUP BY YY,' ||
                     pIn_nNodeType.vColumnsName;
      end if;
      nTSIDRight := pIn_aTsidListResult(pIn_nLineNum).nTSIDRight;
      if nTSIDRight = -1 then
        return;
      end if;
      FMSP_GetLineSQL(pIn_nNodeType       => pIn_nNodeType,
                      pIn_nTimeSeries     => nTSIDRight,
                      pIn_nFirstYear      => pIn_nFirstYear,
                      pIn_nLastYear       => pIn_nLastYear,
                      pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                      pIn_nLastTimePoint  => pIn_nLastTimePoint,
                      pOut_cSql           => cSqlTmp);
      cSqlTmpRight := cSqlTmp;
      nTSIDRight   := pIn_aTsidListResult(pIn_nLineNum * 10 + 1).nTSIDRight;
      if nTSIDRight <> -1 then
        -- when there are two tsids in one column then sum their values
        FMSP_GetLineSQL(pIn_nNodeType       => pIn_nNodeType,
                        pIn_nTimeSeries     => nTSIDRight,
                        pIn_nFirstYear      => pIn_nFirstYear,
                        pIn_nLastYear       => pIn_nLastYear,
                        pIn_nFirstTimePoint => pIn_nFirstTimePoint,
                        pIn_nLastTimePoint  => pIn_nLastTimePoint,
                        pOut_cSql           => cSqlTmp);
        cSqlTmpRight := cSqlSelectALLSUM || ',YY,' ||
                        pIn_nNodeType.vColumnsName || ' FROM (' ||
                        cSqlTmpRight || ' UNION ALL ' || cSqlTmp ||
                        ') GROUP BY YY,' || pIn_nNodeType.vColumnsName;
      end if;
      pOut_cSql := cSqlSelectALLMultiply || ',T.YY,T.' ||
                   pIn_nNodeType.vColumnsName || ' FROM (' || pOut_cSql ||
                   ')  T LEFT JOIN (' || cSqlTmpRight ||
                   ')  S ON(T.YY=S.YY AND T.' || pIn_nNodeType.vColumnsName || '=' || 'S.' ||
                   pIn_nNodeType.vColumnsName || ')';
    end;
  End FMSP_GetFinallyLineSQL;
  procedure FMSP_Merge(pIn_nFirstYear            in number,
                       pIn_nLastYear             in number,
                       pIn_nFirstTimePoint       in number,
                       pIn_nLastTimePoint        in number,
                       pIn_nMinTimePoint         in number,
                       pin_nMaxTimePoint         in number,
                       pIn_nValidateTimeSeriesId in number,
                       pIn_nPrecision            in number,
                       pIn_nNodeType             in nodeType,
                       pIn_cSql                  in clob) IS
  Begin
    declare
      cSql          clob;
      cSqlUpdate    clob;
      cSqlUpdateALL clob;
      cSqlInsert    clob;
      cSqlInsertALL clob;
      cSqlMerge     clob;
      cSqlSource    clob;
      nMaxTimePoint number;
      cSqlSelectALL clob;
      cSqlWhere     clob;
    begin
      cSqlSelectALL := 'SELECT ';
      for i in pIn_nMinTimePoint .. pin_nMaxTimePoint loop
        cSqlSelectALL := cSqlSelectALL || 'T' || to_char(i) || ',';
      end loop;
      cSqlSelectALL := cSqlSelectALL || 'YY,' || pIn_nNodeType.vColumnsName ||
                       ' FROM (';
      -- <> <>
      if pIn_nFirstYear = pIn_nLastYear then
        nMaxTimePoint := pIn_nLastTimePoint;
      else
        nMaxTimePoint := pin_nMaxTimePoint;
      end if;
      cSqlSource    := pIn_cSql;
      cSqlWhere     := ') WHERE YY=' || to_char(pIn_nFirstYear);
      cSqlMerge     := 'MERGE INTO ' || pIn_nNodeType.vNodeLevelTable ||
                       ' V USING(' || cSqlSelectALL || pIn_cSql ||
                       cSqlWhere || ') T ON (V.TSID=' ||
                       to_char(pIn_nValidateTimeSeriesId) ||
                       ' AND V.YY=T.YY ' || 'AND V.' ||
                       pIn_nNodeType.vColumnsName || '=T.' ||
                       pIn_nNodeType.vColumnsName || ')';
      cSqlUpdateALL := ' WHEN MATCHED THEN UPDATE SET ';
      cSqlInsertALL := ' WHEN NOT MATCHED THEN INSERT VALUES(' ||
                       pIn_nNodeType.vSeq || '.nextval,T.' ||
                       pIn_nNodeType.vColumnsName || ',' ||
                       to_char(pIn_nValidateTimeSeriesId) || ',0,T.YY';
      for i in pIn_nMinTimePoint .. pIn_nFirstTimePoint - 1 loop
        cSqlInsertALL := cSqlInsertALL || ',NULL';
      end loop;
      for i in pIn_nFirstTimePoint .. nMaxTimePoint loop
        cSqlUpdateALL := cSqlUpdateALL || 'V.T' || to_char(i) || '=T.T' ||
                         to_char(i);
        if i <> nMaxTimePoint then
          cSqlUpdateALL := cSqlUpdateALL || ',';
        end if;
        cSqlInsertALL := cSqlInsertALL || ',T.T' || to_char(i);
      end loop;
      for i in nMaxTimePoint + 1 .. pin_nMaxTimePoint loop
        cSqlInsertALL := cSqlInsertALL || ',NULL';
      end loop;
      cSqlInsertALL := cSqlInsertALL || ')';
      cSqlMerge     := cSqlMerge || cSqlUpdateALL || cSqlInsertALL;
      fmsp_execsql(pIn_cSql => cSqlMerge); --execute sql here

      if pIn_nFirstYear <> pIn_nLastYear then
        cSqlSource    := pIn_cSql;
        cSqlWhere     := ') WHERE YY=' || to_char(pIn_nLastYear);
        cSqlMerge     := 'MERGE INTO ' || pIn_nNodeType.vNodeLevelTable ||
                         ' V USING(' || cSqlSelectALL || pIn_cSql ||
                         cSqlWhere || ') T ON (V.TSID=' ||
                         to_char(pIn_nValidateTimeSeriesId) ||
                         ' AND V.YY=T.YY ' || 'AND V.' ||
                         pIn_nNodeType.vColumnsName || '=T.' ||
                         pIn_nNodeType.vColumnsName || ')';
        cSqlUpdateALL := ' WHEN MATCHED THEN UPDATE SET ';
        cSqlInsertALL := ' WHEN NOT MATCHED THEN INSERT VALUES(' ||
                         pIn_nNodeType.vSeq || '.nextval,T.' ||
                         pIn_nNodeType.vColumnsName || ',' ||
                         to_char(pIn_nValidateTimeSeriesId) || ',0,T.YY';
        for i in pIn_nMinTimePoint .. pIn_nLastTimePoint loop
          cSqlUpdateALL := cSqlUpdateALL || 'V.T' || to_char(i) || '=T.T' ||
                           to_char(i);
          if i <> pIn_nLastTimePoint then
            cSqlUpdateALL := cSqlUpdateALL || ',';
          end if;
          cSqlInsertALL := cSqlInsertALL || ',T.T' || to_char(i);
        end loop;
        for i in pIn_nLastTimePoint + 1 .. pin_nMaxTimePoint loop
          cSqlInsertALL := cSqlInsertALL || ',NULL';
        end loop;
        cSqlInsertALL := cSqlInsertALL || ')';
        cSqlMerge     := cSqlMerge || cSqlUpdateALL || cSqlInsertALL;
        fmsp_execsql(pIn_cSql => cSqlMerge); --execute sql here
      end if;

      if pIn_nFirstYear + 1 <= pIn_nLastYear - 1 then
        cSqlSource    := pIn_cSql;
        cSqlWhere     := ') WHERE YY>=' || to_char(pIn_nFirstYear + 1) ||
                         ' AND YY<=' || to_char(pIn_nLastYear - 1);
        cSqlMerge     := 'MERGE INTO ' || pIn_nNodeType.vNodeLevelTable ||
                         ' V USING(' || cSqlSelectALL || pIn_cSql ||
                         cSqlWhere || ') T ON (V.TSID=' ||
                         to_char(pIn_nValidateTimeSeriesId) ||
                         ' AND V.YY=T.YY ' || 'AND V.' ||
                         pIn_nNodeType.vColumnsName || '=T.' ||
                         pIn_nNodeType.vColumnsName || ')';
        cSqlUpdateALL := ' WHEN MATCHED THEN UPDATE SET ';
        cSqlInsertALL := ' WHEN NOT MATCHED THEN INSERT VALUES(' ||
                         pIn_nNodeType.vSeq || '.nextval,T.' ||
                         pIn_nNodeType.vColumnsName || ',' ||
                         to_char(pIn_nValidateTimeSeriesId) || ',0,T.YY';
        for i in pIn_nMinTimePoint .. pin_nMaxTimePoint loop
          cSqlUpdateALL := cSqlUpdateALL || 'V.T' || to_char(i) || '=T.T' ||
                           to_char(i);
          if i <> pin_nMaxTimePoint then
            cSqlUpdateALL := cSqlUpdateALL || ',';
          end if;
          cSqlInsertALL := cSqlInsertALL || ',T.T' || to_char(i);
        end loop;
        cSqlInsertALL := cSqlInsertALL || ')';
        cSqlMerge     := cSqlMerge || cSqlUpdateALL || cSqlInsertALL;
        fmsp_execsql(pIn_cSql => cSqlMerge); --execute sql here
      end if;
    end;
  End FMSP_Merge;
  procedure FMSP_validateFCSTACTION(pIn_nSelectionID          in number default null,
                                    pIn_cNodeList             in clob default null,
                                    pIn_nCalculationType      in number,
                                    pIn_vFirstPeriodTime      in varchar2,
                                    pIn_vLastPeriodTime       in varchar2,
                                    pIn_vConditions           in varchar2 default null,
                                    pIn_vSourceTimeSeriesIDs  in varchar2,
                                    pIn_nValidateTimeSeriesId in number,
                                    pIn_nIsPlusTimeSeries5    in number,
                                    pIn_nNodeLevel            in number,
                                    pIn_nChronology           in number,
                                    pIn_nPrecision            in number,
                                    pOut_nSqlCode             out number) IS
  Begin
    declare
      bTsidFlag             boolean := false;
      aTsidListResult       aTsidList;
      nSqlCodeInitNode      number;
      nNodeType             nodeType;
      nFirstYear            number;
      nLastYear             number;
      nFirstTimePoint       number;
      nLastTimePoint        number;
      nMaxTimePoint         number;
      nMinTimePoint         number;
      cSqlLineFirst         clob;
      cSqlLineSecond        clob;
      cSqlLineThird         clob;
      cSqlLineFourth        clob;
      cSqlLineFifth         clob;
      cSqlLineSixth         clob;
      cSqlSelectALLColumns  clob;
      cSqlSelectALLSUM      clob;
      cSqlSelectALLMultiply clob;
      nLeftTSID             number;
      nRightTSID            number;
      cSqlTmp               clob;
      cSql                  clob;
      nTargetTSID           number;
      nTargetTSIDREPLACE    number;
    begin
      pOut_nSqlCode := 0;
      --operate time
      nFirstTimePoint := to_number(substr(pIn_vFirstPeriodTime, 5));
      nLastTimePoint  := to_number(substr(pIn_vLastPeriodTime, 5));
      nFirstYear      := to_number(substr(pIn_vFirstPeriodTime, 0, 4));
      nLastYear       := to_number(substr(pIn_vLastPeriodTime, 0, 4));
      -- init targetlist
      FMSP_InitTargetList;
      -- init TSID LIST
      FMSP_InitTSIDList(pIn_vSourceTimeseriesIDS => pIn_vSourceTimeSeriesIDs,
                        pOut_nTargetTSID         => nTargetTSID,
                        pOut_nTargetTSIDREPLACE  => nTargetTSIDREPLACE,
                        pOut_aTsidListResult     => aTsidListResult,
                        pOut_bFlag               => bTsidFlag);
      for i in 1 .. 6 loop
        if aTsidListResult(i).nTsidLeft <> -1 then
          exit;
        end if;
        if i = 6 and aTsidListResult(i).nTsidLeft = -1 then
          return;
        end if;
      end loop;
      -- init Node list opertation table columnName tmpTable
      FMSP_InitNodeType(pIn_nChronology  => pIn_nChronology,
                        pIn_nNodeLevel   => pIn_nNodeLevel,
                        pIn_nSelectionID => pIn_nSelectionID,
                        pIn_vConditions  => pIn_vConditions,
                        pIn_cNodeList    => pIn_cNodeList,
                        pOut_nSqlCode    => nSqlCodeInitNode,
                        pOut_nNodeType   => nNodeType);
      -- get MaxTimePoint MinTimePoint
      FMSP_GetTimePoint(pIn_nNodeType      => nNodeType,
                        pOut_nMinTimePoint => nMinTimePoint,
                        pOut_nMaxTimePoint => nMaxTimePoint);
      -- init all columns and all sum columns
      cSqlSelectALLColumns  := 'SELECT ';
      cSqlSelectALLSUM      := 'SELECT ';
      cSqlSelectALLMultiply := 'SELECT ';
      for i in nMinTimePoint .. nMaxTimePoint loop
        if length(cSqlSelectALLColumns) > 7 then
          cSqlSelectALLColumns  := cSqlSelectALLColumns || ',';
          cSqlSelectALLSUM      := cSqlSelectALLSUM || ',';
          cSqlSelectALLMultiply := cSqlSelectALLMultiply || ',';
        end if;
        cSqlSelectALLColumns  := cSqlSelectALLColumns || ' T' || to_char(i);
        cSqlSelectALLSUM      := cSqlSelectALLSUM || ' SUM(T' || to_char(i) ||
                                 ') AS T' || to_char(i);
        cSqlSelectALLMultiply := cSqlSelectALLMultiply || '(T' ||
                                 to_char(i) || '*T' || to_char(i) ||
                                 ') AS T' || to_char(i);
      end loop;
      -- operate target tsid
      if bTsidFlag then
        FMSP_OperateTarget(pIn_nFirstYear        => nFirstYear,
                           pIn_nLastYear         => nLastYear,
                           pIn_nFirstTimePoint   => nFirstTimePoint,
                           pIn_nLastTimePoint    => nLastTimePoint,
                           pIn_nMaxTimePoint     => nMaxTimePoint,
                           pIn_nMinTimePoint     => nMinTimePoint,
                           pIn_vTmpTableNodeList => nNodeType.vTmpTable,
                           pIn_vTableNodeLevel   => nNodeType.vNodeLevelTable,
                           pIn_vColumnNodeLevel  => nNodeType.vColumnsName);
      end if;
      -- operate every line
      -- first line
      cSqlLineFirst := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 1,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineFirst);
      -- second line
      cSqlLineSecond := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 2,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineSecond);
      -- third line
      cSqlLineThird := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 3,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineThird);
      -- fourth line
      cSqlLineFourth := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 4,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineFourth);
      -- fifth line
      cSqlLineFifth := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 5,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineFifth);
      -- sixth line
      cSqlLineSixth := null;
      FMSP_GetFinallyLineSQL(pIn_nNodeType       => nNodeType,
                             pIn_nFirstYear      => nFirstYear,
                             pIn_nLastYear       => nLastYear,
                             pIn_nFirstTimePoint => nFirstTimePoint,
                             pIn_nLastTimePoint  => nLastTimePoint,
                             pIn_aTsidListResult => aTsidListResult,
                             pIn_nLineNum        => 6,
                             pIn_nMinTimePoint   => nMinTimePoint,
                             pIn_nMaxTimePoint   => nMaxTimePoint,
                             pOut_cSql           => cSqlLineSixth);
      if pIn_nIsPlusTimeSeries5 = 0 then
        -- line 5 normal
        cSqlTmp := cSqlLineFifth;
      elsif pIn_nIsPlusTimeSeries5 = 1 then
        -- line 5 add
        cSqlTmp := NULL;
      end if;
      if pIn_nCalculationType = GC_FIRSTVALUE then
        FMSP_GetFirstValue(pIn_cSqlLineFirst  => cSqlLineFirst,
                           pIn_cSqlLineSecond => cSqlLineSecond,
                           pIn_cSqlLineThird  => cSqlLineThird,
                           pIn_cSqlLineFourth => cSqlLineFourth,
                           pIn_cSqlLineFifth  => cSqlTmp,
                           pIn_nMaxTimePoint  => nMaxTimePoint,
                           pIn_nMinTimePoint  => nMinTimePoint,
                           pIn_vColumnsName   => nNodeType.vColumnsName,
                           pOut_cSql          => cSql);
      else
        FMSP_GetMinOrMaxValue(pIn_cSqlLineFirst    => cSqlLineFirst,
                              pIn_cSqlLineSecond   => cSqlLineSecond,
                              pIn_cSqlLineThird    => cSqlLineThird,
                              pIn_cSqlLineFourth   => cSqlLineFourth,
                              pIn_cSqlLineFifth    => cSqlTmp,
                              pIn_nMaxTimePoint    => nMaxTimePoint,
                              pIn_nMinTimePoint    => nMinTimePoint,
                              pIn_vColumnsName     => nNodeType.vColumnsName,
                              pIn_nCalculationType => pIn_nCalculationType,
                              pOut_cSql            => cSql);
      end if;
      if pIn_nIsPlusTimeSeries5 = 1 AND cSqlLineFifth is not null then
        cSql := cSqlSelectALLSUM || ',YY,' || nNodeType.vColumnsName ||
                ' FROM (' || cSql || ' UNION ALL ' || cSqlLineFifth ||
                ') GROUP BY YY,' || nNodeType.vColumnsName;
      end if;
      if cSqlLineSixth is not null then
        cSql := cSqlSelectALLSUM || ',YY,' || nNodeType.vColumnsName ||
                ' FROM (' || cSql || ' UNION ALL ' || cSqlLineSixth ||
                ') GROUP BY YY,' || nNodeType.vColumnsName;
      end if;
      FMSP_Merge(pIn_nFirstYear            => nFirstYear,
                 pIn_nLastYear             => nLastYear,
                 pIn_nFirstTimePoint       => nFirstTimePoint,
                 pIn_nLastTimePoint        => nLastTimePoint,
                 pIn_nMinTimePoint         => nMinTimePoint,
                 pin_nMaxTimePoint         => nMaxTimePoint,
                 pIn_nValidateTimeSeriesId => pIn_nValidateTimeSeriesId,
                 pIn_nPrecision            => pIn_nPrecision,
                 pIn_nNodeType             => nNodeType,
                 pIn_cSql                  => cSql);
    end;
  End FMSP_validateFCSTACTION;
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

      FMSP_validateFCSTACTION(pIn_nSelectionID          => pIn_nSelectionID,
                              pIn_cNodeList             => null,
                              pIn_nCalculationType      => pIn_nCalculationType,
                              pIn_vFirstPeriodTime      => pIn_vFirstPeriodTime,
                              pIn_vLastPeriodTime       => pIn_vLastPeriodTime,
                              pIn_vConditions           => pIn_vConditions,
                              pIn_vSourceTimeSeriesIDs  => pIn_vSourceTimeSeriesIDs,
                              pIn_nValidateTimeSeriesId => pIn_nValidateTimeSeriesId,
                              pIn_nIsPlusTimeSeries5    => pIn_nIsPlusTimeSeries5,
                              pIn_nNodeLevel            => pIn_nNodeLevel,
                              pIn_nChronology           => pIn_nChronology,
                              pIn_nPrecision            => pIn_nPrecision,
                              pOut_nSqlCode             => pOut_nSqlCode);
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
    FMSP_validateFCSTACTION(pIn_nSelectionID          => NULL,
                            pIn_cNodeList             => pIn_cNodeList,
                            pIn_nCalculationType      => pIn_nCalculationType,
                            pIn_vFirstPeriodTime      => pIn_vFirstPeriodTime,
                            pIn_vLastPeriodTime       => pIn_vLastPeriodTime,
                            pIn_vConditions           => NULL,
                            pIn_vSourceTimeSeriesIDs  => pIn_vSourceTimeSeriesIDs,
                            pIn_nValidateTimeSeriesId => pIn_nValidateTimeSeriesId,
                            pIn_nIsPlusTimeSeries5    => pIn_nIsPlusTimeSeries5,
                            pIn_nNodeLevel            => pIn_nNodeLevel,
                            pIn_nChronology           => pIn_nChronology,
                            pIn_nPrecision            => pIn_nPrecision,
                            pOut_nSqlCode             => pOut_nSqlCode);
    FMP_LOG.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  END FMSP_validateFCSTByNodeList;

END FMP_ValidateFCST;
/
