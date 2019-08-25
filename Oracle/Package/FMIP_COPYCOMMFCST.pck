create or replace package FMIP_COPYCOMMFCST is

  -- Author  : LZHANG
  -- Created : 2/25/2013 2:57:38 PM
  -- Purpose :

  -- Public type declarations
  procedure FMISP_COPYTSID(pIn_nSelectionID       in number,
                           pIn_nHorizon           in number,
                           pIn_vFFirstPeriodTime  in varchar2,
                           pIn_vTFirstPeriodTime  in varchar2,
                           pIn_vConditions        in varchar2,
                           pIn_vFTimeSeriesIDS    in varchar2,
                           pIn_vTTimeSeriesID     in varchar2,
                           pIn_nDataOperationType in number,
                           pIn_vDefaultVersion    in varchar2,
                           pIn_nNodeLevel         in number,
                           pIn_nChronology        in number,
                           pIn_nOldCF             in number,
                           pOut_nSqlCode          out number);

end FMIP_COPYCOMMFCST;
/
create or replace package body FMIP_COPYCOMMFCST is
  TYPE aColumnList IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER;
  procedure FMSP_COPYDATAACTION(pIn_nFFirstMonth        in number,
                                pIn_nFFirstYear         in number,
                                pIn_nTFirstMonth        in number,
                                pIn_nTFirstYear         in number,
                                pIn_nHorizon            in number,
                                pIn_vFTimeSeriesID      in varchar2,
                                pIn_vTTimeSeriesID      in varchar2,
                                pIn_vFVersion           in varchar2,
                                pIn_vTVersion           in varchar2,
                                pIn_vNodeLevelTableName in varchar2,
                                pIn_vColumnName         in varchar2,
                                pIn_vTmpTableName       in varchar2,
                                pIn_vSeq                in varchar2,
                                pIn_vTableSeqColumnName in varchar2,
                                pIn_aColumnList         in aColumnList) IS
    --*****************************************************************
    -- Description:  it support for month copyTSID .   COPY  data for pIn_vTTimeSeriesID  OR VERSION
    --
    -- Parameters:
    --
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    declare
      cSql          clob;
      cSqlUpdateDel clob;
      cSqlInsertDel clob;
      nMaxYY        number := 0;
      nFFirstMonth  number := pIn_nFFirstMonth;
      nFFirstYear   number := pIn_nFFirstYear;
      nTFirstMonth  number := pIn_nTFirstMonth;
      nTFirstYear   number := pIn_nTFirstYear;
      nCountNum     number := 1;
    begin
      -- ALL DATE FROM pIn_nFirstYear
      if 0 = pIn_nHorizon then
        return;
      end if;
      loop
        -- EXIT BY pIn_nHorizon
        cSqlUpdateDel := ' DELETE WHERE V.T1 is NULL AND V.T2 is NULL AND V.T3 is NULL AND V.T4 is NULL AND V.T5 is NULL ' ||
                         'AND V.T6 is NULL AND V.T7 is NULL AND V.T8 is NULL AND V.T9 is NULL ' ||
                         'AND V.T10 is NULL AND V.T11 is NULL AND V.T12 is NULL ';

        cSql := 'select ' || pIn_aColumnList(nFFirstMonth) || ',' ||
                pIn_vColumnName || ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                pIn_vTmpTableName || ' WHERE YY=' || to_char(nFFirstYear) ||
                ' AND ' || pIn_vColumnName || '=ID AND TSID=' ||
                pIn_vFTimeSeriesID || ' AND VERSION=' || pIn_vFVersion;
        cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName || ' V USING (' || cSql ||
                ') T ON(V.TSID=' || to_char(pIn_vTTimeSeriesID) ||
                ' AND V.' || pIn_vColumnName || '=' || 'T.' ||
                pIn_vColumnName || ' AND V.VERSION=' || pIn_vTVersion ||
                ' AND V.YY=' || to_char(nTFirstYear) ||
                ') WHEN MATCHED THEN  UPDATE Set V.' ||
                pIn_aColumnList(nTFirstMonth) || '=T.' ||
                pIn_aColumnList(nFFirstMonth) || cSqlUpdateDel ||
                ' WHEN NOT MATCHED THEN INSERT (V.' ||
                pIn_vTableSeqColumnName || ',V.' ||
                pIn_aColumnList(nTFirstMonth) || ',V.TSID,V.' ||
                pIn_vColumnName || ',V.VERSION,V.YY)' || ' VALUES(' ||
                pIn_vSeq || '.nextval,T.' || pIn_aColumnList(nFFirstMonth) || ',' ||
                pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                pIn_vTVersion || ',' || to_char(nTFirstYear) || ')' ||
                ' WHERE T.' || pIn_aColumnList(nFFirstMonth) ||
                ' is not null';
        FMSP_ExecSql(cSql);

        nCountNum    := nCountNum + 1;
        nFFirstMonth := nFFirstMonth + 1;
        nTFirstMonth := nTFirstMonth + 1;
        if nFFirstMonth > 12 then
          nFFirstMonth := nFFirstMonth - 12;
          nFFirstYear  := nFFirstYear + 1;
        end if;
        if nTFirstMonth > 12 then
          nTFirstMonth := nTFirstMonth - 12;
          nTFirstYear  := nTFirstYear + 1;
        end if;
        if nMaxYY <> 0 then
          if nFFirstYear > nMaxYY then
            -- WHEN pIn_nHorizon=0 THEN nFFirstYear>nMaxYY EXIT
            exit;
          end if;
        else
          if nCountNum > pIn_nHorizon then
            -- WHEN pIn_nHorizon<>0 THEN nCountNum>pIn_nHorizon EXIT
            exit;
          end if;
        end if;
      END loop;
    end;
  END FMSP_COPYDATAACTION;
  procedure FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth    in number,
                                  pIn_nTFirstYear     in number,
                                  pIn_nTLastMonth     in number,
                                  pIn_nTLastYear      in number,
                                  pIn_vTmpTableName   in varchar2,
                                  pIn_nLevelTableName in varchar2,
                                  pIn_vColumnName     in varchar2,
                                  pIn_vTTimeSeriesID  in varchar2,
                                  pIn_vVersion        in varchar2,
                                  pIn_aColumnList     in aColumnList) IS
    --*****************************************************************
    -- Description:  it support for month copyTSID . blank data for pIn_vTTimeSeriesID
    --
    -- Parameters:
    --
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    declare
      cSql    clob;
      cSqlTmp clob;
      i       number;
    begin
      if pIn_nTLastMonth = 0 AND pIn_nTLastYear = 0 then
        -- horize = 0 operate all Begin FROM start date
        if pIn_nTFirstMonth = 1 then
          cSql := 'DELETE FROM ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                  to_char(pIn_nTFirstYear) || ' AND ' || pIn_vColumnName ||
                  ' in (select id from ' || pIn_vTmpTableName || ')' ||
                  ' AND TSID = ' || pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
        elsif pIn_nTFirstMonth <> 1 then
          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in pIn_nTFirstMonth .. 12 loop
            if i <> pIn_nTFirstMonth then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          --delete all month column is null
          cSql := 'DELETE FROM  ' || pIn_nLevelTableName;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
          cSql := '';
          cSql := 'DELETE FROM  ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                  to_char(pIn_nTFirstYear + 1) || ' AND ' ||
                  pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
        end if;
      elsif pIn_nTFirstMonth = 1 and pIn_nTLastMonth = 12 then
        -- =1 AND = 12
        cSql := 'DELETE FROM ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                to_char(pIn_nTFirstYear) || ' AND YY<=' ||
                to_char(pIn_nTLastYear) || ' AND ' || pIn_vColumnName ||
                ' in (select id from ' || pIn_vTmpTableName || ')' ||
                ' AND TSID = ' || pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                to_char(pIn_vVersion);
        FMSP_ExecSql(cSql);
      elsif pIn_nTFirstMonth = 1 and pIn_nTLastMonth <> 12 then
        -- =1 AND <>12
        cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
        for i in 1 .. pIn_nTLastMonth loop
          if i <> 1 then
            cSql := cSql || ',';
          end if;
          cSql := cSql || pIn_aColumnList(i) || ' = NULL';
        end loop;
        cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) || ' AND ' ||
                pIn_vColumnName || ' in (select id from ' ||
                pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                to_char(pIn_vVersion);
        FMSP_ExecSql(cSql);
        cSql := '';
        -- delete all month column is null
        cSql := 'DELETE FROM ' || pIn_nLevelTableName;
        cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) || ' AND ' ||
                pIn_vColumnName || ' in (select id from ' ||
                pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                to_char(pIn_vVersion) ||
                ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
        FMSP_ExecSql(cSql);
        cSql := '';
        if pIn_nTFirstYear <> pIn_nTLastYear then
          cSql := 'DELETE FROM ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                  to_char(pIn_nTFirstYear) || ' AND YY<=' ||
                  to_char(pIn_nTLastYear - 1) || ' AND ' || pIn_vColumnName ||
                  ' in (select id from ' || pIn_vTmpTableName || ')' ||
                  ' AND TSID = ' || pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
        end if;
      elsif pIn_nTFirstMonth <> 1 and pIn_nTLastMonth = 12 then
        -- <>1 AND =12
        cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
        for i in pIn_nTFirstMonth .. 12 loop
          if i <> pIn_nTFirstMonth then
            cSql := cSql || ',';
          end if;
          cSql := cSql || pIn_aColumnList(i) || ' = NULL';
        end loop;
        cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                to_char(pIn_vVersion);
        FMSP_ExecSql(cSql);
        cSql := '';
        -- delete all month column is null
        cSql := 'DELETE FROM ' || pIn_nLevelTableName;
        cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                to_char(pIn_vVersion) ||
                ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
        FMSP_ExecSql(cSql);
        cSql := '';
        if pIn_nTFirstYear <> pIn_nTLastYear then
          cSql := 'DELETE FROM  ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                  to_char(pIn_nTFirstYear + 1) || ' AND YY<=' ||
                  to_char(pIn_nTLastYear) || ' AND ' || pIn_vColumnName ||
                  ' in (select id from ' || pIn_vTmpTableName || ')' ||
                  ' AND TSID = ' || pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
        end if;
      elsif pIn_nTFirstMonth <> 1 and pIn_nTLastMonth <> 12 then
        -- <>1 AND <>12
        if pIn_nTFirstYear = pIn_nTLastYear then
          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in pIn_nTFirstMonth .. pIn_nTLastMonth loop
            if i <> pIn_nTFirstMonth then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          -- delete all month column is null
          cSql := 'DELETE FROM ' || pIn_nLevelTableName;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
        elsif pIn_nTFirstYear + 1 = pIn_nTLastYear then
          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in pIn_nTFirstMonth .. 12 loop
            if i <> pIn_nTFirstMonth then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          -- delete all month column is null
          cSql := 'DELETE FROM ' || pIn_nLevelTableName;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
          cSql := '';
          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in 1 .. pIn_nTLastMonth loop
            if i <> 1 then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          -- delete all month column is null
          cSql := 'DELETE FROM ' || pIn_nLevelTableName;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
        elsif pIn_nTFirstYear + 1 <= pIn_nTLastYear - 1 then
          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in pIn_nTFirstMonth .. 12 loop
            if i <> pIn_nTFirstMonth then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          -- delete all month column is null
          cSql := 'DELETE FROM  ' || pIn_nLevelTableName;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTFirstYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
          cSql := '';

          cSql := 'UPDATE ' || pIn_nLevelTableName || ' SET ';
          for i in 1 .. pIn_nTLastMonth loop
            if i <> 1 then
              cSql := cSql || ',';
            end if;
            cSql := cSql || pIn_aColumnList(i) || ' = NULL';
          end loop;
          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
          cSql := '';
          -- delete all month column is null
          cSql := 'DELETE FROM ' || pIn_nLevelTableName;

          cSql := cSql || ' WHERE  YY=' || to_char(pIn_nTLastYear) ||
                  ' AND ' || pIn_vColumnName || ' in (select id from ' ||
                  pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                  pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion) ||
                  ' AND T1 is NULL AND T2 is NULL AND T3 is NULL AND T4 is NULL AND T5 is NULL AND T6 is NULL AND T7 is NULL AND T8 is NULL AND T9 is NULL AND T10 is NULL AND T11 is NULL AND T12 is NULL';
          FMSP_ExecSql(cSql);
          cSql := '';
          cSql := 'DELETE FROM  ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                  to_char(pIn_nTFirstYear + 1) || ' AND YY<=' ||
                  to_char(pIn_nTLastYear - 1) || ' AND ' || pIn_vColumnName ||
                  ' in (select id from ' || pIn_vTmpTableName || ')' ||
                  ' AND TSID = ' || pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                  to_char(pIn_vVersion);
          FMSP_ExecSql(cSql);
        end if;
      end if;
    end;
  END FMSP_BLANKTSIDMONTHLY;
  procedure FMSP_COPYTSIDMONTHLY(pIn_nSelectionID       in number,
                                 pIn_nHorizon           in number,
                                 pIn_vFFirstPeriodTime  in varchar2,
                                 pIn_vTFirstPeriodTime  in varchar2,
                                 pIn_vConditions        in varchar2,
                                 pIn_vFTimeSeriesIDs    in varchar2,
                                 pIn_vTTimeSeriesID     in varchar2,
                                 pIn_nDataOperationType in number,
                                 pIn_vDefaultVersion    in varchar2,
                                 pIn_nNodeLevel         in number,
                                 pIn_nOldCF             in number,
                                 pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description:  it support for month copyTSID
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nHorizon ----  horizon greater or equal to 0. 0 means start From pIn_vFFirstPeriodTime end is the date of table's data
    -- pIn_vFFirstPeriodTime ---- the begin of FROM operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vTFirstPeriodTime ---- the begin of TO operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vFTimeSeriesID -- the FROM TSID
    -- pIn_vTTimeSeriesID -- the TO TSID
    -- pIn_nDataOperationType -- data operation type  0 means normal 1 means blank
    -- pIn_nAdjustment -- adjustment oty
    -- pIn_nAdjustmentType -- adjustment oty operation type 0 means normal 1 means percent
    -- pIn_nCopyType -- copy Type  0 means TSID=>TSID  1 means version=>version
    -- pIn_vFVersion -- FROM version
    -- pIn_vTVersion -- TO version
    -- pIn_vDefaultVersion --  default version
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    declare
      aColumnListResult   aColumnList;
      vNodeColumnName     varchar2(100) := 'SELID';
      vSeq                varchar2(10) := 'seq_prb_m';
      vTmpTableName       varchar2(40) := 'TB_TS_AggregateNodeCon';
      vNodeLevel          varchar2(5) := 'prb_m'; -- table_name default prb_m;
      vTableSeqColumnName varchar2(200) := 'prb_mid';
      vFFirstPeriodTime   varchar2(200) := pIn_vFFirstPeriodTime;
      vTFirstperiodTime   varchar2(200) := pIn_vTFirstPeriodTime;
      nFFirstMonth        number := 0; -- From firstMonth
      nTFirstMonth        number := 0; -- To firstMonth
      nFFirstYear         number := 0; -- From firstYear
      nTFirstYear         number := 0; -- To firstYear
      nFLastMonth         number := 0; -- From lastMonth
      nTLastMonth         number := 0; -- To lastMonth
      nFLastYear          number := 0; -- From lastYear
      nTLastYear          number := 0; -- To lastYear
      vFLastPeriodTime    varchar2(200) := '';
      vTLastPeriodTime    varchar2(200) := '';
      sNodeList           sys_refcursor; -- this variable is a cursor for get result of TB_TS_DetailNodeSelCdt
      nSqlCode            number; -- this variable is the parameter for analyze
      TYPE aNodeList IS TABLE OF NUMBER INDEX BY BINARY_INTEGER; -- an array Type stored number
      aNodeListResult aNodeList; -- an arrary for store the result of TB_TS_DetailNodeSelCdt
      cSqlBlank       clob;
      vTabName        varchar2(200) := '';
      nHorizon        number := pIn_nHorizon;
      vFTSIDOne       varchar2(200);
      vFTSIDTwo       varchar2(200);
      nHorizonPart    number := 0;
      nTSIDIndex      number := 0;
    begin
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
      if pIn_nOldCF > 0 then
        vFFirstPeriodTime := to_char(add_months(to_date(vFFirstPeriodTime,
                                                        'YYYYMM'),
                                                pIn_nOldCF),
                                     'YYYYMM');
        nHorizon          := nHorizon - pIn_nOldCF;
      end if;
      nHorizonPart      := months_between(to_date(vTFirstPeriodTime,
                                                  'YYYYMM'),
                                          to_date(vFFirstPeriodTime,
                                                  'YYYYMM')) + 1;
      vTFirstPeriodTime := to_char(add_months(to_date(vTFirstPeriodTime,
                                                      'YYYYMM'),
                                              1),
                                   'YYYYMM');
      nFFirstMonth      := to_number(substr(vFFirstPeriodTime, 5));
      nTFirstMonth      := to_number(substr(vTFirstPeriodTime, 5));
      nFFirstYear       := to_number(substr(vFFirstPeriodTime, 0, 4));
      nTFirstYear       := to_number(substr(vTFirstPeriodTime, 0, 4));

      if pIn_nHorizon > 0 then
        vFLastPeriodTime := to_char(add_months(to_date(vFFirstPeriodTime,
                                                       'YYYYMM'),
                                               nHorizon - 1),
                                    'YYYYMM');
        nFLastMonth      := to_number(substr(vFLastPeriodTime, 5));
        nFLastYear       := to_number(substr(vFLastPeriodTime, 0, 4));
      end if;
      --deal with From TSIDS
      nTSIDIndex := instr(pIn_vFTimeSeriesIDs, ',');
      if nTSIDIndex <> 0 then
        vFTSIDOne := substr(pIn_vFTimeSeriesIDs, 0, nTSIDIndex-1);
        vFTSIDTwo := substr(pIn_vFTimeSeriesIDs, nTSIDIndex + 1);
      else
        vFTSIDOne := pIn_vFTimeSeriesIDs;
      end if;
      -- judge prb_m or don_m  analyze p_conditions for nodelist
      if pIn_nNodeLevel = 1 then
        vNodeLevel          := 'DON_M';
        vNodeColumnName     := 'PVTID';
        vSeq                := 'seq_don_m';
        vTmpTableName       := 'TB_TS_DetailNodeSelCdt';
        vTableSeqColumnName := 'DON_MID';
        p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => pIn_nSelectionID,
                                             P_Conditions  => pIn_vConditions,
                                             P_Sequence    => null, --Sort sequence
                                             p_DetailNode  => sNodeList,
                                             pOut_vTabName => vTabName,
                                             p_SqlCode     => nSqlCode);

      else
        p_aggregation.FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => pIn_nSelectionID,
                                                pIn_vConditions => pIn_vConditions,
                                                pOut_Nodes      => sNodeList,
                                                pOut_nSqlCode   => nSqlCode);

      end if;
      -- get the result of TB_TS_DetailNodeSelCdt (NODELIST)
      open sNodeList for 'select id FROM ' || vTmpTableName;
      fetch sNodeList bulk collect
        into aNodeListresult;
      if aNodeListResult.count = 0 then
        return;
      end if;
      -- juge pIn_nDataOperationType
      if 1 = pIn_nDataOperationType then
        if nTSIDIndex <> 0 then
          FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
                              pIn_nFFirstYear         => nFFirstYear,
                              pIn_nTFirstMonth        => nFFirstMonth,
                              pIn_nTFirstYear         => nFFirstYear,
                              pIn_nHorizon            => nHorizonPart,
                              pIn_vFTimeSeriesID      => vFTSIDOne,
                              pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                              pIn_vFVersion           => pIn_vDefaultVersion,
                              pIn_vTVersion           => pIn_vDefaultVersion,
                              pIn_vNodeLevelTableName => vNodeLevel,
                              pIn_vColumnName         => vNodeColumnName,
                              pIn_vTmpTableName       => vTmpTableName,
                              pIn_vSeq                => vSeq,
                              pIn_vTableSeqColumnName => vTableSeqColumnName,
                              pIn_aColumnList         => aColumnListResult);
          FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth    => nTFirstMonth,
                                pIn_nTFirstYear     => nTFirstYear,
                                pIn_nTLastMonth     => nFLastMonth,
                                pIn_nTLastYear      => nFLastYear,
                                pIn_vTmpTableName   => vTmpTableName,
                                pIn_nLevelTableName => vNodeLevel,
                                pIn_vColumnName     => vNodeColumnName,
                                pIn_vTTimeSeriesID  => pIn_vTTimeSeriesID,
                                pIn_vVersion        => pIn_vDefaultVersion,
                                pIn_aColumnList     => aColumnListResult);
        else
          FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth    => nFFirstMonth,
                                pIn_nTFirstYear     => nFFirstYear,
                                pIn_nTLastMonth     => nFLastMonth,
                                pIn_nTLastYear      => nFLastYear,
                                pIn_vTmpTableName   => vTmpTableName,
                                pIn_nLevelTableName => vNodeLevel,
                                pIn_vColumnName     => vNodeColumnName,
                                pIn_vTTimeSeriesID  => pIn_vTTimeSeriesID,
                                pIn_vVersion        => pIn_vDefaultVersion,
                                pIn_aColumnList     => aColumnListResult);
        end if;
      elsif 0 = pIn_nDataOperationType then
        -- NORMAL
        if nTSIDIndex <> 0 then
          FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
                              pIn_nFFirstYear         => nFFirstYear,
                              pIn_nTFirstMonth        => nFFirstMonth,
                              pIn_nTFirstYear         => nFFirstYear,
                              pIn_nHorizon            => nHorizonPart,
                              pIn_vFTimeSeriesID      => vFTSIDOne,
                              pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                              pIn_vFVersion           => pIn_vDefaultVersion,
                              pIn_vTVersion           => pIn_vDefaultVersion,
                              pIn_vNodeLevelTableName => vNodeLevel,
                              pIn_vColumnName         => vNodeColumnName,
                              pIn_vTmpTableName       => vTmpTableName,
                              pIn_vSeq                => vSeq,
                              pIn_vTableSeqColumnName => vTableSeqColumnName,
                              pIn_aColumnList         => aColumnListResult);
          FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nTFirstMonth,
                              pIn_nFFirstYear         => nTFirstYear,
                              pIn_nTFirstMonth        => nTFirstMonth,
                              pIn_nTFirstYear         => nTFirstYear,
                              pIn_nHorizon            => nHorizon -
                                                         nHorizonPart,
                              pIn_vFTimeSeriesID      => vFTSIDTwo,
                              pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                              pIn_vFVersion           => pIn_vDefaultVersion,
                              pIn_vTVersion           => pIn_vDefaultVersion,
                              pIn_vNodeLevelTableName => vNodeLevel,
                              pIn_vColumnName         => vNodeColumnName,
                              pIn_vTmpTableName       => vTmpTableName,
                              pIn_vSeq                => vSeq,
                              pIn_vTableSeqColumnName => vTableSeqColumnName,
                              pIn_aColumnList         => aColumnListResult);
        else
          FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
                              pIn_nFFirstYear         => nFFirstYear,
                              pIn_nTFirstMonth        => nFFirstMonth,
                              pIn_nTFirstYear         => nFFirstYear,
                              pIn_nHorizon            => nHorizon,
                              pIn_vFTimeSeriesID      => vFTSIDOne,
                              pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                              pIn_vFVersion           => pIn_vDefaultVersion,
                              pIn_vTVersion           => pIn_vDefaultVersion,
                              pIn_vNodeLevelTableName => vNodeLevel,
                              pIn_vColumnName         => vNodeColumnName,
                              pIn_vTmpTableName       => vTmpTableName,
                              pIn_vSeq                => vSeq,
                              pIn_vTableSeqColumnName => vTableSeqColumnName,
                              pIn_aColumnList         => aColumnListResult);
        end if;
      end if;
    end;
  END FMSP_COPYTSIDMONTHLY;
  procedure FMSP_COPYTSIDWEEKLY(pIn_nSelectionID       in number,
                                pIn_nHorizon           in number,
                                pIn_vFFirstPeriodTime  in varchar2,
                                pIn_vTFirstPeriodTime  in varchar2,
                                pIn_vConditions        in varchar2,
                                pIn_vFTimeSeriesIDs    in varchar2,
                                pIn_vTTimeSeriesID     in varchar2,
                                pIn_nDataOperationType in number,
                                pIn_vDefaultVersion    in varchar2,
                                pIn_nNodeLevel         in number,
                                pIn_nOldCF             in number,
                                pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description:  it support for  week copyTSID
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nHorizon ----  horizon
    -- pIn_vFFirstPeriodTime ---- the begin of FROM operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vTFirstPeriodTime ---- the begin of TO operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vFTimeSeriesID -- the FROM TSID
    -- pIn_vTTimeSeriesID -- the TO TSID
    -- pIn_nDataOperationType -- data operation type  0 means normal 1 means blank
    -- pIn_nAdjustment -- adjustment oty
    -- pIn_nAdjustmentType -- adjustment oty operation type 0 means normal 1 means percent
    -- pIn_vFVersion -- FROM version
    -- pIn_vTVersion -- TO version
    -- pIn_vDefaultVersion --  default version
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_COPYTSIDWEEKLY;

  procedure FMSP_COPYTSIDDAILY(pIn_nSelectionID       in number,
                               pIn_nHorizon           in number,
                               pIn_vFFirstPeriodTime  in varchar2,
                               pIn_vTFirstPeriodTime  in varchar2,
                               pIn_vConditions        in varchar2,
                               pIn_vFTimeSeriesIDs    in varchar2,
                               pIn_vTTimeSeriesID     in varchar2,
                               pIn_nDataOperationType in number,
                               pIn_vDefaultVersion    in varchar2,
                               pIn_nNodeLevel         in number,
                               pIn_nOldCF             in number,
                               pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description:  it support for   day copyTSID
    --
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nHorizon ----  horizon
    -- pIn_vFFirstPeriodTime ---- the begin of FROM operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vTFirstPeriodTime ---- the begin of TO operation time
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vFTimeSeriesID -- the FROM TSID
    -- pIn_vTTimeSeriesID -- the TO TSID
    -- pIn_nDataOperationType -- data operation type  0 means normal 1 means blank
    -- pIn_nAdjustment -- adjustment oty
    -- pIn_nAdjustmentType -- adjustment oty operation type 0 means normal 1 means percent
    -- pIn_vFVersion -- FROM version
    -- pIn_vTVersion -- TO version
    -- pIn_vDefaultVersion --  default version
    -- pIn_nNodeLevel ---- -- 0- Detail Level
    -- 1- Aggregate Level
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    null;
  END FMSP_COPYTSIDDAILY;

  procedure FMISP_COPYTSID(pIn_nSelectionID       in number,
                           pIn_nHorizon           in number,
                           pIn_vFFirstPeriodTime  in varchar2,
                           pIn_vTFirstPeriodTime  in varchar2,
                           pIn_vConditions        in varchar2,
                           pIn_vFTimeSeriesIDS    in varchar2,
                           pIn_vTTimeSeriesID     in varchar2,
                           pIn_nDataOperationType in number,
                           pIn_vDefaultVersion    in varchar2,
                           pIn_nNodeLevel         in number,
                           pIn_nChronology        in number,
                           pIn_nOldCF             in number,
                           pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description: this  is the interface of  copyTSID. it support for month copyTSID  AND  week copyTSID  AND  day copyTSID
    -- it support for initialize commericial forecasting and revise commericial forcasts in batch and prepare commercial forecasts in batch
    -- Parameters:
    -- pIn_nSelectionID ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_nHorizon ----  horizon
    -- pIn_vFFirstPeriodTime ---- the begin of FROM operation time
    -- YYYYMM month example 201209
    -- YYYYWW weekly example 201221
    -- YYYYDD day example 201211
    -- pIn_vTFirstPeriodTime ---- the end of times
    -- example: 201204,201305,201107 or 201204
    -- the format of this parameter is same as pIn_nFirstPeriodTime
    -- pIn_vConditions ---- the same with the parameter of P_SELECTION.SP_GetDetailNodeBySelCdt to filter which nodes should be validated.
    -- pIn_vFTimeSeriesID -- the FROM TSIDS divide by ',' . including Actual
    -- example:  123,1233,155 or 123
    -- pIn_vTTimeSeriesID -- the TO TSID
    -- pIn_nDataOperationType -- data operation type  0 means normal 1 means blank
    -- pIn_vDefaultVersion --  default version
    -- pIn_nNodeLevel ---- -- 1- Detail Level
    -- 2- Aggregate Level
    -- pIn_nChronology ---------- it mark month or week or day
    --- 1 means month
    --- 2 means week
    --- 4 means day
    -- pIn_nOldCF ----------------- old commercial forecast is retained
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        25-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    FMP_LOG.FMP_SETVALUE(pIn_nSelectionID);
    FMP_LOG.FMP_SETVALUE(pIn_nHorizon);
    FMP_LOG.FMP_SETVALUE(pIn_vFFirstPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vTFirstPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vConditions);
    FMP_LOG.FMP_SETVALUE(pIn_vFTimeSeriesIDs);
    FMP_LOG.FMP_SETVALUE(pIn_vTTimeSeriesID);
    FMP_LOG.FMP_SETVALUE(pIn_nDataOperationType);
    FMP_LOG.FMP_SETVALUE(pIn_vDefaultVersion);
    FMP_LOG.FMP_SETVALUE(pIn_nNodeLevel);
    FMP_LOG.FMP_SETVALUE(pIn_nChronology);
    FMP_LOG.FMP_SETVALUE(pIn_nOldCF);
    FMP_log.logBegin;
    if p_constant.Monthly = pIn_nChronology then
      FMSP_COPYTSIDMONTHLY(pIn_nSelectionID       => pIn_nSelectionID,
                           pIn_nHorizon           => pIn_nHorizon,
                           pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                           pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                           pIn_vConditions        => pIn_vConditions,
                           pIn_vFTimeSeriesIDs    => pIn_vFTimeSeriesIDs,
                           pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                           pIn_nDataOperationType => pIn_nDataOperationType,
                           pIn_vDefaultVersion    => pIn_vDefaultVersion,
                           pIn_nNodeLevel         => pIn_nNodeLevel,
                           pIn_nOldCF             => pIn_nOldCF,
                           pOut_nSqlCode          => pOut_nSqlCode);
    elsif p_constant.Weekly = pIn_nChronology then
      FMSP_COPYTSIDWEEKLY(pIn_nSelectionID       => pIn_nSelectionID,
                          pIn_nHorizon           => pIn_nHorizon,
                          pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                          pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                          pIn_vConditions        => pIn_vConditions,
                          pIn_vFTimeSeriesIDs    => pIn_vFTimeSeriesIDs,
                          pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                          pIn_nDataOperationType => pIn_nDataOperationType,
                          pIn_vDefaultVersion    => pIn_vDefaultVersion,
                          pIn_nNodeLevel         => pIn_nNodeLevel,
                          pIn_nOldCF             => pIn_nOldCF,
                          pOut_nSqlCode          => pOut_nSqlCode);
    elsif p_constant.Daily = pIn_nChronology then
      FMSP_COPYTSIDDAILY(pIn_nSelectionID       => pIn_nSelectionID,
                         pIn_nHorizon           => pIn_nHorizon,
                         pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                         pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                         pIn_vConditions        => pIn_vConditions,
                         pIn_vFTimeSeriesIDs    => pIn_vFTimeSeriesIDs,
                         pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                         pIn_nDataOperationType => pIn_nDataOperationType,
                         pIn_vDefaultVersion    => pIn_vDefaultVersion,
                         pIn_nNodeLevel         => pIn_nNodeLevel,
                         pIn_nOldCF             => pIn_nOldCF,
                         pOut_nSqlCode          => pOut_nSqlCode);
    end if;
    FMP_LOG.LOGEND;
    pOut_nSqlCode := 0;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  END FMISP_COPYTSID;
end FMIP_COPYCOMMFCST;
/
