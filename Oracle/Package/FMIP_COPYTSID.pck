create or replace package FMIP_COPYTSID is

  -- Author  : LZHANG
  -- Created : 2/20/2013 2:52:20 PM
  -- Purpose :

  -- Public type declarations

  procedure FMISP_COPYTSID(pIn_nSelectionID       in number,
                           pIn_nHorizon           in number,
                           pIn_vFFirstPeriodTime  in varchar2,
                           pIn_vTFirstPeriodTime  in varchar2,
                           pIn_vConditions        in varchar2,
                           pIn_vFTimeSeriesID     in varchar2,
                           pIn_vTTimeSeriesID     in varchar2,
                           pIn_nDataOperationType in number,
                           pIn_nAdjustment        in number,
                           pIn_nAdjustmentType    in number,
                           pIn_nCopyType          in number,
                           pIn_vFVersion          in varchar2,
                           pIn_vTVersion          in varchar2,
                           pIn_vDefaultVersion    in varchar2,
                           pIn_nNodeLevel         in number,
                           pIn_nChronology        in number,
                           pIn_nDecimal           in number,
                           pOut_nSqlCode          out number);
end FMIP_COPYTSID;
/
create or replace package body FMIP_COPYTSID is
  TYPE aColumnList IS TABLE OF varchar2(10) INDEX BY BINARY_INTEGER;
  Function FMF_GetMaxYear(pIn_vVersion            in varchar2,
                          pIn_nFirstYear          in number,
                          pIn_vTmpTableName       in varchar2,
                          pIn_vTimeSeriesID       in varchar2,
                          pIn_vNodeLevelTableName in varchar2,
                          pIn_vColumnName         in varchar2) Return number IS
    --*****************************************************************
    -- Description:  it support for month copyTSID . Get MAX Year
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
    nMaxYY  number;
    sCursor sys_refcursor;
    vSql    varchar2(4000);
  begin
    vSql := 'select MAX(yy) from ' || pIn_vNodeLevelTableName || ',' ||
            pIn_vTmpTableName || ' where ' || pIn_vColumnName || '= id ' ||
            ' AND version=' || pIn_vVersion || ' AND YY>=' ||
            to_char(pIn_nFirstYear) || ' AND TSID=' || pIn_vTimeSeriesID ||
            ' AND YY IS NOT NULL';

    execute immediate vSql
      into nMaxYY;
    if nMaxYY is null then
      nMaxYY := pIn_nFirstYear;
    end if;
    return nMaxYY;
  END FMF_GetMaxYear;

  procedure FMSP_COPYDATAACTIONSSB(pIn_nFFirstMonth        in number,
                                   pIn_nFFirstYear         in number,
                                   pIn_nFLastMonth         in number,
                                   pIn_nFLastYear          in number,
                                   pIn_nTFirstMonth        in number,
                                   pIn_nTFirstYear         in number,
                                   pIn_nTLastMonth         in number,
                                   pIn_nTLastYear          in number,
                                   pIn_nHorizon            in number,
                                   pIn_vFTimeSeriesID      in varchar2,
                                   pIn_vTTimeSeriesID      in varchar2,
                                   pIn_nAdjustment         in number,
                                   pIn_nAdjustmentType     in number,
                                   pIn_vFVersion           in varchar2,
                                   pIn_vTVersion           in varchar2,
                                   pIn_vNodeLevelTableName in varchar2,
                                   pIn_vColumnName         in varchar2,
                                   pIn_vTmpTableName       in varchar2,
                                   pIn_vSeq                in varchar2,
                                   pIn_vTableSeqColumnName in varchar2,
                                   pIn_nDecimal            in number,
                                   pIn_aColumnList         in aColumnList,
                                   pIn_nDataOperationType  in number) IS
    --*****************************************************************
    -- Description:  it support for month copyTSID . WHEN FTSID=VTSID ,TVERSION=FVERSION, FTIME period AND TTIME period be mixed
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
      delete from tb_ts_validatefcstm;

      cSql := 'insert into tb_ts_validatefcstm(nodeid,tsid,YY,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12) select  ' ||
              pIn_vColumnName ||
              ', tsid,YY,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12 FROM ' ||
              pIn_vTmpTableName || ',' || pIn_vNodeLevelTableName ||
              ' WHERE id=' || pIn_vColumnName || ' AND YY>=' ||
              to_char(pIn_nFFirstYear) || ' AND YY<=' ||
              to_char(pIn_nFLastYear) || ' AND TSID=' || pIn_vFTimeSeriesID ||
              ' AND VERSION=' || pIn_vFVersion;
      FMSP_ExecSql(cSql);
      --debug
      /*delete from nodetmp;
      insert into nodetmp
        select * from tb_ts_validatefcstm;
      commit;*/
      --debug
      -- ALL DATE FROM pIn_nFirstYear
      if 0 = pIn_nHorizon then
        nMaxYY := FMF_GetMaxYear(pIn_vVersion            => pIn_vFVersion,
                                 pIn_nFirstYear          => pIn_nFFirstYear,
                                 pIn_vTmpTableName       => pIn_vTmpTableName,
                                 pIn_vTimeSeriesID       => pIn_vFTimeSeriesID,
                                 pIn_vNodeLevelTableName => pIn_vNodeLevelTableName,
                                 pIn_vColumnName         => pIn_vColumnName);
      end if;
      loop
        -- EXIT BY pIn_nHorizon
        cSqlUpdateDel := ' DELETE WHERE V.T1 is NULL AND V.T2 is NULL AND V.T3 is NULL AND V.T4 is NULL AND V.T5 is NULL ' ||
                         'AND V.T6 is NULL AND V.T7 is NULL AND V.T8 is NULL AND V.T9 is NULL ' ||
                         'AND V.T10 is NULL AND V.T11 is NULL AND V.T12 is NULL ';

        if pIn_nAdjustmentType = 0 then
          --normal
          if pIn_nDataOperationType = 0 then
            cSql := 'select rownum,' || pIn_aColumnList(nFFirstMonth) || ',' ||
                    pIn_vColumnName || ' FROM(select nvl(' ||
                    pIn_aColumnList(nFFirstMonth) || ',0) +' ||
                    to_char(pIn_nAdjustment) || ' AS ' ||
                    pIn_aColumnList(nFFirstMonth) || ', nodeid as ' ||
                    pIn_vColumnName || ' FROM ' || 'tb_ts_validatefcstm' || ',' ||
                    pIn_vTmpTableName || ' WHERE YY=' ||
                    to_char(nFFirstYear) || ' AND ' || ' nodeid' ||
                    '=ID AND TSID=' || pIn_vFTimeSeriesID ||
                    ' UNION (select 0+' || to_char(pIn_nAdjustment) ||
                    ' AS ' || pIn_aColumnList(nFFirstMonth) || ',id as' ||
                    pIn_vColumnName || ' FROM ' || pIn_vTmpTableName ||
                    ' minus select 0 +' || to_char(pIn_nAdjustment) ||
                    ' AS ' || pIn_aColumnList(nFFirstMonth) ||
                    ',nodeid as ' || pIn_vColumnName || ' FROM ' ||
                    'tb_ts_validatefcstm' || ',' || pIn_vTmpTableName ||
                    ' WHERE YY=' || to_char(nFFirstYear) || ' AND ' ||
                    'NODEID' || '=ID AND TSID=' || pIn_vFTimeSeriesID || '))';

          end if;
        elsif pIn_nAdjustmentType = 1 then
          --percent
          cSql := 'select rownum,' || pIn_aColumnList(nFFirstMonth) || ' *' ||
                  to_char(1 + pIn_nAdjustment / 100) || ' AS ' ||
                  pIn_aColumnList(nFFirstMonth) || ',nodeid as ' ||
                  pIn_vColumnName || ' FROM ' || 'tb_ts_validatefcstm' || ',' ||
                  pIn_vTmpTableName || ' WHERE YY=' || to_char(nFFirstYear) ||
                  ' AND ' || 'nodeid' || '=ID AND TSID=' ||
                  pIn_vFTimeSeriesID || ' AND ' ||
                  pIn_aColumnList(nFFirstMonth) || ' IS NOT NULL ';
        end if;
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
  END FMSP_COPYDATAACTIONSSB;
  procedure FMSP_COPYDATAACTION(pIn_nFFirstMonth        in number,
                                pIn_nFFirstYear         in number,
                                pIn_nFLastMonth         in number,
                                pIn_nFLastYear          in number,
                                pIn_nTFirstMonth        in number,
                                pIn_nTFirstYear         in number,
                                pIn_nTLastMonth         in number,
                                pIn_nTLastYear          in number,
                                pIn_nHorizon            in number,
                                pIn_vFTimeSeriesID      in varchar2,
                                pIn_vTTimeSeriesID      in varchar2,
                                pIn_nAdjustment         in number,
                                pIn_nAdjustmentType     in number,
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
      nFLastMonth   number := pIn_nFLastMonth;
      nFLastYear    number := pIn_nFLastYear;
      nFLastUseYear number := 0;
      nTFirstMonth  number := pIn_nTFirstMonth;
      nTFirstYear   number := pIn_nTFirstYear;
      nTLastMonth   number := pIn_nTLastMonth;
      nTLastYear    number := pIn_nTLastYear;
      nTLastUseYear number := 0;
      nCountNum     number := 1;
      nCycleIndex   number;
      nDivide       number := 0;
      nResidue      number := 0;
      nHorizon      number := pIn_nHorizon;
      i             number;
      cSqlTmp       clob;
      cSqlFWhere    clob;
      cSqlTOn       clob;
      nInterval     number;
    begin
      -- ALL DATE FROM pIn_nFirstYear
      if pIn_nHorizon = 0 then
        nMaxYY := FMF_GetMaxYear(pIn_vVersion            => pIn_vFVersion,
                                 pIn_nFirstYear          => pIn_nFFirstYear,
                                 pIn_vTmpTableName       => pIn_vTmpTableName,
                                 pIn_vTimeSeriesID       => pIn_vFTimeSeriesID,
                                 pIn_vNodeLevelTableName => pIn_vNodeLevelTableName,
                                 pIn_vColumnName         => pIn_vColumnName);
        if nMaxYY <> nFFirstYear then
          nHorizon := (nMaxYY - nFFirstYear) * 12 - nFFirstMonth + 1;
        else
          nHorizon := 12 - nFFirstMonth + 1;
        end if;
      end if;
      nDivide  := trunc(nHorizon / 12);
      nResidue := mod(nHorizon, 12);
      if nDivide = 0 then
        nCycleIndex := nHorizon;
      else
        nCycleIndex := 12;
      end if;
      nInterval := months_between(to_date(to_char(nTFirstYear) ||
                                          to_char(nTFirstMonth),
                                          'YYYYMM'),
                                  to_date(to_char(nFFirstYear) ||
                                          to_char(nFFirstMonth),
                                          'YYYYMM'));
      For i in 1 .. nCycleIndex Loop
        if nDivide > 0 then
          nFLastUseYear := nFLastYear;
          if i < nResidue then
            nFLastUseYear := nFLastUseYear - 1;
          end if;
        else
          nFlastUseYear := nFlastYear;
          if nFFirstYear <> nFLastYear AND nFFirstMonth < pIn_nFFirstMonth then
            nFLastUseYear := nFLastUseYear - 1;
          end if;
        end if;
        if pIn_nAdjustmentType = 0 then
          --normal
          cSql := 'select YY,nvl(' || pIn_aColumnList(nFFirstMonth) ||
                  ',0) +' || to_char(pIn_nAdjustment) || ' AS ' ||
                  pIn_aColumnList(nFFirstMonth) || ',' || pIn_vColumnName || ',' ||
                  'to_date(YY ||' || to_char(nFFirstMonth) ||
                  ', ''YYYYMM'')' || 'AS monthDate ' || ' FROM ' ||
                  pIn_vNodeLevelTableName || ',' || pIn_vTmpTableName ||
                  ' WHERE YY>=' || to_char(nFFirstYear) || ' AND YY<=' ||
                  to_char(nFLastUseYear) || ' AND ' || pIn_vColumnName ||
                  '=ID AND TSID=' || pIn_vFTimeSeriesID || ' AND VERSION=' ||
                  pIn_vFVersion;
        elsif pIn_nAdjustmentType = 1 then
          --percent
          cSql := 'select YY,' || pIn_aColumnList(nFFirstMonth) || ' *' ||
                  to_char(1 + pIn_nAdjustment / 100) || ' AS ' ||
                  pIn_aColumnList(nFFirstMonth) || ',' || pIn_vColumnName || ',' ||
                  'to_date(YY ||' || to_char(nFFirstMonth) ||
                  ', ''YYYYMM'')' || 'AS monthDate ' || ' FROM ' ||
                  pIn_vNodeLevelTableName || ',' || pIn_vTmpTableName ||
                  ' WHERE YY>=' || to_char(nFFirstYear) || ' AND YY<=' ||
                  to_char(nFLastUseYear) || ' AND ' || pIn_vColumnName ||
                  '=ID AND TSID=' || pIn_vFTimeSeriesID || ' AND VERSION=' ||
                  pIn_vFVersion;
        end if;
        cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName || ' V USING (' || cSql ||
                ') T ON(V.TSID=' || to_char(pIn_vTTimeSeriesID) ||
                ' AND V.' || pIn_vColumnName || '=' || 'T.' ||
                pIn_vColumnName || ' AND V.VERSION=' || pIn_vTVersion ||
                ' AND V.YY>=' || to_char(nTFirstYear) || ' AND V.YY<=' ||
                to_char(nTLastYear) ||
                ' AND months_between(to_date(to_char(' || 'V.YY' ||
                ')||to_char(' || nTFirstMonth ||
                '),''YYYYMM'') ,to_date(to_char(' || 'T.YY' ||
                ')||to_char(' || nFFirstMonth || '),''YYYYMM''))=' ||
                to_char(nInterval) || ') WHEN MATCHED THEN  UPDATE Set V.' ||
                pIn_aColumnList(nTFirstMonth) || '=T.' ||
                pIn_aColumnList(nFFirstMonth) || cSqlUpdateDel ||
                ' WHEN NOT MATCHED THEN INSERT (V.' ||
                pIn_vTableSeqColumnName || ',V.' ||
                pIn_aColumnList(nTFirstMonth) || ',V.TSID,V.' ||
                pIn_vColumnName || ',V.VERSION,V.YY)' || ' VALUES(' ||
                pIn_vSeq || '.nextval,T.' || pIn_aColumnList(nFFirstMonth) || ',' ||
                pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                pIn_vTVersion || ',' ||
                'to_char( add_months(to_date(T.YY||to_char(' ||
                nFFirstMonth || '),''YYYYMM''),' || nInterval ||
                '),''YYYY'')' || ')' || ' WHERE T.' ||
                pIn_aColumnList(nFFirstMonth) || ' is not null ' || ' AND ' ||
                'to_char( add_months(to_date(T.YY||to_char(' ||
                nFFirstMonth || '),''YYYYMM''),' || nInterval ||
                '),''YYYY'')<=' || to_char(pIn_nFLastYear);
        FMSP_ExecSql(cSql);
        --debug
        /*fmp_log.LOGERROR(cSql);*/
        --debug
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
      END Loop;
    end;
  END FMSP_COPYDATAACTION;

  procedure FMSP_COPYDATAACTION(pIn_nFFirstMonth        in number,
                                pIn_nFFirstYear         in number,
                                pIn_nTFirstMonth        in number,
                                pIn_nTFirstYear         in number,
                                pIn_nHorizon            in number,
                                pIn_vFTimeSeriesID      in varchar2,
                                pIn_vTTimeSeriesID      in varchar2,
                                pIn_nAdjustment         in number,
                                pIn_nAdjustmentType     in number,
                                pIn_vFVersion           in varchar2,
                                pIn_vTVersion           in varchar2,
                                pIn_vNodeLevelTableName in varchar2,
                                pIn_vColumnName         in varchar2,
                                pIn_vTmpTableName       in varchar2,
                                pIn_vSeq                in varchar2,
                                pIn_vTableSeqColumnName in varchar2,
                                pIn_nDecimal            in number,
                                pIn_aColumnList         in aColumnList,
                                pIn_nDataOperationType  in number) IS
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
        nMaxYY := FMF_GetMaxYear(pIn_vVersion            => pIn_vFVersion,
                                 pIn_nFirstYear          => pIn_nFFirstYear,
                                 pIn_vTmpTableName       => pIn_vTmpTableName,
                                 pIn_vTimeSeriesID       => pIn_vFTimeSeriesID,
                                 pIn_vNodeLevelTableName => pIn_vNodeLevelTableName,
                                 pIn_vColumnName         => pIn_vColumnName);
      end if;
      loop
        -- EXIT BY pIn_nHorizon
        cSqlUpdateDel := ' DELETE WHERE V.T1 is NULL AND V.T2 is NULL AND V.T3 is NULL AND V.T4 is NULL AND V.T5 is NULL ' ||
                         'AND V.T6 is NULL AND V.T7 is NULL AND V.T8 is NULL AND V.T9 is NULL ' ||
                         'AND V.T10 is NULL AND V.T11 is NULL AND V.T12 is NULL ';

        if pIn_nAdjustmentType = 0 then
          --normal
          if pIn_nDataOperationType = 0 then
            cSql := 'select nvl(' || pIn_aColumnList(nFFirstMonth) ||
                    ',0) +' || to_char(pIn_nAdjustment) || ' AS ' ||
                    pIn_aColumnList(nFFirstMonth) || ',' || pIn_vColumnName ||
                    ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                    pIn_vTmpTableName || ' WHERE YY=' ||
                    to_char(nFFirstYear) || ' AND ' || pIn_vColumnName ||
                    '=ID AND TSID=' || pIn_vFTimeSeriesID ||
                    ' AND VERSION=' || pIn_vFVersion || ' UNION (select 0+' ||
                    to_char(pIn_nAdjustment) || ' AS ' ||
                    pIn_aColumnList(nFFirstMonth) || ',id as' ||
                    pIn_vColumnName || ' FROM ' || pIn_vTmpTableName ||
                    ' minus select 0 +' || to_char(pIn_nAdjustment) ||
                    ' AS ' || pIn_aColumnList(nFFirstMonth) || ',' ||
                    pIn_vColumnName || ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                    pIn_vTmpTableName || ' WHERE YY=' ||
                    to_char(nFFirstYear) || ' AND ' || pIn_vColumnName ||
                    '=ID AND TSID=' || pIn_vFTimeSeriesID ||
                    ' AND VERSION=' || pIn_vFVersion || ')';
          elsif pIn_nDataOperationType = 1 then
            cSql := 'select 0+' || to_char(pIn_nAdjustment) || ' AS ' ||
                    pIn_aColumnList(nFFirstMonth) || ',' || pIn_vColumnName ||
                    ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                    pIn_vTmpTableName || ' WHERE YY=' ||
                    to_char(nFFirstYear) || ' AND ' || pIn_vColumnName ||
                    '=ID AND TSID=' || pIn_vFTimeSeriesID ||
                    ' AND VERSION=' || pIn_vFVersion || ' UNION (select 0+' ||
                    to_char(pIn_nAdjustment) || ' AS ' ||
                    pIn_aColumnList(nFFirstMonth) || ',id as' ||
                    pIn_vColumnName || ' FROM ' || pIn_vTmpTableName ||
                    ' minus select 0 +' || to_char(pIn_nAdjustment) ||
                    ' AS ' || pIn_aColumnList(nFFirstMonth) || ',' ||
                    pIn_vColumnName || ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                    pIn_vTmpTableName || ' WHERE YY=' ||
                    to_char(nFFirstYear) || ' AND ' || pIn_vColumnName ||
                    '=ID AND TSID=' || pIn_vFTimeSeriesID ||
                    ' AND VERSION=' || pIn_vFVersion || ')';
          end if;
        elsif pIn_nAdjustmentType = 1 then
          --percent
          cSql := 'select ' || pIn_aColumnList(nFFirstMonth) || ' *' ||
                  to_char(1 + pIn_nAdjustment / 100) || ' AS ' ||
                  pIn_aColumnList(nFFirstMonth) || ',' || pIn_vColumnName ||
                  ' FROM ' || pIn_vNodeLevelTableName || ',' ||
                  pIn_vTmpTableName || ' WHERE YY=' || to_char(nFFirstYear) ||
                  ' AND ' || pIn_vColumnName || '=ID AND TSID=' ||
                  pIn_vFTimeSeriesID || ' AND VERSION=' || pIn_vFVersion ||
                  ' AND ' || pIn_aColumnList(nFFirstMonth) ||
                  ' IS NOT NULL ';
        end if;
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
  procedure FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth        in number,
                                  pIn_nTFirstYear         in number,
                                  pIn_nTLastMonth         in number,
                                  pIn_nTLastYear          in number,
                                  pIn_vTmpTableName       in varchar2,
                                  pIn_nLevelTableName     in varchar2,
                                  pIn_vColumnName         in varchar2,
                                  pIn_vTTimeSeriesID      in varchar2,
                                  pIn_vVersion            in varchar2,
                                  pIn_aColumnList         in aColumnList,
                                  pIn_nAdjustment         in number,
                                  pIn_nAdjustmentType     in number,
                                  pIn_vSeq                in varchar2,
                                  pIn_vTableSeqColumnName in varchar2,
                                  pIn_nDecimal            in number,
                                  pIn_vNodeLevelTableName in varchar2) IS
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
      cSql                clob;
      cSqlTmp             clob;
      cSqlTmpUpdate       clob;
      cSqlTmpInsert       clob;
      cSqlTmpInsertValues clob;
      i                   number;
      j                   number;
      nTFirstMonth        number := pIn_nTFirstMonth;
      nTFirstYear         number := pIn_nTFirstYear;
      nCountNum           number := 1;
    begin
      if pIn_nAdjustmentType = 1 then
        -- if pIn_nAdjustmentType = 1 means blank so all data will blank
        if pIn_nTLastMonth = 0 AND pIn_nTLastYear = 0 then
          -- horize = 0 operate all Begin FROM start date
          if pIn_nTFirstMonth = 1 then
            cSql := 'DELETE FROM ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                    to_char(pIn_nTFirstYear) || ' AND ' || pIn_vColumnName ||
                    ' in (select id from ' || pIn_vTmpTableName || ')' ||
                    ' AND TSID = ' || pIn_vTTimeSeriesID ||
                    ' AND VERSION = ' || to_char(pIn_vVersion);
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
          if pIn_nTFirstYear <> pIn_nTLastYear then
            cSql := 'DELETE FROM ' || pIn_nLevelTableName || ' WHERE YY>=' ||
                    to_char(pIn_nTFirstYear) || ' AND YY<=' ||
                    to_char(pIn_nTLastYear - 1) || ' AND ' ||
                    pIn_vColumnName || ' in (select id from ' ||
                    pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                    pIn_vTTimeSeriesID || ' AND VERSION = ' ||
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
                    ' AND TSID = ' || pIn_vTTimeSeriesID ||
                    ' AND VERSION = ' || to_char(pIn_vVersion);
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
                    to_char(pIn_nTLastYear - 1) || ' AND ' ||
                    pIn_vColumnName || ' in (select id from ' ||
                    pIn_vTmpTableName || ')' || ' AND TSID = ' ||
                    pIn_vTTimeSeriesID || ' AND VERSION = ' ||
                    to_char(pIn_vVersion);
            FMSP_ExecSql(cSql);
          end if;
        end if;
      elsif pIn_nAdjustmentType = 0 then
        -- if pIn_nAdjustmentType = 0 means add so update all data to pIn_nAdjustment
        if pIn_nTLastMonth = 0 AND pIn_nTLastYear = 0 then
          -- horize = 0 operate all Begin FROM start date
          null;
        elsif pIn_nTFirstMonth = 1 and pIn_nTLastMonth = 12 then
          -- =1 AND = 12
          for j in pIn_nTFirstYear .. pIn_nTLastYear loop
            cSqlTmp := 'select ';
            for i in 1 .. 12 loop
              cSqlTmp := cSqlTmp || 'round(0+' || to_char(pIn_nAdjustment) || ',' ||
                         pIn_nDecimal || ') AS ' || pIn_aColumnList(i);
              if i <> 12 then
                cSqlTmp := cSqlTmp || ',';
              end if;
            end loop;
            cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                       to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
            if j <> pIn_nTLastYear then
              cSqlTmp := cSqlTmp || ' UNION ALL ';
            end if;
            cSql := cSql || cSqlTmp;

          end loop;

          cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName || ' V USING (' || cSql ||
                  ') T ON(V.TSID=' || to_char(pIn_vTTimeSeriesID) ||
                  ' AND V.' || pIn_vColumnName || '=' || 'T.' ||
                  pIn_vColumnName || ' AND V.VERSION=' || pIn_vVersion ||
                  ' AND V.YY=T.YY' ||
                  ') WHEN MATCHED THEN  UPDATE Set V.T1=T.T1,V.T2=T.T2,V.T3=T.T3,' ||
                  'V.T4=T.T4,V.T5=T.T5,V.T6=T.T6,V.T7=T.T7,V.T8=T.T8,V.T9=T.T9' ||
                  ',V.T10=T.T10,V.T11=T.T11,V.T12=T.T12 ' ||
                  ' WHEN NOT MATCHED THEN INSERT (V.' ||
                  pIn_vTableSeqColumnName ||
                  ',V.T1,V.T2,V.T3,V.T4,V.T5,V.T6,V.T7,V.T8,V.T9,V.T10,V.T11,V.T12' ||
                  ',V.TSID,V.' || pIn_vColumnName || ',V.VERSION,V.YY)' ||
                  ' VALUES(' || pIn_vSeq || '.nextval,' ||
                  'T.T1,T.T2,T.T3,T.T4,T.T5,T.T6,T.T7,T.T8,T.T9,T.T10,T.T11,T.T12' || ',' ||
                  pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                  pIn_vVersion || ',T.YY' || ')';
          FMSP_ExecSql(cSql);
          --debug
          --fmp_log.LOGERROR(cSql);
          --debug
        elsif pIn_nTFirstMonth = 1 and pIn_nTLastMonth <> 12 then
          -- =1 AND <>12
          for j in pIn_nTLastYear .. pIn_nTLastYear loop
            cSqlTmp := 'select ';
            for i in 1 .. pIn_nTLastMonth loop
              cSqlTmp := cSqlTmp || 'round(0+' || to_char(pIn_nAdjustment) || ',' ||
                         pIn_nDecimal || ') AS ' || pIn_aColumnList(i);
              if i <> pIn_nTLastMonth then
                cSqlTmp := cSqlTmp || ',';
              end if;
            end loop;
            cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                       to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
            if j <> pIn_nTLastYear then
              cSqlTmp := cSqlTmp || ' UNION ALL ';
            end if;
            cSql := cSql || cSqlTmp;
          end loop;
          cSqlTmpUpdate       := 'UPDATE Set ';
          cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                 ',V.TSID,V.' || pIn_vColumnName ||
                                 ',V.VERSION,V.YY,';
          cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                 pIn_vTTimeSeriesID || ',T.' ||
                                 pIn_vColumnName || ',' || pIn_vVersion ||
                                 ',T.YY,';
          for i in 1 .. pIn_nTLastMonth loop
            cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                   pIn_aColumnList(i) || '=' || 'T.' ||
                                   pIn_aColumnList(i);
            cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                   pIn_aColumnList(i);
            cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                   pIn_aColumnList(i);
            if i <> pIn_nTLastMonth then
              cSqlTmpUpdate       := cSqlTmpUpdate || ',';
              cSqlTmpInsert       := cSqlTmpInsert || ',';
              cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
            end if;
          end loop;
          cSqlTmpInsert       := cSqlTmpInsert || ')';
          cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
          cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                 ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                 to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                 pIn_vColumnName || '=' || 'T.' ||
                                 pIn_vColumnName || ' AND V.VERSION=' ||
                                 pIn_vVersion || ' AND V.YY=T.YY' ||
                                 ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                 ' WHEN NOT MATCHED THEN ' || cSqlTmpInsert ||
                                 cSqlTmpInsertValues;
          FMSP_ExecSql(cSql);
          cSql := '';
          -- deal year
          if pIn_nTFirstYear <> pIn_nTLastYear then
            for j in pIn_nTFirstYear .. pIn_nTLastYear - 1 loop
              cSqlTmp := 'select ';
              for i in 1 .. 12 loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> 12 then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTLastYear - 1 then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;

            end loop;

            cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                    ' V USING (' || cSql || ') T ON(V.TSID=' ||
                    to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                    pIn_vColumnName || '=' || 'T.' || pIn_vColumnName ||
                    ' AND V.VERSION=' || pIn_vVersion || ' AND V.YY=T.YY' ||
                    ') WHEN MATCHED THEN  UPDATE Set V.T1=T.T1,V.T2=T.T2,V.T3=T.T3,' ||
                    'V.T4=T.T4,V.T5=T.T5,V.T6=T.T6,V.T7=T.T7,V.T8=T.T8,V.T9=T.T9' ||
                    ',V.T10=T.T10,V.T11=T.T11,V.T12=T.T12 ' ||
                    ' WHEN NOT MATCHED THEN INSERT (V.' ||
                    pIn_vTableSeqColumnName ||
                    ',V.T1,V.T2,V.T3,V.T4,V.T5,V.T6,V.T7,V.T8,V.T9,V.T10,V.T11,V.T12' ||
                    ',V.TSID,V.' || pIn_vColumnName || ',V.VERSION,V.YY)' ||
                    ' VALUES(' || pIn_vSeq || '.nextval,' ||
                    'T.T1,T.T2,T.T3,T.T4,T.T5,T.T6,T.T7,T.T8,T.T9,T.T10,T.T11,T.T12' || ',' ||
                    pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                    pIn_vVersion || ',T.YY' || ')';
            FMSP_ExecSql(cSql);
          end if;
        elsif pIn_nTFirstMonth <> 1 and pIn_nTLastMonth = 12 then
          -- <>1 AND =12
          for j in pIn_nTFirstYear .. pIn_nTFirstYear loop
            cSqlTmp := 'select ';
            for i in pIn_nTFirstMonth .. 12 loop
              cSqlTmp := cSqlTmp || 'round(0+' || to_char(pIn_nAdjustment) || ',' ||
                         pIn_nDecimal || ') AS ' || pIn_aColumnList(i);
              if i <> 12 then
                cSqlTmp := cSqlTmp || ',';
              end if;
            end loop;
            cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                       to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
            if j <> pIn_nTFirstYear then
              cSqlTmp := cSqlTmp || ' UNION ALL ';
            end if;
            cSql := cSql || cSqlTmp;
          end loop;
          cSqlTmpUpdate       := 'UPDATE Set ';
          cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                 ',V.TSID,V.' || pIn_vColumnName ||
                                 ',V.VERSION,V.YY,';
          cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                 pIn_vTTimeSeriesID || ',T.' ||
                                 pIn_vColumnName || ',' || pIn_vVersion ||
                                 ',T.YY,';
          for i in pIn_nTFirstMonth .. 12 loop
            cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                   pIn_aColumnList(i) || '=' || 'T.' ||
                                   pIn_aColumnList(i);
            cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                   pIn_aColumnList(i);
            cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                   pIn_aColumnList(i);
            if i <> 12 then
              cSqlTmpUpdate       := cSqlTmpUpdate || ',';
              cSqlTmpInsert       := cSqlTmpInsert || ',';
              cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
            end if;
          end loop;
          cSqlTmpInsert       := cSqlTmpInsert || ')';
          cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
          cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                 ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                 to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                 pIn_vColumnName || '=' || 'T.' ||
                                 pIn_vColumnName || ' AND V.VERSION=' ||
                                 pIn_vVersion || ' AND V.YY=T.YY' ||
                                 ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                 ' WHEN NOT MATCHED THEN ' || cSqlTmpInsert ||
                                 cSqlTmpInsertValues;
          FMSP_ExecSql(cSql);
          cSql := '';
          -- deal with
          if pIn_nTFirstYear <> pIn_nTLastYear then
            for j in pIn_nTFirstYear + 1 .. pIn_nTLastYear loop
              cSqlTmp := 'select ';
              for i in 1 .. 12 loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> 12 then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTLastYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;

            end loop;

            cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                    ' V USING (' || cSql || ') T ON(V.TSID=' ||
                    to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                    pIn_vColumnName || '=' || 'T.' || pIn_vColumnName ||
                    ' AND V.VERSION=' || pIn_vVersion || ' AND V.YY=T.YY' ||
                    ') WHEN MATCHED THEN  UPDATE Set V.T1=T.T1,V.T2=T.T2,V.T3=T.T3,' ||
                    'V.T4=T.T4,V.T5=T.T5,V.T6=T.T6,V.T7=T.T7,V.T8=T.T8,V.T9=T.T9' ||
                    ',V.T10=T.T10,V.T11=T.T11,V.T12=T.T12 ' ||
                    ' WHEN NOT MATCHED THEN INSERT (V.' ||
                    pIn_vTableSeqColumnName ||
                    ',V.T1,V.T2,V.T3,V.T4,V.T5,V.T6,V.T7,V.T8,V.T9,V.T10,V.T11,V.T12' ||
                    ',V.TSID,V.' || pIn_vColumnName || ',V.VERSION,V.YY)' ||
                    ' VALUES(' || pIn_vSeq || '.nextval,' ||
                    'T.T1,T.T2,T.T3,T.T4,T.T5,T.T6,T.T7,T.T8,T.T9,T.T10,T.T11,T.T12' || ',' ||
                    pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                    pIn_vVersion || ',T.YY' || ')';
            FMSP_ExecSql(cSql);
          end if;
        elsif pIn_nTFirstMonth <> 1 and pIn_nTLastMonth <> 12 then
          -- <>1 AND <>12
          if pIn_nTFirstYear = pIn_nTLastYear then
            -- pIn_nTFirstYear = pIn_nTLastYear
            for j in pIn_nTFirstYear .. pIn_nTFirstYear loop
              cSqlTmp := 'select ';
              for i in pIn_nTFirstMonth .. pIn_nTLastMonth loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> pIn_nTLastMonth then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTFirstYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;
            end loop;
            cSqlTmpUpdate       := 'UPDATE Set ';
            cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                   ',V.TSID,V.' || pIn_vColumnName ||
                                   ',V.VERSION,V.YY,';
            cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                   pIn_vTTimeSeriesID || ',T.' ||
                                   pIn_vColumnName || ',' || pIn_vVersion ||
                                   ',T.YY,';
            for i in pIn_nTFirstMonth .. pIn_nTLastMonth loop
              cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                     pIn_aColumnList(i) || '=' || 'T.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                     pIn_aColumnList(i);
              if i <> pIn_nTLastMonth then
                cSqlTmpUpdate       := cSqlTmpUpdate || ',';
                cSqlTmpInsert       := cSqlTmpInsert || ',';
                cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
              end if;
            end loop;
            cSqlTmpInsert       := cSqlTmpInsert || ')';
            cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
            cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                   ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                   to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                   pIn_vColumnName || '=' || 'T.' ||
                                   pIn_vColumnName || ' AND V.VERSION=' ||
                                   pIn_vVersion || ' AND V.YY=T.YY' ||
                                   ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                   ' WHEN NOT MATCHED THEN ' ||
                                   cSqlTmpInsert || cSqlTmpInsertValues;
            FMSP_ExecSql(cSql);
          elsif pIn_nTFirstYear + 1 = pIn_nTLastYear then
            -- deal with pIn_nTFirstYear + 1 = pIn_nTLastYear
            for j in pIn_nTLastYear .. pIn_nTLastYear loop
              -- deal with pIn_nTLastMonth
              cSqlTmp := 'select ';
              for i in 1 .. pIn_nTLastMonth loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> pIn_nTLastMonth then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTLastYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;
            end loop;
            cSqlTmpUpdate       := 'UPDATE Set ';
            cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                   ',V.TSID,V.' || pIn_vColumnName ||
                                   ',V.VERSION,V.YY,';
            cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                   pIn_vTTimeSeriesID || ',T.' ||
                                   pIn_vColumnName || ',' || pIn_vVersion ||
                                   ',T.YY,';
            for i in 1 .. pIn_nTLastMonth loop
              cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                     pIn_aColumnList(i) || '=' || 'T.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                     pIn_aColumnList(i);
              if i <> pIn_nTLastMonth then
                cSqlTmpUpdate       := cSqlTmpUpdate || ',';
                cSqlTmpInsert       := cSqlTmpInsert || ',';
                cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
              end if;
            end loop;
            cSqlTmpInsert       := cSqlTmpInsert || ')';
            cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
            cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                   ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                   to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                   pIn_vColumnName || '=' || 'T.' ||
                                   pIn_vColumnName || ' AND V.VERSION=' ||
                                   pIn_vVersion || ' AND V.YY=T.YY' ||
                                   ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                   ' WHEN NOT MATCHED THEN ' ||
                                   cSqlTmpInsert || cSqlTmpInsertValues;
            FMSP_ExecSql(cSql);
            cSql := '';
            for j in pIn_nTFirstYear .. pIn_nTFirstYear loop
              -- deal with pIn_nTFirstMonth
              cSqlTmp := 'select ';
              for i in pIn_nTFirstMonth .. 12 loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> 12 then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTFirstYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;
            end loop;
            cSqlTmpUpdate       := 'UPDATE Set ';
            cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                   ',V.TSID,V.' || pIn_vColumnName ||
                                   ',V.VERSION,V.YY,';
            cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                   pIn_vTTimeSeriesID || ',T.' ||
                                   pIn_vColumnName || ',' || pIn_vVersion ||
                                   ',T.YY,';
            for i in pIn_nTFirstMonth .. 12 loop
              cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                     pIn_aColumnList(i) || '=' || 'T.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                     pIn_aColumnList(i);
              if i <> 12 then
                cSqlTmpUpdate       := cSqlTmpUpdate || ',';
                cSqlTmpInsert       := cSqlTmpInsert || ',';
                cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
              end if;
            end loop;
            cSqlTmpInsert       := cSqlTmpInsert || ')';
            cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
            cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                   ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                   to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                   pIn_vColumnName || '=' || 'T.' ||
                                   pIn_vColumnName || ' AND V.VERSION=' ||
                                   pIn_vVersion || ' AND V.YY=T.YY' ||
                                   ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                   ' WHEN NOT MATCHED THEN ' ||
                                   cSqlTmpInsert || cSqlTmpInsertValues;
            FMSP_ExecSql(cSql);

          elsif pIn_nTFirstYear + 1 <= pIn_nTLastYear - 1 then
            for j in pIn_nTLastYear .. pIn_nTLastYear loop
              -- deal with pIn_nTLastMonth
              cSqlTmp := 'select ';
              for i in 1 .. pIn_nTLastMonth loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> pIn_nTLastMonth then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTLastYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;
            end loop;
            cSqlTmpUpdate       := 'UPDATE Set ';
            cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                   ',V.TSID,V.' || pIn_vColumnName ||
                                   ',V.VERSION,V.YY,';
            cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                   pIn_vTTimeSeriesID || ',T.' ||
                                   pIn_vColumnName || ',' || pIn_vVersion ||
                                   ',T.YY,';
            for i in 1 .. pIn_nTLastMonth loop
              cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                     pIn_aColumnList(i) || '=' || 'T.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                     pIn_aColumnList(i);
              if i <> pIn_nTLastMonth then
                cSqlTmpUpdate       := cSqlTmpUpdate || ',';
                cSqlTmpInsert       := cSqlTmpInsert || ',';
                cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
              end if;
            end loop;
            cSqlTmpInsert       := cSqlTmpInsert || ')';
            cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
            cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                   ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                   to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                   pIn_vColumnName || '=' || 'T.' ||
                                   pIn_vColumnName || ' AND V.VERSION=' ||
                                   pIn_vVersion || ' AND V.YY=T.YY' ||
                                   ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                   ' WHEN NOT MATCHED THEN ' ||
                                   cSqlTmpInsert || cSqlTmpInsertValues;
            FMSP_ExecSql(cSql);
            cSql := '';
            for j in pIn_nTFirstYear .. pIn_nTFirstYear loop
              -- deal with pIn_nTFirstMonth
              cSqlTmp := 'select ';
              for i in pIn_nTFirstMonth .. 12 loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> 12 then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTFirstYear then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;
            end loop;
            cSqlTmpUpdate       := 'UPDATE Set ';
            cSqlTmpInsert       := 'INSERT (V.' || pIn_vTableSeqColumnName ||
                                   ',V.TSID,V.' || pIn_vColumnName ||
                                   ',V.VERSION,V.YY,';
            cSqlTmpInsertValues := ' VALUES(' || pIn_vSeq || '.nextval,' ||
                                   pIn_vTTimeSeriesID || ',T.' ||
                                   pIn_vColumnName || ',' || pIn_vVersion ||
                                   ',T.YY,';
            for i in pIn_nTFirstMonth .. 12 loop
              cSqlTmpUpdate       := cSqlTmpUpdate || ' V.' ||
                                     pIn_aColumnList(i) || '=' || 'T.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsert       := cSqlTmpInsert || ' V.' ||
                                     pIn_aColumnList(i);
              cSqlTmpInsertValues := cSqlTmpInsertValues || ' T.' ||
                                     pIn_aColumnList(i);
              if i <> 12 then
                cSqlTmpUpdate       := cSqlTmpUpdate || ',';
                cSqlTmpInsert       := cSqlTmpInsert || ',';
                cSqlTmpInsertValues := cSqlTmpInsertValues || ',';
              end if;
            end loop;
            cSqlTmpInsert       := cSqlTmpInsert || ')';
            cSqlTmpInsertValues := cSqlTmpInsertValues || ')';
            cSql                := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                                   ' V USING (' || cSql || ') T ON(V.TSID=' ||
                                   to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                                   pIn_vColumnName || '=' || 'T.' ||
                                   pIn_vColumnName || ' AND V.VERSION=' ||
                                   pIn_vVersion || ' AND V.YY=T.YY' ||
                                   ') WHEN MATCHED THEN ' || cSqlTmpUpdate ||
                                   ' WHEN NOT MATCHED THEN ' ||
                                   cSqlTmpInsert || cSqlTmpInsertValues;
            FMSP_ExecSql(cSql);
            for j in pIn_nTFirstYear + 1 .. pIn_nTLastYear - 1 loop
              -- deal with year
              cSqlTmp := 'select ';
              for i in 1 .. 12 loop
                cSqlTmp := cSqlTmp || 'round(0+' ||
                           to_char(pIn_nAdjustment) || ',' || pIn_nDecimal ||
                           ') AS ' || pIn_aColumnList(i);
                if i <> 12 then
                  cSqlTmp := cSqlTmp || ',';
                end if;
              end loop;
              cSqlTmp := cSqlTmp || ',id as ' || pIn_vColumnName || ',' ||
                         to_char(j) || ' AS YY FROM ' || pIn_vTmpTableName;
              if j <> pIn_nTLastYear - 1 then
                cSqlTmp := cSqlTmp || ' UNION ALL ';
              end if;
              cSql := cSql || cSqlTmp;

            end loop;

            cSql := 'MERGE INTO ' || pIn_vNodeLevelTableName ||
                    ' V USING (' || cSql || ') T ON(V.TSID=' ||
                    to_char(pIn_vTTimeSeriesID) || ' AND V.' ||
                    pIn_vColumnName || '=' || 'T.' || pIn_vColumnName ||
                    ' AND V.VERSION=' || pIn_vVersion || ' AND V.YY=T.YY' ||
                    ') WHEN MATCHED THEN  UPDATE Set V.T1=T.T1,V.T2=T.T2,V.T3=T.T3,' ||
                    'V.T4=T.T4,V.T5=T.T5,V.T6=T.T6,V.T7=T.T7,V.T8=T.T8,V.T9=T.T9' ||
                    ',V.T10=T.T10,V.T11=T.T11,V.T12=T.T12 ' ||
                    ' WHEN NOT MATCHED THEN INSERT (V.' ||
                    pIn_vTableSeqColumnName ||
                    ',V.T1,V.T2,V.T3,V.T4,V.T5,V.T6,V.T7,V.T8,V.T9,V.T10,V.T11,V.T12' ||
                    ',V.TSID,V.' || pIn_vColumnName || ',V.VERSION,V.YY)' ||
                    ' VALUES(' || pIn_vSeq || '.nextval,' ||
                    'T.T1,T.T2,T.T3,T.T4,T.T5,T.T6,T.T7,T.T8,T.T9,T.T10,T.T11,T.T12' || ',' ||
                    pIn_vTTimeSeriesID || ',T.' || pIn_vColumnName || ',' ||
                    pIn_vVersion || ',T.YY' || ')';
            FMSP_ExecSql(cSql);
          end if;
        end if;
      end if;
    end;
  END FMSP_BLANKTSIDMONTHLY;

  procedure FMSP_COPYTSIDMONTHLY(pIn_nSelectionID       in number,
                                 pIn_nHorizon           in number,
                                 pIn_vFFirstPeriodTime  in varchar2,
                                 pIn_vTFirstPeriodTime  in varchar2,
                                 pIn_vConditions        in varchar2,
                                 pIn_vFTimeSeriesID     in varchar2,
                                 pIn_vTTimeSeriesID     in varchar2,
                                 pIn_nDataOperationType in number,
                                 pIn_nAdjustment        in number,
                                 pIn_nAdjustmentType    in number,
                                 pIn_nCopyType          in number,
                                 pIn_vFVersion          in varchar2,
                                 pIn_vTVersion          in varchar2,
                                 pIn_vDefaultVersion    in varchar2,
                                 pIn_nNodeLevel         in number,
                                 pIn_nDecimal           in number,
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

      nFFirstMonth := to_number(substr(pIn_vFFirstPeriodTime, 5));
      nTFirstMonth := to_number(substr(pIn_vTFirstPeriodTime, 5));
      nFFirstYear  := to_number(substr(pIn_vFFirstPeriodTime, 0, 4));
      nTFirstYear  := to_number(substr(pIn_vTFirstPeriodTime, 0, 4));

      if pIn_nHorizon > 0 then
        vFLastPeriodTime := to_char(add_months(to_date(pIn_vFFirstPeriodTime,
                                                       'YYYYMM'),
                                               pIn_nHorizon - 1),
                                    'yyyymm');
        vTLastPeriodTime := to_char(add_months(to_date(pIn_vTFirstPeriodTime,
                                                       'YYYYMM'),
                                               pIn_nHorizon - 1),
                                    'yyyymm');
        nFLastMonth      := to_number(substr(vFLastPeriodTime, 5));
        nTLastMonth      := to_number(substr(vTLastPeriodTime, 5));
        nFLastYear       := to_number(substr(vFLastPeriodTime, 0, 4));
        nTLastYear       := to_number(substr(vTLastPeriodTime, 0, 4));
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
      -- DEBUG
      /*delete from nodelist;
      insert into nodelist
        select id from TB_TS_DetailNodeSelCdt;
      commit;*/
      -- DEBUG
      -- juge pIn_nDataOperationType
      if 1 = pIn_nDataOperationType then
        -- BLANK
        if 0 = pIn_nCopyType then
          -- TSID
          if pIn_nAdjustmentType = 1 then
            FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth        => nTFirstMonth,
                                  pIn_nTFirstYear         => nTFirstYear,
                                  pIn_nTLastMonth         => nTLastMonth,
                                  pIn_nTLastYear          => nTLastYear,
                                  pIn_vTmpTableName       => vTmpTableName,
                                  pIn_nLevelTableName     => vNodeLevel,
                                  pIn_vColumnName         => vNodeColumnName,
                                  pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                                  pIn_vVersion            => pIn_vDefaultVersion,
                                  pIn_aColumnList         => aColumnListResult,
                                  pIn_nAdjustment         => pIn_nAdjustment,
                                  pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                  pIn_vSeq                => vSeq,
                                  pIn_vTableSeqColumnName => vTableSeqColumnName,
                                  pIn_nDecimal            => pIn_nDecimal,
                                  pIn_vNodeLevelTableName => vNodeLevel);
          elsif pIn_nAdjustmentType = 0 then
            FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth        => nTFirstMonth,
                                  pIn_nTFirstYear         => nTFirstYear,
                                  pIn_nTLastMonth         => nTLastMonth,
                                  pIn_nTLastYear          => nTLastYear,
                                  pIn_vTmpTableName       => vTmpTableName,
                                  pIn_nLevelTableName     => vNodeLevel,
                                  pIn_vColumnName         => vNodeColumnName,
                                  pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                                  pIn_vVersion            => pIn_vDefaultVersion,
                                  pIn_aColumnList         => aColumnListResult,
                                  pIn_nAdjustment         => pIn_nAdjustment,
                                  pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                  pIn_vSeq                => vSeq,
                                  pIn_vTableSeqColumnName => vTableSeqColumnName,
                                  pIn_nDecimal            => pIn_nDecimal,
                                  pIn_vNodeLevelTableName => vNodeLevel);
          end if;
        elsif 1 = pIn_nCopyType then
          -- VERSION
          FMSP_BLANKTSIDMONTHLY(pIn_nTFirstMonth        => nTFirstMonth,
                                pIn_nTFirstYear         => nTFirstYear,
                                pIn_nTLastMonth         => nTLastMonth,
                                pIn_nTLastYear          => nTLastYear,
                                pIn_vTmpTableName       => vTmpTableName,
                                pIn_nLevelTableName     => vNodeLevel,
                                pIn_vColumnName         => vNodeColumnName,
                                pIn_vTTimeSeriesID      => pIn_vFTimeSeriesID,
                                pIn_vVersion            => pIn_vTVersion,
                                pIn_aColumnList         => aColumnListResult,
                                pIn_nAdjustment         => pIn_nAdjustment,
                                pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                pIn_vSeq                => vSeq,
                                pIn_vTableSeqColumnName => vTableSeqColumnName,
                                pIn_nDecimal            => pIn_nDecimal,
                                pIn_vNodeLevelTableName => vNodeLevel);
        end if;
      elsif 0 = pIn_nDataOperationType then
        -- NORMAL
        if 0 = pIn_nCopyType then
          -- TSID
          /*FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
          pIn_nFFirstYear         => nFFirstYear,
          pIn_nFLastMonth         => nFLastMonth,
          pIn_nFLastYear          => nFLastYear,
          pIn_nTFirstMonth        => nTFirstMonth,
          pIn_nTFirstYear         => nTFirstYear,
          pIn_nTLastMonth         => nTLastMonth,
          pIn_nTLastYear          => nTLastYear,
          pIn_nHorizon            => pIn_nHorizon,
          pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
          pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
          pIn_nAdjustment         => pIn_nAdjustment,
          pIn_nAdjustmentType     => pIn_nAdjustmentType,
          pIn_vFVersion           => pIn_vDefaultVersion,
          pIn_vTVersion           => pIn_vDefaultVersion,
          pIn_vNodeLevelTableName => vNodeLevel,
          pIn_vColumnName         => vNodeColumnName,
          pIn_vTmpTableName       => vTmpTableName,
          pIn_vSeq                => vSeq,
          pIn_vTableSeqColumnName => vTableSeqColumnName,
          pIn_aColumnList         => aColumnListResult);*/
          IF pIn_vFTimeSeriesID = pIn_vTTimeSeriesID AND
             (nTFirstYear < nFLastYear or
             (nTFirstYear = nFLastYear AND nTFirstMonth <= nFLastMonth)) THEN
            FMSP_COPYDATAACTIONSSB(pIn_nFFirstMonth        => nFFirstMonth,
                                   pIn_nFFirstYear         => nFFirstYear,
                                   pIn_nFLastMonth         => nFLastMonth,
                                   pIn_nFLastYear          => nFLastYear,
                                   pIn_nTFirstMonth        => nTFirstMonth,
                                   pIn_nTFirstYear         => nTFirstYear,
                                   pIn_nTLastMonth         => nTLastMonth,
                                   pIn_nTLastYear          => nTLastYear,
                                   pIn_nHorizon            => pIn_nHorizon,
                                   pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
                                   pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                                   pIn_nAdjustment         => pIn_nAdjustment,
                                   pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                   pIn_vFVersion           => pIn_vDefaultVersion,
                                   pIn_vTVersion           => pIn_vDefaultVersion,
                                   pIn_vNodeLevelTableName => vNodeLevel,
                                   pIn_vColumnName         => vNodeColumnName,
                                   pIn_vTmpTableName       => vTmpTableName,
                                   pIn_vSeq                => vSeq,
                                   pIn_vTableSeqColumnName => vTableSeqColumnName,
                                   pIn_nDecimal            => pIn_nDecimal,
                                   pIn_aColumnList         => aColumnListResult,
                                   pIn_nDataOperationType  => pIn_nDataOperationType);
          ELSE
            FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
                                pIn_nFFirstYear         => nFFirstYear,
                                pIn_nTFirstMonth        => nTFirstMonth,
                                pIn_nTFirstYear         => nTFirstYear,
                                pIn_nHorizon            => pIn_nHorizon,
                                pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
                                pIn_vTTimeSeriesID      => pIn_vTTimeSeriesID,
                                pIn_nAdjustment         => pIn_nAdjustment,
                                pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                pIn_vFVersion           => pIn_vDefaultVersion,
                                pIn_vTVersion           => pIn_vDefaultVersion,
                                pIn_vNodeLevelTableName => vNodeLevel,
                                pIn_vColumnName         => vNodeColumnName,
                                pIn_vTmpTableName       => vTmpTableName,
                                pIn_vSeq                => vSeq,
                                pIn_vTableSeqColumnName => vTableSeqColumnName,
                                pIn_nDecimal            => pIn_nDecimal,
                                pIn_aColumnList         => aColumnListResult,
                                pIn_nDataOperationType  => pIn_nDataOperationType);
          END IF;
        elsif 1 = pIn_nCopyType then
          -- VERSION
          /*FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
          pIn_nFFirstYear         => nFFirstYear,
          pIn_nFLastMonth         => nFLastMonth,
          pIn_nFLastYear          => nFLastYear,
          pIn_nTFirstMonth        => nTFirstMonth,
          pIn_nTFirstYear         => nTFirstYear,
          pIn_nTLastMonth         => nTLastMonth,
          pIn_nTLastYear          => nTLastYear,
          pIn_nHorizon            => pIn_nHorizon,
          pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
          pIn_vTTimeSeriesID      => pIn_vFTimeSeriesID,
          pIn_nAdjustment         => pIn_nAdjustment,
          pIn_nAdjustmentType     => pIn_nAdjustmentType,
          pIn_vFVersion           => pIn_vFVersion,
          pIn_vTVersion           => pIn_vTVersion,
          pIn_vNodeLevelTableName => vNodeLevel,
          pIn_vColumnName         => vNodeColumnName,
          pIn_vTmpTableName       => vTmpTableName,
          pIn_vSeq                => vSeq,
          pIn_vTableSeqColumnName => vTableSeqColumnName,
          pIn_aColumnList         => aColumnListResult);*/
          if nTFirstYear < nFLastYear or
             (nTFirstYear = nFLastYear AND nTFirstMonth <= nFLastMonth) THEN
            FMSP_COPYDATAACTIONSSB(pIn_nFFirstMonth        => nFFirstMonth,
                                   pIn_nFFirstYear         => nFFirstYear,
                                   pIn_nFLastMonth         => nFLastMonth,
                                   pIn_nFLastYear          => nFLastYear,
                                   pIn_nTFirstMonth        => nTFirstMonth,
                                   pIn_nTFirstYear         => nTFirstYear,
                                   pIn_nTLastMonth         => nTLastMonth,
                                   pIn_nTLastYear          => nTLastYear,
                                   pIn_nHorizon            => pIn_nHorizon,
                                   pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
                                   pIn_vTTimeSeriesID      => pIn_vFTimeSeriesID,
                                   pIn_nAdjustment         => pIn_nAdjustment,
                                   pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                   pIn_vFVersion           => pIn_vFVersion,
                                   pIn_vTVersion           => pIn_vTVersion,
                                   pIn_vNodeLevelTableName => vNodeLevel,
                                   pIn_vColumnName         => vNodeColumnName,
                                   pIn_vTmpTableName       => vTmpTableName,
                                   pIn_vSeq                => vSeq,
                                   pIn_vTableSeqColumnName => vTableSeqColumnName,
                                   pIn_nDecimal            => pIn_nDecimal,
                                   pIn_aColumnList         => aColumnListResult,
                                   pIn_nDataOperationType  => pIn_nDataOperationType);
          else
            FMSP_COPYDATAACTION(pIn_nFFirstMonth        => nFFirstMonth,
                                pIn_nFFirstYear         => nFFirstYear,
                                pIn_nTFirstMonth        => nTFirstMonth,
                                pIn_nTFirstYear         => nTFirstYear,
                                pIn_nHorizon            => pIn_nHorizon,
                                pIn_vFTimeSeriesID      => pIn_vFTimeSeriesID,
                                pIn_vTTimeSeriesID      => pIn_vFTimeSeriesID,
                                pIn_nAdjustment         => pIn_nAdjustment,
                                pIn_nAdjustmentType     => pIn_nAdjustmentType,
                                pIn_vFVersion           => pIn_vFVersion,
                                pIn_vTVersion           => pIn_vTVersion,
                                pIn_vNodeLevelTableName => vNodeLevel,
                                pIn_vColumnName         => vNodeColumnName,
                                pIn_vTmpTableName       => vTmpTableName,
                                pIn_vSeq                => vSeq,
                                pIn_vTableSeqColumnName => vTableSeqColumnName,
                                pIn_nDecimal            => pIn_nDecimal,
                                pIn_aColumnList         => aColumnListResult,
                                pIn_nDataOperationType  => pIn_nDataOperationType);
          end if;
        end if;
      end if;
    end;
  END FMSP_COPYTSIDMONTHLY;
  procedure FMSP_COPYTSIDWEEKLY(pIn_nSelectionID       in number,
                                pIn_nHorizon           in number,
                                pIn_vFFirstPeriodTime  in varchar2,
                                pIn_vTFirstPeriodTime  in varchar2,
                                pIn_vConditions        in varchar2,
                                pIn_vFTimeSeriesID     in varchar2,
                                pIn_vTTimeSeriesID     in varchar2,
                                pIn_nDataOperationType in number,
                                pIn_nAdjustment        in number,
                                pIn_nAdjustmentType    in number,
                                pIn_nCopyType          in number,
                                pIn_vFVersion          in varchar2,
                                pIn_vTVersion          in varchar2,
                                pIn_vDefaultVersion    in varchar2,
                                pIn_nNodeLevel         in number,
                                pIn_nDecimal           in number,
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
                               pIn_vFTimeSeriesID     in varchar2,
                               pIn_vTTimeSeriesID     in varchar2,
                               pIn_nDataOperationType in number,
                               pIn_nAdjustment        in number,
                               pIn_nAdjustmentType    in number,
                               pIn_nCopyType          in number,
                               pIn_vFVersion          in varchar2,
                               pIn_vTVersion          in varchar2,
                               pIn_vDefaultVersion    in varchar2,
                               pIn_nNodeLevel         in number,
                               pIn_nDecimal           in number,
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
                           pIn_vFTimeSeriesID     in varchar2,
                           pIn_vTTimeSeriesID     in varchar2,
                           pIn_nDataOperationType in number,
                           pIn_nAdjustment        in number,
                           pIn_nAdjustmentType    in number,
                           pIn_nCopyType          in number,
                           pIn_vFVersion          in varchar2,
                           pIn_vTVersion          in varchar2,
                           pIn_vDefaultVersion    in varchar2,
                           pIn_nNodeLevel         in number,
                           pIn_nChronology        in number,
                           pIn_nDecimal           in number,
                           pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description: this  is the interface of  copyTSID. it support for month copyTSID  AND  week copyTSID  AND  day copyTSID
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
    --  V7.0        20-Feb-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    FMP_LOG.FMP_SETVALUE(pIn_nSelectionID);
    FMP_LOG.FMP_SETVALUE(pIn_nHorizon);
    FMP_LOG.FMP_SETVALUE(pIn_vFFirstPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vTFirstPeriodTime);
    FMP_LOG.FMP_SETVALUE(pIn_vConditions);
    FMP_LOG.FMP_SETVALUE(pIn_vFTimeSeriesID);
    FMP_LOG.FMP_SETVALUE(pIn_vTTimeSeriesID);
    FMP_LOG.FMP_SETVALUE(pIn_nDataOperationType);
    FMP_LOG.FMP_SETVALUE(pIn_nAdjustment);
    FMP_LOG.FMP_SETVALUE(pIn_nAdjustmentType);
    FMP_LOG.FMP_SETVALUE(pIn_nCopyType);
    FMP_LOG.FMP_SETVALUE(pIn_vFVersion);
    FMP_LOG.FMP_SETVALUE(pIn_vTVersion);
    FMP_LOG.FMP_SETVALUE(pIn_vDefaultVersion);
    FMP_LOG.FMP_SETVALUE(pIn_nNodeLevel);
    FMP_LOG.FMP_SETVALUE(pIn_nChronology);
    FMP_LOG.FMP_SETVALUE(pIn_nDecimal);
    
    FMP_log.logBegin;
    if p_constant.Monthly = pIn_nChronology then
      FMSP_COPYTSIDMONTHLY(pIn_nSelectionID       => pIn_nSelectionID,
                           pIn_nHorizon           => pIn_nHorizon,
                           pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                           pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                           pIn_vConditions        => pIn_vConditions,
                           pIn_vFTimeSeriesID     => pIn_vFTimeSeriesID,
                           pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                           pIn_nDataOperationType => pIn_nDataOperationType,
                           pIn_nAdjustment        => pIn_nAdjustment,
                           pIn_nAdjustmentType    => pIn_nAdjustmentType,
                           pIn_nCopyType          => pIn_nCopyType,
                           pIn_vFVersion          => pIn_vFVersion,
                           pIn_vTVersion          => pIn_vTVersion,
                           pIn_vDefaultVersion    => pIn_vDefaultVersion,
                           pIn_nNodeLevel         => pIn_nNodeLevel,
                           pIn_nDecimal           => pIn_nDecimal,
                           pOut_nSqlCode          => pOut_nSqlCode);
    elsif p_constant.Weekly = pIn_nChronology then
      FMSP_COPYTSIDWEEKLY(pIn_nSelectionID       => pIn_nSelectionID,
                          pIn_nHorizon           => pIn_nHorizon,
                          pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                          pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                          pIn_vConditions        => pIn_vConditions,
                          pIn_vFTimeSeriesID     => pIn_vFTimeSeriesID,
                          pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                          pIn_nDataOperationType => pIn_nDataOperationType,
                          pIn_nAdjustment        => pIn_nAdjustment,
                          pIn_nAdjustmentType    => pIn_nAdjustmentType,
                          pIn_nCopyType          => pIn_nCopyType,
                          pIn_vFVersion          => pIn_vFVersion,
                          pIn_vTVersion          => pIn_vTVersion,
                          pIn_vDefaultVersion    => pIn_vDefaultVersion,
                          pIn_nNodeLevel         => pIn_nNodeLevel,
                          pIn_nDecimal           => pIn_nDecimal,
                          pOut_nSqlCode          => pOut_nSqlCode);
    elsif p_constant.Daily = pIn_nChronology then
      FMSP_COPYTSIDDAILY(pIn_nSelectionID       => pIn_nSelectionID,
                         pIn_nHorizon           => pIn_nHorizon,
                         pIn_vFFirstPeriodTime  => pIn_vFFirstPeriodTime,
                         pIn_vTFirstPeriodTime  => pIn_vTFirstPeriodTime,
                         pIn_vConditions        => pIn_vConditions,
                         pIn_vFTimeSeriesID     => pIn_vFTimeSeriesID,
                         pIn_vTTimeSeriesID     => pIn_vTTimeSeriesID,
                         pIn_nDataOperationType => pIn_nDataOperationType,
                         pIn_nAdjustment        => pIn_nAdjustment,
                         pIn_nAdjustmentType    => pIn_nAdjustmentType,
                         pIn_nCopyType          => pIn_nCopyType,
                         pIn_vFVersion          => pIn_vFVersion,
                         pIn_vTVersion          => pIn_vTVersion,
                         pIn_vDefaultVersion    => pIn_vDefaultVersion,
                         pIn_nNodeLevel         => pIn_nNodeLevel,
                         pIn_nDecimal           => pIn_nDecimal,
                         pOut_nSqlCode          => pOut_nSqlCode);
    end if;
    FMP_LOG.LOGEND;
    pOut_nSqlCode := 0;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  END FMISP_COPYTSID;
end FMIP_COPYTSID;
/
