CREATE OR REPLACE PACKAGE FMP_Event IS

  procedure FMSP_GetExternalEvents(pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
                                   pIn_arrNodeAddr in clob,
                                   pIn_nChronology in number,
                                   pOut_vTableName out varchar2,
                                   pOut_nSQLCode   out number);

  procedure FMISP_GetExternalEvents(pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
                                    pIn_vTabName    in varchar2,
                                    pIn_nChronology in number,
                                    pOut_vTableName out varchar2,
                                    pOut_nSQLCode   out number);

end FMP_Event;
/
CREATE OR REPLACE PACKAGE BODY FMP_Event IS
  --*****************************************************************
  -- Description: locate a external event of a node

  -- Author:     wfq
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        15-3-2012     wfq     Created.
  -- **************************************************************

  procedure FMSP_GetExternalEvents(pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
                                   pIn_arrNodeAddr in clob default null,
                                   pIn_vTabName    in varchar2 default null,
                                   -- pIn_nChronology in number,
                                   --pIn_nBeginYear  in number,
                                   -- pIn_nEndYear    in number,
                                   pOut_vTableName out varchar2,
                                   pOut_nSQLCode   out number)

    --*****************************************************************
    -- Description: locate a external event of a node
    -- Parameters:
    --       pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
    --       pIn_arrNodeAddr: the string contains node id,seperated by comma
    --       pIn_nChronology:--1: monthly, 2: weekly, 3: daily
    --       pIn_nBeginYear
    --       pIn_nEndYear
    --       pOut_vTableName
    --       pOut_nSqlCode:error code
    -- Error Conditions Raised:
    --
    -- Author:     wfq
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        17-2-2012     wfq     Created.
    -- **************************************************************
   as

    vStrSql    varchar(8000);
    nNodeType  int;
    vTableName varchar(80);
  BEGIN
    pOut_nSqlCode := 0;

    nNodeType := pIn_nNodeType - 10002;
    IF pIn_vTabName IS NULL THEN
      FMP_GetBDG.FMSP_ConverttoBDG(pIn_nNodeType     => nNodeType, --1  Detail Node  2  Aggregate Node 3 BDG
                                   pIn_arrNodeAddr   => pIn_arrNodeAddr,
                                   pOut_strTableName => vTableName, --temporary table name
                                   pOut_nSQLCode     => pOut_nSQLCode);
    ELSE
      FMP_GetBDG.FMISP_ConverttoBDG(pIn_nNodeType     => nNodeType, --1  Detail Node  2  Aggregate Node 3 BDG
                                    pIn_vTabName      => pIn_vTabName,
                                    pOut_strTableName => vTableName, --temporary table name
                                    pOut_nSQLCode     => pOut_nSQLCode);
    END IF;
    IF pOut_nSQLCode <> 0 THEN
      RETURN;
    END IF;

    pOut_vTableName := fmf_gettmptablename(); --'TB' || pOut_strTableName;

    vStrSql := 'CREATE TABLE ' || pOut_vTableName || '(
      NodeID NUMBER ,
      exoID NUMBER ,
      exo_cle NVARCHAR2(60) ,
      exo_desc NVARCHAR2(120) ,
      exo_dure INTEGER ,
      exo_type RAW(52),
      BGCtype int,
      bgcID number,
      YY int
        ';
    for i in 1 .. 14 loop
      vStrSql := vStrSql || ',T_' || i || ' NUMBER';
    end loop;
    vStrSql := vStrSql || ')';
    execute immediate vStrSql;

    vStrSql := ' select t.bdgID,
         e.exo_em_addr,
         e.exo_cle,
         e.exo_desc,
         e.exo_dure,
         e.exo_type,
         1 BGCtype
         ,b.bgc_em_addr
         ,d.annee_bdg ';
    for i in 1 .. 14 loop
      vStrSql := vStrSql || ',d.m_bdg_' || i || ' T' || i || '';
    end loop;

    vStrSql := vStrSql || ' from ' || vTableName ||
               ' t, bgc b, exo e, bud d
   where t.BDGID = b.bdg31_em_addr
     and b.exo43_em_addr = e.exo_em_addr
     and b.bgc_em_addr = d.bgc32_em_addr';
    /*union
    select t.bdgID,
         e.exo_em_addr,
         e.exo_cle,
         e.exo_desc,
         e.exo_dure,
         e.exo_type,
         2 BGCtype
         ,s.serie_budget_em_addr
         ,d.annee_budget ';
    for i in 1 .. 14 loop
      vStrSql := vStrSql || ',d.m_don_budget_' || i || ' T' || i || '';
    end loop;

    vStrSql := vStrSql || ' from ' || vTableName ||
               ' t, serie_budget s, exo e, don_budget d
   where t.BDGID = s.bdg70_em_addr
     and s.exo72_em_addr = e.exo_em_addr
     and s.serie_budget_em_addr = d.serie_budget71_em_addr';*/

    
    vStrSql := 'insert /*+ append */ into ' || pOut_vTableName || vStrSql;
    --add log
    Fmp_Log.logInfo(pIn_cSqlText => vStrSql);

    execute immediate vStrSql;
    commit;

    --drop temp table
    vStrSql := 'drop table '||vTableName;
    execute immediate vStrSql;

  exception
    when others then
      pOut_nSqlCode := sqlcode;
      raise_application_error(-20006, sqlcode || sqlerrm);
  end;

  procedure FMSP_GetExternalEvents(pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
                                   pIn_arrNodeAddr in clob,
                                   pIn_nChronology in number,
                                   pOut_vTableName out varchar2,
                                   pOut_nSQLCode   out number) as
  BEGIN

    --add log
    Fmp_log.FMP_SetValue(pIn_nNodeType);
    Fmp_log.FMP_SetValue(pIn_arrNodeAddr);
    Fmp_log.FMP_SetValue(pIn_nChronology);
    Fmp_log.LOGBEGIN;
    FMSP_GetExternalEvents(pIn_nNodeType   => pIn_nNodeType,
                           pIn_arrNodeAddr => pIn_arrNodeAddr,
                           pIn_vTabName    => null,
                           pOut_vTableName => pOut_vTableName,
                           pOut_nSQLCode   => pOut_nSQLCode);
    fmp_log.LOGEND;

  END;

  procedure FMISP_GetExternalEvents(pIn_nNodeType   in number, --10003: PVT, 10004:SEL, 10005:BDG
                                    pIn_vTabName    in varchar2,
                                    pIn_nChronology in number,
                                    pOut_vTableName out varchar2,
                                    pOut_nSQLCode   out number) as
  BEGIN

    --add log
    Fmp_log.FMP_SetValue(pIn_nNodeType);
    Fmp_log.FMP_SetValue(pIn_vTabName);
    Fmp_log.FMP_SetValue(pIn_nChronology);
    Fmp_log.LOGBEGIN;
    FMSP_GetExternalEvents(pIn_nNodeType   => pIn_nNodeType,
                           pIn_vTabName    => pIn_vTabName,
                           pOut_vTableName => pOut_vTableName,
                           pOut_nSQLCode   => pOut_nSQLCode);
    fmp_log.LOGEND;

  END;

END FMP_Event;
/
