CREATE OR REPLACE PACKAGE FMP_Coeff IS

  g_eException exception; --comments

  procedure FMSP_GetDimensionCoeff(pIn_nNodeType      in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                                   pIn_nChronology    in number, --1: monthly, 2: weekly, 4: daily
                                   pIn_arrNodeAddr    in clob, --db addr separated by ","
                                   pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
                                   pOut_strTableName  out varchar2, --temporary table name used to save coefficient/UOM data
                                   pOut_nPeriodCount  out number, --period count of coefficient
                                   pOut_nSQLCode      out number);

  procedure FMSP_GetCoeffSQL(pIn_nNodeType  in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                             pIn_nType      in number, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                             pIn_iCycle     in int,
                             pIn_vTableName in varchar2,
                             pIn_iBeginYY   in int,
                             pIn_iEndYY     in int,
                             pOut_vSTRSQL   out clob);

  procedure FMISP_GetDimensionCoeff(pIn_nNodeType      in number,
                                    pIn_nChronology    in number,
                                    pIn_vTabName       in varchar2,
                                    pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
                                    pOut_strTableName  out varchar2,
                                    pOut_nPeriodCount  out number,
                                    pOut_nSQLCode      out number);
END FMP_Coeff;
/
CREATE OR REPLACE PACKAGE BODY FMP_Coeff IS

  --*****************************************************************
  -- Description: Get Dimension coefficient.
  --
  -- Author:      <wfq>
  -- Revise
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        24-12-2012     wfq           Created.

  -- **************************************************************

  procedure FMSP_GetDimensionCoeff(pIn_nNodeType      in number,
                                   pIn_cNodeList      in clob default null,
                                   pIn_vTabName       in varchar2 default null,
                                   pIn_nChronology    in number,
                                   pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
                                   pOut_strTableName  out varchar2,
                                   pOut_nPeriodCount  out number,
                                   pOut_nSQLCode      out number) as
    --*****************************************************************
    -- Description: Get Dimension coefficient.
    --
    -- Parameters:
    --  pIn_nNodeType     in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
    --  pIn_cNodeList:node list
    --   pIn_arrNodeAddr   in clob, --db addr separated by ","
    --  pIn_nChronology   in number, --1: monthly, 2: weekly, 4: daily
    --  pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
    --   pOut_strTableName out varchar2, --temporary table name used to save coefficient/UOM data
    --   pOut_nPeriodCount out number, --period count of coefficient

    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JYLiu           Copy code from FMISP_GetDimensionCoeff
    --  V7.0        18-4-2013     JYLiu           add daliy
    -- **************************************************************
    vStrSql clob := '';

    iCycle     int;
    i          int := 0;
    vTableName varchar2(80);
    iPeriod    int := 0;

    iBeginYY int := 0;
    iEndYY   int := 0;

    vSQL varchar2(128);
  BEGIN
    pOut_nSqlCode := 0;
    if pIn_cNodeList is not null then
      FMSP_ClobToTable(pIn_cClob     => pIn_cNodeList,
                       pOut_nSqlCode => pOut_nSqlCode);
    else
      fmsp_execsql(pIn_cSql => 'truncate table tb_node');
      vSQL := ' insert into tb_node select id from ' || pIn_vTabName;
      fmsp_execsql(pIn_cSql => vSql);
    end if;

    IF pIn_nChronology = p_constant.Monthly THEN
      --1: monthly
      vTableName := '_M';
    ELSIF pIn_nChronology = p_constant.Weekly THEN
      --2: weekly
      vTableName := '_W';
    ELSIF pIn_nChronology = p_constant.Daily THEN
      --4: daily
      vTableName := '_D';
    END IF;

    iCycle := pIn_nPeriodPerYear;

    -- get max Period
    vStrSql := '
    select nvl(Min(YY),0) minYY,nvl(Max(YY),0) maxyy  from (
    select distinct YY from trf' || vTableName || '
    union
    select YY from dvs' || vTableName || '
    union
    select YY from rms' || vTableName || '
    )';
    execute immediate vStrSql
      into iBeginYY, iEndYY;

    iPeriod           := (iEndYY - iBeginYY + 1) * iCycle;
    pOut_nPeriodCount := iPeriod;

    --select seq_tb_pimport.Nextval into pOut_strTableName from dual;
    pOut_strTableName := fmf_gettmptablename(); --'TB' || pOut_strTableName;

    vStrSql := 'CREATE TABLE ' || pOut_strTableName || '(
      type int ,
      coefficientID int ,
      dimensionID int ,
      No int ,
      Key varchar(50),
      Description varchar(500),
      Target number,
      UOM varchar(500),
      BeginYY int,
      Beginperiod int,
      EndYY int,
      Endperiod int
        ';
    for i in 1 .. iPeriod loop
      vStrSql := vStrSql || ',T_' || i || ' NUMBER';
    end loop;
    vStrSql := vStrSql || ')';
    execute immediate vStrSql;

    --add log
    Fmp_Log.loginfo(vStrSql);
    if pOut_nSQLCode <> 0 then
      return;
    end if;

    --**********************Dimension coefficient***********************************

    --10008: UOM--------------------------------------------------------------

    FMSP_GetCoeffSQL(pIn_nNodeType  => pIn_nNodeType, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                     pIn_nType      => 10008, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                     pIn_iCycle     => iCycle,
                     pIn_vTableName => 'scl' || vTableName,
                     pIn_iBeginYY   => iBeginYY,
                     pIn_iEndYY     => iEndYY,
                     pOut_vSTRSQL   => vStrSql);
    vStrSql := 'insert  into ' || pOut_strTableName || vStrSql;
    fmsp_execsql(vStrSql);


    --10009:product--------------------------------------------------------------

    FMSP_GetCoeffSQL(pIn_nNodeType  => pIn_nNodeType, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                     pIn_nType      => 10009, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                     pIn_iCycle     => iCycle,
                     pIn_vTableName => 'trf' || vTableName,
                     pIn_iBeginYY   => iBeginYY,
                     pIn_iEndYY     => iEndYY,
                     pOut_vSTRSQL   => vStrSql);
    vStrSql := 'insert  into ' || pOut_strTableName || vStrSql;
    fmsp_execsql(vStrSql);

    --10010: trade channel,--------------------------------------------------------------

    FMSP_GetCoeffSQL(pIn_nNodeType  => pIn_nNodeType, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                     pIn_nType      => 10010, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                     pIn_iCycle     => iCycle,
                     pIn_vTableName => 'rms' || vTableName,
                     pIn_iBeginYY   => iBeginYY,
                     pIn_iEndYY     => iEndYY,
                     pOut_vSTRSQL   => vStrSql);
    vStrSql := 'insert  into ' || pOut_strTableName || vStrSql;
    fmsp_execsql(vStrSql);

    --10011:sales territory--------------------------------------------------------------

    FMSP_GetCoeffSQL(pIn_nNodeType  => pIn_nNodeType, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                     pIn_nType      => 10011, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                     pIn_iCycle     => iCycle,
                     pIn_vTableName => 'dvs' || vTableName,
                     pIn_iBeginYY   => iBeginYY,
                     pIn_iEndYY     => iEndYY,
                     pOut_vSTRSQL   => vStrSql);
    vStrSql := 'insert  into ' || pOut_strTableName || vStrSql;
    fmsp_execsql(vStrSql);

  exception
    when others then
      pOut_nSqlCode := SQLCODE;
      Fmp_Log.LOGERROR;
      raise;
  END;

  procedure FMSP_GetCoeffSQL(pIn_nNodeType  in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                             pIn_nType      in number, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
                             pIn_iCycle     in int,
                             pIn_vTableName in varchar2,
                             pIn_iBeginYY   in int,
                             pIn_iEndYY     in int,
                             pOut_vSTRSQL   out clob) as
    --*****************************************************************
    -- Description: Get Dimension coefficient.
    --
    -- Parameters:
    --   pIn_nNodeType   in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
    --  pIn_nType       in number, --10009:product, 10011:sales territory, 10010: trade channel,10008: UOM
    --     pIn_arrNodeAddr in clob, --db addr separated by ","
    --     pIn_iCycle      in int,
    --     pIn_vTableName  in varchar2,
    --     pIn_iBeginYY    in int,
    --     pIn_iEndYY      in int,
    --     pOut_vSTRSQL    out varchar2

    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        29-12-2012     wfq           Created.
    -- **************************************************************
    vStrSql clob := '';

    vStrfrom clob := '';
    -- vStrWhere  varchar(8000) := '';
    i          int := 0;
    vTableName varchar2(80);
    vField     varchar2(80);
    iPeriod    int := 0;

    vtssql     clob;
    vtsfromsql clob;
    iYY        int;
    vTarget    varchar2(500);
    vUOM       varchar2(500);
  BEGIN

    --**********************Dimension coefficient***********************************
    IF pIn_nType = 10008 THEN
      --10008: UOM
      vTarget    := 'unite Target';
      vUOM       := 'num_crt vUOM';
      vTableName := 'fmv_UomVct';
      vField     := 'fam4_em_addr';
    ELSIF pIn_nType = 10009 THEN
      --10009:product
      vTarget    := 'price Target';
      vUOM       := 'null vUOM';
      vTableName := 'FMV_famtrf';
      vField     := 'fam4_em_addr';
    ELSIF pIn_nType = 10010 THEN
      --10010: trade channel,
      vTarget    := 'null Target';
      vUOM       := 'null vUOM';
      vTableName := 'FMV_DisRms';
      vField     := 'dis6_em_addr';
    ELSIF pIn_nType = 10011 THEN
      --10011:sales territory
      vTarget    := 'null Target';
      vUOM       := 'null vUOM';
      vTableName := 'FMV_Geodvs';
      vField     := 'Geo5_em_addr';
    END IF;

    --10003: node in pvt, 10004: node in sel, 10005:node in bdg********************************************
    IF pIn_nNodeType = 10003 THEN
      --10003: node in pvt

      vStrfrom := ' from pvt T join tb_node n on t.pvt_em_addr=n.ID ';
    ELSIF pIn_nNodeType = 10004 THEN
      --10004: node in sel
      vStrfrom := ' from v_aggnodetodimension T  join tb_node n on t.sel_em_addr=n.ID ';

    END IF;
    --*************************************************************************************

    vStrSql := ' select distinct ' || pIn_nType ||
               ', coefficientID, dimensionID,No,Key ,Description,' ||
               vTarget || ' ,' || vUOM ||
               ',minYY BeginYY,(case when minYY>0 then 1 else null end) Beginperiod, MaxYY EndYY, (case when MaxYY>0 then ' ||
               pIn_iCycle || ' else null end) Endperiod';

    vStrfrom := vStrfrom || ' join ' || vTableName || ' f on t.' || vField ||
                '=f.dimensionID ';
    --Time series SQL
    vtssql     := ' select a.Nodeid,minYY,maxyy';
    vtsfromsql := ' from (select NodeID, Min(YY) minYY,Max(YY) maxyy from ' ||
                  pIn_vTableName || ' group by NodeID) a ';

    FOR iYY in pIn_iBeginYY .. pIn_iEndYY LOOP
      FOR iPeriod in 1 .. pIn_iCycle LOOP
        i       := i + 1;
        vtssql  := vtssql || ',t' || iYY || '.T' || iPeriod || ' T' || i;
        vStrSql := vStrSql || ',T' || i;
      END LOOP;

      vtsfromsql := vtsfromsql || ' left join ' || pIn_vTableName || ' t' || iYY ||
                    ' on a.Nodeid=t' || iYY || '.NodeID and t' || iYY ||
                    '.YY=' || iYY;

    END LOOP;
    vtssql := substr(vtssql, 2);

    vStrfrom := vStrfrom || ' left join (' || vtssql || vtsfromsql ||
                ') ts  on f.coefficientID=ts.NodeID ';

    pOut_vSTRSQL := vStrSql || vStrfrom;
    --add log
    Fmp_Log.logInfo(pIn_cSqlText => pOut_vSTRSQL);

  exception
    when others then
      Fmp_Log.LOGERROR;
  END;

  procedure FMSP_GetDimensionCoeff(pIn_nNodeType      in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                                   pIn_nChronology    in number, --1: monthly, 2: weekly, 4: daily
                                   pIn_arrNodeAddr    in clob, --db addr separated by ","
                                   pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
                                   pOut_strTableName  out varchar2, --temporary table name used to save coefficient/UOM data
                                   pOut_nPeriodCount  out number, --period count of coefficient
                                   pOut_nSQLCode      out number) as
    --*****************************************************************
    -- Description: Get Dimension coefficient.
    --
    -- Parameters:
    --  pIn_nNodeType     in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
    --  pIn_nChronology   in number, --1: monthly, 2: weekly, 4: daily
    --   pIn_arrNodeAddr   in clob, --db addr separated by ","
    --   pOut_strTableName out varchar2, --temporary table name used to save coefficient/UOM data
    --   pOut_nPeriodCount out number, --period count of coefficient

    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        24-12-2012     wfq           Created.
    -- **************************************************************
  BEGIN
    fmp_log.FMP_SetValue(pIn_nNodeType);
    fmp_log.FMP_SetValue(pIn_nChronology);
    fmp_log.FMP_SetValue(pIn_arrNodeAddr);
    fmp_log.FMP_SetValue(pIn_nPeriodPerYear);
    fmp_log.LOGBEGIN;
    FMSP_GetDimensionCoeff(pIn_nNodeType      => pIn_nNodeType,
                           pIn_cNodeList      => pIn_arrNodeAddr,
                           pIn_nChronology    => pIn_nChronology,
                           pIn_nPeriodPerYear => pIn_nPeriodPerYear,
                           pOut_strTableName  => pOut_strTableName,
                           pOut_nPeriodCount  => pOut_nPeriodCount,
                           pOut_nSQLCode      => pOut_nSQLCode);
    fmp_log.LOGEND;
  END;

  procedure FMISP_GetDimensionCoeff(pIn_nNodeType      in number,
                                    pIn_nChronology    in number,
                                    pIn_vTabName       in varchar2,
                                    pIn_nPeriodPerYear in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
                                    pOut_strTableName  out varchar2,
                                    pOut_nPeriodCount  out number,
                                    pOut_nSQLCode      out number) as
    --*****************************************************************
    -- Description: Get Dimension coefficient.
    --
    -- Parameters:
    --  pIn_nNodeType     in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
    --  pIn_nChronology   in number, --1: monthly, 2: weekly, 4: daily
    --   pIn_arrNodeAddr   in clob, --db addr separated by ","
    --   pOut_strTableName out varchar2, --temporary table name used to save coefficient/UOM data
    --   pOut_nPeriodCount out number, --period count of coefficient

    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JYLiu           Copy code from FMISP_GetDimensionCoeff
    -- **************************************************************
  BEGIN

    fmp_log.FMP_SetValue(pIn_nNodeType);
    fmp_log.FMP_SetValue(pIn_nChronology);
    fmp_log.FMP_SetValue(pIn_vTabName);
    fmp_log.FMP_SetValue(pIn_nPeriodPerYear);
    fmp_log.LOGBEGIN;
    FMSP_GetDimensionCoeff(pIn_nNodeType      => pIn_nNodeType,
                           pIn_vTabName       => pIn_vTabName,
                           pIn_nChronology    => pIn_nChronology,
                           pIn_nPeriodPerYear => pIn_nPeriodPerYear,
                           pOut_strTableName  => pOut_strTableName,
                           pOut_nPeriodCount  => pOut_nPeriodCount,
                           pOut_nSQLCode      => pOut_nSQLCode);
    fmp_log.LOGEND;

  END;
END FMP_Coeff;
/
