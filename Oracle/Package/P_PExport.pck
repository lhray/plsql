create or replace package P_PExport authid current_user is

  -- Author  : YPWANG
  -- Created : 10/12/2012 1:57:59 PM
  -- Purpose : procedure for pexport batch command

  function splitstr2list(p_nNodeType       in integer,
                         p_nBeginDate      in integer,
                         p_nEndDate        in integer,
                         p_strSeparator    in varchar2,
                         p_isNotNull       in number,
                         p_strTmpTableName in varchar2)
    return fmt_NodeTimeSeries
    pipelined
    parallel_enable;

  procedure PutDataToList(p_nNodeType       in integer,
                          p_nBeginDate      in integer,
                          p_nEndDate        in integer,
                          p_strSeparator    in varchar2,
                          pin_strTableName  in varchar2,
                          p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                          pout_TmpListTable out varchar2);

  --pexport sp_ProcessExportTimeseries
  procedure sp_ProcessExportTimeseries(p_nCommandNumber   in integer, --batch command number, reserved information
                                       p_nChronology      in integer, --1 Month ,2 Week,4 Day, reserved information
                                       p_nDBTimeSeriesID  in integer, --The real time series ID in orcle database
                                       p_nNodeType        in integer, --1:Detail Node, 2: Aggregate Node
                                       p_nBDGFlag         in integer, --0: is not a BDG time series, 1: is a BDG time series
                                       p_strFMUSER        in varchar2, --Current user name, reserved information
                                       p_strOptions       in varchar2, --batch command switchs, use ## as separator
                                       p_nBeginDate       in integer, --Begin date of time series
                                       p_nEndDate         in integer, --End date of time series
                                       p_nPeriodCountYear in integer, --period count every year
                                       --This procedure will generate a temporary table to save time series data,
                                       --this out parameter is used to return the table name to FMWin
                                       p_strSeparator    in varchar2,
                                       p_strDecimals     in number, --Decimals config
                                       p_strTmpTableName out varchar2,
                                       p_nSqlCode        out integer --return code, 0: correct, not 0: error
                                       );

  --Parse date to 2 parameters: year and period
  procedure sp_ParseDate(p_nChronology in integer, --1 Month ,2 Week,4 Day, reserved information
                         p_nDate       in integer, --date to be parse, with format YYYYPP or YYYYPPP according to p_nChronologyType
                         p_nYear       out integer,
                         p_nPeriod     out integer,
                         p_nSqlCode    in out integer);

  --create a temporary table
  procedure sp_CreateTemporaryTable(p_nChronology  in integer, --reference to sp_ProcessExportTimeseries.p_nChronology
                                    p_bDetailNode  in boolean, --reference to sp_ProcessExportTimeseries.v_bDetailNode
                                    p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType, --reference to sp_ProcessExportTimeseries.v_oOptions
                                    p_strTableName out varchar2, --reference to sp_ProcessExportTimeseries.p_strTmpTableName
                                    p_nSqlCode     in out integer);

  --Put time series data to temporary table
  function F_PutDataToTmpTable(p_nChronology      in integer, --reference to sp_ProcessExportTimeseries.p_nChronology
                               p_nDBTimeSeriesID  in integer, --reference to sp_ProcessExportTimeseries.p_nDBTimeSeriesID
                               p_bDetailNode      in boolean, --reference to sp_ProcessExportTimeseries.v_bDetailNode
                               p_bInBDG           in boolean, --reference to sp_ProcessExportTimeseries.v_bInBDG
                               p_oOptions         in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType, --reference to sp_ProcessExportTimeseries.v_oOptions
                               p_nBeginYear       in integer, --reference to sp_ProcessExportTimeseries.v_nBeginYear
                               p_nBeginPeriod     in integer, --reference to sp_ProcessExportTimeseries.v_nBeginPeriod
                               p_nEndYear         in integer, --reference to sp_ProcessExportTimeseries.v_nEndYear
                               p_nEndPeriod       in integer, --reference to sp_ProcessExportTimeseries.v_nEndPeriod
                               p_nPeriodCountYear in integer, --refernce to sp_ProcessExportTimeseries.p_nPeriodCountYear
                               p_strSeparator     in varchar2,
                               p_strDecimals      in number,
                               p_strTmpTableName  in varchar2 --reference to sp_ProcessExportTimeseries.p_strTmpTableName
                               ) return boolean;
  procedure FMISP_SetDate(pIn_cDatePeriod   in clob,
                          pIn_vTableName    in varchar2,
                          pIn_v1stSeperator in varchar,
                          pIn_v2ndSeperator in varchar,
                          pOut_nSqlcode     out number);

end P_PExport;
/
create or replace package body P_PExport is

  vSep       varchar2(20) := '';
  bBegindate boolean := false;
  bEnddate   boolean := false;
  procedure fmsp_updatedate(pIn_vTableName in varchar2);
  -- Procedure implementations
  function splitstr2list(p_nNodeType       in integer,
                         p_nBeginDate      in integer,
                         p_nEndDate        in integer,
                         p_strSeparator    in varchar2,
                         p_isNotNull       in number,
                         p_strTmpTableName in varchar2)
    return fmt_NodeTimeSeries
    pipelined
    parallel_enable as
    pragma INLINE(str2nlist, 'YES');
    type t_exp is record(
      AGGNODE varchar2(200),
      product varchar2(60),
      sales   varchar2(60),
      trade   varchar2(60),
      yy      varchar2(4),
      mm      varchar2(2),
      t_data  clob);
    type explist is table of t_exp;
    v_explist      explist;
    v_Cycle        number;
    v_strSeparator varchar2(10);
    v_post         pls_integer := 0;
    v_nextpost     pls_integer := 0;
    v_cnt          pls_integer;
    v_beginmm      number;
    v_beginyy      number;
    v_mm           number;
    v_yy           number;
    v_data         number;
    v_sqlstr       varchar2(1000);
    v_cursor       sys_refcursor;
  begin
    if p_strSeparator = 'ESP' then
      v_strSeparator := ' ';
    else
      v_strSeparator := p_strSeparator;
    end if;
  
    v_beginyy := to_number(substr(to_char(p_nBeginDate), 1, 4));
    v_beginmm := to_number(substr(to_char(p_nBeginDate), 5));
    case
      when p_nNodeType = P_CONSTANT.DETAIL_NODE_TYPE then
        v_sqlstr := 'select '''' AGGNODE, product, sales, trade, null yy, null mm ,t_data from ' ||
                    p_strTmpTableName;
      when p_nNodeType = P_CONSTANT.Aggregate_Node_Type then
        v_sqlstr := 'select AGGNODE, product, sales, trade, null yy, null mm ,t_data from ' ||
                    p_strTmpTableName;
    end case;
    v_Cycle := 12;
    v_cnt   := (substr(p_nEndDate, 1, 4) - substr(p_nBeginDate, 1, 4)) *
               v_Cycle +
               (substr(p_nEndDate, 5) - substr(p_nBeginDate, 5) + 1);
  
    open v_cursor for v_sqlstr;
    loop
      fetch v_cursor bulk collect
        into v_explist limit 10000;
      for j in 1 .. v_explist.count loop
      
        v_yy := v_beginyy;
      
        /*        v_cnt  := length(v_explist(j).t_data) -
        length(replace(v_explist(j).t_data, v_strSeparator));*/
        if substr(v_explist(j).t_data, -1) <> v_strSeparator then
          v_explist(j).t_data := v_explist(j).t_data || v_strSeparator;
        end if;
      
        v_post := 0;
        for i in 1 .. v_cnt loop
          v_mm := mod(v_beginmm + i - 2, 12) + 1;
          if i > 1 and v_mm = 1 then
            v_yy := v_yy + 1;
          end if;
        
          v_nextpost := dbms_lob.instr(v_explist(j).t_data,
                                       v_strSeparator,
                                       v_post + 1,
                                       1);
          v_data     := to_number(dbms_lob.substr(v_explist(j).t_data,
                                                  v_nextpost - v_post - 1,
                                                  v_post + 1));
          if p_isNotNull = 1 and v_data is null then
            null;
          else
            pipe row(fmt_NodeTS(v_explist(j).aggnode,
                                v_explist(j).product,
                                v_explist(j).sales,
                                v_explist(j).trade,
                                trim(to_char(v_yy)),
                                trim(to_char(v_mm, '00')),
                                v_data));
          
          end if;
          v_post := v_nextpost;
        end loop;
      
      end loop;
      exit when v_cursor%notfound;
    end loop;
  
    return;
  end;

  procedure PutDataToList(p_nNodeType       in integer,
                          p_nBeginDate      in integer,
                          p_nEndDate        in integer,
                          p_strSeparator    in varchar2,
                          pin_strTableName  in varchar2,
                          p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                          pout_TmpListTable out varchar2) as
    v_strsql    varchar2(1000);
    v_stryp     varchar2(50);
    v_isnotnull number;
  begin
    pout_TmpListTable := fmf_gettmptablename();
  
    if p_oOptions.bNobl then
      v_isnotnull := 1;
    else
      v_isnotnull := 0;
    end if;
  
    -- No date switch: YYYY+Period
    -- -a_m :YYYY, PP
    -- -aa_mm :YY, PP
    if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm then
      v_stryp := 'substr(yy,3,2)||''' || p_strSeparator || '''||mm enddate';
    elsif p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m then
      v_stryp := 'yy||''' || p_strSeparator || '''||mm enddate';
    else
      v_stryp := 'yy||mm enddate';
    end if;
    dbms_output.put_line(v_stryp);
    case
      when p_nNodeType = P_CONSTANT.DETAIL_NODE_TYPE then
        if p_oOptions.bDebut or not p_oOptions.bSdate then
          v_strsql := 'create table ' || pout_TmpListTable || '
          (
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            enddate VARCHAR2(20),
            t_data  number )';
          execute immediate v_strsql;
        
          v_strsql := 'insert into ' || pout_TmpListTable ||
                      ' select product, sales, trade, ' || v_stryp ||
                      ', t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
          --dbms_output.put_line(v_strsql);
          execute immediate v_strSQL
            using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
        else
          v_strsql := 'create table ' || pout_TmpListTable || '
          (
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            t_data  number )';
          execute immediate v_strsql;
          v_strsql := 'insert into  ' || pout_TmpListTable ||
                      ' select product, sales, trade, t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
          execute immediate v_strSQL
            using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
        end if;
      
      when p_nNodeType = P_CONSTANT.Aggregate_Node_Type then
        if p_oOptions.bDebut or not p_oOptions.bSdate then
          v_strsql := 'create table ' || pout_TmpListTable || '
          (
            AGGNODE VARCHAR2(200),
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            enddate VARCHAR2(20),
            t_data  number
          )';
          execute immediate v_strsql;
        
          v_strsql := 'insert into  ' || pout_TmpListTable ||
                      '  select AGGNODE,  product, sales, trade, ' ||
                      v_stryp ||
                      ', t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
          execute immediate v_strSQL
            using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
        else
          v_strsql := 'create table ' || pout_TmpListTable || '
          (
            AGGNODE VARCHAR2(200),
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            t_data  number
          )';
          execute immediate v_strsql;
        
          v_strsql := 'insert into  ' || pout_TmpListTable ||
                      '  select AGGNODE,  product, sales, trade, t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
          execute immediate v_strSQL
            using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
        end if;
    end case;
  
  end;

  --pexport sp_ProcessExportTimeseries
  procedure sp_ProcessExportTimeseries(p_nCommandNumber   in integer,
                                       p_nChronology      in integer,
                                       p_nDBTimeSeriesID  in integer,
                                       p_nNodeType        in integer,
                                       p_nBDGFlag         in integer,
                                       p_strFMUSER        in varchar2,
                                       p_strOptions       in varchar2,
                                       p_nBeginDate       in integer,
                                       p_nEndDate         in integer,
                                       p_nPeriodCountYear in integer, --period count every year
                                       p_strSeparator     in varchar2,
                                       p_strDecimals      in number, --Decimals config
                                       p_strTmpTableName  out varchar2,
                                       p_nSqlCode         out integer) is
    v_bDetailNode  boolean := true; --    false: Aggregate Node, true:Detail Node
    v_bInBDG       boolean := true; --    false: note in BDG table, true: in BDG table
    v_oOptions     P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType; --   switches of command line
    v_nBeginYear   integer := 0; --    year of begin date
    v_nBeginPeriod integer := 0; --    period of begin date
    v_nEndYear     integer := 0; --    year of end date
    v_nEndPeriod   integer := 0; --    period of end date
    v_nPeriodCount integer := 0;
    v_bFuncRet     boolean := true; --use this variant to get function return value
    v_strSeparator varchar2(10);
  begin
    p_nSqlCode := 0;
  
    Fmp_Log.FMP_SetValue(p_nCommandNumber);
    Fmp_log.FMP_SetValue(p_nChronology);
    Fmp_log.FMP_SetValue(p_nDBTimeSeriesID);
    Fmp_log.FMP_SetValue(p_nNodeType);
    Fmp_Log.FMP_SetValue(p_nBDGFlag);
    Fmp_log.FMP_SetValue(p_strFMUSER);
    Fmp_log.FMP_SetValue(p_strOptions);
    Fmp_log.FMP_SetValue(p_nBeginDate);
    Fmp_log.FMP_SetValue(p_nEndDate);
    Fmp_log.FMP_SetValue(p_nPeriodCountYear);
    Fmp_log.FMP_SetValue(p_strSeparator);
    Fmp_log.FMP_SetValue(p_strDecimals);
    Fmp_log.LOGBEGIN;
    vSep          := p_strSeparator;
    v_bDetailNode := (p_nNodeType = P_CONSTANT.DETAIL_NODE_TYPE);
    v_bInBDG      := (p_nBDGFlag = 1);
  
    if p_strSeparator = 'ESP' then
      v_strSeparator := ' ';
    else
      v_strSeparator := p_strSeparator;
    end if;
  
    --Parse options
    P_BATCHCOMMAND_COMMON.sp_ParseOptions(p_strOptions,
                                          v_oOptions,
                                          p_nSqlCode);
  
    --Parse begin date
    sp_ParseDate(p_nChronology,
                 p_nBeginDate,
                 v_nBeginYear,
                 v_nBeginPeriod,
                 p_nSqlCode);
  
    --Parse end date
    sp_ParseDate(p_nChronology,
                 p_nEndDate,
                 v_nEndYear,
                 v_nEndPeriod,
                 p_nSqlCode);
  
    --Create temporary table for output
    sp_CreateTemporaryTable(p_nChronology,
                            v_bDetailNode,
                            v_oOptions,
                            p_strTmpTableName,
                            p_nSqlCode);
  
    --Put time series data to temporary table
    v_bFuncRet := F_PutDataToTmpTable(p_nChronology,
                                      p_nDBTimeSeriesID,
                                      v_bDetailNode,
                                      v_bInBDG,
                                      v_oOptions,
                                      v_nBeginYear,
                                      v_nBeginPeriod,
                                      v_nEndYear,
                                      v_nEndPeriod,
                                      p_nPeriodCountYear,
                                      v_strSeparator,
                                      p_strDecimals,
                                      p_strTmpTableName);
    --added by zy
    if v_oOptions.bPar1val then
      PutDataToList(p_nNodeType       => p_nNodeType,
                    p_nBeginDate      => p_nBeginDate,
                    p_nEndDate        => p_nEndDate,
                    p_strSeparator    => p_strSeparator,
                    pin_strTableName  => p_strTmpTableName,
                    p_oOptions        => v_oOptions,
                    pout_TmpListTable => p_strTmpTableName);
    end if;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end sp_ProcessExportTimeseries;

  --Parse date to 2 parameters: year and period
  procedure sp_ParseDate(p_nChronology in integer, -- 1:Monthly, 2:Weekly, 3:Daily
                         p_nDate       in integer,
                         p_nYear       out integer,
                         p_nPeriod     out integer,
                         p_nSqlCode    in out integer) is
  begin
  
    case p_nChronology
      when p_constant.Monthly then
        --Monthly
        p_nYear   := trunc(p_nDate / 100);
        p_nPeriod := mod(p_nDate, 100);
      
      when p_constant.Weekly then
        --Weekly
        p_nYear   := trunc(p_nDate / 100);
        p_nPeriod := mod(p_nDate, 100);
      
      else
        --Daily
        p_nYear   := trunc(p_nDate / 1000);
        p_nPeriod := mod(p_nDate, 1000);
      
    end case;
  
  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_ParseDate;

  --create a temporary table
  procedure sp_CreateTemporaryTable(p_nChronology  in integer,
                                    p_bDetailNode  in boolean,
                                    p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                    p_strTableName out varchar2,
                                    p_nSqlCode     in out integer) as
    v_strSQL varchar2(5000) := ''; --SQL command string
  begin
  
    --use sequence number to combin with prefix 'TB' to generate name of temporary table
    --select seq_tb_pimport.Nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename(); --'TB' || p_strTableName;
  
    v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
  
    --field 'AGGNODE'
    if not p_bDetailNode and not p_oOptions.bR2P then
      v_strSQL := v_strSQL || 'AGGNODE VARCHAR2(200),';
    end if;
  
    --field 'PRODUCT'
    v_strSQL := v_strSQL || 'PRODUCT VARCHAR2(60),';
  
    --field 'SALES'
    if not p_oOptions.bNoGeo then
      v_strSQL := v_strSQL || 'SALES VARCHAR2(60),';
    end if;
  
    --field 'TRADE'
    if not p_oOptions.bNoDis then
      v_strSQL := v_strSQL || 'TRADE VARCHAR2(60),';
    end if;
  
    if p_oOptions.bDebut then
      --export begin period
      v_strSQL := v_strSQL || 'beginperiod VARCHAR2(20),';
      -- if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
      v_strSQL   := v_strSQL || 'begindate VARCHAR2(20),';
      bBegindate := true;
      --end if;
    end if;
  
    if not p_oOptions.bSdate then
      -- export end period
      v_strSQL := v_strSQL || 'endperiod VARCHAR2(20),';
      --if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
      v_strSQL := v_strSQL || 'enddate VARCHAR2(20),';
      --end if;
      bEnddate := true;
    end if;
  
    v_strSQL := v_strSQL || ' T_DATA CLOB NULL )';
    --Execute
    execute immediate v_strSQL;
  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_CreateTemporaryTable;

  -- Refactored procedure sp_GenerateCommonSelectSQL
  procedure sp_GenerateCommonSelectSQL(p_nChronology      in integer,
                                       p_bDetailNode      in boolean,
                                       p_bInBDG           in boolean,
                                       p_oOptions         in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                       p_nBeginYear       in integer,
                                       p_nBeginPeriod     in integer,
                                       p_nEndYear         in integer,
                                       p_nEndPeriod       in integer,
                                       p_nPeriodCountYear in integer,
                                       p_strSeparator     in varchar2,
                                       p_strDecimals      in number, --Decimals config
                                       p_strTmpTableName  in varchar2,
                                       v_strSQL           in out clob) is
    v_nMaxRcdLen       integer := 0;
    v_strBeginYear     varchar(20) := '';
    v_strEndYear       varchar(20) := '';
    v_strBeginPeriod   varchar(20) := '';
    v_strEndPeriod     varchar(20) := '';
    v_strTableName     varchar(32) := '';
    v_strFieldPrefix   varchar(32) := '';
    v_strDecimalsValue varchar2(1000);
  begin
    v_nMaxRcdLen       := p_nPeriodCountYear;
    v_strDecimalsValue := '';
  
    if p_strDecimals > 0 then
      v_strDecimalsValue := 'FM999999999999990.';
      for i in 1 .. p_strDecimals loop
        v_strDecimalsValue := v_strDecimalsValue || '0';
      end loop;
    else
      v_strDecimalsValue := 'FM999999999999990';
    end if;
  
    --field 'YY'
    v_strBeginYear := to_char(p_nBeginYear);
    v_strEndYear   := to_char(p_nEndYear);
  
    if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm then
      --Only show last 2 number of the year for swith -aa_mm
      v_strBeginYear := substr(v_strBeginYear, 3);
      v_strEndYear   := substr(v_strEndYear, 3);
    end if;
  
    v_strBeginYear := '''' || v_strBeginYear;
    v_strEndYear   := '''' || v_strEndYear;
  
    --field 'MM/WW/DD'
    if length(p_nBeginPeriod) = 1 then
      v_strBeginPeriod := lpad(p_nBeginPeriod, 2, 0) || '''';
    else
      v_strBeginPeriod := to_char(p_nBeginPeriod) || '''';
    end if;
    if length(p_nEndPeriod) = 1 then
      v_strEndPeriod := lpad(p_nEndPeriod, 2, 0) || '''';
    else
      v_strEndPeriod := to_char(p_nEndPeriod) || '''';
    end if;
  
    --select
    v_strSQL := 'insert into ' || p_strTmpTableName || ' select';
  
    --field AGGNODE
    if not p_bDetailNode and not p_oOptions.bR2P then
      v_strSQL := v_strSQL || ' sel.sel_cle as AGGNODE,';
    end if;
  
    --field PRODUCT
    v_strSQL := v_strSQL || ' fam.f_cle as PRODUCT,';
  
    --field 'SALES'
    if not p_oOptions.bNoGeo then
      v_strSQL := v_strSQL ||
                  ' decode(geo.g_cle,chr(1),null,geo.g_cle) as SALES,';
    end if;
  
    --field 'TRADE'
    if not p_oOptions.bNoDis then
      v_strSQL := v_strSQL ||
                  ' decode(dis.d_cle,chr(1),null,dis.d_cle) as TRADE,';
    end if;
  
    if p_oOptions.bDebut then
      if p_oOptions.nDateFormat between
         P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm and
         P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m then
        v_strSQL := v_strSQL || v_strBeginYear || p_strSeparator ||
                    v_strBeginPeriod || ' as beginperiod, ';
      else
        v_strSQL := v_strSQL || v_strBeginYear || v_strBeginPeriod ||
                    ' as beginperiod, ';
      end if;
      v_strSQL := v_strSQL || 'null as BEGINDate,';
    end if;
  
    if not p_oOptions.bSdate then
      if p_oOptions.nDateFormat between
         P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm and
         P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m then
        v_strSQL := v_strSQL || v_strEndYear || p_strSeparator ||
                    v_strEndPeriod || ' as endperiod,';
      else
        v_strSQL := v_strSQL || v_strEndYear || v_strEndPeriod ||
                    ' as endperiod,';
      end if;
      v_strSQL := v_strSQL || 'null as endDate,';
    end if;
    v_strFieldPrefix := 'T';
  
    if p_nEndYear > p_nBeginYear then
      for i in p_nBeginYear .. p_nEndYear loop
        v_strTableName := 't_year_' || to_char(i);
        if i = p_nBeginYear then
          for j in p_nBeginPeriod .. v_nMaxRcdLen loop
            v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                        v_strFieldPrefix || to_char(j) || ',''' ||
                        v_strDecimalsValue || ''')||''' || p_strSeparator ||
                        '''||';
          end loop;
        else
          if i > p_nBeginYear and i < p_nEndYear then
            for j in 1 .. v_nMaxRcdLen loop
              v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                          v_strFieldPrefix || to_char(j) || ',''' ||
                          v_strDecimalsValue || ''')||''' || p_strSeparator ||
                          '''||';
            end loop;
          else
            if i = p_nEndYear then
              for j in 1 .. p_nEndPeriod loop
                v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                            v_strFieldPrefix || to_char(j) || ',''' ||
                            v_strDecimalsValue || ''')||''' ||
                            p_strSeparator || '''||';
              end loop;
            end if;
          end if;
        end if;
      end loop;
    else
      v_strTableName := 't_year_' || p_nEndYear;
      for j in p_nBeginPeriod .. p_nEndPeriod loop
        v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                    v_strFieldPrefix || to_char(j) || ',''' ||
                    v_strDecimalsValue || ''')||''' || p_strSeparator ||
                    '''||';
      end loop;
    end if;
  
    --remove last ','
    v_strSQL := substr(v_strSQL, 1, length(v_strSQL) - 7);
  end sp_GenerateCommonSelectSQL;

  --Put time series data to temporary table
  function F_PutDataToTmpTable(p_nChronology      in integer,
                               p_nDBTimeSeriesID  in integer,
                               p_bDetailNode      in boolean,
                               p_bInBDG           in boolean,
                               p_oOptions         in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                               p_nBeginYear       in integer,
                               p_nBeginPeriod     in integer,
                               p_nEndYear         in integer,
                               p_nEndPeriod       in integer,
                               p_nPeriodCountYear in integer,
                               p_strSeparator     in varchar2,
                               p_strDecimals      in number,
                               p_strTmpTableName  in varchar2) return boolean is
    v_strSQL            clob := ''; --generated SQL command
    v_nSelOrAggRuleID   integer := 0; --prv.prv_em_addr
    v_bRet              boolean := true;
    v_tableName         varchar(50) := '';
    v_nDecimals         number;
    vConditions         varchar2(4000) := '';
    nSelID              number := 0;
    rResultSetNotUseful sys_refcursor;
    vTableNameNotUseful varchar2(30);
    nSqlCodeNotUseful   number;
  begin
  
    --added begin by zhangxf 20130328 add switch nd:4
    if p_oOptions.bNd then
      v_nDecimals := p_oOptions.strNb;
    else
      v_nDecimals := p_strDecimals;
    end if;
    --added end
    sp_GenerateCommonSelectSQL(p_nChronology,
                               p_bDetailNode,
                               p_bInBDG,
                               p_oOptions,
                               p_nBeginYear,
                               p_nBeginPeriod,
                               p_nEndYear,
                               p_nEndPeriod,
                               p_nPeriodCountYear,
                               p_strSeparator,
                               v_nDecimals,
                               p_strTmpTableName,
                               v_strSQL);
  
    --get aggregation rule id if "-sel" switch is specified for exporting at aggregation level
    if p_oOptions.bSel then
      if p_bDetailNode then
        select nvl(max(sel.sel_em_addr), 0)
          into v_nSelOrAggRuleID
          from sel
         where sel.sel_cle = p_oOptions.strSel;
      else
        select nvl(max(prv.prv_em_addr), 0)
          into v_nSelOrAggRuleID
          from prv
         where prv.prv_cle = p_oOptions.strSel;
      end if;
    end if;
  
    --Calculate length of valid data in a row for don table
    if p_nChronology = p_constant.Monthly then
      v_tableName := '_M';
    elsif p_nChronology = p_constant.Weekly then
      v_tableName := '_W';
    else
      v_tableName := '_D';
    end if;
  
    if p_bDetailNode and not p_bInBDG then
      --export detail time series in pvt table
      v_tableName := 'don' || v_tableName;
      --from
      if p_oOptions.bSel then
        select nvl(max(sel_em_addr), 0)
          into nSelID
          from sel s
         where s.sel_cle = p_oOptions.strSel;
      end if;
      if p_oOptions.bSelCondit then
        vConditions := FMCP_FUNCTIONS.FMF_GetConditionBySel(p_oOptions.strSelCondit);
        p_selection.SP_GetDetailNodeBySelCdt(P_SelectionID => nSelID,
                                             P_Conditions  => vConditions,
                                             P_Sequence    => null,
                                             p_DetailNode  => rResultSetNotUseful,
                                             pOut_vTabName => vTableNameNotUseful,
                                             p_SqlCode     => nSqlCodeNotUseful);
        v_strSQL := v_strSQL ||
                    ' from (select * from pvt where exists (
        select 1 from TB_TS_DetailNodeSelCdt t where t.id=pvt.pvt_em_addr) ) pvt ';
      else
        v_strSQL := v_strSQL || ' from pvt';
      end if;
      --left join
      v_strSQL := v_strSQL ||
                  ' left join fam on fam.fam_em_addr=pvt.fam4_em_addr';
      v_strSQL := v_strSQL ||
                  ' left join geo on geo.geo_em_addr=pvt.geo5_em_addr';
      v_strSQL := v_strSQL ||
                  ' left join dis on dis.dis_em_addr=pvt.dis6_em_addr';
      /*v_strSQL := v_strSQL ||
      ' left join rpd on rpd.pvt17_em_addr=pvt.PVT_EM_ADDR and rpd.num_serie=' ||
      p_nDBTimeSeriesID;*/
    
      --field T_1 to T_N
      for i in p_nBeginYear .. p_nEndYear loop
        v_strSQL := v_strSQL || ' left join ' || v_tableName || ' t_year_' ||
                    to_char(i) || ' on t_year_' || to_char(i) || '.YY=' ||
                    to_char(i) || ' and t_year_' || to_char(i) || '.TSID=' ||
                    to_char(p_nDBTimeSeriesID) || ' and t_year_' ||
                    to_char(i) || '.Version=' ||
                    to_char(p_oOptions.nVersion) || ' and t_year_' ||
                    to_char(i) || '.PVTID=pvt.PVT_EM_ADDR';
      end loop;
    
      if p_oOptions.bSel then
        v_strSQL := v_strSQL ||
                    ' left join rsp on rsp.pvt14_em_addr=pvt.pvt_em_addr';
        --where
        v_strSQL := v_strSQL || ' where rsp.sel13_em_addr=' ||
                    to_char(v_nSelOrAggRuleID);
      end if;
    
      --v_strSQL := v_strSQL || ' order by pvt.pvt_cle';
    else
      if not p_bDetailNode and not p_bInBDG then
        --export aggregation time series in sel table
        v_tableName := 'prb' || v_tableName;
        --from
        if p_oOptions.bSel then
          select nvl(max(p.prv_em_addr), 0)
            into nSelID
            from prv p
           where p.prv_cle = p_oOptions.strSel;
        end if;
        if p_oOptions.bSelCondit then
          vConditions := FMCP_FUNCTIONS.FMF_GetConditionBySel(p_oOptions.strSelCondit);
          fmp_getaggregatenodes.FMCSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => nSelID,
                                                           pIn_vConditions => vConditions,
                                                           pOut_Nodes      => rResultSetNotUseful,
                                                           pOut_nSqlCode   => nSqlCodeNotUseful);
        
          v_strSQL := v_strSQL ||
                      ' from (select * from sel where exists (
        select 1 from TB_TS_AggregateNodeCon t where t.id=sel.sel_em_addr) ) sel ';
        else
          v_strSQL := v_strSQL || ' from sel ';
        end if;
        --left join
        v_strSQL := v_strSQL ||
                    ' left join v_aggnodetodimension t_cdt on t_cdt.sel_em_addr=sel.sel_em_addr';
        v_strSQL := v_strSQL ||
                    ' left join fam on fam.fam_em_addr=t_cdt.fam4_em_addr';
        v_strSQL := v_strSQL ||
                    ' left join geo on geo.geo_em_addr=t_cdt.geo5_em_addr';
        v_strSQL := v_strSQL ||
                    ' left join dis on dis.dis_em_addr=t_cdt.dis6_em_addr';
        --field T_1 to T_N
        for i in p_nBeginYear .. p_nEndYear loop
          v_strSQL := v_strSQL || ' left join ' || v_tableName ||
                      ' t_year_' || to_char(i) || ' on t_year_' ||
                      to_char(i) || '.YY=' || to_char(i) || ' and t_year_' ||
                      to_char(i) || '.TSID=' || to_char(p_nDBTimeSeriesID) ||
                      ' and t_year_' || to_char(i) || '.Version=' ||
                      to_char(p_oOptions.nVersion) || ' and t_year_' ||
                      to_char(i) || '.selID=sel.sel_em_addr';
        end loop;
        if p_oOptions.bSel then
          v_strSQL := v_strSQL ||
                      ' left join prvsel on prvsel.sel16_em_addr=sel.sel_em_addr';
        end if;
        --where
        v_strSQL := v_strSQL || ' where sel.sel_bud=' ||
                    to_char(p_constant.ID_SEL_AggregationNode);
        if p_oOptions.bSel then
          v_strSQL := v_strSQL || ' and prvsel.prv15_em_addr=' ||
                      to_char(v_nSelOrAggRuleID);
        end if;
        v_strSQL := v_strSQL || ' order by sel.sel_cle';
      else
        if p_bDetailNode and p_bInBDG then
          --export detail time series in bdg table
          v_tableName := 'bud' || v_tableName;
          --from
          v_strSQL := v_strSQL || ' from pvt';
          --left join
          v_strSQL := v_strSQL ||
                      ' left join bdg on bdg.b_cle=pvt.pvt_cle and bdg.id_bdg=' ||
                      to_char(p_constant.ID_DetailNode);
          v_strSQL := v_strSQL ||
                      ' left join fam on fam.fam_em_addr=pvt.fam4_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join geo on geo.geo_em_addr=pvt.geo5_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join dis on dis.dis_em_addr=pvt.dis6_em_addr';
        
          --field T_1 to T_N
          for i in p_nBeginYear .. p_nEndYear loop
            v_strSQL := v_strSQL || ' left join ' || v_tableName ||
                        ' t_year_' || to_char(i) || ' on t_year_' ||
                        to_char(i) || '.YY=' || to_char(i) ||
                        ' and t_year_' || to_char(i) || '.TSID=' ||
                        to_char(p_nDBTimeSeriesID) || ' and t_year_' ||
                        to_char(i) || '.Version=' ||
                        to_char(p_oOptions.nVersion) || ' and t_year_' ||
                        to_char(i) || '.PVTID=pvt.PVT_EM_ADDR';
          end loop;
        
          if p_oOptions.bSel then
            v_strSQL := v_strSQL ||
                        ' left join rsp on rsp.pvt14_em_addr=pvt.pvt_em_addr';
            --where
            v_strSQL := v_strSQL || ' where rsp.sel13_em_addr=' ||
                        to_char(v_nSelOrAggRuleID);
          end if;
        
          --v_strSQL := v_strSQL || ' order by pvt.pvt_cle';
        else
          --export aggregation time series in bdg table
          v_tableName := 'bud' || v_tableName;
          --from
          v_strSQL := v_strSQL || ' from sel ';
          --left join
          v_strSQL := v_strSQL ||
                      ' left join bdg on bdg.b_cle=sel.sel_cle and bdg.id_bdg=' ||
                      to_char(p_constant.ID_AggregationNode);
          v_strSQL := v_strSQL ||
                      ' left join v_aggnodetodimension t_cdt on t_cdt.sel_em_addr=sel.sel_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join fam on fam.fam_em_addr=t_cdt.fam4_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join geo on geo.geo_em_addr=t_cdt.geo5_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join dis on dis.dis_em_addr=t_cdt.dis6_em_addr';
          --field T_1 to T_N
          for i in p_nBeginYear .. p_nEndYear loop
            v_strSQL := v_strSQL || ' left join ' || v_tableName ||
                        ' t_year_' || to_char(i) || ' on t_year_' ||
                        to_char(i) || '.YY=' || to_char(i) ||
                        ' and t_year_' || to_char(i) || '.TSID=' ||
                        to_char(p_nDBTimeSeriesID) || ' and t_year_' ||
                        to_char(i) || '.Version=' ||
                        to_char(p_oOptions.nVersion) || ' and t_year_' ||
                        to_char(i) || '.bdgID=bdg_EM_ADDR';
          end loop;
          if p_oOptions.bSel then
            v_strSQL := v_strSQL ||
                        ' left join prvsel on prvsel.sel16_em_addr=sel.sel_em_addr';
          end if;
          --where
          v_strSQL := v_strSQL || ' where sel.sel_bud=' ||
                      to_char(p_constant.ID_SEL_AggregationNode);
          if p_oOptions.bSel then
            v_strSQL := v_strSQL || ' and prvsel.prv15_em_addr=' ||
                        to_char(v_nSelOrAggRuleID);
          end if;
          -- v_strSQL := v_strSQL || ' order by sel.sel_cle';
        end if;
      end if;
    end if;
  
    fmp_log.LOGDEBUG(pIn_vText => 'pexport', pIn_cSqlText => v_strSQL);
    execute immediate v_strSQL;
  
    --added begin by zhangxf 20130328  add switch Nobl
    if p_oOptions.bNobl then
      v_strSQL := ' delete from  ' || p_strTmpTableName || '
                  where replace(to_char(t_data),  ''' ||
                  p_strSeparator || '''
                  ,null) is null ';
    
      execute immediate v_strSQL;
    end if;
    --added end
  
    if p_oOptions.bKeyGeo then
      --process -key_geo
      v_strSQL := 'update ' || p_strTmpTableName || ' set sales=' || '''' ||
                  p_oOptions.strKeyGeo || '''';
      execute immediate v_strSQL;
    else
      if p_oOptions.bKeyGeoDefault then
        --process -key_geo_default
        v_strSQL := 'update ' || p_strTmpTableName || ' set sales=' || '''' ||
                    p_oOptions.strKeyGeoDefault || '''' ||
                    ' where sales is null';
        execute immediate v_strSQL;
      end if;
    end if;
  
    if p_oOptions.bKeyDis then
      --process -key_dis
      v_strSQL := 'update ' || p_strTmpTableName || ' set trade=' || '''' ||
                  p_oOptions.strKeyDis || '''';
      execute immediate v_strSQL;
    else
      if p_oOptions.bKeyDisDefault then
        --process -key_dis_default
        v_strSQL := 'update ' || p_strTmpTableName || ' set trade=' || '''' ||
                    p_oOptions.strKeyDisDefault || '''' ||
                    ' where trade is null';
        execute immediate v_strSQL;
      end if;
    end if;
    if p_oOptions.nDateFormat <> P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
      fmsp_updatedate(pIn_vTableName => p_strTmpTableName);
    end if;
    return v_bRet;
  end F_PutDataToTmpTable;

  procedure fmsp_ParsePeriodToDate(pIn_cPeriodAndDate in clob,
                                   pIn_v1stSeperator  in varchar,
                                   pIn_v2ndSeperator  in varchar) is
    type rPrdDate is record(
      yymmdd varchar2(20),
      period varchar2(20));
    type tPrdDate is table of rPrdDate;
    PrdDate           tPrdDate := tPrdDate();
    cDate             clob;
    vTempString       varchar2(40);
    nCurrentIndex     number := 0;
    n2ndSeperatoIndex number;
    i                 number := 1;
  
  begin
    if dbms_lob.substr(pIn_cPeriodAndDate,
                       1,
                       dbms_lob.getlength(pIn_cPeriodAndDate)) <>
       pIn_v2ndSeperator then
      cDate := pIn_cPeriodAndDate || pIn_v2ndSeperator;
    else
      cDate := pIn_cPeriodAndDate;
    end if;
  
    n2ndSeperatoIndex := dbms_lob.instr(cDate,
                                        pIn_v2ndSeperator,
                                        nCurrentIndex + 1);
    while n2ndSeperatoIndex <> 0 loop
      vTempString   := dbms_lob.substr(cDate,
                                       n2ndSeperatoIndex - nCurrentIndex - 1,
                                       nCurrentIndex + 1);
      nCurrentIndex := n2ndSeperatoIndex;
      PrdDate.extend();
    
      PrdDate(i).yymmdd := substr(vTempString,
                                  1,
                                  instr(vTempString, pIn_v1stSeperator) - 1);
      PrdDate(i).period := substr(vTempString,
                                  instr(vTempString, pIn_v1stSeperator) + 1);
      i := i + 1;
      n2ndSeperatoIndex := dbms_lob.instr(cDate,
                                          pIn_v2ndSeperator,
                                          nCurrentIndex + 1);
    end loop;
    fmsp_execsql(pIn_cSql => 'truncate table tmp_prd2date');
    forall i in 1 .. PrdDate.count
      insert into tmp_prd2date values PrdDate (i);
  exception
    when others then
      raise;
  end;

  procedure FMISP_SetDate(pIn_cDatePeriod   in clob,
                          pIn_vTableName    in varchar2,
                          pIn_v1stSeperator in varchar,
                          pIn_v2ndSeperator in varchar,
                          pOut_nSqlcode     out number)
  --*****************************************************************
    -- Description: set date value to the table
    --
    -- Parameters:
    --pIn_cDatePeriod:date and periiod,format :20130422,201317;
    --pIn_vTableName:the target table name
    --pIn_v1stSeperator;the seperator between date and period
    --pIn_v2ndSeperator:the seperator between 2 groups data
    --pOut_nSqlcode
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        22-Apr-2013     JY.Liu      Created.
    -- **************************************************************
   is
    ncnt number;
    csql clob;
  begin
    pOut_nSqlcode := 0;
    fmp_log.FMP_SetValue(pIn_cDatePeriod);
    fmp_log.FMP_SetValue(pIn_vTableName);
    fmp_log.FMP_SetValue(pIn_v1stSeperator);
    fmp_log.FMP_SetValue(pIn_v2ndSeperator);
    fmp_log.LOGBEGIN;
    fmsp_ParsePeriodToDate(pIn_cPeriodAndDate => pIn_cDatePeriod,
                           pIn_v1stSeperator  => pIn_v1stSeperator,
                           pIn_v2ndSeperator  => pIn_v2ndSeperator);
    select COUNT(0)
      INTO ncnt
      from user_tab_cols u
     where u.TABLE_NAME = upper(pIn_vTableName)
       and u.COLUMN_NAME = 'BEGINDATE';
    if ncnt = 1 then
      csql := 'merge into ' || pIn_vTableName ||
              ' p using (select * from tmp_prd2date) t
      on (p.beginperiod=t.period)
      when matched then
        update set p.begindate=t.yymmdd';
      fmsp_execsql(csql);
    end if;
  
    select COUNT(0)
      INTO ncnt
      from user_tab_cols u
     where u.TABLE_NAME = upper(pIn_vTableName)
       and u.COLUMN_NAME = 'ENDDATE';
    if ncnt = 1 then
      csql := 'merge into ' || pIn_vTableName ||
              ' p using (select * from tmp_prd2date) t
      on (p.endperiod=t.period)
      when matched then
        update set p.enddate=t.yymmdd';
      fmsp_execsql(csql);
    end if;
    fmp_log.LOGEND;
  exception
    when others then
      pOut_nSqlcode := sqlcode;
      raise;
  end;

  procedure fmsp_updatedate(pIn_vTableName in varchar2) is
    cSql  clob;
    nFlag int := 0;
  begin
    cSql := 'update ' || pIn_vTableName || ' set ';
    if bEnddate then
      cSql  := cSql || ' enddate = endperiod';
      nFlag := 1;
    end if;
    if bBegindate then
      if nFlag = 1 then
        cSql := cSql || ',begindate = beginperiod';
      else
        cSql := cSql || 'begindate = beginperiod';
      end if;
      nFlag := 2;
    end if;
    if nFlag > 0 then
      fmsp_execsql(pIn_cSql => cSql);
    end if;
  end;

end P_PExport;
/
