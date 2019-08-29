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
  procedure sp_ProcessExportTimeseries(p_nCommandNumber  in integer, --batch command number, reserved information
                                       p_nChronology     in integer, --1 Month ,2 Week,3 Day, reserved information
                                       p_nDBTimeSeriesID in integer, --The real time series ID in orcle database
                                       p_nNodeType       in integer, --1:Detail Node, 2: Aggregate Node
                                       p_nBDGFlag        in integer, --0: is not a BDG time series, 1: is a BDG time series
                                       p_strFMUSER       in varchar2, --Current user name, reserved information
                                       p_strOptions      in varchar2, --batch command switchs, use ## as separator
                                       p_nBeginDate      in integer, --Begin date of time series
                                       p_nEndDate        in integer, --End date of time series
                                       --This procedure will generate a temporary table to save time series data,
                                       --this out parameter is used to return the table name to FMWin
                                       p_strSeparator    in varchar2,
                                       p_strDecimals     in number, --Decimals config
                                       p_strTmpTableName out varchar2,
                                       p_nSqlCode        out integer --return code, 0: correct, not 0: error
                                       );

  --Parse date to 2 parameters: year and period
  procedure sp_ParseDate(p_nChronology in integer, --1 Month ,2 Week,3 Day, reserved information
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

  --Get period count of time series data
  function F_GetPeriodCount(p_nChronology  in integer, --reference to sp_ProcessExportTimeseries.p_nChronology
                            p_nBeginYear   in integer, --reference to sp_ProcessExportTimeseries.v_nBeginYear
                            p_nBeginPeriod in integer, --reference to sp_ProcessExportTimeseries.v_nBeginPeriod
                            p_nEndYear     in integer, --reference to sp_ProcessExportTimeseries.v_nEndYear
                            p_nEndPeriod   in integer --reference to sp_ProcessExportTimeseries.v_nEndPeriod
                            ) return integer;

  --Put time series data to temporary table
  function F_PutDataToTmpTable(p_nChronology     in integer, --reference to sp_ProcessExportTimeseries.p_nChronology
                               p_nDBTimeSeriesID in integer, --reference to sp_ProcessExportTimeseries.p_nDBTimeSeriesID
                               p_bDetailNode     in boolean, --reference to sp_ProcessExportTimeseries.v_bDetailNode
                               p_bInBDG          in boolean, --reference to sp_ProcessExportTimeseries.v_bInBDG
                               p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType, --reference to sp_ProcessExportTimeseries.v_oOptions
                               p_nBeginYear      in integer, --reference to sp_ProcessExportTimeseries.v_nBeginYear
                               p_nBeginPeriod    in integer, --reference to sp_ProcessExportTimeseries.v_nBeginPeriod
                               p_nEndYear        in integer, --reference to sp_ProcessExportTimeseries.v_nEndYear
                               p_nEndPeriod      in integer, --reference to sp_ProcessExportTimeseries.v_nEndPeriod
                               p_strSeparator    in varchar2,
                               p_strDecimals     in number,
                               p_strTmpTableName in varchar2 --reference to sp_ProcessExportTimeseries.p_strTmpTableName
                               ) return boolean;

end P_PExport;
/
create or replace package body P_PExport is

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
        v_sqlstr := 'select '''' AGGNODE, product, sales, trade, yy, mm ,t_data from ' ||
                    p_strTmpTableName;
      when p_nNodeType = P_CONSTANT.Aggregate_Node_Type then
        v_sqlstr := 'select AGGNODE, product, sales, trade, yy, mm ,t_data from ' ||
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
    v_isnotnull number;
  begin
    pout_TmpListTable := fmf_gettmptablename(); --'TB' || p_strTableName;
  
    if p_oOptions.bNobl then
      v_isnotnull := 1;
    else
      v_isnotnull := 0;
    end if;
  
    case
      when p_nNodeType = P_CONSTANT.DETAIL_NODE_TYPE then
        v_strsql := 'create table ' || pout_TmpListTable || '
          (
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            yy      VARCHAR2(4),
            mm      VARCHAR2(2),
            t_data  number
          )';
        execute immediate v_strsql;
      
        v_strsql := 'insert into  ' || pout_TmpListTable ||
                    '  select   product, sales, trade, yy, mm, t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
        dbms_output.put_line(v_strsql);
        execute immediate v_strSQL
          using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
      
      when p_nNodeType = P_CONSTANT.Aggregate_Node_Type then
        v_strsql := 'create table ' || pout_TmpListTable || '
          (
            AGGNODE VARCHAR2(200),
            product VARCHAR2(60),
            sales   VARCHAR2(60),
            trade   VARCHAR2(60),
            yy      VARCHAR2(4),
            mm      VARCHAR2(2),
            t_data  number
          )';
        execute immediate v_strsql;
      
        v_strsql := 'insert into  ' || pout_TmpListTable ||
                    '  select AGGNODE,  product, sales, trade, yy, mm, t_data from table(P_PExport.splitstr2list(:p_nNodeType,:p_nBeginDate,:p_nEndDate,:p_strSeparator,:p_oOptions, :ptable))';
        dbms_output.put_line(v_strsql);
        execute immediate v_strSQL
          using p_nNodeType, p_nBeginDate, p_nEndDate, p_strSeparator, v_isnotnull, pin_strTableName;
      
    end case;
    commit;
  end;

  --pexport sp_ProcessExportTimeseries
  procedure sp_ProcessExportTimeseries(p_nCommandNumber  in integer,
                                       p_nChronology     in integer,
                                       p_nDBTimeSeriesID in integer,
                                       p_nNodeType       in integer,
                                       p_nBDGFlag        in integer,
                                       p_strFMUSER       in varchar2,
                                       p_strOptions      in varchar2,
                                       p_nBeginDate      in integer,
                                       p_nEndDate        in integer,
                                       p_strSeparator    in varchar2,
                                       p_strDecimals     in number, --Decimals config
                                       p_strTmpTableName out varchar2,
                                       p_nSqlCode        out integer) is
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
    Fmp_log.FMP_SetValue(p_strSeparator);
    Fmp_log.FMP_SetValue(p_strDecimals);
    Fmp_log.LOGBEGIN;
  
    v_bDetailNode := (p_nNodeType = P_CONSTANT.DETAIL_NODE_TYPE);
    v_bInBDG      := (p_nBDGFlag = 1);
  
    if p_strSeparator = 'ESP' then
      v_strSeparator := ' ';
    else
      v_strSeparator := p_strSeparator;
      /* if p_strSeparator = '''' then
        v_strSeparator := '''';
      end if;
      if p_strSeparator = '_' then
        v_strSeparator := '\_';
      end if;
      if p_strSeparator = '%' then
        v_strSeparator := '\%';
      else
        v_strSeparator := p_strSeparator;
      end if;
      */
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
  
    --Calculate time series data period count
    v_nPeriodCount := F_GetPeriodCount(p_nChronology,
                                       v_nBeginYear,
                                       v_nBeginPeriod,
                                       v_nEndYear,
                                       v_nEndPeriod);
  
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
    --added end
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end sp_ProcessExportTimeseries;

  --Parse date to 2 parameters: year and period
  procedure sp_ParseDate(p_nChronology in integer,
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
        --reserved for daily version
        null;
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
      --field 'BEGIN_YY'
      v_strSQL := v_strSQL || 'BEGIN_YY VARCHAR2(4),';
    
      --field 'MM'/'DD'/'WW'
      if p_nChronology = p_constant.Monthly then
        --Monthly
        v_strSQL := v_strSQL || 'BEGIN_MM VARCHAR2(10),';
        if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
          v_strSQL := v_strSQL || 'BEGIN_DD VARCHAR2(2),';
        end if;
      elsif p_nChronology = p_constant.Weekly then
        --weelky
        v_strSQL := v_strSQL || 'BEGIN_WW VARCHAR2(10),';
      else
        --reserved for  daily version time series
        null;
      end if;
    end if;
  
    if not p_oOptions.bSdate then
      --field 'YY'
      v_strSQL := v_strSQL || 'YY VARCHAR2(4),';
    
      --field 'MM'/'DD'/'WW'
      if p_nChronology = p_constant.Monthly then
        --Monthly
        v_strSQL := v_strSQL || 'MM VARCHAR2(2),';
        if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
          v_strSQL := v_strSQL || 'DD VARCHAR2(10),';
        end if;
      elsif p_nChronology = p_constant.Weekly then
        --weelky
        v_strSQL := v_strSQL || 'WW VARCHAR2(10),';
      else
        --reserved for weekly and daily version time series
        null;
      end if;
    end if;
  
    v_strSQL := v_strSQL || ' T_DATA CLOB NULL )';
  
    --Execute
    execute immediate v_strSQL;
  
  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_CreateTemporaryTable;

  -- Function implementations
  --Get period count of time series data
  function F_GetPeriodCount(p_nChronology  in integer,
                            p_nBeginYear   in integer,
                            p_nBeginPeriod in integer,
                            p_nEndYear     in integer,
                            p_nEndPeriod   in integer) return integer is
    v_nPeriodCount integer := 0;
  begin
  
    case p_nChronology
      when p_constant.Monthly then
        --Monthly
        v_nPeriodCount := (p_nEndYear - p_nBeginYear) * 12 +
                          (p_nEndPeriod - p_nBeginPeriod + 1);
      
      when p_constant.Weekly then
        --Weekly
        v_nPeriodCount := (p_nEndYear - p_nBeginYear) * 52 +
                          (p_nEndPeriod - p_nBeginPeriod + 1);
      else
        --reserved for daily/weekly version
        null;
    end case;
  
    if v_nPeriodCount < 0 then
      v_nPeriodCount := 1;
    end if;
    return v_nPeriodCount;
  end F_GetPeriodCount;

  -- Refactored procedure sp_GenerateCommonSelectSQL
  procedure sp_GenerateCommonSelectSQL(p_nChronology     in integer,
                                       p_bDetailNode     in boolean,
                                       p_bInBDG          in boolean,
                                       p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                       p_nBeginYear      in integer,
                                       p_nBeginPeriod    in integer,
                                       p_nEndYear        in integer,
                                       p_nEndPeriod      in integer,
                                       p_strSeparator    in varchar2,
                                       p_strDecimals     in number, --Decimals config
                                       p_strTmpTableName in varchar2,
                                       v_strSQL          in out varchar2) is
    v_nMaxRcdLen       integer := 0;
    v_strBeginYear     varchar(20) := '';
    v_strEndYear       varchar(20) := '';
    v_strBeginPeriod   varchar(20) := '';
    v_strEndPeriod     varchar(20) := '';
    v_strTableName     varchar(32) := '';
    v_strFieldPrefix   varchar(32) := '';
    v_strDecimalsValue varchar2(1000);
  begin
    --Calculate length of valid data in a row for don table
    if p_nChronology = p_constant.Monthly then
      v_nMaxRcdLen := 12;
    elsif p_nChronology = p_constant.Weekly then
      v_nMaxRcdLen := 52;
    else
      v_nMaxRcdLen := 14;
    end if;
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
    v_strBeginYear := '''' || v_strBeginYear || '''';
    v_strEndYear   := '''' || v_strEndYear || '''';
    --field 'MM/WW/DD'
    v_strBeginPeriod := '''' || to_char(p_nBeginPeriod) || '''';
    v_strEndPeriod   := '''' || to_char(p_nEndPeriod) || '''';
  
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
      --field 'BEGIN_YY'
      v_strSQL := v_strSQL || v_strBeginYear || ' as BEGIN_YY,';
      --field 'BEGIN_MM'/'BEGIN_DD'/'BEGIN_WW'
      if p_nChronology = p_constant.Monthly then
        --Monthly
        v_strSQL := v_strSQL || v_strBeginPeriod || ' as BEGIN_MM,';
        if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
          v_strSQL := v_strSQL || '1 as BEGIN_DD,';
        end if;
      else
        --reserved for weekly and daily version time series
        null;
      end if;
    end if;
  
    if not p_oOptions.bSdate then
      --field 'YY'
      v_strSQL := v_strSQL || v_strEndYear || ' as YY,';
      --field 'MM'/'DD'/'WW'
      if p_nChronology = p_constant.Monthly then
        --Monthly
        v_strSQL := v_strSQL || v_strEndPeriod || ' as MM,';
        if p_oOptions.nDateFormat = P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j then
          v_strSQL := v_strSQL || '1 as DD,';
        end if;
      elsif p_nChronology = p_constant.Weekly then
        --weelky
        v_strSQL := v_strSQL || v_strEndPeriod || ' as WW,';
      else
        --reserved for weekly and daily version time series
        null;
      end if;
    end if;
  
    /* --field T_1 to T_N
    if p_bInBDG then
      --time series data field prefix in bud table
      v_strFieldPrefix := 'm_bdg_';
    else
      if p_bDetailNode then
        --time series data field prefix in don table
        v_strFieldPrefix := 'm_';
      else
        --time series data field prefix in prb table
        v_strFieldPrefix := 'm_prv_';
      end if;
    end if;*/
    v_strFieldPrefix := 'T';
  
    /*    if p_nEndYear > p_nBeginYear then
          for i in p_nBeginYear .. p_nEndYear loop
            v_strTableName := 't_year_' || i;
            if i = p_nBeginYear then
              for j in p_nBeginPeriod .. v_nMaxRcdLen loop
                v_strSQL := v_strSQL || v_strTableName || '.' ||
                            v_strFieldPrefix || j || '||''' || p_strSeparator ||
                            '''||';
              end loop;
            else
              if i > p_nBeginYear and i < p_nEndYear then
                for j in 1 .. v_nMaxRcdLen loop
                  v_strSQL := v_strSQL || v_strTableName || '.' ||
                              v_strFieldPrefix || j || '||''' || p_strSeparator ||
                              '''||';
                end loop;
              else
                if i = p_nEndYear then
                  for j in 1 .. p_nEndPeriod loop
                    v_strSQL := v_strSQL || v_strTableName || '.' ||
                                v_strFieldPrefix || j || '||''' ||
                                p_strSeparator || '''||';
                  end loop;
                end if;
              end if;
            end if;
          end loop;
        else
          v_strTableName := 't_year_' || p_nEndYear;
          for j in p_nBeginPeriod .. p_nEndPeriod loop
            v_strSQL := v_strSQL || v_strTableName || '.' || v_strFieldPrefix || j ||
                        '||''' || p_strSeparator || '''||';
          end loop;
    end if;*/
  
    if p_nEndYear > p_nBeginYear then
      for i in p_nBeginYear .. p_nEndYear loop
        v_strTableName := 't_year_' || i;
        if i = p_nBeginYear then
          for j in p_nBeginPeriod .. v_nMaxRcdLen loop
            v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                        v_strFieldPrefix || j || ',''' ||
                        v_strDecimalsValue || ''')||''' || p_strSeparator ||
                        '''||';
          end loop;
        else
          if i > p_nBeginYear and i < p_nEndYear then
            for j in 1 .. v_nMaxRcdLen loop
              v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                          v_strFieldPrefix || j || ',''' ||
                          v_strDecimalsValue || ''')||''' || p_strSeparator ||
                          '''||';
            end loop;
          else
            if i = p_nEndYear then
              for j in 1 .. p_nEndPeriod loop
                v_strSQL := v_strSQL || ' to_char(' || v_strTableName || '.' ||
                            v_strFieldPrefix || j || ',''' ||
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
                    v_strFieldPrefix || j || ',''' || v_strDecimalsValue ||
                    ''')||''' || p_strSeparator || '''||';
      end loop;
    
    end if;
  
    --remove last ','
    v_strSQL := substr(v_strSQL, 1, length(v_strSQL) - 7);
  
  end sp_GenerateCommonSelectSQL;

  --Put time series data to temporary table
  function F_PutDataToTmpTable(p_nChronology     in integer,
                               p_nDBTimeSeriesID in integer,
                               p_bDetailNode     in boolean,
                               p_bInBDG          in boolean,
                               p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                               p_nBeginYear      in integer,
                               p_nBeginPeriod    in integer,
                               p_nEndYear        in integer,
                               p_nEndPeriod      in integer,
                               p_strSeparator    in varchar2,
                               p_strDecimals     in number,
                               p_strTmpTableName in varchar2) return boolean is
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
      v_tableName := '';
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
        v_strSQL := v_strSQL || ' left join ' || v_tableName || ' t_year_' || i ||
                    ' on t_year_' || i || '.YY=' || i || ' and t_year_' || i ||
                    '.TSID=' || p_nDBTimeSeriesID || ' and t_year_' || i ||
                    '.Version=' || p_oOptions.nVersion || ' and t_year_' || i ||
                    '.PVTID=pvt.PVT_EM_ADDR';
      end loop;
    
      if p_oOptions.bSel then
        v_strSQL := v_strSQL ||
                    ' left join rsp on rsp.pvt14_em_addr=pvt.pvt_em_addr';
        --where
        v_strSQL := v_strSQL || ' where rsp.sel13_em_addr=' ||
                    v_nSelOrAggRuleID;
      end if;
    
      v_strSQL := v_strSQL || ' order by pvt.pvt_cle';
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
        /*v_strSQL := v_strSQL ||
        ' left join rbp on rbp.sel21_em_addr=sel.sel_em_addr and rbp.num_prv=' ||
        p_nDBTimeSeriesID;*/
      
        --field T_1 to T_N
        for i in p_nBeginYear .. p_nEndYear loop
          /*v_strSQL := v_strSQL || ' left join prb t_year_' || i ||
          ' on t_year_' || i || '.annee_prv=' || i ||
          ' and t_year_' || i ||
          '.rbp22_em_addr=rbp.rbp_em_addr';*/
        
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
                      v_nSelOrAggRuleID;
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
                      p_constant.ID_DetailNode;
          v_strSQL := v_strSQL ||
                      ' left join fam on fam.fam_em_addr=pvt.fam4_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join geo on geo.geo_em_addr=pvt.geo5_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join dis on dis.dis_em_addr=pvt.dis6_em_addr';
          /*v_strSQL := v_strSQL ||
          ' left join bgc on bgc.bdg31_em_addr=bdg.bdg_EM_ADDR and bgc.num_bdg=' ||
          p_nDBTimeSeriesID;*/
          --field T_1 to T_N
          for i in p_nBeginYear .. p_nEndYear loop
            /*v_strSQL := v_strSQL || ' left join bud t_year_' || i ||
            ' on t_year_' || i || '.annee_bdg=' || i ||
            ' and t_year_' || i ||
            '.bgc32_em_addr=bgc.bgc_em_addr';*/
          
            v_strSQL := v_strSQL || ' left join ' || v_tableName ||
                        ' t_year_' || i || ' on t_year_' || i || '.YY=' || i ||
                        ' and t_year_' || i || '.TSID=' ||
                        p_nDBTimeSeriesID || ' and t_year_' || i ||
                        '.Version=' || p_oOptions.nVersion ||
                        ' and t_year_' || i || '.PVTID=pvt.PVT_EM_ADDR';
          end loop;
        
          if p_oOptions.bSel then
            v_strSQL := v_strSQL ||
                        ' left join rsp on rsp.pvt14_em_addr=pvt.pvt_em_addr';
            --where
            v_strSQL := v_strSQL || ' where rsp.sel13_em_addr=' ||
                        v_nSelOrAggRuleID;
          end if;
        
          v_strSQL := v_strSQL || ' order by pvt.pvt_cle';
        else
          --export aggregation time series in bdg table
          v_tableName := 'bud' || v_tableName;
          --from
          v_strSQL := v_strSQL || ' from sel ';
          --left join
          v_strSQL := v_strSQL ||
                      ' left join bdg on bdg.b_cle=sel.sel_cle and bdg.id_bdg=' ||
                      p_constant.ID_AggregationNode;
          v_strSQL := v_strSQL ||
                      ' left join v_aggnodetodimension t_cdt on t_cdt.sel_em_addr=sel.sel_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join fam on fam.fam_em_addr=t_cdt.fam4_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join geo on geo.geo_em_addr=t_cdt.geo5_em_addr';
          v_strSQL := v_strSQL ||
                      ' left join dis on dis.dis_em_addr=t_cdt.dis6_em_addr';
          /*v_strSQL := v_strSQL ||
          ' left join bgc on bgc.bdg31_em_addr=bdg.bdg_EM_ADDR and bgc.num_bdg=' ||
          p_nDBTimeSeriesID;*/
          --field T_1 to T_N
          for i in p_nBeginYear .. p_nEndYear loop
            /*v_strSQL := v_strSQL || ' left join bud t_year_' || i ||
            ' on t_year_' || i || '.annee_bdg=' || i ||
            ' and t_year_' || i ||
            '.bgc32_em_addr=bgc.bgc_em_addr';*/
            v_strSQL := v_strSQL || ' left join ' || v_tableName ||
                        ' t_year_' || i || ' on t_year_' || i || '.YY=' || i ||
                        ' and t_year_' || i || '.TSID=' ||
                        p_nDBTimeSeriesID || ' and t_year_' || i ||
                        '.Version=' || p_oOptions.nVersion ||
                        ' and t_year_' || i || '.bdgID=bdg_EM_ADDR';
          end loop;
          if p_oOptions.bSel then
            v_strSQL := v_strSQL ||
                        ' left join prvsel on prvsel.sel16_em_addr=sel.sel_em_addr';
          end if;
          --where
          v_strSQL := v_strSQL || ' where sel.sel_bud=' ||
                      p_constant.ID_SEL_AggregationNode;
          if p_oOptions.bSel then
            v_strSQL := v_strSQL || ' and prvsel.prv15_em_addr=' ||
                        v_nSelOrAggRuleID;
          end if;
          v_strSQL := v_strSQL || ' order by sel.sel_cle';
        end if;
      end if;
    end if;
  
    --add log
    commit;
  
    /*    execute immediate 'truncate table t_test';
    
    insert into t_test values (v_strSQL);
    commit;*/
    --execute
    fmp_log.LOGDEBUG(pIn_vText => 'pexport', pIn_cSqlText => v_strSQL);
    execute immediate v_strSQL;
  
    commit;
  
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
  
    return v_bRet;
  end F_PutDataToTmpTable;

end P_PExport;
/
