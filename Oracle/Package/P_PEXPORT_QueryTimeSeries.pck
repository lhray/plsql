create or replace package P_PEXPORT_QueryTimeSeries is
  --Authid Current_User is
  -- Author  : junhuazuo
  -- Created : 12/18/2012 2:35:40 PM
  -- Public function and procedure declarations
  type tt_timeseries_type is table of varchar2(100) INDEX BY BINARY_INTEGER;

  procedure FMISP_GetAllTimeSeries(pIn_nNodeType         in integer,
                                   pIn_vTabName          in varchar2,
                                   pIn_arrTimeSeriesDBID in clob,
                                   pIn_arrVersion        in clob,
                                   pIn_arrChronology     in clob,
                                   pIn_arrBeginYear      in clob,
                                   pIn_arrBeginPeriod    in clob,
                                   pIn_arrEndYear        in clob,
                                   pIn_arrEndPeriod      in clob,
                                   pOut_vTableName       out varchar2,
                                   pOut_nSQLCode         out integer);

  procedure sp_GetAllTimeSeries(p_nNodeType         in integer, --10003: node in pvt, 10004: node in sel, 10005: node in bdg
                                p_arrNodeAddr       in clob, --node ID list, separated by comma
                                p_arrTimeSeriesDBID in clob, --time series id, separated by comma
                                p_arrVersion        in clob, -- time series version, separated by comma
                                p_arrChronology     in clob, -- 1: monthly, 2: weekly, 4: daily, separated by comma
                                p_arrBeginYear      in clob, -- begin year such as 2012, separated by comma
                                p_arrBeginPeriod    in clob, -- begin period such as 10  separated by comma
                                p_arrEndYear        in clob, -- end year such as 2012 ,separated by comma
                                p_arrEndPeriod      in clob, -- end period such as 10,separated by comma
                                p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                p_nSQLCode          out integer --return exception code,0 is no error.
                                );

  procedure sp_GetElementFromArrayByIndex(Liststr    in clob,
                                          p_tt_type  out tt_timeseries_type,
                                          icount     out integer,
                                          p_nSQLCODE out integer);

  procedure SP_GetDetailNodeTSBySelID(P_SelectionID       in number,
                                      p_IsDynamic         in number,
                                      P_Sequence          in varchar2,
                                      p_arrTimeSeriesDBID in clob,
                                      p_arrVersion        in clob,
                                      p_arrChronology     in clob,
                                      p_arrBeginYear      in clob,
                                      p_arrBeginPeriod    in clob,
                                      p_arrEndYear        in clob,
                                      p_arrEndPeriod      in clob,
                                      p_strTableName      out varchar2,
                                      p_nSQLCode          out integer);

  procedure SP_GetDetailNodeTSByConditions(P_Conditions        in varchar2,
                                           P_Sequence          in varchar2,
                                           p_arrTimeSeriesDBID in clob,
                                           p_arrVersion        in clob,
                                           p_arrChronology     in clob,
                                           p_arrBeginYear      in clob,
                                           p_arrBeginPeriod    in clob,
                                           p_arrEndYear        in clob,
                                           p_arrEndPeriod      in clob,
                                           p_strTableName      out varchar2,
                                           p_nSQLCode          out integer);

  procedure SP_GetDetailNodeTSBySelCdt(P_SelectionID       in number,
                                       P_Conditions        in varchar2,
                                       P_Sequence          in varchar2,
                                       p_arrTimeSeriesDBID in clob,
                                       p_arrVersion        in clob,
                                       p_arrChronology     in clob,
                                       p_arrBeginYear      in clob,
                                       p_arrBeginPeriod    in clob,
                                       p_arrEndYear        in clob,
                                       p_arrEndPeriod      in clob,
                                       p_strTableName      out varchar2,
                                       p_nSQLCode          out integer);

  procedure SP_GetAggregateNodesTS(P_AggregateRuleID   in number,
                                   P_Sequence          in varchar2,
                                   p_arrTimeSeriesDBID in clob,
                                   p_arrVersion        in clob,
                                   p_arrChronology     in clob,
                                   p_arrBeginYear      in clob,
                                   p_arrBeginPeriod    in clob,
                                   p_arrEndYear        in clob,
                                   p_arrEndPeriod      in clob,
                                   p_strTableName      out varchar2,
                                   p_nSQLCode          out integer);
  procedure SP_GetAggNodesTSByConditions(P_AggregateRuleID   in number,
                                         P_Conditions        in varchar2,
                                         P_Sequence          in varchar2,
                                         p_arrTimeSeriesDBID in clob,
                                         p_arrVersion        in clob,
                                         p_arrChronology     in clob,
                                         p_arrBeginYear      in clob,
                                         p_arrBeginPeriod    in clob,
                                         p_arrEndYear        in clob,
                                         p_arrEndPeriod      in clob,
                                         p_strTableName      out varchar2,
                                         p_nSQLCode          out integer);
end P_PEXPORT_QueryTimeSeries;
/
create or replace package body P_PEXPORT_QueryTimeSeries is
  --Authid Current_User is
  -- Author  : junhuazuo
  -- Created : 12/18/2012 2:35:40 PM
  -- Public function and procedure declarations

  function FMF_GetColsStr(pIn_chronology       in char,
                          pIn_bIncludeDataType in boolean default true,
                          pIn_vTableAlias      in varchar2 default null)
    return varchar2
  --*****************************************************************
    -- Description:the function return a string  like T1  NUMBER,T2 NUMBER..... or  T1,T2,T3 ...
    --
    -- Parameters:
    --          pIn_chronology: monthly weekly daily
    --          pIn_bIncludeDataType:include data type or not
    --          pIn_vTableAlias:table alias name in SQL
  
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        17-APR-2013     JY.Liu     created.
    -- **************************************************************
   as
    result     varchar2(32767);
    nMaxPeriod number;
  begin
    nMaxPeriod := case lower(pIn_chronology)
                    when 'm' then
                     12
                    when 'w' then
                     53
                    when 'd' then
                     371
                  end;
    for iPeriod in 1 .. nMaxPeriod loop
      if iPeriod = nMaxPeriod then
        if pIn_bIncludeDataType then
          result := result || ' T' || iPeriod || ' number';
        else
          result := result || pIn_vTableAlias || ' T' || iPeriod || '';
        end if;
      else
        if pIn_bIncludeDataType then
          result := result || ' T' || iPeriod || ' number,';
        else
          result := result || pIn_vTableAlias || ' T' || iPeriod || ' ,';
        end if;
      end if;
    end loop;
    return result;
  end;
  procedure FMSP_GetAllTimeSeries(pIn_nNodeType         in integer,
                                  pIn_cNodeList         in clob default null,
                                  pIn_vTabName          in varchar2 default null,
                                  pIn_arrTimeSeriesDBID in clob,
                                  pIn_arrVersion        in clob,
                                  pIn_arrChronology     in clob,
                                  pIn_arrBeginYear      in clob,
                                  pIn_arrBeginPeriod    in clob,
                                  pIn_arrEndYear        in clob,
                                  pIn_arrEndPeriod      in clob,
                                  pOut_vTableName       out varchar2,
                                  pOut_nSQLCode         out integer)
  --*****************************************************************
    -- Description:
    --
    -- Parameters:
    --          pIn_nNodeType
    --          pIn_cNodeList:node list seperated by ,
    --          pIn_vTabName:table stored all nodes ID
    --          pIn_arrTimeSeriesDBID
    --          pIn_arrVersion
    --          pIn_arrChronology
    --          pIn_arrBeginYear
    --          pIn_arrBeginPeriod
    --          pIn_arrEndYear
    --          pIn_arrEndPeriod
    --          pOut_vTableName
    --          pOut_nSQLCode
  
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JY.Liu     created.
    -- **************************************************************
   is
    v_strsql       varchar2(32767) := '';
    v_strsql_from  varchar2(32767) := '';
    v_strsql_where varchar2(32767) := '';
    v_c_lob        clob;
    v_nsqlcode     number;
    nperiods2      number;
    nperiods3      number;
    nperiods4      number;
    nperiods5      number;
    nperiods6      number;
    nperiods7      number;
    nperiods8      number;
    vOut_tNestTab  fmt_nest_tab_nodeid;
    vOut_nSqlCode  number;
  
    v_arrTimeSeriesDBID tt_timeseries_type;
    v_arrVersion        tt_timeseries_type;
    v_arrChronology     tt_timeseries_type;
    v_arrBeginYear      tt_timeseries_type;
    v_arrBeginPeriod    tt_timeseries_type;
    v_arrEndYear        tt_timeseries_type;
    v_arrEndPeriod      tt_timeseries_type;
    vSQL                varchar2(128);
  begin
    pOut_nSQLCode := 0;
  
    if pIn_cNodeList is not null then
      FMSP_ClobToTable(pIn_cClob     => pIn_cNodeList,
                       pOut_nSqlCode => pOut_nSqlCode);
    else
      fmsp_execsql(pIn_cSql => 'truncate table tb_node');
      vSQL := '   insert into tb_node select id from ' || pIn_vTabName;
      fmsp_execsql(pIn_cSql => vsql);
    end if;
  
    v_strsql := '';
  
    sp_GetElementFromArrayByIndex(pIn_arrTimeSeriesDBID,
                                  v_arrTimeSeriesDBID,
                                  nperiods2,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrVersion,
                                  v_arrVersion,
                                  nperiods3,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrChronology,
                                  v_arrChronology,
                                  nperiods4,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrBeginYear,
                                  v_arrBeginYear,
                                  nperiods5,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrBeginPeriod,
                                  v_arrBeginPeriod,
                                  nperiods6,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrEndYear,
                                  v_arrEndYear,
                                  nperiods7,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(pIn_arrEndPeriod,
                                  v_arrEndPeriod,
                                  nperiods8,
                                  v_nsqlcode);
  
    if nperiods2 = nperiods3 and nperiods2 = nperiods4 and
       nperiods2 = nperiods5 and nperiods2 = nperiods6 and
       nperiods2 = nperiods7 and nperiods2 = nperiods8 then
    
      pOut_vTableName := fmf_gettmptablename();
    
      v_strsql := ' Create table ' || pOut_vTableName || ' (';
      v_strsql := v_strsql || '   Field_NO        number,';
      v_strsql := v_strsql || '   Node_addr       number,';
      v_strsql := v_strsql || '   Chronology      varchar2(50),';
      v_strsql := v_strsql || '   Time_series_id  number,';
      v_strsql := v_strsql || '   Version         number,';
      v_strsql := v_strsql || '   Annee           number,';
      v_strsql := v_strsql || '   Begin_year      number,';
      v_strsql := v_strsql || '   Begin_period    number,';
      v_strsql := v_strsql || '   End_year        number,';
      v_strsql := v_strsql || '   End_period      number,';
      v_strsql := v_strsql || FMF_GetColsStr(pIn_chronology => 'd'); --the table always contains 371 columns which can cover monthily weekly daily
      v_strsql := v_strsql || '   )';
    
      fmsp_execsql(pIn_cSql => v_strsql);
    
      for j in 0 .. nperiods2 loop
        v_strsql := '';
        v_strsql := 'insert into ' || pOut_vTableName || ' (';
        v_strsql := v_strsql || '   Field_NO,';
        v_strsql := v_strsql || '   Node_addr,';
        v_strsql := v_strsql || '   Chronology,';
        v_strsql := v_strsql || '   Time_series_id,';
        v_strsql := v_strsql || '   Version,';
        v_strsql := v_strsql || '   Annee,';
        v_strsql := v_strsql || '   Begin_year,';
        v_strsql := v_strsql || '   Begin_period,';
        v_strsql := v_strsql || '   End_year,';
        v_strsql := v_strsql || '   End_period,';
        case v_arrChronology(j)
          when P_CONSTANT.Monthly then
            --monthly
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'm',
                                       pIn_bIncludeDataType => false);
          when P_CONSTANT.Weekly then
            --weekly
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'w',
                                       pIn_bIncludeDataType => false);
          when P_CONSTANT.Daily then
            --daily
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'd',
                                       pIn_bIncludeDataType => false);
        end case;
        v_strsql := v_strsql || '   ) ';
        v_strsql := v_strsql || ' select SEQ_NODE_TS.NEXTVAL,';
        v_strsql := v_strsql || 'm.id,';
        v_strsql := v_strsql || '''' || v_arrChronology(j) || ''',';
        v_strsql := v_strsql || v_arrTimeSeriesDBID(j) || ',';
        v_strsql := v_strsql || v_arrVersion(j) || ',';
        v_strsql := v_strsql || ' n.yy,';
        v_strsql := v_strsql || v_arrBeginYear(j) || ',';
        v_strsql := v_strsql || v_arrBeginPeriod(j) || ',';
        v_strsql := v_strsql || v_arrEndYear(j) || ',';
        v_strsql := v_strsql || v_arrEndPeriod(j) || ',';
      
        case v_arrChronology(j)
          when P_CONSTANT.Monthly then
            --monthly
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'm',
                                       pIn_bIncludeDataType => false,
                                       pIn_vTableAlias      => 'n.');
          when P_CONSTANT.Weekly then
            --weekly
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'w',
                                       pIn_bIncludeDataType => false,
                                       pIn_vTableAlias      => 'n.');
          
          when P_CONSTANT.Daily then
            --daily
            v_strsql := v_strsql ||
                        FMF_GetColsStr(pIn_chronology       => 'd',
                                       pIn_bIncludeDataType => false,
                                       pIn_vTableAlias      => 'n.');
        end case;
      
        v_strsql_from  := ' from ';
        v_strsql_where := ' ';
      
        if pIn_nNodeType = 10003 then
          if v_arrChronology(j) = P_CONSTANT.Monthly then
            v_strsql_from := v_strsql_from || ' don_m n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Weekly then
            v_strsql_from := v_strsql_from || ' don_w n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Daily then
            v_strsql_from := v_strsql_from || ' don_d n ,tb_node m ';
          end if;
        elsif pIn_nNodeType = 10004 then
          if v_arrChronology(j) = P_CONSTANT.Monthly then
            v_strsql_from := v_strsql_from || ' prb_m n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Weekly then
            v_strsql_from := v_strsql_from || ' prb_w n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Daily then
            v_strsql_from := v_strsql_from || ' prb_d n ,tb_node m ';
          end if;
        elsif pIn_nNodeType = 10005 then
        
          if v_arrChronology(j) = P_CONSTANT.Monthly then
            v_strsql_from := v_strsql_from || ' bud_m n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Weekly then
            v_strsql_from := v_strsql_from || ' bud_w n ,tb_node m ';
          elsif v_arrChronology(j) = P_CONSTANT.Daily then
            v_strsql_from := v_strsql_from || ' bud_d n ,tb_node m ';
          end if;
        end if;
      
        v_strsql_where := v_strsql_where || ' where n.tsid = ' ||
                          v_arrTimeSeriesDBID(j);
        v_strsql_where := v_strsql_where || ' and n.version =  ' ||
                          v_arrVersion(j);
        v_strsql_where := v_strsql_where || ' and n.yy between ' ||
                          v_arrBeginYear(j) || ' and ' || v_arrEndYear(j);
      
        if pIn_nNodeType = 10003 then
          v_strsql_where := v_strsql_where || ' and n.pvtid = m.id';
        elsif pIn_nNodeType = 10004 then
          v_strsql_where := v_strsql_where || ' and n.selid = m.id';
        elsif pIn_nNodeType = 10005 then
          v_strsql_where := v_strsql_where || ' and n.bdgid = m.id';
        end if;
      
        v_c_lob := v_strsql || v_strsql_from || v_strsql_where;
        fmsp_execsql(pIn_cSql => v_c_lob);
      end loop;
    end if;
  exception
    when others then
      pOut_nSQLCode := sqlcode;
      rollback;
      raise;
  end;

  procedure sp_GetAllTimeSeries(p_nNodeType         in integer,
                                p_arrNodeAddr       in clob,
                                p_arrTimeSeriesDBID in clob,
                                p_arrVersion        in clob,
                                p_arrChronology     in clob,
                                p_arrBeginYear      in clob,
                                p_arrBeginPeriod    in clob,
                                p_arrEndYear        in clob,
                                p_arrEndPeriod      in clob,
                                p_strTableName      out varchar2,
                                p_nSQLCode          out integer) is
  begin
    fmp_log.FMP_SetValue(p_nNodeType);
    fmp_log.FMP_SetValue(p_arrNodeAddr);
    fmp_log.FMP_SetValue(p_arrTimeSeriesDBID);
    fmp_log.FMP_SetValue(p_arrVersion);
    fmp_log.FMP_SetValue(p_arrChronology);
    fmp_log.FMP_SetValue(p_arrBeginYear);
    fmp_log.FMP_SetValue(p_arrBeginPeriod);
    fmp_log.FMP_SetValue(p_arrEndYear);
    fmp_log.FMP_SetValue(p_arrEndPeriod);
    fmp_log.LOGBEGIN;
    FMSP_GetAllTimeSeries(pIn_nNodeType         => p_nNodeType,
                          pIn_cNodeList         => p_arrNodeAddr,
                          pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                          pIn_arrVersion        => p_arrVersion,
                          pIn_arrChronology     => p_arrChronology,
                          pIn_arrBeginYear      => p_arrBeginYear,
                          pIn_arrBeginPeriod    => p_arrBeginPeriod,
                          pIn_arrEndYear        => p_arrEndYear,
                          pIn_arrEndPeriod      => p_arrEndPeriod,
                          pOut_vTableName       => p_strTableName,
                          pOut_nSQLCode         => p_nSQLCode);
    fmp_log.LOGEND;
  end;

  procedure sp_GetElementFromArrayByIndex(Liststr    in clob,
                                          p_tt_type  out tt_timeseries_type,
                                          icount     out integer,
                                          p_nSQLCODE out integer) is
    TmpStr   varchar2(100);
    Str      clob;
    j        integer;
    sPlitVal varchar2(2) := ',';
  begin
    p_nSQLCODE := 0;
    Str        := Liststr;
    j          := 0;
    IF Instr(Liststr, sPlitVal, 1, 1) = 0 THEN
      p_tt_type(j) := Liststr;
      j := j + 1;
    else
      While Instr(str, sPlitVal, 1, 1) > 0 Loop
        TmpStr := Substr(str, 1, Instr(str, sPlitVal, 1, 1) - 1);
        p_tt_type(j) := TmpStr;
        str := SubStr(Str,
                      Instr(str, sPlitVal, 1, 1) + length(sPlitVal),
                      length(str));
        j := j + 1;
      end loop;
      if not str is null then
        --save then last one item
        p_tt_type(j) := str;
        j := j + 1;
      end if;
    end if;
    icount := j - 1;
  exception
    when others then
      p_nSQLCODE := sqlcode;
  end;

  procedure SP_GetDetailNodeTSBySelID(P_SelectionID       in number,
                                      p_IsDynamic         in number,
                                      P_Sequence          in varchar2,
                                      p_arrTimeSeriesDBID in clob,
                                      p_arrVersion        in clob,
                                      p_arrChronology     in clob,
                                      p_arrBeginYear      in clob,
                                      p_arrBeginPeriod    in clob,
                                      p_arrEndYear        in clob,
                                      p_arrEndPeriod      in clob,
                                      p_strTableName      out varchar2,
                                      p_nSQLCode          out integer) is
    p_DetailNode     sys_refcursor;
    p_strtemptabname varchar2(30);
    vTabName         varchar2(30);
  begin
    p_Selection.SP_GetDetailNodeBySelectionID(P_SelectionID => P_SelectionID,
                                              p_IsDynamic   => p_IsDynamic,
                                              P_Sequence    => P_Sequence,
                                              p_DetailNode  => p_DetailNode,
                                              pOut_vTabName => vTabName,
                                              p_SqlCode     => p_nSqlCode);
  
    if p_nSqlCode = 0 then
      FMSP_GetAllTimeSeries(pIn_nNodeType         => 10003,
                            pIn_vTabName          => vTabName,
                            pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                            pIn_arrVersion        => p_arrVersion,
                            pIn_arrChronology     => p_arrChronology,
                            pIn_arrBeginYear      => p_arrBeginYear,
                            pIn_arrBeginPeriod    => p_arrBeginPeriod,
                            pIn_arrEndYear        => p_arrEndYear,
                            pIn_arrEndPeriod      => p_arrEndPeriod,
                            pOut_vTableName       => p_strTableName,
                            pOut_nSQLCode         => p_nSQLCode);
    
    end if;
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetDetailNodeTSByConditions(P_Conditions        in varchar2,
                                           P_Sequence          in varchar2,
                                           p_arrTimeSeriesDBID in clob,
                                           p_arrVersion        in clob,
                                           p_arrChronology     in clob,
                                           p_arrBeginYear      in clob,
                                           p_arrBeginPeriod    in clob,
                                           p_arrEndYear        in clob,
                                           p_arrEndPeriod      in clob,
                                           p_strTableName      out varchar2,
                                           p_nSQLCode          out integer) is
    p_DetailNode     sys_refcursor;
    p_strtemptabname varchar2(30);
    vTabName         varchar2(30);
  begin
    p_Selection.SP_GetDetailNodeByConditions(P_Conditions  => P_Conditions,
                                             P_Sequence    => P_Sequence,
                                             p_DetailNode  => p_DetailNode,
                                             pOut_vTabName => vTabName,
                                             p_SqlCode     => p_nSqlCode);
    if p_nSqlCode = 0 then
      FMSP_GetAllTimeSeries(pIn_nNodeType         => 10003,
                            pIn_vTabName          => vTabName,
                            pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                            pIn_arrVersion        => p_arrVersion,
                            pIn_arrChronology     => p_arrChronology,
                            pIn_arrBeginYear      => p_arrBeginYear,
                            pIn_arrBeginPeriod    => p_arrBeginPeriod,
                            pIn_arrEndYear        => p_arrEndYear,
                            pIn_arrEndPeriod      => p_arrEndPeriod,
                            pOut_vTableName       => p_strTableName,
                            pOut_nSQLCode         => p_nSQLCode);
    end if;
  
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetDetailNodeTSBySelCdt(P_SelectionID       in number,
                                       P_Conditions        in varchar2,
                                       P_Sequence          in varchar2,
                                       p_arrTimeSeriesDBID in clob,
                                       p_arrVersion        in clob,
                                       p_arrChronology     in clob,
                                       p_arrBeginYear      in clob,
                                       p_arrBeginPeriod    in clob,
                                       p_arrEndYear        in clob,
                                       p_arrEndPeriod      in clob,
                                       p_strTableName      out varchar2,
                                       p_nSQLCode          out integer) is
    p_DetailNode     sys_refcursor;
    p_strtemptabname varchar2(30);
    vTabName         varchar2(30);
  begin
    p_Selection.SP_GetDetailNodeBySelCdt(P_SelectionID => P_SelectionID,
                                         P_Conditions  => P_Conditions,
                                         P_Sequence    => P_Sequence,
                                         p_DetailNode  => p_DetailNode,
                                         pOut_vTabName => vTabName,
                                         p_SqlCode     => p_nSqlCode);
  
    if p_nSqlCode = 0 then
      FMSP_GetAllTimeSeries(pIn_nNodeType         => 10003,
                            pIn_vTabName          => vTabName,
                            pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                            pIn_arrVersion        => p_arrVersion,
                            pIn_arrChronology     => p_arrChronology,
                            pIn_arrBeginYear      => p_arrBeginYear,
                            pIn_arrBeginPeriod    => p_arrBeginPeriod,
                            pIn_arrEndYear        => p_arrEndYear,
                            pIn_arrEndPeriod      => p_arrEndPeriod,
                            pOut_vTableName       => p_strTableName,
                            pOut_nSQLCode         => p_nSQLCode);
    end if;
  
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetAggregateNodesTS(P_AggregateRuleID   in number,
                                   P_Sequence          in varchar2,
                                   p_arrTimeSeriesDBID in clob,
                                   p_arrVersion        in clob,
                                   p_arrChronology     in clob,
                                   p_arrBeginYear      in clob,
                                   p_arrBeginPeriod    in clob,
                                   p_arrEndYear        in clob,
                                   p_arrEndPeriod      in clob,
                                   p_strTableName      out varchar2,
                                   p_nSQLCode          out integer) is
    v_strsql       varchar2(32767) := '';
    v_strsql_from  varchar2(32767) := '';
    v_strsql_where varchar2(32767) := '';
    v_c_lob        clob;
    v_nsqlcode     number;
  
    vOut_tNestTab fmt_nest_tab_nodeid;
    vOut_nSqlCode number;
  
    p_nNodeType      number;
    p_strtemptabname varchar2(30) := '';
    p_AggregateNode  sys_refcursor;
    vTabname         varchar2(30);
  
  begin
    p_nNodeType := 10004;
    p_Aggregation.SP_GetAggregateNodes(P_AggregateRuleID => P_AggregateRuleID,
                                       P_Sequence        => P_Sequence,
                                       p_AggregateNode   => p_AggregateNode,
                                       pOut_vTabName     => vTabname,
                                       p_SqlCode         => p_nSqlCode);
    if p_nSqlCode = 0 then
      FMSP_GetAllTimeSeries(pIn_nNodeType         => 10004,
                            pIn_vTabName          => vTabName,
                            pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                            pIn_arrVersion        => p_arrVersion,
                            pIn_arrChronology     => p_arrChronology,
                            pIn_arrBeginYear      => p_arrBeginYear,
                            pIn_arrBeginPeriod    => p_arrBeginPeriod,
                            pIn_arrEndYear        => p_arrEndYear,
                            pIn_arrEndPeriod      => p_arrEndPeriod,
                            pOut_vTableName       => p_strTableName,
                            pOut_nSQLCode         => p_nSQLCode);
    end if;
  
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetAggNodesTSByConditions(P_AggregateRuleID   in number,
                                         P_Conditions        in varchar2,
                                         P_Sequence          in varchar2,
                                         p_arrTimeSeriesDBID in clob,
                                         p_arrVersion        in clob,
                                         p_arrChronology     in clob,
                                         p_arrBeginYear      in clob,
                                         p_arrBeginPeriod    in clob,
                                         p_arrEndYear        in clob,
                                         p_arrEndPeriod      in clob,
                                         p_strTableName      out varchar2,
                                         p_nSQLCode          out integer) is
    p_AggregateNode  sys_refcursor;
    p_strtemptabname varchar2(30);
    vTabName         varchar2(30);
  begin
    p_Aggregation.SP_GetAggNodesByConditions(P_AggregateRuleID => P_AggregateRuleID,
                                             P_Conditions      => P_Conditions,
                                             P_Sequence        => P_Sequence,
                                             p_AggregateNode   => p_AggregateNode,
                                             pOut_vTabName     => vTabName,
                                             p_SqlCode         => p_nSqlCode);
    if p_nSqlCode = 0 then
      FMSP_GetAllTimeSeries(pIn_nNodeType         => 10004,
                            pIn_vTabName          => vTabName,
                            pIn_arrTimeSeriesDBID => p_arrTimeSeriesDBID,
                            pIn_arrVersion        => p_arrVersion,
                            pIn_arrChronology     => p_arrChronology,
                            pIn_arrBeginYear      => p_arrBeginYear,
                            pIn_arrBeginPeriod    => p_arrBeginPeriod,
                            pIn_arrEndYear        => p_arrEndYear,
                            pIn_arrEndPeriod      => p_arrEndPeriod,
                            pOut_vTableName       => p_strTableName,
                            pOut_nSQLCode         => p_nSQLCode);
    end if;
  
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure FMISP_GetAllTimeSeries(pIn_nNodeType         in integer,
                                   pIn_vTabName          in varchar2,
                                   pIn_arrTimeSeriesDBID in clob,
                                   pIn_arrVersion        in clob,
                                   pIn_arrChronology     in clob,
                                   pIn_arrBeginYear      in clob,
                                   pIn_arrBeginPeriod    in clob,
                                   pIn_arrEndYear        in clob,
                                   pIn_arrEndPeriod      in clob,
                                   pOut_vTableName       out varchar2,
                                   pOut_nSQLCode         out integer)
  --*****************************************************************
    -- Description:
    --
    -- Parameters:
    --          pIn_nNodeType
    --          pIn_vTabName:table stored all nodes ID
    --          pIn_arrTimeSeriesDBID
    --          pIn_arrVersion
    --          pIn_arrChronology
    --          pIn_arrBeginYear
    --          pIn_arrBeginPeriod
    --          pIn_arrEndYear
    --          pIn_arrEndPeriod
    --          pOut_vTableName
    --          pOut_nSQLCode
  
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JY.Liu     created.
    -- **************************************************************
   is
  begin
    fmp_log.FMP_SetValue(pIn_nNodeType);
    fmp_log.FMP_SetValue(pIn_vTabName);
    fmp_log.FMP_SetValue(pIn_arrTimeSeriesDBID);
    fmp_log.FMP_SetValue(pIn_arrVersion);
    fmp_log.FMP_SetValue(pIn_arrChronology);
    fmp_log.FMP_SetValue(pIn_arrBeginYear);
    fmp_log.FMP_SetValue(pIn_arrBeginPeriod);
    fmp_log.FMP_SetValue(pIn_arrEndYear);
    fmp_log.FMP_SetValue(pIn_arrEndPeriod);
    fmp_log.LOGBEGIN;
    FMSP_GetAllTimeSeries(pIn_nNodeType         => pIn_nNodeType,
                          pIn_vTabName          => pIn_vTabName,
                          pIn_arrTimeSeriesDBID => pIn_arrTimeSeriesDBID,
                          pIn_arrVersion        => pIn_arrVersion,
                          pIn_arrChronology     => pIn_arrChronology,
                          pIn_arrBeginYear      => pIn_arrBeginYear,
                          pIn_arrBeginPeriod    => pIn_arrBeginPeriod,
                          pIn_arrEndYear        => pIn_arrEndYear,
                          pIn_arrEndPeriod      => pIn_arrEndPeriod,
                          pOut_vTableName       => pOut_vTableName,
                          pOut_nSQLCode         => pOut_nSQLCode);
    fmp_log.LOGEND;
  end;

end P_PEXPORT_QueryTimeSeries;
/
