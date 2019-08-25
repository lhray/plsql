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
                                p_arrNodeAddr       in clob, --em_addr in record, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                p_nSQLCode          out integer --return exception code,0 is no error.
                                );
  procedure sp_Get_Middle_AllTimeSeries(p_nNodeType         in integer, --10003: node in pvt, 10004: node in sel, 10005: node in bdg
                                        p_strtemptabname    in varchar2, --temptabname
                                        p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                        p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                        p_nSQLCode          out integer --return exception code,0 is no error.
                                        );
  procedure sp_GetElementFromArrayByIndex(Liststr    in clob,
                                          p_tt_type  out tt_timeseries_type,
                                          icount     out integer,
                                          p_nSQLCODE out integer);

  --create or replace package body p_Selection is
  --SP_GetDetailNodeTSBySelectionID
  procedure SP_GetDetailNodeTSBySelID(P_SelectionID in number,
                                      p_IsDynamic   in number, --1:true,0:false
                                      P_Sequence    in varchar2, --Sort sequence
                                      --p_DetailNode  out sys_refcursor,
                                      p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                      p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                      p_nSQLCode          out integer --return exception code,0 is no error.
                                      );

  procedure SP_GetDetailNodeTSByConditions(P_Conditions in varchar2,
                                           P_Sequence   in varchar2, --Sort sequence
                                           --p_DetailNode out sys_refcursor,
                                           p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                           p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                           p_nSQLCode          out integer --return exception code,0 is no error.
                                           );

  procedure SP_GetDetailNodeTSBySelCdt(P_SelectionID in number,
                                       P_Conditions  in varchar2,
                                       P_Sequence    in varchar2, --Sort sequence
                                       --p_DetailNode  out sys_refcursor,
                                       p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                       p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                       p_nSQLCode          out integer --return exception code,0 is no error.
                                       );

  procedure SP_GetAggregateNodesTS(P_AggregateRuleID in number,
                                   P_Sequence        in varchar2, --Sort sequence
                                   --p_AggregateNode   out sys_refcursor,
                                   p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                   p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                   p_nSQLCode          out integer --return exception code,0 is no error.
                                   );
  procedure SP_GetAggNodesTSByConditions(P_AggregateRuleID in number,
                                         P_Conditions      in varchar2,
                                         P_Sequence        in varchar2, --Sort sequence
                                         --p_AggregateNode   out nocopy sys_refcursor,
                                         p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrVersion        in clob, -- time series version, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrBeginYear      in clob, -- begin year such as??2011?ˋ那㏒ˋˋ那ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrBeginPeriod    in clob, -- begin period such as??10?ˋ那㏒ˋˋ∫ˋ豕, separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrEndYear        in clob, -- end year such as??2012?ˋ那㏒ˋˋ那ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_arrEndPeriod      in clob, -- end period such as??12?ˋ那㏒ˋˋ∫ˋ豕,separated by ??,?ˋ那㏒ˋˋ那ˋ豕
                                         p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                         p_nSQLCode          out integer --return exception code,0 is no error.
                                         );
end P_PEXPORT_QueryTimeSeries;
/
create or replace package body P_PEXPORT_QueryTimeSeries is
  --Authid Current_User is
  -- Author  : junhuazuo
  -- Created : 12/18/2012 2:35:40 PM
  -- Public function and procedure declarations

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
      pOut_nSQLCode := 0;
      --select seq_tb_pimport.Nextval into p_strTableName from dual;
      pOut_vTableName := fmf_gettmptablename(); --'T_TS_' || p_strTableName;
      v_strsql        := ' Create table ' || pOut_vTableName || ' (';
      v_strsql        := v_strsql || '   Field_NO        number,';
      v_strsql        := v_strsql || '   Node_addr       number,';
      v_strsql        := v_strsql || '   Chronology      varchar2(50),';
      v_strsql        := v_strsql || '   Time_series_id  number,';
      v_strsql        := v_strsql || '   Version         number,';
      v_strsql        := v_strsql || '   Annee           number,';
      v_strsql        := v_strsql || '   Begin_year      number,';
      v_strsql        := v_strsql || '   Begin_period    number,';
      v_strsql        := v_strsql || '   End_year        number,';
      v_strsql        := v_strsql || '   End_period      number,';
      v_strsql        := v_strsql || '   T1  number,';
      v_strsql        := v_strsql || '   T2  number,';
      v_strsql        := v_strsql || '   T3  number,';
      v_strsql        := v_strsql || '   T4  number,';
      v_strsql        := v_strsql || '   T5  number,';
      v_strsql        := v_strsql || '   T6  number,';
      v_strsql        := v_strsql || '   T7  number,';
      v_strsql        := v_strsql || '   T8  number,';
      v_strsql        := v_strsql || '   T9  number,';
      v_strsql        := v_strsql || '   T10  number,';
      v_strsql        := v_strsql || '   T11  number,';
      v_strsql        := v_strsql || '   T12  number,';
      v_strsql        := v_strsql || '   T13  number,';
      v_strsql        := v_strsql || '   T14  number,';
      v_strsql        := v_strsql || '   T15  number,';
      v_strsql        := v_strsql || '   T16  number,';
      v_strsql        := v_strsql || '   T17  number,';
      v_strsql        := v_strsql || '   T18  number,';
      v_strsql        := v_strsql || '   T19  number,';
      v_strsql        := v_strsql || '   T20  number,';
      v_strsql        := v_strsql || '   T21  number,';
      v_strsql        := v_strsql || '   T22  number,';
      v_strsql        := v_strsql || '   T23  number,';
      v_strsql        := v_strsql || '   T24  number,';
      v_strsql        := v_strsql || '   T25  number,';
      v_strsql        := v_strsql || '   T26  number,';
      v_strsql        := v_strsql || '   T27  number,';
      v_strsql        := v_strsql || '   T28  number,';
      v_strsql        := v_strsql || '   T29  number,';
      v_strsql        := v_strsql || '   T30  number,';
      v_strsql        := v_strsql || '   T31  number,';
      v_strsql        := v_strsql || '   T32  number,';
      v_strsql        := v_strsql || '   T33  number,';
      v_strsql        := v_strsql || '   T34  number,';
      v_strsql        := v_strsql || '   T35  number,';
      v_strsql        := v_strsql || '   T36  number,';
      v_strsql        := v_strsql || '   T37  number,';
      v_strsql        := v_strsql || '   T38  number,';
      v_strsql        := v_strsql || '   T39  number,';
      v_strsql        := v_strsql || '   T40  number,';
      v_strsql        := v_strsql || '   T41  number,';
      v_strsql        := v_strsql || '   T42  number,';
      v_strsql        := v_strsql || '   T43  number,';
      v_strsql        := v_strsql || '   T44  number,';
      v_strsql        := v_strsql || '   T45  number,';
      v_strsql        := v_strsql || '   T46  number,';
      v_strsql        := v_strsql || '   T47  number,';
      v_strsql        := v_strsql || '   T48  number,';
      v_strsql        := v_strsql || '   T49  number,';
      v_strsql        := v_strsql || '   T50  number,';
      v_strsql        := v_strsql || '   T51  number,';
      v_strsql        := v_strsql || '   T52  number,';
      v_strsql        := v_strsql || '   T53  number ';
      v_strsql        := v_strsql || '   )';

      execute immediate v_strsql;
      commit;

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
        if v_arrChronology(j) = 1 then
          --monthly
          v_strsql := v_strsql || '   T1,';
          v_strsql := v_strsql || '   T2,';
          v_strsql := v_strsql || '   T3,';
          v_strsql := v_strsql || '   T4,';
          v_strsql := v_strsql || '   T5,';
          v_strsql := v_strsql || '   T6,';
          v_strsql := v_strsql || '   T7,';
          v_strsql := v_strsql || '   T8,';
          v_strsql := v_strsql || '   T9,';
          v_strsql := v_strsql || '   T10,';
          v_strsql := v_strsql || '   T11,';
          v_strsql := v_strsql || '   T12';
        end if;
        if v_arrChronology(j) = 2 then
          v_strsql := v_strsql || '   T1,';
          v_strsql := v_strsql || '   T2,';
          v_strsql := v_strsql || '   T3,';
          v_strsql := v_strsql || '   T4,';
          v_strsql := v_strsql || '   T5,';
          v_strsql := v_strsql || '   T6,';
          v_strsql := v_strsql || '   T7,';
          v_strsql := v_strsql || '   T8,';
          v_strsql := v_strsql || '   T9,';
          v_strsql := v_strsql || '   T10,';
          v_strsql := v_strsql || '   T11,';
          v_strsql := v_strsql || '   T12,';
          v_strsql := v_strsql || '   T13,';
          v_strsql := v_strsql || '   T14,';
          v_strsql := v_strsql || '   T15,';
          v_strsql := v_strsql || '   T16,';
          v_strsql := v_strsql || '   T17,';
          v_strsql := v_strsql || '   T18,';
          v_strsql := v_strsql || '   T19,';
          v_strsql := v_strsql || '   T20,';
          v_strsql := v_strsql || '   T21,';
          v_strsql := v_strsql || '   T22,';
          v_strsql := v_strsql || '   T23,';
          v_strsql := v_strsql || '   T24,';
          v_strsql := v_strsql || '   T25,';
          v_strsql := v_strsql || '   T26,';
          v_strsql := v_strsql || '   T27,';
          v_strsql := v_strsql || '   T28,';
          v_strsql := v_strsql || '   T29,';
          v_strsql := v_strsql || '   T30,';
          v_strsql := v_strsql || '   T31,';
          v_strsql := v_strsql || '   T32,';
          v_strsql := v_strsql || '   T33,';
          v_strsql := v_strsql || '   T34,';
          v_strsql := v_strsql || '   T35,';
          v_strsql := v_strsql || '   T36,';
          v_strsql := v_strsql || '   T37,';
          v_strsql := v_strsql || '   T38,';
          v_strsql := v_strsql || '   T39,';
          v_strsql := v_strsql || '   T40,';
          v_strsql := v_strsql || '   T41,';
          v_strsql := v_strsql || '   T42,';
          v_strsql := v_strsql || '   T43,';
          v_strsql := v_strsql || '   T44,';
          v_strsql := v_strsql || '   T45,';
          v_strsql := v_strsql || '   T46,';
          v_strsql := v_strsql || '   T47,';
          v_strsql := v_strsql || '   T48,';
          v_strsql := v_strsql || '   T49,';
          v_strsql := v_strsql || '   T50,';
          v_strsql := v_strsql || '   T51,';
          v_strsql := v_strsql || '   T52,';
          v_strsql := v_strsql || '   T53 ';
        end if;
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

        if v_arrChronology(j) = 1 then
          --monthly
          v_strsql := v_strsql || '   n.T1,';
          v_strsql := v_strsql || '   n.T2,';
          v_strsql := v_strsql || '   n.T3,';
          v_strsql := v_strsql || '   n.T4,';
          v_strsql := v_strsql || '   n.T5,';
          v_strsql := v_strsql || '   n.T6,';
          v_strsql := v_strsql || '   n.T7,';
          v_strsql := v_strsql || '   n.T8,';
          v_strsql := v_strsql || '   n.T9,';
          v_strsql := v_strsql || '   n.T10,';
          v_strsql := v_strsql || '   n.T11,';
          v_strsql := v_strsql || '   n.T12 ';
        end if;

        if v_arrChronology(j) = 2 then
          v_strsql := v_strsql || '   n.T1,';
          v_strsql := v_strsql || '   n.T2,';
          v_strsql := v_strsql || '   n.T3,';
          v_strsql := v_strsql || '   n.T4,';
          v_strsql := v_strsql || '   n.T5,';
          v_strsql := v_strsql || '   n.T6,';
          v_strsql := v_strsql || '   n.T7,';
          v_strsql := v_strsql || '   n.T8,';
          v_strsql := v_strsql || '   n.T9,';
          v_strsql := v_strsql || '   n.T10,';
          v_strsql := v_strsql || '   n.T11,';
          v_strsql := v_strsql || '   n.T12,';
          v_strsql := v_strsql || '   n.T13,';
          v_strsql := v_strsql || '   n.T14,';
          v_strsql := v_strsql || '   n.T15,';
          v_strsql := v_strsql || '   n.T16,';
          v_strsql := v_strsql || '   n.T17,';
          v_strsql := v_strsql || '   n.T18,';
          v_strsql := v_strsql || '   n.T19,';
          v_strsql := v_strsql || '   n.T20,';
          v_strsql := v_strsql || '   n.T21,';
          v_strsql := v_strsql || '   n.T22,';
          v_strsql := v_strsql || '   n.T23,';
          v_strsql := v_strsql || '   n.T24,';
          v_strsql := v_strsql || '   n.T25,';
          v_strsql := v_strsql || '   n.T26,';
          v_strsql := v_strsql || '   n.T27,';
          v_strsql := v_strsql || '   n.T28,';
          v_strsql := v_strsql || '   n.T29,';
          v_strsql := v_strsql || '   n.T30,';
          v_strsql := v_strsql || '   n.T31,';
          v_strsql := v_strsql || '   n.T32,';
          v_strsql := v_strsql || '   n.T33,';
          v_strsql := v_strsql || '   n.T34,';
          v_strsql := v_strsql || '   n.T35,';
          v_strsql := v_strsql || '   n.T36,';
          v_strsql := v_strsql || '   n.T37,';
          v_strsql := v_strsql || '   n.T38,';
          v_strsql := v_strsql || '   n.T39,';
          v_strsql := v_strsql || '   n.T40,';
          v_strsql := v_strsql || '   n.T41,';
          v_strsql := v_strsql || '   n.T42,';
          v_strsql := v_strsql || '   n.T43,';
          v_strsql := v_strsql || '   n.T44,';
          v_strsql := v_strsql || '   n.T45,';
          v_strsql := v_strsql || '   n.T46,';
          v_strsql := v_strsql || '   n.T47,';
          v_strsql := v_strsql || '   n.T48,';
          v_strsql := v_strsql || '   n.T49,';
          v_strsql := v_strsql || '   n.T50,';
          v_strsql := v_strsql || '   n.T51,';
          v_strsql := v_strsql || '   n.T52,';
          v_strsql := v_strsql || '   n.T53 ';
        end if;

        v_strsql_from  := ' from ';
        v_strsql_where := ' ';

        if pIn_nNodeType = 10003 then
          if v_arrChronology(j) = 1 then
            v_strsql_from := v_strsql_from || ' don_m n ,tb_node m ';
          elsif v_arrChronology(j) = 2 then
            v_strsql_from := v_strsql_from || ' don_w n ,tb_node m ';
          end if;
        elsif pIn_nNodeType = 10004 then

          if v_arrChronology(j) = 1 then
            v_strsql_from := v_strsql_from || ' prb_m n ,tb_node m ';
          elsif v_arrChronology(j) = 2 then
            v_strsql_from := v_strsql_from || ' prb_w n ,tb_node m ';
          end if;
        elsif pIn_nNodeType = 10005 then

          if v_arrChronology(j) = 1 then
            v_strsql_from := v_strsql_from || ' bud_m n ,tb_node m ';
          elsif v_arrChronology(j) = 2 then
            v_strsql_from := v_strsql_from || ' bud_w n ,tb_node m ';
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
        execute immediate v_c_lob;
        commit;

      end loop;

    end if;
    commit;
  exception
    when others then
      pOut_nSQLCode := sqlcode;
      rollback;
      raise;
  end;

  procedure sp_GetAllTimeSeries(p_nNodeType         in integer, --10003: node in pvt, 10004: node in sel, 10005: node in bdg
                                p_arrNodeAddr       in clob, --em_addr in record, separated by ??,?㏒ˋ㏒∟
                                p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                p_nSQLCode          out integer --return exception code,0 is no error.
                                ) is
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
    Str      varchar2(4000);
    j        integer;
    sPlitVal varchar2(2);
  begin
    p_nSQLCODE := 0;
    sPlitVal   := ',';
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

    icount     := j - 1;
    p_nSQLCODE := 0;
  exception
    when others then
      p_nSQLCODE := sqlcode;
  end;

  procedure sp_Get_Middle_AllTimeSeries(p_nNodeType         in integer, --10003: node in pvt, 10004: node in sel, 10005: node in bdg
                                        p_strtemptabname    in varchar2, --temptabname
                                        p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                        p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                        p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                        p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                        p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                        p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                        p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                        p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                        p_nSQLCode          out integer --return exception code,0 is no error.
                                        ) is
    v_strsql       varchar2(32767) := '';
    v_strsql_from  varchar2(32767) := '';
    v_strsql_where varchar2(32767) := '';
    v_c_lob        clob;
    v_nsqlcode     number;

    nperiods2     number;
    nperiods3     number;
    nperiods4     number;
    nperiods5     number;
    nperiods6     number;
    nperiods7     number;
    nperiods8     number;
    vOut_tNestTab fmt_nest_tab_nodeid;
    vOut_nSqlCode number;

    v_arrTimeSeriesDBID tt_timeseries_type;
    v_arrVersion        tt_timeseries_type;
    v_arrChronology     tt_timeseries_type;
    v_arrBeginYear      tt_timeseries_type;
    v_arrBeginPeriod    tt_timeseries_type;
    v_arrEndYear        tt_timeseries_type;
    v_arrEndPeriod      tt_timeseries_type;
  begin
    v_strsql := '';
    sp_GetElementFromArrayByIndex(p_arrTimeSeriesDBID,
                                  v_arrTimeSeriesDBID,
                                  nperiods2,
                                  v_nsqlcode);

    sp_GetElementFromArrayByIndex(p_arrVersion,
                                  v_arrVersion,
                                  nperiods3,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(p_arrChronology,
                                  v_arrChronology,
                                  nperiods4,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(p_arrBeginYear,
                                  v_arrBeginYear,
                                  nperiods5,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(p_arrBeginPeriod,
                                  v_arrBeginPeriod,
                                  nperiods6,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(p_arrEndYear,
                                  v_arrEndYear,
                                  nperiods7,
                                  v_nsqlcode);
    sp_GetElementFromArrayByIndex(p_arrEndPeriod,
                                  v_arrEndPeriod,
                                  nperiods8,
                                  v_nsqlcode);

    if nperiods2 = nperiods3 and nperiods2 = nperiods4 and
       nperiods2 = nperiods5 and nperiods2 = nperiods7 and
       nperiods2 = nperiods7 and nperiods2 = nperiods8 then
      p_nSQLCode := 0;
      --select seq_tb_pimport.Nextval into p_strTableName from dual;
      p_strTableName := fmf_gettmptablename(); --'T_TS_' || p_strTableName;
      v_strsql       := ' Create table ' || p_strTableName || ' (';
      v_strsql       := v_strsql || '   Field_NO        number,';
      v_strsql       := v_strsql || '   Node_addr       number,';
      v_strsql       := v_strsql || '   Chronology      varchar2(50),';
      v_strsql       := v_strsql || '   Time_series_id  number,';
      v_strsql       := v_strsql || '   Version         number,';
      v_strsql       := v_strsql || '   Annee           number,';
      v_strsql       := v_strsql || '   Begin_year      number,';
      v_strsql       := v_strsql || '   Begin_period    number,';
      v_strsql       := v_strsql || '   End_year        number,';
      v_strsql       := v_strsql || '   End_period      number,';
      v_strsql       := v_strsql || '   T1  number,';
      v_strsql       := v_strsql || '   T2  number,';
      v_strsql       := v_strsql || '   T3  number,';
      v_strsql       := v_strsql || '   T4  number,';
      v_strsql       := v_strsql || '   T5  number,';
      v_strsql       := v_strsql || '   T6  number,';
      v_strsql       := v_strsql || '   T7  number,';
      v_strsql       := v_strsql || '   T8  number,';
      v_strsql       := v_strsql || '   T9  number,';
      v_strsql       := v_strsql || '   T10  number,';
      v_strsql       := v_strsql || '   T11  number,';
      v_strsql       := v_strsql || '   T12  number,';
      v_strsql       := v_strsql || '   T13  number,';
      v_strsql       := v_strsql || '   T14  number,';
      v_strsql       := v_strsql || '   T15  number,';
      v_strsql       := v_strsql || '   T16  number,';
      v_strsql       := v_strsql || '   T17  number,';
      v_strsql       := v_strsql || '   T18  number,';
      v_strsql       := v_strsql || '   T19  number,';
      v_strsql       := v_strsql || '   T20  number,';
      v_strsql       := v_strsql || '   T21  number,';
      v_strsql       := v_strsql || '   T22  number,';
      v_strsql       := v_strsql || '   T23  number,';
      v_strsql       := v_strsql || '   T24  number,';
      v_strsql       := v_strsql || '   T25  number,';
      v_strsql       := v_strsql || '   T26  number,';
      v_strsql       := v_strsql || '   T27  number,';
      v_strsql       := v_strsql || '   T28  number,';
      v_strsql       := v_strsql || '   T29  number,';
      v_strsql       := v_strsql || '   T30  number,';
      v_strsql       := v_strsql || '   T31  number,';
      v_strsql       := v_strsql || '   T32  number,';
      v_strsql       := v_strsql || '   T33  number,';
      v_strsql       := v_strsql || '   T34  number,';
      v_strsql       := v_strsql || '   T35  number,';
      v_strsql       := v_strsql || '   T36  number,';
      v_strsql       := v_strsql || '   T37  number,';
      v_strsql       := v_strsql || '   T38  number,';
      v_strsql       := v_strsql || '   T39  number,';
      v_strsql       := v_strsql || '   T40  number,';
      v_strsql       := v_strsql || '   T41  number,';
      v_strsql       := v_strsql || '   T42  number,';
      v_strsql       := v_strsql || '   T43  number,';
      v_strsql       := v_strsql || '   T44  number,';
      v_strsql       := v_strsql || '   T45  number,';
      v_strsql       := v_strsql || '   T46  number,';
      v_strsql       := v_strsql || '   T47  number,';
      v_strsql       := v_strsql || '   T48  number,';
      v_strsql       := v_strsql || '   T49  number,';
      v_strsql       := v_strsql || '   T50  number,';
      v_strsql       := v_strsql || '   T51  number,';
      v_strsql       := v_strsql || '   T52  number,';
      v_strsql       := v_strsql || '   T53  number ';
      v_strsql       := v_strsql || '   )';

      execute immediate v_strsql;
      commit;

      for j in 0 .. nperiods2 loop
        v_strsql := '';
        v_strsql := 'insert into ' || p_strTableName || ' (';
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
        if v_arrChronology(j) = 1 then
          --monthly
          v_strsql := v_strsql || '   T1,';
          v_strsql := v_strsql || '   T2,';
          v_strsql := v_strsql || '   T3,';
          v_strsql := v_strsql || '   T4,';
          v_strsql := v_strsql || '   T5,';
          v_strsql := v_strsql || '   T6,';
          v_strsql := v_strsql || '   T7,';
          v_strsql := v_strsql || '   T8,';
          v_strsql := v_strsql || '   T9,';
          v_strsql := v_strsql || '   T10,';
          v_strsql := v_strsql || '   T11,';
          v_strsql := v_strsql || '   T12';
        end if;
        if v_arrChronology(j) = 2 then
          v_strsql := v_strsql || '   T1,';
          v_strsql := v_strsql || '   T2,';
          v_strsql := v_strsql || '   T3,';
          v_strsql := v_strsql || '   T4,';
          v_strsql := v_strsql || '   T5,';
          v_strsql := v_strsql || '   T6,';
          v_strsql := v_strsql || '   T7,';
          v_strsql := v_strsql || '   T8,';
          v_strsql := v_strsql || '   T9,';
          v_strsql := v_strsql || '   T10,';
          v_strsql := v_strsql || '   T11,';
          v_strsql := v_strsql || '   T12,';
          v_strsql := v_strsql || '   T13,';
          v_strsql := v_strsql || '   T14,';
          v_strsql := v_strsql || '   T15,';
          v_strsql := v_strsql || '   T16,';
          v_strsql := v_strsql || '   T17,';
          v_strsql := v_strsql || '   T18,';
          v_strsql := v_strsql || '   T19,';
          v_strsql := v_strsql || '   T20,';
          v_strsql := v_strsql || '   T21,';
          v_strsql := v_strsql || '   T22,';
          v_strsql := v_strsql || '   T23,';
          v_strsql := v_strsql || '   T24,';
          v_strsql := v_strsql || '   T25,';
          v_strsql := v_strsql || '   T26,';
          v_strsql := v_strsql || '   T27,';
          v_strsql := v_strsql || '   T28,';
          v_strsql := v_strsql || '   T29,';
          v_strsql := v_strsql || '   T30,';
          v_strsql := v_strsql || '   T31,';
          v_strsql := v_strsql || '   T32,';
          v_strsql := v_strsql || '   T33,';
          v_strsql := v_strsql || '   T34,';
          v_strsql := v_strsql || '   T35,';
          v_strsql := v_strsql || '   T36,';
          v_strsql := v_strsql || '   T37,';
          v_strsql := v_strsql || '   T38,';
          v_strsql := v_strsql || '   T39,';
          v_strsql := v_strsql || '   T40,';
          v_strsql := v_strsql || '   T41,';
          v_strsql := v_strsql || '   T42,';
          v_strsql := v_strsql || '   T43,';
          v_strsql := v_strsql || '   T44,';
          v_strsql := v_strsql || '   T45,';
          v_strsql := v_strsql || '   T46,';
          v_strsql := v_strsql || '   T47,';
          v_strsql := v_strsql || '   T48,';
          v_strsql := v_strsql || '   T49,';
          v_strsql := v_strsql || '   T50,';
          v_strsql := v_strsql || '   T51,';
          v_strsql := v_strsql || '   T52,';
          v_strsql := v_strsql || '   T53 ';
        end if;
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
        if v_arrChronology(j) = 1 then
          --monthly
          v_strsql := v_strsql || '   n.T1,';
          v_strsql := v_strsql || '   n.T2,';
          v_strsql := v_strsql || '   n.T3,';
          v_strsql := v_strsql || '   n.T4,';
          v_strsql := v_strsql || '   n.T5,';
          v_strsql := v_strsql || '   n.T6,';
          v_strsql := v_strsql || '   n.T7,';
          v_strsql := v_strsql || '   n.T8,';
          v_strsql := v_strsql || '   n.T9,';
          v_strsql := v_strsql || '   n.T10,';
          v_strsql := v_strsql || '   n.T11,';
          v_strsql := v_strsql || '   n.T12 ';
        end if;

        if v_arrChronology(j) = 2 then
          v_strsql := v_strsql || '   n.T1,';
          v_strsql := v_strsql || '   n.T2,';
          v_strsql := v_strsql || '   n.T3,';
          v_strsql := v_strsql || '   n.T4,';
          v_strsql := v_strsql || '   n.T5,';
          v_strsql := v_strsql || '   n.T6,';
          v_strsql := v_strsql || '   n.T7,';
          v_strsql := v_strsql || '   n.T8,';
          v_strsql := v_strsql || '   n.T9,';
          v_strsql := v_strsql || '   n.T10,';
          v_strsql := v_strsql || '   n.T11,';
          v_strsql := v_strsql || '   n.T12,';
          v_strsql := v_strsql || '   n.T13,';
          v_strsql := v_strsql || '   n.T14,';
          v_strsql := v_strsql || '   n.T15,';
          v_strsql := v_strsql || '   n.T16,';
          v_strsql := v_strsql || '   n.T17,';
          v_strsql := v_strsql || '   n.T18,';
          v_strsql := v_strsql || '   n.T19,';
          v_strsql := v_strsql || '   n.T20,';
          v_strsql := v_strsql || '   n.T21,';
          v_strsql := v_strsql || '   n.T22,';
          v_strsql := v_strsql || '   n.T23,';
          v_strsql := v_strsql || '   n.T24,';
          v_strsql := v_strsql || '   n.T25,';
          v_strsql := v_strsql || '   n.T26,';
          v_strsql := v_strsql || '   n.T27,';
          v_strsql := v_strsql || '   n.T28,';
          v_strsql := v_strsql || '   n.T29,';
          v_strsql := v_strsql || '   n.T30,';
          v_strsql := v_strsql || '   n.T31,';
          v_strsql := v_strsql || '   n.T32,';
          v_strsql := v_strsql || '   n.T33,';
          v_strsql := v_strsql || '   n.T34,';
          v_strsql := v_strsql || '   n.T35,';
          v_strsql := v_strsql || '   n.T36,';
          v_strsql := v_strsql || '   n.T37,';
          v_strsql := v_strsql || '   n.T38,';
          v_strsql := v_strsql || '   n.T39,';
          v_strsql := v_strsql || '   n.T40,';
          v_strsql := v_strsql || '   n.T41,';
          v_strsql := v_strsql || '   n.T42,';
          v_strsql := v_strsql || '   n.T43,';
          v_strsql := v_strsql || '   n.T44,';
          v_strsql := v_strsql || '   n.T45,';
          v_strsql := v_strsql || '   n.T46,';
          v_strsql := v_strsql || '   n.T47,';
          v_strsql := v_strsql || '   n.T48,';
          v_strsql := v_strsql || '   n.T49,';
          v_strsql := v_strsql || '   n.T50,';
          v_strsql := v_strsql || '   n.T51,';
          v_strsql := v_strsql || '   n.T52,';
          v_strsql := v_strsql || '   n.T53 ';
        end if;

        v_strsql_from  := ' from ';
        v_strsql_where := ' ';

        if p_nNodeType = 10003 then

          if v_arrChronology(j) = 1 then
            --v_strsql_from := v_strsql_from || ' don_m n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' don_m n ,' ||
                             p_strtemptabname || ' m ';

          elsif v_arrChronology(j) = 2 then
            --v_strsql_from := v_strsql_from || ' don_w n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' don_w n ,' ||
                             p_strtemptabname || ' m ';
          end if;
        elsif p_nNodeType = 10004 then

          if v_arrChronology(j) = 1 then
            --v_strsql_from := v_strsql_from || ' prb_m n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' prb_m n ,' ||
                             p_strtemptabname || ' m ';
            --elsif upper(v4(j)) = 'WEEKLY' then
          elsif v_arrChronology(j) = 2 then
            --v_strsql_from := v_strsql_from || ' prb_w n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' prb_w n ,' ||
                             p_strtemptabname || ' m ';
          end if;
        elsif p_nNodeType = 10005 then
          if v_arrChronology(j) = 1 then
            --v_strsql_from := v_strsql_from || ' bud_m n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' bud_m n ,' ||
                             p_strtemptabname || ' m ';
          elsif v_arrChronology(j) = 2 then
            --v_strsql_from := v_strsql_from || ' bud_w n ,tb_node m ';
            v_strsql_from := v_strsql_from || ' bud_w n ,' ||
                             p_strtemptabname || ' m ';
          end if;
        end if;

        v_strsql_where := v_strsql_where || ' where n.tsid = ' ||
                          v_arrTimeSeriesDBID(j);
        v_strsql_where := v_strsql_where || ' and n.version =  ' ||
                          v_arrVersion(j);
        v_strsql_where := v_strsql_where || ' and n.yy between ' ||
                          v_arrBeginYear(j) || ' and ' || v_arrEndYear(j);

        if p_nNodeType = 10003 then
          v_strsql_where := v_strsql_where || ' and n.pvtid = m.id';
        elsif p_nNodeType = 10004 then
          v_strsql_where := v_strsql_where || ' and n.selid = m.id';
        elsif p_nNodeType = 10005 then
          v_strsql_where := v_strsql_where || ' and n.bdgid = m.id';
        end if;

        v_c_lob := v_strsql || v_strsql_from || v_strsql_where;
        execute immediate v_c_lob;
        commit;

      end loop;
    end if;

    commit;
  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  --create or replace package body p_Selection is
  --SP_GetDetailNodeTSBySelectionID
  procedure SP_GetDetailNodeTSBySelID(P_SelectionID in number,
                                      p_IsDynamic   in number, --1:true,0:false
                                      P_Sequence    in varchar2, --Sort sequence
                                      --p_DetailNode  out sys_refcursor,
                                      p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                      p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                      p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                      p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                      p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                      p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                      p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                      p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                      p_nSQLCode          out integer --return exception code,0 is no error.
                                      ) is
    p_DetailNode     sys_refcursor;
    p_strtemptabname varchar2(30);
    vTabName         varchar2(30);
  begin
    p_Selection.SP_GetDetailNodeBySelectionID(P_SelectionID => P_SelectionID,
                                              p_IsDynamic   => p_IsDynamic, --1:true,0:false
                                              P_Sequence    => P_Sequence, --Sort sequence
                                              p_DetailNode  => p_DetailNode,
                                              pOut_vTabName => vTabName,
                                              p_SqlCode     => p_nSqlCode);

    if p_nSqlCode = 0 then
      p_strtemptabname := 'TB_TS_DetailNodeSel';
      sp_Get_Middle_AllTimeSeries(10003,
                                  p_strtemptabname,
                                  p_arrTimeSeriesDBID,
                                  p_arrVersion,
                                  p_arrChronology,
                                  p_arrBeginYear,
                                  p_arrBeginPeriod,
                                  p_arrEndYear,
                                  p_arrEndPeriod,
                                  p_strTableName, --out varchar2, --temporary table name which is filled by time series data
                                  p_nSQLCode --out integer --return exception code,0 is no error.
                                  );
    end if;

  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetDetailNodeTSByConditions(P_Conditions in varchar2,
                                           P_Sequence   in varchar2, --Sort sequence
                                           --p_DetailNode out sys_refcursor,
                                           p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                           p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                           p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                           p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                           p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                           p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                           p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                           p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                           p_nSQLCode          out integer --return exception code,0 is no error.
                                           ) is
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
      p_strtemptabname := 'TB_TS_DetailNodeCondition';
      sp_Get_Middle_AllTimeSeries(10003,
                                  p_strtemptabname,
                                  p_arrTimeSeriesDBID,
                                  p_arrVersion,
                                  p_arrChronology,
                                  p_arrBeginYear,
                                  p_arrBeginPeriod,
                                  p_arrEndYear,
                                  p_arrEndPeriod,
                                  p_strTableName, --out varchar2, --temporary table name which is filled by time series data
                                  p_nSQLCode --out integer --return exception code,0 is no error.
                                  );
    end if;

  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetDetailNodeTSBySelCdt(P_SelectionID in number,
                                       P_Conditions  in varchar2,
                                       P_Sequence    in varchar2, --Sort sequence
                                       --p_DetailNode  out sys_refcursor,
                                       p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                       p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                       p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                       p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                       p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                       p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                       p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                       p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                       p_nSQLCode          out integer --return exception code,0 is no error.
                                       ) is
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
      p_strtemptabname := 'TB_TS_DetailNodeSelCdt';
      sp_Get_Middle_AllTimeSeries(10003,
                                  p_strtemptabname,
                                  p_arrTimeSeriesDBID,
                                  p_arrVersion,
                                  p_arrChronology,
                                  p_arrBeginYear,
                                  p_arrBeginPeriod,
                                  p_arrEndYear,
                                  p_arrEndPeriod,
                                  p_strTableName, --out varchar2, --temporary table name which is filled by time series data
                                  p_nSQLCode --out integer --return exception code,0 is no error.
                                  );
    end if;

  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  --create or replace package body p_Aggregation is
  procedure SP_GetAggregateNodesTS(P_AggregateRuleID in number,
                                   P_Sequence        in varchar2, --Sort sequence
                                   --p_AggregateNode   out sys_refcursor,
                                   p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                   p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                   p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                   p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                   p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                   p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                   p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                   p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                   p_nSQLCode          out integer --return exception code,0 is no error.
                                   ) is
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

    /*create global temporary table TB_TS_DetailNodeSel(ID number) on commit preserve rows;
    create global temporary table TB_TS_DetailNodeCondition(ID number) on commit preserve rows;
    create global temporary table TB_TS_DetailNodeSelCdt(ID number) on commit preserve rows;

    create global temporary table TB_TS_AggregateNode(ID number) on commit preserve rows;
    create global temporary table TB_TS_AggregateNodeCondition(ID number) on commit preserve rows;*/

    p_nNodeType := 10004;
    p_Aggregation.SP_GetAggregateNodes(P_AggregateRuleID => P_AggregateRuleID,
                                       P_Sequence        => P_Sequence,
                                       p_AggregateNode   => p_AggregateNode,
                                       pOut_vTabName     => vTabname,
                                       p_SqlCode         => p_nSqlCode);
    if p_nSqlCode = 0 then
      p_strtemptabname := 'TB_TS_AggregateNode';
      sp_Get_Middle_AllTimeSeries(10004,
                                  p_strtemptabname,
                                  p_arrTimeSeriesDBID,
                                  p_arrVersion,
                                  p_arrChronology,
                                  p_arrBeginYear,
                                  p_arrBeginPeriod,
                                  p_arrEndYear,
                                  p_arrEndPeriod,
                                  p_strTableName, --out varchar2, --temporary table name which is filled by time series data
                                  p_nSQLCode --out integer --return exception code,0 is no error.
                                  );
    end if;

  exception
    when others then
      p_nSQLCode := sqlcode;
      rollback;
  end;

  procedure SP_GetAggNodesTSByConditions(P_AggregateRuleID in number,
                                         P_Conditions      in varchar2,
                                         P_Sequence        in varchar2, --Sort sequence
                                         --p_AggregateNode   out nocopy sys_refcursor,
                                         p_arrTimeSeriesDBID in clob, --time series id, separated by ??,?㏒ˋ㏒∟
                                         p_arrVersion        in clob, -- time series version, separated by ??,?㏒ˋ㏒∟
                                         p_arrChronology     in clob, -- 1: monthly, 2: weekly, 3: daily, separated by ??,?㏒ˋ㏒∟
                                         p_arrBeginYear      in clob, -- begin year such as??2011?㏒ˋ㏒∟, separated by ??,?㏒ˋ㏒∟
                                         p_arrBeginPeriod    in clob, -- begin period such as??10?㏒ˋ“∟, separated by ??,?㏒ˋ㏒∟
                                         p_arrEndYear        in clob, -- end year such as??2012?㏒ˋ㏒∟,separated by ??,?㏒ˋ㏒∟
                                         p_arrEndPeriod      in clob, -- end period such as??12?㏒ˋ“∟,separated by ??,?㏒ˋ㏒∟
                                         p_strTableName      out varchar2, --temporary table name which is filled by time series data
                                         p_nSQLCode          out integer --return exception code,0 is no error.
                                         ) is
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
      p_strtemptabname := 'TB_TS_AggregateNodeCon';
      sp_Get_Middle_AllTimeSeries(10004,
                                  p_strtemptabname,
                                  p_arrTimeSeriesDBID,
                                  p_arrVersion,
                                  p_arrChronology,
                                  p_arrBeginYear,
                                  p_arrBeginPeriod,
                                  p_arrEndYear,
                                  p_arrEndPeriod,
                                  p_strTableName, --out varchar2, --temporary table name which is filled by time series data
                                  p_nSQLCode --out integer --return exception code,0 is no error.
                                  );
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
