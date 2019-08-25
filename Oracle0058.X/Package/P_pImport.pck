create or replace package P_pImport authid current_user is

  g_nNodetype number;
  --pImport save time series
  procedure sp_PimportSaveTimeseries(p_Type      in number, --1 Month ,2 Week,3 Day
                                     p_Period    in number,
                                     P_TableName in varchar2,
                                     p_SqlCode   out number);

  --create a temporary table save time series
  procedure sp_CreateSaveTSTable(p_Period    in number,
                                 p_TableName out varchar2,
                                 p_SqlCode   out number);

  --Generate one Period detail node or Aggregate Node time series
  procedure FMSP_SaveOnePeriodTS(pIn_nType      in number, --1 Month ,2 Week,3 Day
                                 PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                 PIn_VTableName in varchar2,
                                 pOut_nSqlCode  out number);

  --Generate detail node or Aggregate Node time series
  procedure sp_SaveTimeSeries(p_Type      in number, --1 Month ,2 Week,3 Day
                              P_NodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                              P_TableName in varchar2,
                              p_SqlCode   out number);

  --Generate SQL about Save  one Period time series
  procedure FMSP_SaveOnePeriodTStoSQL(pIn_nType      in number, --1 Monthly ,2 Weekly,3 Dayly
                                      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                      pIn_VTableName in varchar2,
                                      PIn_nPeriod    in int,
                                      pOut_VStrSql   out varchar2,
                                      pOut_nSqlCode  out number);

  --Generate SQL about Save time series
  procedure SP_SaveTStoSQL(p_Type        in number, --1 Monthly ,2 Weekly,3 Dayly
                           P_NodeType    in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                           p_TableName   in varchar2,
                           p_YY          in number,
                           p_MBegin      in number,
                           p_TBegin      in number,
                           p_Number      in number,
                           P_BeginYY     in int,
                           P_BeginPeriod in int,
                           P_EndYY       in int,
                           P_EndPeriod   in int,
                           p_StrSql      out varchar2,
                           p_SqlCode     out number);

  --pImport sp_pImportTimeseries
  procedure sp_pImportTimeseries(p_Type      in number, --1 Month ,2 Week,3 Day
                                 p_TSType    in number,
                                 P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                                 P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                                 P_Desc      in varchar2,
                                 P_FMUSER    in varchar2,
                                 P_strUnit   in varchar2, --set Unit
                                 P_StrOption in varchar2, --## as separator
                                 P_TableName in varchar2,
                                 p_period    in number,
                                 p_BeginData in number,
                                 p_EndData   in number,
                                 P_version   in number,
                                 p_SqlCode   out number);
  --create a temporary table
  procedure sp_CreateTSTable(p_Period    in number,
                             p_TableName out varchar2,
                             p_SqlCode   out number);

  --Option handling
  procedure SP_OptionHandle(p_Type      in number default 1, --1 Month ,2 Week,3 Day
                            P_StrOption in varchar2,
                            P_TableName in varchar2,
                            p_period    in number,
                            P_strUnit   in varchar2, --set Unit
                            p_version   in out number,
                            p_SqlCode   out number);

  --Processing of the temporary table
  procedure SP_TSTableHandle(p_Type      in number, --1 Month ,2 Week,3 Day
                             P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                             P_TableName in varchar2,
                             p_Period    in number,
                             p_BeginData in number,
                             p_EndData   in number,
                             p_SqlCode   out number);

  --Dimension to the field and field where
  procedure sp_DimensionToField(P_TableName in varchar2,
                                -- P_Field     out varchar2,
                                P_Fieldpvt out varchar2,
                                P_PCount   out number,
                                P_SCount   out number,
                                P_TCount   out number,
                                p_SqlCode  out number);

  --Generate basic data
  procedure sp_initial_datas(P_TableName in varchar2,
                             P_FMUSER    in varchar2,
                             P_Desc      in varchar2,
                             P_PCount    in number,
                             P_SCount    in number,
                             P_TCount    in number,
                             p_SqlCode   out number);

  --Generate detail node
  procedure sp_DetailNode(P_TableName in varchar2,
                          P_FMUSER    in varchar2,
                          P_Fieldpvt  in varchar2,
                          P_field     in varchar2,
                          p_SqlCode   out number);

  --update Temporary table pvtID from node ID
  procedure sp_NodeIDToTable(P_TableName in varchar2,
                             P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                             P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                             P_Fieldpvt  in varchar2,
                             p_SqlCode   out number);

  --Time series and details node or Aggregate Node  to establish a connection
  procedure sp_TimeseriestoNode(p_TSType    in number,
                                P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                                P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                                P_TableName in varchar2,
                                P_BeginYY   in number,
                                P_BeginMM   in number,
                                P_EndYY     in number,
                                P_EndMM     in number,
                                p_version   in number,
                                p_SqlCode   out number);

  procedure FMSP_PimportOnePeriodTS(pIn_nType      in number, --1 Month ,2 Week,3 Day
                                    PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                    pIn_nTSType    in number,
                                    pIn_nversion   in number,
                                    PIn_VTableName in varchar2,
                                    pOut_nSqlCode  out number);

  --Generate SQL about Save  one Period time series
  procedure FMSP_PimportOnePeriodTStoSQL(pIn_nType      in number, --1 Monthly ,2 Weekly,3 Dayly
                                         PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                         pIn_nTSType    in number,
                                         pIn_nversion   in number,
                                         pIn_VTableName in varchar2,
                                         PIn_nPeriod    in int,
                                         pOut_VStrSql   out varchar2,
                                         pOut_nSqlCode  out number);

  --Generate detail node and Aggregate Node time series
  procedure sp_timeseries(p_Type      in number, --1 Month ,2 Week,3 Day
                          p_TSType    in number,
                          P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                          P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                          P_TableName in varchar2,
                          p_period    in number,
                          p_BeginData in number,
                          p_EndData   in number,
                          p_version   in number,
                          p_SqlCode   out number);

  --Generate SQL about detail node time series
  procedure SP_TSRangetoSQL(p_Type      in number, --1 Month ,2 Week,3 Day
                            p_TSType    in number,
                            p_version   in number,
                            p_TableName in varchar2,
                            P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                            P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                            p_YY        in number,
                            p_MBegin    in number,
                            p_TBegin    in number,
                            p_Number    in number,
                            P_Enddata   in number,
                            p_StrSql    out varchar2,
                            p_SqlCode   out number);

  --sum DetailNode timeseries to AggregateNode timeseries
  procedure sp_sum_DetailTStoAggTS(p_TSType  in number,
                                   p_SqlCode out number);

  --Null value is replaced by 1E-20
  procedure sp_TSNullreplace( --p_TSType   in number,
                             P_NodeType in number, --1  Detail Node  2  Aggregate Node
                             P_ifaddbdg in number, --0 not add bdg, 1 add bdg
                             p_SqlCode  out number);

  --delete is null time series
  procedure sp_Del_Nulltimeseries(pIn_nType     in number, --1 Monthly ,2 Weekly,3 Dayly
                                  PIn_nNodeType in number, --1  Detail Node  2  Aggregate Node
                                  p_SqlCode     out number);

  --delete temporary table
  procedure sp_DropTSTable(P_TableName in varchar2, p_SqlCode out number);

end P_pImport;
/
create or replace package body P_pImport is

  --pImport sp_PimportSaveTimeseries
  procedure sp_PimportSaveTimeseries(p_Type      in number, --1 Month ,2 Week,3 Day
                                     p_Period    in number,
                                     P_TableName in varchar2,
                                     p_SqlCode   out number) as
    v_strsql varchar2(8000);
  
    v_ISPVT int;
    v_ISAgg int;
  
  begin
    p_SqlCode := 0;
  
    v_ISPVT := 0;
    v_ISAgg := 0;
  
    --add log
    Fmp_Log.FMP_SetValue(p_Type);
    Fmp_Log.FMP_SetValue(p_Period);
    Fmp_Log.FMP_SetValue(P_TableName);
    Fmp_Log.LOGBEGIN;
    if p_SqlCode <> 0 then
      return;
    end if;
  
    --Delete duplicate records
    v_strsql := 'delete from ' || P_TableName || ' a
      where rowid <
      (
      select max(rowid) from ' || P_TableName || ' b
      where a.NodeType=b.NodeType and a.PVTID=b.PVTID and a.TSType=b.TSType
       and a.version=b.version and a.BeginYY=b.BeginYY and a.Beginperiod=b.Beginperiod
       and a.EndYY=b.EndYY and a.Endperiod=b.Endperiod
      )';
  
    execute immediate v_strsql;
    commit;
  
    --edit bdg data to pvt
    v_strsql := 'update (select t.pvtid, p.pvt_em_addr, t.nodetype, 1 type
      from bdg b, pvt p, ' || P_TableName || ' t
      where b.b_cle = p.pvt_cle and b.ID_BDG=80
      and b.bdg_em_addr = t.pvtid
      and t.nodetype = 3) m
      set m.pvtid = m.pvt_em_addr,m.nodetype=m.type';
    execute immediate v_strsql;
    commit;
  
    --edit bdg data to sel
    v_strsql := 'update (select t.pvtid,s.sel_em_addr , t.nodetype, 2 type
      from bdg b, sel s, ' || P_TableName || ' t
      where b.b_cle = s.sel_cle and b.ID_BDG=71
      and b.bdg_em_addr = t.pvtid
      and t.nodetype = 3) m
      set m.pvtid = m.sel_em_addr,m.nodetype=m.type';
    execute immediate v_strsql;
    commit;
  
    v_strsql := ' select
      sum(case nodetype when 1 then 1 else 0 end) v_ISPVT
      ,sum(case nodetype when 2 then 1 else 0 end) v_ISAgg
       from ' || P_TableName;
    execute immediate v_strsql
      into v_ISPVT, v_ISAgg;
  
    --=========================     Detail Node    ===========================================================
    if v_ISPVT > 0 then
      -- Detail Node
      if p_Period = 1 then
        FMSP_SaveOnePeriodTS(pIn_nType      => p_Type, --1 Month ,2 Week,3 Day
                             PIn_nNodeType  => 1, --1  Detail Node , 2  Aggregate Node ,3 bdg
                             PIn_VTableName => P_TableName,
                             pOut_nSqlCode  => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      else
        sp_SaveTimeSeries(p_Type      => p_Type, --1 Month ,2 Week,3 Day
                          P_NodeType  => 1, --1  Detail Node , 2  Aggregate Node ,3 bdg
                          p_TableName => P_TableName,
                          p_SqlCode   => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      end if;
    end if;
  
    --=========================     Aggregate Node    ===========================================================
    if v_ISAgg > 0 then
      -- Aggregate Node
      if p_Period = 1 then
        FMSP_SaveOnePeriodTS(pIn_nType      => p_Type, --1 Month ,2 Week,3 Day
                             PIn_nNodeType  => 1, --1  Detail Node , 2  Aggregate Node ,3 bdg
                             PIn_VTableName => P_TableName,
                             pOut_nSqlCode  => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      else
        sp_SaveTimeSeries(p_Type      => p_Type, --1 Month ,2 Week,3 Day
                          P_NodeType  => 2, --1  Detail Node , 2  Aggregate Node ,3 bdg
                          p_TableName => P_TableName,
                          p_SqlCode   => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      end if;
    end if;
  
    --add log
    Fmp_Log.LOGEND;
    if p_SqlCode <> 0 then
      return;
    end if;
  
  exception
    when others then
      p_SqlCode := sqlcode;
      --add log
      Fmp_Log.LOGERROR;
      if p_SqlCode <> 0 then
        return;
      end if;
  end;

  --create a temporary table save time series
  procedure sp_CreateSaveTSTable(p_Period    in number,
                                 p_TableName out varchar2,
                                 p_SqlCode   out number) as
  
    v_strsql clob; --varchar2(8000);
    i        integer;
  begin
  
    p_SqlCode := 0;
  
    /* create sequence seq_tb_pimport
    minvalue 1
    maxvalue 9999999999
    start with 1
    increment by 1
    order;*/
  
    --select seq_tb_pimport.Nextval into p_TableName from dual;
    p_TableName := fmf_gettmptablename(); -- 'TB' || p_TableName;
  
    v_strsql := 'CREATE TABLE ' || p_TableName || '(
      NodeType int ,
      PVTID int ,
      TSType int ,
      version int ,
      BeginYY int,
      Beginperiod int,
      EndYY int,
      Endperiod int
        ';
  
    for i in 1 .. p_Period loop
      v_strsql := v_strsql || ',T_' || i || ' NUMBER';
    end loop;
    v_strsql := v_strsql || ') nologging';
  
    execute immediate v_strsql;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  procedure FMSP_SaveOnePeriodTS(pIn_nType      in number, --1 Month ,2 Week,3 Day
                                 PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                 PIn_VTableName in varchar2,
                                 pOut_nSqlCode  out number)
  --*****************************************************************
    -- Description: Generate  one Period detail node or Aggregate Node time series
    --
    -- Parameters:
    --      pIn_nType      in number, --1 Month ,2 Week,3 Day
    --      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
    --      PIn_VTableName in varchar2,
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-12-2012     wfq           Created.
    -- **************************************************************
  
   as
    VStrSql varchar2(8000);
    vsql    varchar2(8000);
    iPeriod int;
  
    tc_DataYM sys_refcursor;
  
  begin
    pOut_nSqlCode := 0;
  
    --insert  or update one period Time series
  
    vsql := 'select distinct beginperiod  from ' || PIn_VTableName;
  
    open tc_DataYM for vsql;
    loop
      fetch tc_DataYM
        into iPeriod;
    
      exit when tc_DataYM%notfound;
    
      --Dealing with the first data
    
      FMSP_SaveOnePeriodTStoSQL(pIn_nType      => pIn_nType, --1 Month ,2 Week,3 Day
                                PIn_nNodeType  => PIn_nNodeType, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                PIn_VTableName => PIn_VTableName,
                                PIn_nperiod    => iPeriod,
                                pOut_vStrSql   => VStrSql,
                                pOut_nSqlCode  => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    
      execute immediate VStrSql;
      commit;
    
    end loop;
  
    close tc_DataYM;
  
  exception
    when others then
      pOut_nSqlCode := sqlcode;
    
  end;

  procedure FMSP_SaveTStoSQL(pIn_nType      in number,
                             PIn_nNodeType  in number,
                             pIn_vTableName in varchar2,
                             pIn_nMBegin    in number,
                             pIn_nTBegin    in number,
                             pIn_nNumber    in number,
                             pOut_nSqlCode  out number) as
  
    v_Updatesql varchar2(8000);
    v_strM      varchar2(2000);
    v_strT      varchar2(2000);
    M           int;
    T           int;
    --v_File      varchar2(50);
    v_TBName varchar2(50);
    V_ID     varchar2(50);
  
    v_Type varchar2(2);
  
    cStrSql         clob;
    vWhereIsnotnull clob := ' where 1=1 ';
    vWhereIsnull    clob := ' where 1=1 ';
  
  begin
    M             := pIn_nMBegin;
    T             := pIn_nTBegin;
    pOut_nsqlcode := 0;
  
    if pIn_nType = 1 then
      v_Type := 'M';
      --2 Week
    elsif pIn_nType = 2 then
      v_Type := 'W';
    else
      v_Type := 'D';
    end if;
  
    if pIn_nNodeType = 1 then
      --1  Detail Node
      v_TBName := 'DON_' || v_Type;
      V_ID     := 'PVTID';
    elsif pIn_nNodeType = 2 then
      --2  Aggregate Node
      v_TBName := 'prb_' || v_Type;
      V_ID     := 'SELID';
    end if;
  
    for i in 1 .. pIn_nNumber loop
      vWhereIsnotnull := vWhereIsnotnull || ' or n.T_' || T ||
                         ' is not null';
      vWhereIsnull    := vWhereIsnull || ' and n.T_' || T || ' is  null';
    
      v_strM      := v_strM || ',T' || M;
      v_strT      := v_strT || ',T_' || T;
      v_Updatesql := v_Updatesql || ',T' || M || '= T_' || T;
      M           := M + 1;
      T           := T + 1;
    end loop;
  
    v_Updatesql := substr(v_Updatesql, 2);
  
    cStrSql := '
          MERGE /*+use_hash(d,n)*/  INTO ' || v_TBName || ' d
          USING (select pvtID,TSID,Version, YY' || v_strT ||
               ' from ' || pIn_vTableName || ') n
             ON (d.YY = n.YY and d.' || V_ID ||
               '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    cStrSql := cStrSql || v_Updatesql || ' delete ' || vWhereIsnull;
    cStrSql := cStrSql || '
           WHEN NOT MATCHED THEN
            INSERT (' || v_TBName || 'ID,' || V_ID ||
               ',TSID,Version,YY' || v_strM || ')
            VALUES (seq_' || v_TBName ||
               '.nextval,n.pvtID,n.TSID,n.Version,n.YY' || v_strT || ')' ||
               vWhereIsnotnull;
    fmsp_execsql(pIn_cSql => cStrSql);
  exception
    when others then
      pOut_nSqlCode := sqlcode;
  end;

  --Generate detail node or Aggregate Node time series
  procedure sp_SaveTimeSeries(p_Type      in number, --1 Month ,2 Week,3 Day
                              P_NodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                              P_TableName in varchar2,
                              p_SqlCode   out number) as
    v_strsql  varchar2(8000);
    v_sql     varchar2(8000);
    v_BeginYY int;
    v_BeginMM int;
    v_EndYY   int;
    v_EndMM   int;
    --v_MMabs   int;
    k  int;
    YY number;
  
    v_Number  int;
    v_Cycle   int;
    v_period  int;
    tc_DataYM sys_refcursor;
  
    cSqlMidTableText clob;
    vNewMidTablename varchar2(30);
    nMidFlag         number := 0;
  begin
    p_SqlCode := 0;
    fmp_log.FMP_SetValue(pIn_vVar2 => p_Type);
    fmp_log.FMP_SetValue(pIn_vVar2 => P_NodeType);
    fmp_log.FMP_SetValue(pIn_vVar2 => P_TableName);
    fmp_log.LOGBEGIN;
  
    if p_Type = 1 then
      v_Cycle := 12;
    elsif p_Type = 2 then
      v_Cycle := 52;
    end if;
  
    v_sql := 'select distinct beginYY,beginperiod,endYY,Endperiod from ' ||
             P_TableName;
    k     := 1;
    open tc_DataYM for v_sql;
    loop
      fetch tc_DataYM
        into v_BeginYY, v_BeginMM, v_EndYY, v_EndMM;
    
      exit when tc_DataYM%notfound;
    
      v_period := (v_EndYY * v_Cycle + v_EndMM) -
                  (v_BeginYY * v_Cycle + v_BeginMM) + 1;
    
      k := 1;
    
      if v_BeginYY = v_EndYY then
        v_Number := v_EndMM - v_BeginMM + 1;
      elsif v_period < v_Cycle - v_BeginMM + 1 then
        v_Number := v_period + 1;
      else
        v_Number := v_Cycle - v_BeginMM + 1;
      end if;
    
      SP_SaveTStoSQL(p_Type        => p_Type, --1 Month ,2 Week,3 Day
                     P_NodeType    => P_NodeType, --1  Detail Node , 2  Aggregate Node ,3 bdg
                     p_TableName   => P_TableName,
                     p_YY          => v_BeginYY,
                     p_MBegin      => v_BeginMM,
                     p_TBegin      => K,
                     p_Number      => v_Number,
                     P_BeginYY     => v_BeginYY,
                     P_BeginPeriod => v_BeginMM,
                     P_EndYY       => V_endYY,
                     P_EndPeriod   => v_EndMM,
                     p_StrSql      => v_strsql,
                     p_SqlCode     => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    
      execute immediate v_strsql;
      commit;
    
      k := k + v_Cycle - v_BeginMM + 1;
      for YY in v_BeginYY + 1 .. v_EndYY loop
      
        if YY <> v_EndYY then
          --Dealing with the intermediate data
          nMidFlag := 1;
        
          cSqlMidTableText := cSqlMidTableText ||
                              ' union all select pvtID,TSTYPE TSID,Version,' || yy || ',';
          for tt in 0 .. v_cycle - 1 loop
            if tt = v_cycle - 1 then
              cSqlMidTableText := cSqlMidTableText || ' t_' ||
                                  to_char(tt + k);
            else
              cSqlMidTableText := cSqlMidTableText || ' t_' ||
                                  to_char(tt + k) || ',';
            end if;
          end loop;
          cSqlMidTableText := cSqlMidTableText || ' from  ' || P_TableName ||
                              ' where NodeType=' || P_NodeType ||
                              ' and BeginYY=' || v_BeginYY ||
                              ' and BeginPeriod=' || v_BeginMM ||
                              ' and EndYY=' || V_endYY || ' and EndPeriod=' ||
                              v_EndMM;
        
          k := k + v_Cycle;
        else
          --Dealing with the last data
          SP_SaveTStoSQL(p_Type        => p_Type, --1 Month ,2 Week,3 Day
                         P_NodeType    => P_NodeType, --1  Detail Node , 2  Aggregate Node ,3 bdg
                         p_TableName   => P_TableName,
                         p_YY          => YY,
                         p_MBegin      => 1,
                         p_TBegin      => K,
                         p_Number      => v_EndMM,
                         P_BeginYY     => v_BeginYY,
                         P_BeginPeriod => v_BeginMM,
                         P_EndYY       => V_endYY,
                         P_EndPeriod   => v_EndMM,
                         p_StrSql      => v_strsql,
                         p_SqlCode     => p_SqlCode);
          if p_SqlCode <> 0 then
            return;
          end if;
        
          execute immediate v_strsql;
          commit;
        
        end if;
      end loop;
    
      if nMidFlag = 1 then
        vNewMidTablename := fmf_gettmptablename();
        cSqlMidTableText := ' from dual where 1=2 ' || cSqlMidTableText;
        --add columns in the front
        for m in reverse 1 .. v_Cycle loop
          cSqlMidTableText := ', null t_' || m || cSqlMidTableText;
        end loop;
      
        cSqlMidTableText := 'create table ' || vNewMidTablename ||
                            ' as select null pvtID,null TSID,null Version,null yy' ||
                            cSqlMidTableText;
        fmsp_execsql(cSqlMidTableText);
        --reset the value to default
        cSqlMidTableText := '';
        nMidFlag         := 0;
      
        FMSP_SaveTStoSQL(pIn_nType      => p_Type,
                         PIn_nNodeType  => P_NodeType,
                         pIn_vTableName => vNewMidTablename,
                         pIn_nMBegin    => 1,
                         pIn_nTBegin    => 1,
                         pIn_nNumber    => v_Cycle,
                         pOut_nSqlCode  => p_SqlCode);
      end if;
    
    end loop;
  
    close tc_DataYM;
    fmp_log.LOGEND;
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  --Generate SQL about Save  one Period time series
  procedure FMSP_SaveOnePeriodTStoSQL(pIn_nType      in number, --1 Monthly ,2 Weekly,3 Dayly
                                      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                      pIn_VTableName in varchar2,
                                      PIn_nPeriod    in int,
                                      pOut_VStrSql   out varchar2,
                                      pOut_nSqlCode  out number)
  --*****************************************************************
    -- Description: Construct the SQL statement to timeseries (insert and update)
    -- Parameters:
    --      pIn_nType      in number, --1 Month ,2 Week,3 Day
    --      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
    --      PIn_VTableName in varchar2,
    --      PIn_nPeriod    in int
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-12-2012     wfq           Created.
    -- **************************************************************
   as
  
    VUpdatesql varchar2(8000);
    vstrM      varchar2(2000);
    vTBName    varchar2(50);
    VID        varchar2(50);
  
    vType varchar2(2);
  begin
  
    pOut_nSqlCode := 0;
  
    if pIn_nType = 1 then
      vType := 'M';
      --2 Week
    elsif pIn_nType = 2 then
      vType := 'W';
    else
      vType := 'D';
    end if;
  
    if PIn_nNodeType = 1 then
      --1  Detail Node
      vTBName := 'DON_' || vType;
      VID     := 'PVTID';
    elsif PIn_nNodeType = 2 then
      --2  Aggregate Node
      vTBName := 'prb_' || vType;
      VID     := 'SELID';
    
    elsif PIn_nNodeType = 3 then
      --3  bdg
      vTBName := 'bud_' || vType;
      VID     := 'bdgID';
    end if;
  
    vstrM      := 'T' || PIn_nPeriod;
    VUpdatesql := 'T' || PIn_nPeriod || '= T_1';
  
    pOut_VStrSql := '
          MERGE /*+ parallel*/ INTO ' || vTBName || ' d
          USING (select pvtID,TSTYPE TSID,Version,Beginyy as YY,T_1 from ' ||
                    pIn_VTableName || ' where NodeType=' || PIn_nNodeType ||
                    ' and BeginPeriod=' || PIn_nPeriod ||
                    ' ) n
             ON (d.YY = n.YY and d.' || VID ||
                    '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    pOut_VStrSql := pOut_VStrSql || VUpdatesql;
    pOut_VStrSql := pOut_VStrSql || '
           WHEN NOT MATCHED THEN
           INSERT  (' || vTBName || 'ID,' || VID ||
                    ',TSID,Version,YY,' || vstrM || ')
            VALUES (seq_' || vTBName ||
                    '.nextval,n.pvtID,n.TSID,n.Version,n.YY,n.T_1)';
  
  exception
    when others then
      pOut_nSqlCode := sqlcode;
    
  end;

  --Generate SQL about Save time series
  procedure SP_SaveTStoSQL(p_Type        in number, --1 Monthly ,2 Weekly,3 Dayly
                           P_NodeType    in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                           p_TableName   in varchar2,
                           p_YY          in number,
                           p_MBegin      in number,
                           p_TBegin      in number,
                           p_Number      in number,
                           P_BeginYY     in int,
                           P_BeginPeriod in int,
                           P_EndYY       in int,
                           P_EndPeriod   in int,
                           p_StrSql      out varchar2,
                           p_SqlCode     out number) as
    /*Construct the SQL statement to timeseries (insert and update)*/
  
    v_Updatesql varchar2(8000);
    v_strM      varchar2(2000);
    v_strT      varchar2(2000);
    M           int;
    T           int;
    --v_File      varchar2(50);
    v_TBName varchar2(50);
    V_ID     varchar2(50);
  
    v_Type varchar2(2);
  
    vWhereIsnotnull clob := ' where 1=1 ';
    vWhereIsnull    clob := ' where 1=1 ';
  
  begin
    M         := p_MBegin;
    T         := p_TBegin;
    p_sqlcode := 0;
  
    if p_Type = 1 then
      v_Type := 'M';
      --2 Week
    elsif p_Type = 2 then
      v_Type := 'W';
    end if;
  
    -- v_File := v_Type || '_';
  
    if P_NodeType = 1 then
      --1  Detail Node
      v_TBName := 'DON_' || v_Type;
      V_ID     := 'PVTID';
    elsif P_NodeType = 2 then
      --2  Aggregate Node
      v_TBName := 'prb_' || v_Type;
      V_ID     := 'SELID';
      /*
      elsif P_NodeType = 3 then
        --3  bdg
        v_TBName := 'bud_' || v_Type;
        V_ID     := 'bdgID';*/
    end if;
  
    for i in 1 .. p_Number loop
      -- vWhereIsnotnull := vWhereIsnotnull || ' or n.T_' || T ||' is not null';
      -- vWhereIsnull    := vWhereIsnull || ' and n.T_' || T || ' is  null';
    
      v_strM      := v_strM || ',T' || M;
      v_strT      := v_strT || ',T_' || T;
      v_Updatesql := v_Updatesql || ',T' || M || '= T_' || T;
      M           := M + 1;
      T           := T + 1;
    end loop;
  
    v_Updatesql := substr(v_Updatesql, 2);
  
    p_StrSql := '
          MERGE /*+use_hash(d,n)*/  INTO ' || v_TBName || ' d
          USING (select pvtID,TSTYPE TSID,Version,' || p_YY ||
                ' as YY' || v_strT || ' from ' || P_TableName ||
                ' where NodeType=' || P_NodeType || ' and BeginYY=' ||
                P_BeginYY || ' and BeginPeriod=' || P_BeginPeriod ||
                ' and EndYY=' || P_EndYY || ' and EndPeriod=' ||
                P_EndPeriod || ' ) n
             ON (d.YY = n.YY and d.' || V_ID ||
                '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    p_StrSql := p_StrSql || v_Updatesql; --|| ' delete ' || vWhereIsnull;
    p_StrSql := p_StrSql || '
           WHEN NOT MATCHED THEN
            INSERT (' || v_TBName || 'ID,' || V_ID ||
                ',TSID,Version,YY' || v_strM || ')
            VALUES (seq_' || v_TBName ||
                '.nextval,n.pvtID,n.TSID,n.Version,n.YY' || v_strT || ')'; -- ||vWhereIsnotnull;
  
  exception
    when others then
      p_SqlCode := sqlcode;
  end;

  --pImport sp_pImportTimeseries
  procedure sp_pImportTimeseries(p_Type      in number, --1 Month ,2 Week,3 Day
                                 p_TSType    in number, --Time Series TypeID
                                 P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                                 P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                                 P_Desc      in varchar2, --Description
                                 P_FMUSER    in varchar2, --User name
                                 P_strUnit   in varchar2, --set Unit
                                 P_StrOption in varchar2, --## as separator
                                 P_TableName in varchar2, --cteate table Name
                                 p_period    in number, --period
                                 p_BeginData in number, -- Begin data
                                 p_EndData   in number, -- End Data
                                 P_version   in number, --version of Time Series TypeID
                                 p_SqlCode   out number) as
    pragma autonomous_transaction;
    v_version   number;
    P_Field     varchar2(500);
    P_Fieldpvt  varchar2(1000);
    P_PCount    number;
    P_SCount    number;
    P_TCount    number;
    tSwitches   FMP_BATCH.g_FMRT_Switches;
    vTimeSeries varchar2(2000);
  begin
    p_SqlCode := 0;
    v_version := P_version;
    Fmp_Log.FMP_SetValue(p_Type);
    Fmp_Log.FMP_SetValue(p_TSType);
    Fmp_Log.FMP_SetValue(P_NodeType);
    Fmp_Log.FMP_SetValue(P_ifaddbdg);
    Fmp_Log.FMP_SetValue(P_Desc);
    Fmp_Log.FMP_SetValue(P_FMUSER);
    Fmp_Log.FMP_SetValue(P_strUnit);
    Fmp_Log.FMP_SetValue(P_StrOption);
    Fmp_Log.FMP_SetValue(P_TableName);
    Fmp_Log.FMP_SetValue(p_period);
    Fmp_Log.FMP_SetValue(p_BeginData);
    Fmp_Log.FMP_SetValue(p_EndData);
    Fmp_Log.FMP_SetValue(P_version);
    Fmp_Log.LOGBEGIN;
    g_nNodetype := P_NodeType;
    fmsp_execsql('alter session enable parallel dml');
    --Option handling
    SP_OptionHandle(p_Type      => p_Type,
                    P_StrOption => P_StrOption,
                    p_TableName => P_TableName,
                    p_period    => p_period,
                    P_strUnit   => P_strUnit,
                    P_version   => v_version,
                    p_SqlCode   => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
  
    --Processing of the temporary table
    SP_TSTableHandle(p_Type      => p_Type,
                     P_NodeType  => P_NodeType,
                     p_TableName => P_TableName,
                     p_period    => p_period,
                     p_BeginData => p_BeginData,
                     p_EndData   => p_EndData,
                     p_SqlCode   => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
  
    --Dimension to the field and field where
    sp_DimensionToField(p_TableName => P_TableName,
                        -- P_Field     => P_Field,
                        P_Fieldpvt => P_Fieldpvt,
                        P_PCount   => P_PCount,
                        P_SCount   => P_SCount,
                        P_TCount   => P_TCount,
                        p_SqlCode  => p_SqlCode);
  
    if p_SqlCode <> 0 then
      return;
    end if;
  
    if P_Fieldpvt is not null then
      --1  Detail Node
      if P_NodeType = 1 then
        --Generate the basic data
        sp_initial_datas(p_TableName => P_TableName,
                         P_FMUSER    => P_FMUSER,
                         P_Desc      => P_Desc,
                         P_PCount    => P_PCount,
                         P_SCount    => P_SCount,
                         P_TCount    => P_TCount,
                         p_SqlCode   => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      
        --Generate the Detail Node data
        sp_DetailNode(p_TableName => P_TableName,
                      P_FMUSER    => P_FMUSER,
                      P_Fieldpvt  => P_Fieldpvt,
                      P_Field     => P_Field,
                      p_SqlCode   => p_SqlCode);
        if p_SqlCode <> 0 then
          return;
        end if;
      
      end if;
    
      --update Temporary table pvtID from node ID
      sp_NodeIDToTable(p_TableName => P_TableName,
                       P_NodeType  => P_NodeType,
                       P_ifaddbdg  => P_ifaddbdg,
                       P_Fieldpvt  => P_Fieldpvt,
                       p_SqlCode   => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    
      --Generate detail node time series and Aggregate Node time series
      if p_Period > 1 then
        sp_timeseries(p_Type      => p_Type,
                      p_TSType    => p_TSType,
                      P_NodeType  => P_NodeType,
                      P_ifaddbdg  => P_ifaddbdg,
                      p_TableName => P_TableName,
                      p_period    => p_period,
                      p_BeginData => p_BeginData,
                      p_EndData   => p_EndData,
                      p_version   => v_version,
                      p_SqlCode   => p_SqlCode);
      
      elsif p_Period = 1 then
        FMSP_PimportOnePeriodTS(pIn_nType      => p_Type, --1 Month ,2 Week,3 Day
                                PIn_nNodeType  => P_NodeType, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                pIn_nTSType    => p_TSType,
                                pIn_nversion   => v_version,
                                PIn_VTableName => P_TableName,
                                pOut_nSqlCode  => p_SqlCode);
      end if;
    
      if p_SqlCode <> 0 then
        return;
      end if;
    
      --delete is all is null detail node time series and Aggregate Node time series data
      sp_Del_Nulltimeseries(pIn_nType     => p_Type,
                            PIn_nNodeType => P_NodeType,
                            p_SqlCode     => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    end if;
  
    fmp_batch.FMSP_Parse(pIn_vSwitches  => P_StrOption,
                         pOut_tSwitches => tSwitches,
                         pOut_nSqlCode  => p_sqlcode);
    if tSwitches.maj_sel then
      vTimeSeries := case p_Type
                       when 1 then
                        '(' || p_TSType || ');'
                       when 2 then
                        '();(' || p_TSType || ');'
                       when 3 then
                        '();();(' || p_TSType || ')'
                     end;
      p_summarize.FMSP_Summarize(p_TimeSeriesNo => vTimeSeries,
                                 p_UoMConfig    => P_strUnit,
                                 p_Config       => 1, --note  hard code here.need process.reviewed by wfq
                                 p_SqlCode      => p_sqlcode);
    end if;
    Fmp_Log.LOGEND;
  exception
    when others then
      p_SqlCode := sqlcode;
      --add log
      Fmp_Log.LOGERROR;
      if p_SqlCode <> 0 then
        return;
      end if;
  end;

  --create a temporary table
  procedure sp_CreateTSTable(p_Period    in number,
                             p_TableName out varchar2,
                             p_SqlCode   out number) as
  
    v_strsql clob; --varchar2(8000);
    i        integer;
  begin
  
    p_SqlCode := 0;
  
    /* create sequence seq_tb_pimport
    minvalue 1
    maxvalue 9999999999
    start with 1
    increment by 1
    order;*/
  
    --select seq_tb_pimport.Nextval into p_TableName from dual;
    p_TableName := fmf_gettmptablename(); --'TB' || p_TableName;
  
    v_strsql := 'CREATE TABLE ' || p_TableName || '(
      IFPVT NUMBER ,
      PVTID NUMBER ,
      Aggnode varchar2(200) ,
      Product varchar2(60) ,
      productold varchar2(60) ,
      Sales varchar2(60) ,
      Trade varchar2(60) ,
      BeginDate varchar2(6),
      EndDate varchar2(6),
      YY varchar2(4) ,
      MM varchar2(2) ,
      DD varchar2(2) ,
      WW varchar2(2)
        ';
  
    for i in 1 .. p_Period loop
      v_strsql := v_strsql || ',T_' || i || ' NUMBER ';
    
    end loop;
    v_strsql := v_strsql || ') nologging';
  
    execute immediate v_strsql;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  -- Option handling
  procedure SP_OptionHandle(p_Type      in number default 1, --1 Month ,2 Week,3 Day
                            P_StrOption in varchar2, --## as separator
                            P_TableName in varchar2,
                            p_period    in number,
                            P_strUnit   in varchar2, --set Unit
                            P_version   in out number,
                            p_SqlCode   out number) as
  
    v_Option    varchar2(5000);
    v_StrOption varchar2(5000);
    v_next      integer;
    v_position  integer;
    v_length    int := 0;
    v_Key       varchar2(50);
    v_value     varchar2(50);
    v_strsql    varchar2(5000);
    v_strset    varchar2(5000);
  
    P_Fieldpvt varchar2(1000);
    p_PCount   int := 0;
    p_SCount   int := 0;
    p_TCount   int := 0;
  
    V_setunit varchar2(10);
  begin
    p_SqlCode := 0;
    if P_StrOption is null then
      return;
    end if;
  
    v_StrOption := trim(upper(P_StrOption));
    v_length    := length(v_StrOption);
    V_setunit   := P_strUnit;
  
    /*if p_Type = 1 then
    --1 Month*/
  
    while v_length > 0 loop
      --'##' Separated values
      v_next := instr(v_StrOption, '##', 1, 1);
    
      if v_next = 0 then
        v_Option := v_StrOption;
        v_length := 0;
      end if;
    
      if v_next > 1 then
        v_Option    := trim(substr(v_StrOption, 0, v_next - 1));
        v_StrOption := trim(substr(v_StrOption, v_next + 2));
        v_length    := length(v_StrOption);
      end if;
    
      --':' Separated values
      v_position := INSTR(v_Option, ':', 1, 1);
      if v_position = 0 then
        v_Key := v_Option;
      else
        v_Key := trim(substr(v_Option, 1, v_position - 1));
      end if;
    
      v_Key   := rtrim(v_Key);
      v_value := trim(substr(v_Option, v_position + 1));
    
      case v_Key
      --=========================================Date formats==========================================
        when 'A_M' then
          --YYYY,MM/WW
          if p_Type = 1 then
            v_strsql := 'update ' || P_TableName || ' set enddate=YY||MM ';
          elsif p_Type = 2 then
            v_strsql := 'update ' || P_TableName || ' set enddate=YY||WW ';
          else
            RAISE_APPLICATION_ERROR(-20123, 'Invald chronology!', true);
          end if;
          execute immediate v_strsql;
        
        when 'A_M_J' then
          --YYYY,MM,DD
          v_strsql := 'update ' || P_TableName || ' set enddate=YY||MM ';
          execute immediate v_strsql;
        
        when 'AA_MM' then
          --YY,MM
          v_strsql := 'update ' || P_TableName ||
                      ' set enddate=''20''||enddate ';
          execute immediate v_strsql;
        
      --====================================Key formats==============================================
        when 'KEY_DIS' then
          --trade channel
          v_strsql := 'update ' || P_TableName || ' set trade=''' ||
                      v_value || '''';
          execute immediate v_strsql;
        
        when 'KEY_DIS_DEFAULT' then
          --trade channel default
          v_strsql := 'update ' || P_TableName || ' set trade=''' ||
                      v_value || ''' where trade is null';
          execute immediate v_strsql;
        
        when 'KEY_GEO' then
          --sales territory
          v_strsql := 'update ' || P_TableName || ' set sales=''' ||
                      v_value || '''';
          execute immediate v_strsql;
        
        when 'KEY_GEO_DEFAULT' then
          --sales territory default
          v_strsql := 'update ' || P_TableName || ' set sales=''' ||
                      v_value || ''' where sales is null';
          execute immediate v_strsql;
        
      ------------------------------------------------------------------
        when 'PRO' then
          --product (delete is not product in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select f_cle from fam f where f.f_cle=t.product)';
          execute immediate v_strsql;
        
        when 'GEO' then
          --sales territory (delete is not sales territory in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select g_cle from geo g where g.g_cle=t.sales )';
          execute immediate v_strsql;
        
        when 'DIS' then
          --trade channel (delete is not trade channel in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select d_cle from dis d where d.d_cle=t.trade)';
          execute immediate v_strsql;
        
      ---------------------------------------------------------------
      
        when 'PRO_N0CRT' then
          --product  attribute
        
          v_value  := 48 + v_value;
          v_strsql := 'update ' || P_TableName ||
                      ' set product = (select max(f_cle) from v_productattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = product )
             where exists
             (select 1 from v_productattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = product)';
        
          execute immediate v_strsql;
        
        when 'FAM_N0CRT' then
        
          v_value := 48 + v_value;
        
          --product to productold
          v_strsql := 'update ' || P_TableName || ' set productold=product';
          execute immediate v_strsql;
        
          --product group attribute ,aggregate TS ,Find attrvalue by product group
          v_strsql := 'update ' || P_TableName ||
                      ' set product = (select max(f_cle) from v_productattrvalue P, vct v where p.nlevel > 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = product )
             where exists
             (select 1 from v_productattrvalue P, vct v where p.nlevel > 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = product)';
        
          execute immediate v_strsql;
        
          --update AGGNode
          v_strsql := 'update ' || P_TableName ||
                      ' set AggNode=replace(AggNode,productold,product)';
          execute immediate v_strsql;
        
        when 'GEO_N0CRT' then
        
          --sales territory attribute
          v_value := 48 + v_value;
        
          v_strsql := 'update ' || P_TableName ||
                      ' set sales = (select max(g_cle) from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = sales )
             where exists
             (select 1 from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = sales)';
        
          execute immediate v_strsql;
        
        when 'DIS_N0CRT' then
          --trade channel attribute
          v_value := 48 + v_value;
        
          v_strsql := 'update ' || P_TableName ||
                      ' set trade = (select max(d_cle) from v_tradechannelattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = trade )
             where exists
             (select 1 from v_tradechannelattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = trade)';
        
          execute immediate v_strsql;
        
      ---------------------------------------------------------------
        when 'NODIS' then
          v_strsql := 'update ' || P_TableName || ' set trade=null';
          execute immediate v_strsql;
        
        when 'NOGEO' then
          v_strsql := 'update ' || P_TableName || ' set sales=null';
          execute immediate v_strsql;
        
      ---------------------------------------------------------------
        when '2KEYS' then
          v_strsql := 'update ' || P_TableName ||
                      ' set trade=null,ifpvt=2 ';
          execute immediate v_strsql;
        
      ---------------------------------------------------------------
        when 'P2R' then
        
          --Dimension to the field and field where
          sp_DimensionToField(p_TableName => P_TableName,
                              -- P_Field     => P_Field,
                              P_Fieldpvt => p_Fieldpvt,
                              P_PCount   => P_PCount,
                              P_SCount   => P_SCount,
                              P_TCount   => P_TCount,
                              p_SqlCode  => p_SqlCode);
        
          if p_SqlCode <> 0 then
            return;
          end if;
        
          --update AGGNode
          v_strsql := 'update ' || P_TableName || ' set AggNode=' ||
                      p_Fieldpvt;
          execute immediate v_strsql;
        
      -- when 'r2p' then
      
      --========================================Field separators=========================================
      --when '-sdlt' then
      
      --==============================================Value formats===========================================
        when 'UNIT' then
          --unit:x
          --detail:product
          --AggNode:
        
          V_setunit := substr(V_setunit, v_value - 1, 1);
          if V_setunit = '1' then
            V_setunit := '*';
          elsif V_setunit = '0' then
            V_setunit := '/';
          end if;
        
          v_strsql := 'update (select t.product,nvl(f.unite_' || v_value ||
                      ',1) as val,nvl(f.unite_2,1) as val2 ';
        
          for i in 1 .. p_period loop
            v_strsql := v_strsql || ',T_' || i;
            if v_value = 3 then
              v_strset := v_strset || ',T_' || i || '=T_' || i || V_setunit ||
                          '(val*val2)';
            else
              v_strset := v_strset || ',T_' || i || '=T_' || i || V_setunit ||
                          'val';
            end if;
          end loop;
        
          v_strsql := v_strsql || ' from fam f, ' || P_TableName ||
                      ' t where f.f_cle = t.product) b set ';
          v_strsql := v_strsql || substr(v_strset, 2);
        
          execute immediate v_strsql;
        
        when 'VERSION' then
          --version:x rpd table mode_rpd field
          P_version := v_value - 1;
        else
          null;
      end case;
    
    end loop;
    commit;
  
  exception
    when others then
      --rollback;
      p_SqlCode := sqlcode;
  end;

  --Processing of the temporary table
  procedure SP_TSTableHandle(p_Type      in number, --1 Month ,2 Week,3 Day
                             P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                             P_TableName in varchar2,
                             p_Period    in number,
                             p_BeginData in number,
                             p_EndData   in number,
                             p_SqlCode   out number) as
    v_strsql varchar2(8000);
    v_Cycle  int;
  begin
  
    p_SqlCode := 0;
    if p_Type = 1 then
      v_Cycle := 12;
    elsif p_Type = 2 then
      v_Cycle := 52;
    end if;
  
    --Delete duplicate records
    if P_NodeType = 1 then
      ----1  Detail Node
      if p_Period > 1 then
        v_strsql := 'delete from ' || P_TableName || ' a
      where rowid <
      (
      select max(rowid) from ' || P_TableName || ' b
      where nvl(a.Product,chr(1))=nvl(b.Product,chr(1)) and nvl(a.sales,chr(1))=nvl(b.Sales,chr(1)) and nvl(a.trade,chr(1))=nvl(b.Trade,chr(1))
      )';
      elsif p_Period = 1 then
        v_strsql := 'delete from ' || P_TableName || ' a
      where rowid <
      (
      select max(rowid) from ' || P_TableName || ' b
      where nvl(a.Product,chr(1))=nvl(b.Product,chr(1)) and nvl(a.sales,chr(1))=nvl(b.Sales,chr(1)) and nvl(a.trade,chr(1))=nvl(b.Trade,chr(1))
       and a.enddate=b.enddate
      )';
      end if;
    elsif P_NodeType = 2 then
      --2  Aggregate Node
      if p_Period = 1 then
        v_strsql := 'delete from ' || P_TableName || ' a
        where rowid <
        (
        select max(rowid) from ' || P_TableName || ' b
        where a.aggnode=b.aggnode and a.EndDate=b.EndDate
        )';
      else
        v_strsql := 'delete from ' || P_TableName || ' a
        where rowid <
        (
        select max(rowid) from ' || P_TableName || ' b
        where a.aggnode=b.aggnode
        )';
      end if;
    end if;
    execute immediate v_strsql;
    commit;
    --Generate the start date to the end date and number of installments
    v_strsql := 'update ' || P_TableName ||
                ' set IFPVT=nvl(IFPVT ,0),begindate=trunc((substr(enddate,1,4)*' ||
                v_Cycle || '+substr(enddate,5,2)-' || p_Period || ')/' ||
                v_Cycle || ')||lpad(mod((substr(enddate,1,4)*' || v_Cycle ||
                '+substr(enddate,5,2)+1-' || p_Period || '),' || v_Cycle ||
                '),2,0)';
    execute immediate v_strsql;
    commit;
    --delete date not time series
    v_strsql := 'delete ' || P_TableName || ' where (begindate>' ||
                p_EndData || ') or (enddate<' || p_BeginData || ')';
  
    execute immediate v_strsql;
    commit;
    --pvt_cle to productold
    v_strsql := 'update ' || P_TableName || ' t set t.productold =
       (select p.name
          from (select t.Product, t.Sales, min(p.pvt_cle) name
                   from pvt p, ' || P_TableName || ' t
                  where p.pvt_cle like t.Product || ''-'' || t.Sales || ''%''
                  group by t.Product, t.Sales) p
         where t.Product = p.Product
           and t.Sales = p.Sales)
           where ifpvt=2 ';
    execute immediate v_strsql;
    commit;
    --productold(pvt_cle) to pvtID
    v_strsql := 'update ' || P_TableName || ' nologging ';
    v_strsql := v_strsql ||
                ' set pvtid=(select pvt_em_addr from pvt p
                 where productold=p.pvt_cle)
                 where ifpvt=2 and productold is not null ';
    execute immediate v_strsql;
  
    commit;
  
  exception
    when others then
      --rollback;
      p_SqlCode := sqlcode;
      raise;
  end;

  function FMF_GetNodeName return varchar2
  --*****************************************************************
    -- Description: get sql context of detail node name
    --
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        28-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    vStrSql varchar2(4000);
  begin
    if g_nNodeType = 1 then
      vStrSql := 'nvl2(product,product,'''')||nvl2(sales,''-''||sales,'''')||nvl2(trade,''-'' ||trade,'''')';
      return vStrSql;
    else
      ----detail node AND aggregate node
      vStrSql := vStrSql ||
                 ' case  when (product is not null and sales is not null and trade is not null) then ';
      --P1-S1-T1
      vStrSql := vStrSql || ' product ||''' || '-' || ''' || sales ||''' || '-' ||
                 '''|| trade ';
      vStrSql := vStrSql ||
                 ' when (product is not null and sales is not null and trade is null) then ';
      --P1-S1
      vStrSql := vStrSql || ' product ||''' || '-' || '''|| sales ';
      vStrSql := vStrSql ||
                 ' when (product is not null and sales is null and trade is not null) then ';
      --P1-T1
      vStrSql := vStrSql || ' product ||''' || '-' || '''|| trade ';
      vStrSql := vStrSql ||
                 '  when (product is not null and sales is null and trade is null) then ';
      --P1
      vStrSql := vStrSql || ' product ';
    
      -----aggregate node
      vStrSql := vStrSql ||
                 ' when product is null  and sales is not null  and trade is null then ';
      --S1
      vStrSql := vStrSql || ' sales  ';
    
      vStrSql := vStrSql ||
                 ' when product is null  and sales is  null  and trade is not null then ';
      --T1
      vStrSql := vStrSql || '  trade  ';
    
      vStrSql := vStrSql ||
                 ' when product is null  and sales is not null  and trade is not null then ';
    
      --S1-T1
      vStrSql := vStrSql || ' sales ||''' || '-' || '''|| trade ';
    
      vStrSql := vStrSql || ' end';
    end if;
  
    return vStrSql;
  end;
  --Dimension to the field and field where
  procedure sp_DimensionToField(P_TableName in varchar2,
                                P_Fieldpvt  out varchar2,
                                P_PCount    out number,
                                P_SCount    out number,
                                P_TCount    out number,
                                p_SqlCode   out number) as
    v_strsql varchar2(8000);
  
  begin
  
    p_SqlCode := 0;
  
    P_PCount := 0;
    P_SCount := 0;
    P_TCount := 0;
  
    P_Fieldpvt := FMF_GetNodeName();
  
    -- not Product cols
    v_strsql := 'select count(*)  from ' || P_TableName ||
                ' where Product is not null and rownum<2';
    execute immediate v_strsql
      into P_PCount;
  
    -- not Sales cols
    v_strsql := 'select count(*)  from ' || P_TableName ||
                ' where Sales is not null and rownum<2';
    execute immediate v_strsql
      into P_SCount;
  
    -- not Trade cols
    v_strsql := 'select count(*)  from ' || P_TableName ||
                ' where Trade is not null and rownum<2 ';
    execute immediate v_strsql
      into P_TCount;
  
    -- P_Fieldpvt := REPLACE(P_Fieldpvt, '--', '-');
  
    if P_Fieldpvt is not null then
      --update state existence  the detail node
      v_strsql := 'update ' || P_TableName || ' nologging ';
      v_strsql := v_strsql ||
                  ' set ifpvt=1
      where (pvtID is not null) or (pvtid is null and ' ||
                  P_Fieldpvt || ' in (select pvt_cle from pvt ) )
                ';
      execute immediate v_strsql;
      commit;
    end if;
  
  exception
    when others then
      p_SqlCode := sqlcode;
  end;

  --Generate basic data
  procedure sp_initial_datas(P_TableName in varchar2,
                             P_FMUSER    in varchar2,
                             P_Desc      in varchar2,
                             P_PCount    in number,
                             P_SCount    in number,
                             P_TCount    in number,
                             p_SqlCode   out number) as
    v_strsql  varchar2(8000);
    v_famID   int;
    v_geoID   int;
    v_disID   int;
    v_Desc    varchar2(800);
    v_nowDate number;
  begin
    p_SqlCode := 0;
    v_nowDate := F_ConvertDateToOleDateTime(sysdate);
    if P_Desc is null then
      v_Desc := 'created automatically';
    else
      v_Desc := P_Desc;
    end if;
  
    --Add new Product
    if P_PCount > 0 then
      v_strsql := 'select fam_em_addr from fam where ID_fam=1 and F_cle is null';
      execute immediate v_strsql
        into v_famID;
      --commit;
      v_strsql := 'insert /*+ append */ into fam(fam_em_addr,id_fam,f_cle,F_Desc,user_create_fam,date_create_fam,fam0_em_addr)';
      v_strsql := v_strsql ||
                  ' select seq_fam.nextval,f.* from (select distinct 80 ID,Product,''' ||
                  v_Desc || ''' F_Desc,''' || P_FMUSER || ''' FMuser,' ||
                  v_nowDate || ' Createdate,' || v_famID || ' fID from ' ||
                  P_TableName || ' t  where Product is not null and (ifpvt=0 or (ifpvt=2 and pvtID is null))
                and not exists (select  1 from fam f where
                t.product =f.f_cle ) ) f';
      execute immediate v_strsql;
    
    end if;
  
    --Add new Sales SalesTerritory
    if P_SCount > 0 then
      v_strsql := 'select geo_em_addr from geo where ascii(g_cle)=1';
      execute immediate v_strsql
        into v_geoID;
      --commit;
    
      v_strsql := 'insert /*+ append */ into geo(geo_em_addr,g_cle,g_desc,user_create_geo,date_create_geo,geo1_em_addr)
      select seq_geo.nextval,g.* from (select distinct Sales,''' ||
                  v_Desc || ''' g_desc,''' || P_FMUSER || ''' fUser,' ||
                  v_nowDate || ' createDate,' || v_geoID || ' gID from ' ||
                  P_TableName || ' t  where Sales is not null and (ifpvt=0 or (ifpvt=2 and pvtID is null))
                and not exists (select  1 from geo g where
                t.Sales =g.g_cle )) g';
      execute immediate v_strsql;
      --commit;
    end if;
  
    --Add new Trade TradeTrade
    if P_TCount > 0 then
      v_strsql := 'select dis_em_addr from dis where ascii(d_cle)=1';
      execute immediate v_strsql
        into v_disID;
      --commit;
    
      v_strsql := 'insert /*+ append */ into dis(dis_em_addr,d_cle,d_desc,user_create_dis,date_create_dis,dis2_em_addr)
      select seq_dis.nextval,d.* from (select distinct Trade,''' ||
                  v_Desc || ''' d_desc,''' || P_FMUSER || ''' fmuser,' ||
                  v_nowDate || ' createDate,' || v_disID || ' dID from ' ||
                  P_TableName || ' t  where Trade is not null and (ifpvt=0 or (ifpvt=2 and pvtID is null))
                and not exists (select  1 from dis d where
                t.Trade =d.d_cle )) d';
      execute immediate v_strsql;
      --commit;
    end if;
  
    commit;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  --Generate detail node
  procedure sp_DetailNode(P_TableName in varchar2,
                          P_FMUSER    in varchar2,
                          P_Fieldpvt  in varchar2,
                          P_field     in varchar2,
                          p_SqlCode   out number) as
    v_strsql  varchar2(5000);
    v_nowDate number;
  begin
    p_SqlCode := 0;
    v_nowDate := F_ConvertDateToOleDateTime(sysdate);
    --add new Detail Node
  
    --insert into  detail node
  
    v_strsql := 'insert /*+ append */ into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr,adr_pro,adr_geo,adr_dis
             ,user_create_pvt,date_create_pvt
             ,pvt_cle,pvt_desc)
      select  seq_pvt.nextval,t.* from (select distinct f.fam_em_addr PID,g.geo_em_addr GID,d.dis_em_addr DID,f.fam_em_addr,g.geo_em_addr,d.dis_em_addr,''' ||
                P_FMUSER || ''',' || v_nowDate || ',' || P_Fieldpvt ||
                ',substr(nvl2(f_desc,''-''||f_desc,'''')||nvl2(g_desc,''-''||g_desc,'''')||nvl2(d_desc,''-''||d_desc,''''),2,60)' ||
                P_field || ' nologging
      from ' || P_TableName ||
                ' a  left join fam f on Product=f_cle
      left join geo g on Sales=g_cle
      left join dis d on Trade=d_cle
      where ifpvt=0 or (ifpvt=2 and pvtID is null)) t  ';
  
    execute immediate v_strsql;
    commit;
  
    v_strsql := 'insert /*+ append */ into bdg(bdg_em_addr,ID_bdg,b_cle,bdg_desc)
    select seq_bdg.nextval,80,t.* from (select distinct p.pvt_cle,p.pvt_desc
    from pvt p,' || P_TableName || '  T
    where ' || P_Fieldpvt ||
                '=p.pvt_cle
    and (ifpvt=0 or (ifpvt=2 and pvtID is null))) t ';
  
    execute immediate v_strsql;
    commit;
  
  exception
    when others then
      p_SqlCode := sqlcode;
  end;

  --update Temporary table pvtID from node ID
  procedure sp_NodeIDToTable(P_TableName in varchar2,
                             P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                             P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                             P_Fieldpvt  in varchar2,
                             p_SqlCode   out number) as
    v_strsql varchar2(5000);
  
  begin
  
    p_SqlCode := 0;
    --1  Detail Node
    if P_NodeType = 1 then
      --update detail node ID
    
      --1 not add bdg
      if P_ifaddbdg = p_constant.IsNotBDG then
        v_strsql := 'update ' || P_TableName || ' nologging ';
        v_strsql := v_strsql || ' set pvtid=(select pvt_em_addr from pvt p
                 where ' || P_Fieldpvt ||
                    '=p.pvt_cle)
                 where pvtid is null ';
        execute immediate v_strsql;
        commit;
      elsif P_ifaddbdg = p_constant.IsBDG then
        --2 add bdg
        v_strsql := 'update ' || P_TableName || ' nologging ';
        v_strsql := v_strsql || ' set pvtid=(select bdg_em_addr from bdg b
                 where ' || P_Fieldpvt ||
                    '=b.b_cle and id_bdg=80 )
                    where pvtid is null ';
        execute immediate v_strsql;
        commit;
      end if;
    elsif P_NodeType = 2 then
      --2  Aggregate Node
    
      --1 not add bdg
      if P_ifaddbdg = p_constant.IsNotBDG then
        v_strsql := 'update ' || P_TableName || ' nologging ';
        v_strsql := v_strsql || ' set pvtid=(select sel_em_addr from sel s
                 where s.sel_cle=AggNode)
                 where pvtid is null ';
        execute immediate v_strsql;
        commit;
      elsif P_ifaddbdg = p_constant.IsBDG then
        --2 add bdg
        v_strsql := 'update ' || P_TableName || ' nologging ';
        v_strsql := v_strsql || ' set pvtid=(select bdg_em_addr from bdg b
                     where b.b_cle=AggNode and id_bdg=71 )
                     where pvtid is null ';
        execute immediate v_strsql;
        commit;
      end if;
    
      --delete not Aggregate Node data
      v_strsql := 'delete ' || P_TableName || ' where pvtid is null ';
      execute immediate v_strsql;
      commit;
    end if;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;
  --Time series and details node or Aggregate Node  to establish a connection
  procedure sp_TimeseriestoNode(p_TSType    in number,
                                P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                                P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                                P_TableName in varchar2,
                                P_BeginYY   in number,
                                P_BeginMM   in number,
                                P_EndYY     in number,
                                P_EndMM     in number,
                                p_version   in number,
                                p_SqlCode   out number) as
    v_sql varchar2(5000);
  begin
    p_SqlCode := 0;
  
    --1 not add bdg
    if P_ifaddbdg = p_constant.IsNotBDG then
      --1  Detail Node======================================================================
      if P_NodeType = 1 then
        ---------------------------------------------------------------------------------------
        begin
        
          --Time series and details node to establish a connection
          v_sql := ' MERGE INTO rpd r
           USING (select ' || p_TSType || ' as TSType,' ||
                   P_BeginYY || ' as BeginYY,' || P_BeginMM ||
                   ' as BeginMM,' || P_EndYY || ' as EndYY,' || P_EndMM ||
                   ' as Endmm,' || p_version || ' as moderpd,1 as modifyrpd,pvtID
            from ' || P_TableName || ') n
            ON (r.num_serie = n.TSType and r.pvt17_em_addr=n.pvtID  and r.mode_rpd=n.moderpd)
            WHEN MATCHED THEN
            UPDATE
             SET r.annee_debut=n.BeginYY,r.periode_debut=n.BeginMM,r.annee_fin=n.EndYY, r.periode_fin=n.EndMM,r.date_modify_rpd=n.modifyrpd
            WHEN NOT MATCHED THEN
             INSERT (rpd_em_addr,num_serie,annee_debut,periode_debut,annee_fin, periode_fin,mode_rpd,date_modify_rpd,pvt17_em_addr)
             VALUES (seq_rpd.nextval,TSType,BeginYY,BeginMM,EndYY,EndMM,moderpd,modifyrpd,pvtID)';
        
          execute immediate v_sql;
          commit;
        
        end;
        -------------------------------------------------------------------------------
      
        --2  Aggregate Node================================================
      elsif P_NodeType = 2 then
        ----------------------------------------------------------------------------------------------
        begin
        
          v_sql := ' MERGE INTO rbp r
         USING (select ' || p_TSType || ' as TSType,' ||
                   P_BeginYY || ' as BeginYY,' || P_BeginMM ||
                   ' as BeginMM,' || P_EndYY || ' as EndYY,' || P_EndMM ||
                   ' as Endmm,' || p_version || ' as moderpd,1 as modifyrpd,pvtID
          from ' || P_TableName || ') n
          ON (r.num_prv = n.TSType and r.sel21_em_addr=n.pvtID  and r.mode_rbp=n.moderpd)
          WHEN MATCHED THEN
          UPDATE
           SET r.annee_debut_rbp=n.BeginYY,r.periode_debut_rbp=n.BeginMM,r.annee_fin_rbp=n.EndYY, r.periode_fin_rbp=n.EndMM,r.date_modify_rbp=n.modifyrpd
          WHEN NOT MATCHED THEN
           INSERT (rbp_em_addr,num_prv,annee_debut_rbp,periode_debut_rbp,annee_fin_rbp, periode_fin_rbp,mode_rbp,date_modify_rbp,sel21_em_addr)
           VALUES (seq_rbp.nextval,TSType,BeginYY,BeginMM,EndYY,EndMM,moderpd,modifyrpd,pvtID)';
        
          execute immediate v_sql;
          commit;
        
        end;
        ----------------------------------------------------------------------------------
      
      end if;
    
      --add bdg+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    elsif P_ifaddbdg = p_constant.IsBDG then
      ----------------------------------------------------------------------------------------------
    
      begin
      
        --Time series and details node to establish a connection
        v_sql := ' MERGE INTO bgc r
         USING (select ' || p_TSType || ' as TSType,' ||
                 P_BeginYY || ' as BeginYY,' || P_BeginMM || ' as BeginMM,' ||
                 P_EndYY || ' as EndYY,' || P_EndMM || ' as Endmm,' ||
                 p_version || ' as moderpd,1 as modifyrpd,pvtID
          from ' || P_TableName || ') n
          ON (r.num_bdg = n.TSType and r.bdg31_em_addr=n.pvtID  and r.mode_bdg=n.moderpd)
          WHEN MATCHED THEN
          UPDATE
           SET r.annee_debut_bdg=n.BeginYY,r.periode_debut_bdg=n.BeginMM,r.annee_fin_bdg=n.EndYY, r.periode_fin_bdg=n.EndMM,r.date_modify_bgc=n.modifyrpd
          WHEN NOT MATCHED THEN
           INSERT (bgc_em_addr,num_bdg,annee_debut_bdg,periode_debut_bdg,annee_fin_bdg, periode_fin_bdg,mode_bdg,date_modify_bgc,bdg31_em_addr)
           VALUES (seq_bgc.nextval,TSType,BeginYY,BeginMM,EndYY,EndMM,moderpd,modifyrpd,pvtID)';
      
        execute immediate v_sql;
        commit;
      
      end;
      ----------------------------------------------------------------------------------
    
    end if;
  exception
    when others then
      p_SqlCode := sqlcode;
      --add log
      Fmp_Log.LOGERROR;
    
  end;

  procedure FMSP_PimportOnePeriodTS(pIn_nType      in number, --1 Month ,2 Week,3 Day
                                    PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                    pIn_nTSType    in number,
                                    pIn_nversion   in number,
                                    PIn_VTableName in varchar2,
                                    pOut_nSqlCode  out number)
  --*****************************************************************
    -- Description: Generate  one Period detail node or Aggregate Node time series
    --
    -- Parameters:
    --      pIn_nType      in number, --1 Month ,2 Week,3 Day
    --      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
    --      pIn_nTSType    in number,
    --      pIn_nversion   in number,
    --      PIn_VTableName in varchar2,
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        6-1-2013     wfq           Created.
    -- **************************************************************
  
   as
    VStrSql varchar2(8000);
    vsql    varchar2(8000);
    iPeriod int;
  
    tc_DataYM sys_refcursor;
  
  begin
    pOut_nSqlCode := 0;
  
    -- insert or update one period Time series
  
    vsql := 'select distinct substr(enddate,5,2)  from ' || PIn_VTableName;
  
    open tc_DataYM for vsql;
    loop
      fetch tc_DataYM
        into iPeriod;
    
      exit when tc_DataYM%notfound;
    
      --Dealing with the first data
    
      FMSP_PimportOnePeriodTStoSQL(pIn_nType      => pIn_nType, --1 Month ,2 Week,3 Day
                                   PIn_nNodeType  => PIn_nNodeType, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                   pIn_nTSType    => pIn_nTSType,
                                   pIn_nversion   => pIn_nversion,
                                   PIn_VTableName => PIn_VTableName,
                                   PIn_nperiod    => iPeriod,
                                   pOut_vStrSql   => VStrSql,
                                   pOut_nSqlCode  => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    
      execute immediate VStrSql;
      commit;
    
    end loop;
  
    close tc_DataYM;
  
  exception
    when others then
      pOut_nSqlCode := sqlcode;
    
  end;

  --Generate SQL about Save  one Period time series
  procedure FMSP_PimportOnePeriodTStoSQL(pIn_nType      in number, --1 Monthly ,2 Weekly,3 Dayly
                                         PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
                                         pIn_nTSType    in number,
                                         pIn_nversion   in number,
                                         pIn_VTableName in varchar2,
                                         PIn_nPeriod    in int,
                                         pOut_VStrSql   out varchar2,
                                         pOut_nSqlCode  out number)
  --*****************************************************************
    -- Description: Construct the SQL statement to timeseries (insert and update)
    -- Parameters:
    --      pIn_nType      in number, --1 Month ,2 Week,3 Day
    --      PIn_nNodeType  in number, --1  Detail Node , 2  Aggregate Node ,3 bdg
    --      pIn_nTSType    in number,
    --      pIn_nversion   in number,
    --      PIn_VTableName in varchar2,
    --      PIn_nPeriod    in int
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        6-1-2013     wfq           Created.
    -- **************************************************************
   as
  
    VUpdatesql varchar2(8000);
    vstrM      varchar2(2000);
    vTBName    varchar2(50);
    VID        varchar2(50);
  
    vType varchar2(2);
  begin
  
    pOut_nSqlCode := 0;
  
    if pIn_nType = 1 then
      vType := 'M';
      --2 Week
    elsif pIn_nType = 2 then
      vType := 'W';
    else
      vType := 'D';
    end if;
  
    if PIn_nNodeType = 1 then
      --1  Detail Node
      vTBName := 'DON_' || vType;
      VID     := 'PVTID';
    elsif PIn_nNodeType = 2 then
      --2  Aggregate Node
      vTBName := 'prb_' || vType;
      VID     := 'SELID';
    
    elsif PIn_nNodeType = 3 then
      --3  bdg
      vTBName := 'bud_' || vType;
      VID     := 'bdgID';
    end if;
  
    vstrM      := 'T' || PIn_nPeriod;
    VUpdatesql := 'T' || PIn_nPeriod || '= T_1';
  
    pOut_VStrSql := '
          MERGE /*+ parallel*/ INTO ' || vTBName || ' d
          USING (select pvtID,' || pIn_nTSType ||
                    ' TSID,' || pIn_nversion ||
                    ' Version,substr(enddate,1,4) as YY,T_1 from ' ||
                    pIn_VTableName || ' where substr(enddate,5,2)=' ||
                    PIn_nPeriod || ' ) n
             ON (d.YY = n.YY and d.' || VID ||
                    '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    pOut_VStrSql := pOut_VStrSql || VUpdatesql;
    pOut_VStrSql := pOut_VStrSql || '
           WHEN NOT MATCHED THEN
            INSERT (' || vTBName || 'ID,' || VID ||
                    ',TSID,Version,YY,' || vstrM || ')
            VALUES (seq_' || vTBName ||
                    '.nextval,n.pvtID,n.TSID,n.Version,n.YY,n.T_1)';
  
  exception
    when others then
      pOut_nSqlCode := sqlcode;
    
  end;

  procedure FMSP_TSRangetoSQL(p_Type      in number, --1 Month ,2 Week,3 Day
                              p_TSType    in number,
                              p_version   in number,
                              p_TableName in varchar2,
                              P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                              P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                              p_MBegin    in number,
                              p_TBegin    in number,
                              p_Number    in number,
                              p_SqlCode   out number) as
  
    cSQL            clob;
    v_Updatesql     clob;
    v_strM          clob;
    v_strT          clob;
    M               int;
    T               int;
    v_File          varchar2(50);
    v_TBName        varchar2(50);
    V_ID            varchar2(50);
    vWhereIsnotnull clob := ' where 1=1 ';
    vWhereIsnull    clob := ' where 1=1 ';
  begin
    M         := p_MBegin;
    T         := p_TBegin;
    p_sqlcode := 0;
  
    if p_Type = 1 then
      --monthly
      v_TBName := '_M';
    
    elsif p_Type = 2 then
      --2 Weekly
      v_TBName := '_W';
    end if;
  
    if P_NodeType = 1 then
      --1  Detail Node
      v_TBName := 'DON' || v_TBName;
      V_ID     := 'PVTID';
    elsif P_NodeType = 2 then
      --2  Aggregate Node
      v_TBName := 'prb' || v_TBName;
      V_ID     := 'SELID';
    end if;
  
    --1 add bdg
    if P_ifaddbdg = p_constant.IsBDG then
      v_TBName := 'bud' || v_TBName;
      V_ID     := 'bdgID';
    end if;
  
    v_File := 'T';
  
    for i in 1 .. p_Number loop
      vWhereIsnotnull := vWhereIsnotnull || ' or n.T_' || T ||
                         ' is not null';
      vWhereIsnull    := vWhereIsnull || ' and n.T_' || T || ' is  null';
      v_strM          := v_strM || ',' || v_File || M;
      v_strT          := v_strT || ',T_' || T;
      v_Updatesql     := v_Updatesql || ',' || v_File || M || '= T_' || T;
      M               := M + 1;
      T               := T + 1;
    end loop;
  
    v_Updatesql := substr(v_Updatesql, 2);
  
    cSQL := '
          MERGE /*+use_hash(d,n) parallel */ INTO ' || v_TBName || ' d
          USING (select pvtID,' || p_TSType || ' TSID,' ||
            p_version || ' Version,  YY' || v_strT || ' from ' ||
            P_TableName || ' ) n
             ON (d.YY = n.YY and d.' || V_ID || '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ' || v_Updatesql || ' delete ' || vWhereIsnull || '
             WHEN NOT MATCHED THEN
            INSERT (' || v_TBName || 'ID,' || V_ID ||
            ',TSID,Version,YY' || v_strM || ')
            VALUES (seq_' || v_TBName ||
            '.nextval,n.pvtID,n.TSID,n.Version,n.YY' || v_strT || ')' ||
            vWhereIsnotnull;
    sp_execsql(p_Sql => cSQL);
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  --Generate detail node and Aggregate Node time series
  procedure sp_TimeSeries(p_Type      in number, --1 Month ,2 Week,3 Day
                          p_TSType    in number,
                          P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                          P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                          P_TableName in varchar2,
                          p_period    in number,
                          p_BeginData in number,
                          p_EndData   in number,
                          p_version   in number,
                          p_SqlCode   out number) as
    v_strsql  varchar2(8000);
    v_sql     varchar2(8000);
    v_BeginYY int;
    v_BeginMM int;
    v_EndYY   int;
    v_EndMM   int;
    v_MMabs   int;
    k         int;
    YY        number;
  
    v_Number    int;
    v_Cycle     int;
    v_Begindata int;
    v_Enddata   int;
    tc_DataYM   sys_refcursor;
  
    cSqlMidTableText clob;
    vNewMidTablename varchar2(30);
    nMidFlag         number := 0;
  begin
    p_SqlCode := 0;
  
    if p_Type = 1 then
      v_Cycle := 12;
    elsif p_Type = 2 then
      v_Cycle := 52;
    end if;
  
    v_BeginYY := substr(p_BeginData, 1, 4);
    v_BeginMM := substr(p_BeginData, 5, 2);
    v_EndYY   := substr(p_EndData, 1, 4);
    v_EndMM   := substr(p_EndData, 5, 2);
  
    execute immediate '  alter session set db_file_multiblock_read_count=500 ';
    -- insert or update Time series
  
    v_sql := 'select distinct begindate,enddate from ' || P_TableName;
    k     := 1;
    open tc_DataYM for v_sql;
    loop
      fetch tc_DataYM
        into v_Begindata, v_Enddata;
      --into v_BeginYY, v_BeginMM, v_MMabs, v_EndYY, v_EndMM;
    
      exit when tc_DataYM%notfound;
    
      v_BeginYY := substr(greatest(v_Begindata, p_BeginData), 1, 4);
      v_BeginMM := substr(greatest(v_Begindata, p_BeginData), 5, 2);
      --
      v_MMabs := p_period - ((substr(v_Enddata, 1, 4) * v_Cycle +
                 substr(v_Enddata, 5, 2)) -
                 (substr(p_BeginData, 1, 4) * v_Cycle +
                 substr(p_BeginData, 5, 2)));
      v_EndYY := substr(least(v_Enddata, p_EndData), 1, 4);
      v_EndMM := substr(least(v_Enddata, p_EndData), 5, 2);
      --Dealing with the first data
      if v_MMabs < 1 then
        k := 1;
      else
        k := v_MMabs;
      end if;
    
      if v_BeginYY = v_EndYY then
        v_Number := v_EndMM - v_BeginMM + 1;
      elsif p_period < v_Cycle - v_BeginMM + 1 then
        v_Number := p_period + 1;
      else
        v_Number := v_Cycle - v_BeginMM + 1;
      end if;
    
      SP_TSRangetoSQL(p_Type      => p_Type, --1 Month ,2 Week,3 Day
                      p_TSType    => p_TSType,
                      p_version   => p_version,
                      p_TableName => P_TableName,
                      P_NodeType  => P_NodeType,
                      P_ifaddbdg  => P_ifaddbdg, --0 not add bdg, 1 add bdg
                      p_YY        => v_BeginYY,
                      p_MBegin    => v_BeginMM,
                      p_TBegin    => K,
                      p_Number    => v_Number,
                      P_Enddata   => v_Enddata,
                      p_StrSql    => v_strsql,
                      p_SqlCode   => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    
      sp_execsql(p_Sql => v_strsql);
    
      k := k + v_Cycle - v_BeginMM + 1;
      for YY in v_BeginYY + 1 .. v_EndYY loop
        if YY <> v_EndYY then
          nMidFlag         := 1;
          cSqlMidTableText := cSqlMidTableText ||
                              ' union all select pvtid,' || yy || ',';
          for tt in 0 .. v_cycle - 1 loop
            if tt = v_cycle - 1 then
              cSqlMidTableText := cSqlMidTableText || ' t_' ||
                                  to_char(tt + k);
            else
              cSqlMidTableText := cSqlMidTableText || ' t_' ||
                                  to_char(tt + k) || ',';
            end if;
          end loop;
          cSqlMidTableText := cSqlMidTableText || ' from  ' || P_TableName ||
                              ' where pvtid is not null and enddate=' ||
                              v_Enddata;
        
          k := k + v_Cycle;
        
        else
          --Dealing with the last data
          SP_TSRangetoSQL(p_Type      => p_Type, --1 Month ,2 Week,3 Day
                          p_TSType    => p_TSType,
                          p_version   => p_version,
                          p_TableName => P_TableName,
                          P_NodeType  => P_NodeType,
                          P_ifaddbdg  => P_ifaddbdg, --0 not add bdg, 1 add bdg
                          p_YY        => YY,
                          p_MBegin    => 1,
                          p_TBegin    => K,
                          p_Number    => v_EndMM,
                          P_Enddata   => v_Enddata,
                          p_StrSql    => v_strsql,
                          p_SqlCode   => p_SqlCode);
          if p_SqlCode <> 0 then
            return;
          end if;
          sp_execsql(v_strsql);
        end if;
      end loop;
      if nMidFlag = 1 then
        vNewMidTablename := fmf_gettmptablename();
        cSqlMidTableText := ' from dual where 1=2 ' || cSqlMidTableText;
        --add columns in the front
        for m in reverse 1 .. v_Cycle loop
          cSqlMidTableText := ', null t_' || m || cSqlMidTableText;
        end loop;
      
        cSqlMidTableText := 'create table ' || vNewMidTablename ||
                            ' as select null pvtid , null yy ' ||
                            cSqlMidTableText;
        sp_execsql(cSqlMidTableText);
        --reset the value to default
        cSqlMidTableText := '';
        nMidFlag         := 0;
        --as per to the middle table ,to merge all the datas to the target table
        FMSP_TSRangetoSQL(p_Type      => p_Type,
                          p_TSType    => p_TSType,
                          p_version   => p_version,
                          p_TableName => vNewMidTablename,
                          P_NodeType  => P_NodeType,
                          P_ifaddbdg  => P_ifaddbdg,
                          p_MBegin    => 1,
                          p_TBegin    => 1,
                          p_Number    => v_Cycle,
                          p_SqlCode   => p_SqlCode);
      end if;
    end loop;
    close tc_DataYM;
  exception
    when others then
      p_SqlCode := sqlcode;
  end;
  --Generate SQL about detail node time seri
  procedure SP_TSRangetoSQL(p_Type      in number, --1 Month ,2 Week,3 Day
                            p_TSType    in number,
                            p_version   in number,
                            p_TableName in varchar2,
                            P_NodeType  in number, --1  Detail Node  2  Aggregate Node
                            P_ifaddbdg  in number, --0 not add bdg, 1 add bdg
                            p_YY        in number,
                            p_MBegin    in number,
                            p_TBegin    in number,
                            p_Number    in number,
                            P_Enddata   in number,
                            p_StrSql    out varchar2,
                            p_SqlCode   out number) as
    /*Construct the SQL statement to timeseries (insert and update)*/
  
    v_Updatesql varchar2(8000);
    v_strM      varchar2(2000);
    v_strT      varchar2(2000);
    M           int;
    T           int;
    v_File      varchar2(50);
    v_TBName    varchar2(50);
    V_ID        varchar2(50);
  
    vWhereIsnotnull clob := ' where 1=1 ';
    vWhereIsnull    clob := ' where 1=1 ';
  begin
    M         := p_MBegin;
    T         := p_TBegin;
    p_sqlcode := 0;
  
    if p_Type = 1 then
      --monthly
      v_TBName := '_M';
    
    elsif p_Type = 2 then
      --2 Weekly
      v_TBName := '_W';
    end if;
  
    if P_NodeType = 1 then
      --1  Detail Node
      v_TBName := 'DON' || v_TBName;
      V_ID     := 'PVTID';
    elsif P_NodeType = 2 then
      --2  Aggregate Node
      v_TBName := 'prb' || v_TBName;
      V_ID     := 'SELID';
    end if;
  
    --1 add bdg
    if P_ifaddbdg = p_constant.IsBDG then
      v_TBName := 'bud' || v_TBName;
      V_ID     := 'bdgID';
    end if;
  
    v_File := 'T';
  
    for i in 1 .. p_Number loop
      vWhereIsnotnull := vWhereIsnotnull || ' or n.T_' || T ||
                         ' is not null';
      vWhereIsnull    := vWhereIsnull || ' and n.T_' || T || ' is  null';
    
      v_strM      := v_strM || ',' || v_File || M;
      v_strT      := v_strT || ',T_' || T;
      v_Updatesql := v_Updatesql || ',' || v_File || M || '= T_' || T;
      M           := M + 1;
      T           := T + 1;
    end loop;
  
    v_Updatesql := substr(v_Updatesql, 2);
  
    p_StrSql := '
          MERGE /*+use_hash(d,n) parallel */ INTO ' ||
                v_TBName || ' d
          USING (select pvtID,' || p_TSType || ' TSID,' ||
                p_version || ' Version,' || p_YY || ' as YY' || v_strT ||
                ' from ' || P_TableName ||
                ' where pvtID is not null and Enddate=' || P_Enddata || ') n
             ON (d.YY = n.YY and d.' || V_ID ||
                '=n.pvtID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    p_StrSql := p_StrSql || v_Updatesql || ' delete ' || vWhereIsnull;
    p_StrSql := p_StrSql || '
           WHEN NOT MATCHED THEN
            INSERT (' || v_TBName || 'ID,' || V_ID ||
                ',TSID,Version,YY' || v_strM || ')
            VALUES (seq_' || v_TBName ||
                '.nextval,n.pvtID,n.TSID,n.Version,n.YY' || v_strT || ') ' ||
                vWhereIsnotnull;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  --sum DetailNode timeseries to AggregateNode timeseries
  procedure sp_sum_DetailTStoAggTS(p_TSType  in number,
                                   p_SqlCode out number) as
    v_strAggID varchar2(5000);
    --v_strsql   varchar2(8000);
  begin
    p_SqlCode := 0;
    if p_TSType = 1 then
      null;
    end if;
    select '(' || ltrim(max(sys_connect_by_path(t.sel_em_addr, ',')), ',') || ')' val
      into v_strAggID
      from (select a.sel_em_addr,
                   row_number() over(order by a.sel_em_addr) cur,
                   row_number() over(order by a.sel_em_addr) + 1 prev
              from (select distinct s.sel13_em_addr as sel_em_addr
                      from rpd r, rsp s
                     where r.date_modify_rpd = 1
                       and r.pvt17_em_addr = s.pvt14_em_addr) a) t
     start with t.cur = 1
    connect by prior t.prev = t.cur;
  exception
    when others then
      p_SqlCode := sqlcode;
  end;

  --update date_modify 1 to 0
  procedure sp_TSNullreplace( --p_TSType   in number,
                             P_NodeType in number, --1  Detail Node  2  Aggregate Node
                             P_ifaddbdg in number, --0 not add bdg, 1 add bdg
                             p_SqlCode  out number) as
  
    /* v_File      varchar2(50);
    v_table     varchar2(50);
    v_where     varchar2(500);*/
    v_strmodify varchar2(5000);
    -- i           int;
  begin
  
    p_SqlCode := 0;
  
    --1 not add bdg======================================================================================================
    if P_ifaddbdg = p_constant.IsNotBDG then
    
      if P_NodeType = 1 then
      
        v_strmodify := 'update rpd set date_modify_rpd = 0 where date_modify_rpd = 1';
      
      elsif P_NodeType = 2 then
      
        v_strmodify := 'update rbp set date_modify_rbp = 0 where date_modify_rbp = 1';
      
      end if;
    
      --2 add bdg====================================================================================================================
    elsif P_ifaddbdg = p_constant.IsBDG then
    
      v_strmodify := 'update bgc  set date_modify_bgc = 0 where date_modify_bgc = 1';
    
    end if;
  
    execute immediate v_strmodify;
    commit;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

  --delete is null time series
  procedure sp_Del_Nulltimeseries(pIn_nType     in number, --1 Monthly ,2 Weekly,3 Dayly
                                  PIn_nNodeType in number, --1  Detail Node  2  Aggregate Node
                                  p_SqlCode     out number) as
    v_strsql varchar2(2000);
    vType    varchar2(50);
    vTBName  varchar2(50);
    vCycle   int;
  begin
    p_SqlCode := 0;
  
    if pIn_nType = 1 then
      vType  := 'M';
      vCycle := 12;
      --2 Week
    elsif pIn_nType = 2 then
      vType  := 'W';
      vCycle := 52;
    else
      vType  := 'D';
      vCycle := 365;
    end if;
  
    if PIn_nNodeType = 1 then
      --1  Detail Node
      vTBName := 'DON_' || vType;
    elsif PIn_nNodeType = 2 then
      --2  Aggregate Node
      vTBName := 'prb_' || vType;
    
    elsif PIn_nNodeType = 3 then
      --3  bdg
      vTBName := 'bud_' || vType;
    end if;
  
    v_strsql := 'delete /*+ parallel */' || vTBName ||
                ' where T1 is null  ';
    for i in 2 .. vCycle loop
      v_strsql := v_strsql || 'and T' || i || ' is null ';
    end loop;
    execute immediate v_strsql;
    commit;
  
  exception
    when others then
      p_SqlCode := sqlcode;
      raise_application_error(p_constant.e_oraerr, p_sqlcode);
  end;

  --delete temporary table
  procedure sp_DropTSTable(P_TableName in varchar2, p_SqlCode out number) as
    v_strsql varchar2(2000);
    v_cnt    number;
  begin
  
    p_SqlCode := 0;
  
    select count(*)
      into v_cnt
      from user_tables t
     where t.TABLE_NAME = upper(p_TableName);
  
    if v_cnt > 0 then
      v_strsql := 'Drop table ' || p_TableName || ' purge ';
      execute immediate v_strsql;
    end if;
  
  exception
    when others then
      p_SqlCode := sqlcode;
    
  end;

end P_pImport;
/
