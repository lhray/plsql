create or replace package p_Aggregation is

  -- public var
  e_parallel_err exception;
  e_summarize_err exception;
  e_rebuildNormalSel_err exception;

  v_parallel_cnt number; --parallel degree
  v_Is2DExisted  number; --0  mean the detail node is not made by  GEO.
  v_Is3DExisted  number;

  type t_cols_array is array(1) of varchar2(20);

  v_str_ping_at_col varchar2(20) := '||''-''||';
  v_single_quote    varchar2(10) := '''';

  v_ACTIVE constant varchar2(6) := 'ACTIVE';
  v_column_flag number := -1; --flag the current  var is col name or const var

  v_unknown_exception exception;
  procedure SP_BuildAggregateRule(P_AggregateRuleID    in number,
                                  p_TimeSeriesNoForUoM in varchar2,
                                  p_TimeSeriesNoNomal  in varchar2,
                                  p_UoMConfig          in varchar2, --a string like 110000001
                                  p_SqlCode            out number);

  procedure SP_UpdateAggregation(p_TimeSeriesNoForUoM in varchar2,
                                 p_TimeSeriesNoNomal  in varchar2,
                                 p_UoMConfig          in varchar2, --a string like 110000001
                                 p_Config             in varchar2,
                                 p_SqlCode            out number);
  procedure SP_DestroyAggregateRule(P_AggregateRuleID in number,
                                    p_SqlCode         out number);

  --as AggregateRuleID return AggregateNodes
  procedure SP_GetAggregateNodes(P_AggregateRuleID  in number,
                                 pIn_bNeedCreateTab in boolean default false,
                                 P_Sequence         in varchar2, --Sort sequence
                                 p_AggregateNode    out sys_refcursor,
                                 pOut_vTabName      out varchar2,
                                 p_SqlCode          out number);

  --as AggregateRuleID and conditions return AggregateNodes
  procedure SP_GetAggNodesByConditions(P_AggregateRuleID  in number,
                                       pIn_bNeedCreateTab in boolean default false,
                                       P_Conditions       in varchar2,
                                       P_Sequence         in varchar2, --Sort sequence
                                       p_AggregateNode    out sys_refcursor,
                                       pOut_vTabName      out varchar2,
                                       p_SqlCode          out number);

  --genereate all the detail node of the aggregation rule;
  procedure sp_GenerateDetailNodeOfTheRule(p_AggRuleID  in number,
                                           pIn_vObjId   in varchar2,
                                           pIn_nObjType in number,
                                           p_Sqlcode    out number);

  procedure sp_RuleIDToSequenceSQL(P_AggregateRuleID in number,
                                   P_Sequence        in varchar2, --Sort sequence
                                   pIn_nType         in number,
                                   p_Strsql          out varchar2,
                                   p_SqlCode         out number);
  procedure sp_AsyncExecTask(p_TaskName in varchar2,
                             p_sqlcode  out number,
                             p_JobNo    out number);
  procedure sp_PutTaskIntoPipe(p_PipeName in varchar2);
  function f_GetStrToConcat(f_LastStrType    in number,
                            f_CurrentStrType in number,
                            f_CurrentStr     in varchar2) return varchar2;
  procedure FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  in number,
                                      pIn_vSequence   in varchar2 default null,
                                      pIn_vConditions in varchar2,
                                      pOut_Nodes      out sys_refcursor,
                                      pOut_nSqlCode   out number);

  procedure FMISP_GetAggregateNodes(pIn_nAggregationID in number,
                                    PIn_vSequence      in varchar2,
                                    pOut_AggregateNode out sys_refcursor,
                                    pOut_vTabName      out varchar2,
                                    pOut_nSqlCode      out number);

  procedure FMISP_GetAggNodesByConditions(pIn_nAggregationID in number,
                                          pIn_vConditions    in varchar2,
                                          pIn_vSequence      in varchar2,
                                          pOut_AggregateNode out sys_refcursor,
                                          pOut_vTabName      out varchar2,
                                          pOut_SqlCode       out number);

  procedure FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID in number,
                                       pIn_vObjId           in varchar2,
                                       pIn_nType            in number,
                                       pIn_nObjType         in number,
                                       pOut_nSqlCode        out number);
  procedure spprv_ProduceAggNodeAndLinks(P_AggregateRuleID in number,
                                         pIn_nType         in number,
                                         p_sqlcode         out number);
  procedure spprv_updateAggregation(p_TimeSeriesNoForUoM in varchar2,
                                    p_TimeSeriesNoNomal  in varchar2,
                                    p_UoMConfig          in varchar2,
                                    p_Config             in varchar2,
                                    p_Startid            in number,
                                    P_Endid              in number);
  procedure spprv_GetColsSQLOfRule(P_AggregateRuleID in number,
                                   p_tab_cols        out varchar2,
                                   p_distinct_cols   out varchar2,
                                   p_ids_cols        out varchar2,
                                   p_names_cols      out varchar2,
                                   p_desc_cols       out varchar2,
                                   p_sqlcode         out number);

  procedure spprv_GetColsSQL3DAttrs(p_P_AggregateRuleID in number,
                                    p_TableType         in number,
                                    p_tab_cols          out varchar2,
                                    p_distinct_cols     out varchar2,
                                    p_NodeIDs           out varchar2,
                                    p_NodeName          out varchar2,
                                    p_NodeDesc          out varchar2,
                                    p_sqlcode           out number);

  procedure spprv_ParallelBuildNormalSEL(P_startid in number,
                                         p_endid   in number);

  procedure SPPRV_BuildAggregateRule(P_AggregateRuleID     in number,
                                     p_TimeSeriesNoForUoM  in varchar2,
                                     p_TimeSeriesNoNomal   in varchar2,
                                     p_UoMConfig           in varchar2, --a string like 110000001
                                     p_IsUpdateAggregation in number default 0, -- 1 update aggregation call it
                                     p_Config              in varchar2 default '0',
                                     p_SqlCode             out number);

end p_Aggregation;
/
create or replace package body p_Aggregation is

  procedure spprv_GetColsSQLDataAttrs(p_P_AggregateRuleID in number,
                                      p_TableType         in number default 10055,
                                      p_tab_cols          out varchar2,
                                      p_distinct_cols     out varchar2,
                                      p_NodeIDs           out varchar2,
                                      p_NodeName          out varchar2,
                                      p_NodeDesc          out varchar2,
                                      pOut_nColumnFlag    out number,
                                      p_sqlcode           out number);
  procedure SP_BuildAggregateRule(P_AggregateRuleID    in number,
                                  p_TimeSeriesNoForUoM in varchar2,
                                  p_TimeSeriesNoNomal  in varchar2,
                                  p_UoMConfig          in varchar2,
                                  p_SqlCode            out number) as
  begin
    p_sqlcode := 0;
    Fmp_Log.FMP_SetValue(P_AggregateRuleID);
    Fmp_log.FMP_SetValue(p_TimeSeriesNoForUoM);
    Fmp_log.FMP_SetValue(p_TimeSeriesNoNomal);
    Fmp_log.FMP_SetValue(p_UoMConfig);
    Fmp_log.LOGBEGIN;
    SPPRV_BuildAggregateRule(P_AggregateRuleID     => P_AggregateRuleID,
                             p_TimeSeriesNoForUoM  => p_TimeSeriesNoForUoM,
                             p_TimeSeriesNoNomal   => p_TimeSeriesNoNomal,
                             p_UoMConfig           => p_UoMConfig,
                             p_IsUpdateAggregation => 0,
                             p_SqlCode             => p_SqlCode);
    Fmp_Log.LOGEND;
  exception
    when others then
      p_SqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  end;
  procedure FMSP_BuildAggregate_Tmp(PIn_nAggregateRuleID in number,
                                    pIn_vObjId           in varchar2,
                                    pIn_nType            in number,
                                    pIn_nObjType         in number,
                                    pOut_nSqlCode        out number)
  
   as
    v_BetweenValCnt number;
    v_cnt           number;
  begin
    pOut_nSqlCode := 0;
  
    --if only one value specifed when the oprerator is <<.return immediately.
    select nvl(max(count(*)), 0)
      into v_BetweenValCnt
      from cdt c
     where c.prv12_em_addr = PIn_nAggregateRuleID
       and c.operant = 3
     group by c.rcd_cdt, c.n0_cdt
    having count(*) <> 2;
    if v_BetweenValCnt > 0 then
      return;
    end if;
  
    if pIn_vObjId is null and pIn_nObjType is null and pIn_nType = 1 then
      delete from rsp r
       where exists (select 1
                from prvsel l
               where l.prv15_em_addr = PIn_nAggregateRuleID
                 and r.sel13_em_addr = l.sel16_em_addr);
    end if;
  
    if pIn_nType = 0 then
    
      fmsp_execsql('truncate table AGG_DETAILNODE_info');
    end if;
    --insert all of the detail node of this rule into temp table tmp_detail_node
    sp_GenerateDetailNodeOfTheRule(p_AggRuleID  => PIn_nAggregateRuleID,
                                   pIn_vObjId   => pIn_vObjid,
                                   pIn_nObjType => pIn_nObjType,
                                   p_Sqlcode    => pOut_nSqlCode);
    if pOut_nSqlCode <> 0 then
      return;
    end if;
  
    if pIn_nType = 0 then
      select count(*)
        into v_cnt
        from AGG_DETAILNODE_info
       where aggregationid = PIn_nAggregateRuleID
         and rownum < 50;
    else
      select count(*)
        into v_cnt
        from aggregation_detailnode
       where aggregationid = PIn_nAggregateRuleID
         and rownum < 50; -- will compare this value to v_parallel_cnt
    end if;
  
    if v_cnt = 0 then
      return;
    end if;
    spprv_ProduceAggNodeAndLinks(P_AggregateRuleID => PIn_nAggregateRuleID,
                                 pIn_nType         => pIn_nType,
                                 p_sqlcode         => pOut_nSqlCode);
  
    --rerun
    /* for k in (select l.sel16_em_addr
                from prvsel l
               where l.prv15_em_addr = PIn_nAggregateRuleID
                 and not exists
               (select 1
                        from rsp r
                       where r.sel13_em_addr = l.sel16_em_addr)) loop
      p_selection.SP_ReBuildSelection(p_SelectionID => k.sel16_em_addr,
                                      p_SqlCode     => pOut_nSqlCode);
    end loop;*/
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      raise;
  end;

  --build AggregateNode??????|AggregateNodecondition??????|AggregateNode and DetailNode
  procedure SPPRV_BuildAggregateRule(P_AggregateRuleID     in number,
                                     p_TimeSeriesNoForUoM  in varchar2,
                                     p_TimeSeriesNoNomal   in varchar2,
                                     p_UoMConfig           in varchar2,
                                     p_IsUpdateAggregation in number default 0, -- 1 update aggregation call it
                                     p_Config              in varchar2 default '0', -- update aggregation options
                                     p_SqlCode             out number) as
  
  begin
    p_SqlCode := 0;
  
    --if only one value specifed when the oprerator is <<.return immediately.
    /*delete from rsp r
     where exists (select 1
              from prvsel l
             where l.prv15_em_addr = P_AggregateRuleID
               and r.sel13_em_addr = l.sel16_em_addr);
    commit;*/
  
    FMSP_BuildAggregate_Tmp(PIn_nAggregateRuleID => P_AggregateRuleID,
                            pIn_vObjId           => null,
                            pIn_nType            => 1,
                            pIn_nObjType         => null,
                            pOut_nSqlCode        => p_SqlCode);
    --summarize
    if p_IsUpdateAggregation = 1 then
      null; --if update aggregation sum all the datas in procedure SP_UpdateAggregation
    else
      P_Summarize.SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => P_AggregateRuleID,
                                                p_TimeSeriesNo    => p_TimeSeriesNoForUoM,
                                                p_UoMConfig       => p_UoMConfig,
                                                p_Config          => p_Config,
                                                p_SqlCode         => p_sqlcode);
    end if;
    if p_sqlcode <> 0 then
      raise e_summarize_err;
    end if;
    commit;
    p_SqlCode := 0;
  
  exception
    when e_parallel_err then
      p_sqlcode := -20998; --parallel err
      Fmp_log.LOGERROR;
    when e_summarize_err then
      p_sqlcode := -20997; --summarize err
      Fmp_log.LOGERROR;
    when others then
      p_SqlCode := sqlcode;
      Fmp_log.LOGERROR;
  end;

  --update all of the aggregation
  procedure SP_UpdateAggregation(p_TimeSeriesNoForUoM in varchar2,
                                 p_TimeSeriesNoNomal  in varchar2,
                                 p_UoMConfig          in varchar2,
                                 p_Config             in varchar2,
                                 p_SqlCode            out number) as
    v_SQLStmt      clob;
    v_SelRowsCnt   number;
    v_PvtRowsCnt   number;
    v_PipeName     varchar2(30);
    v_jobno        number;
    v_IsJobExisted number;
  begin
    p_SqlCode := 0;
    Fmp_log.FMP_SetValue(p_TimeSeriesNoForUoM);
    Fmp_log.FMP_SetValue(p_TimeSeriesNoNomal);
    Fmp_log.FMP_SetValue(p_UoMConfig);
    Fmp_log.FMP_SetValue(p_Config);
    Fmp_Log.LOGBEGIN;
  
    execute immediate 'truncate table rsp';
    -- process selection when update aggregation
    select count(*) into v_SelRowsCnt from sel t where t.sel_bud = 0;
    if v_SelRowsCnt > 0 then
      v_PipeName := dbms_pipe.unique_session_name;
      sp_AsyncExecTask(p_TaskName => 'begin p_aggregation.sp_PutTaskIntoPipe(''' ||
                                     v_PipeName || '''); end;',
                       p_sqlcode  => p_sqlcode,
                       p_JobNo    => v_jobno);
    end if;
  
    -- Parallel update aggregation
    select count(*) into v_PvtRowsCnt from prv;
    if v_PvtRowsCnt > 0 then
      v_SQLStmt := 'begin p_aggregation.spprv_updateAggregation(''' ||
                   p_TimeSeriesNoForUoM || ''',''' || p_TimeSeriesNoNomal ||
                   ''',''' || p_UoMConfig || ''',' || p_Config ||
                   ',:start_id, :end_id); end; ';
      p_ParallelTaskMgr.sp_ParallelExecTaskByCol(p_TaskName   => 'UpdateAggregation',
                                                 p_TableName  => 'PRV',
                                                 p_ColumnName => 'PRV_EM_ADDR',
                                                 p_ChunkSize  => 1,
                                                 p_SQLStmt    => v_SQLStmt,
                                                 p_sqlcode    => p_sqlcode);
    end if;
  
    -- check the bad aggregate node(Redo)
    delete from sel s
     where s.sel_bud = 71
       and not exists
     (select 1 from rsp r where r.sel13_em_addr = s.sel_em_addr);
    delete from aggregatenode_fullid a
     where not exists
     (select 1 from rsp r where r.sel13_em_addr = a.aggregatenodeid);
    delete from prvselpvt p
     where not exists (select 1 from rsp r where r.sel13_em_addr = p.selid);
    delete from prvsel p
     where not exists
     (select 1 from rsp r where r.sel13_em_addr = p.sel16_em_addr);
    commit;
    -- process bad aggregate node end;
  
    for j in (select prv_em_addr from prv order by prv_em_addr) loop
      P_Summarize.SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => j.prv_em_addr,
                                                p_TimeSeriesNo    => p_TimeSeriesNoForUoM,
                                                p_UoMConfig       => p_UoMConfig,
                                                p_Config          => p_Config,
                                                p_SqlCode         => p_sqlcode);
    end loop;
    p_SqlCode := 0;
    if v_SelRowsCnt > 0 then
      loop
        dbms_lock.sleep(1);
        select count(*)
          into v_IsJobExisted
          from user_jobs u
         where u.JOB = v_jobno;
        exit when v_IsJobExisted = 0;
      end loop;
    end if;
    Fmp_Log.LOGEND;
  exception
    when e_rebuildNormalSel_err then
      sp_ExecSql('alter trigger tr_rsp enable');
      Fmp_log.LOGERROR;
    when e_parallel_err then
      p_sqlcode := -20998;
      Fmp_log.LOGERROR;
    when others then
      p_Sqlcode := sqlcode;
      Fmp_log.LOGERROR;
  end;

  --delete AggregateNode of the sepecified aggregation rule and all datas related to the aggregate node
  procedure SP_DestroyAggregateRule(P_AggregateRuleID in number,
                                    p_SqlCode         out number) as
  begin
    p_SqlCode := 0;
    --delete AggregateNode to DetailNode link
    delete rsp
     where sel13_em_addr in
           (select sel16_em_addr
              from prvsel
             where prv15_em_addr = P_AggregateRuleID);
  
    --delete Aggregate node data
    delete from prb_m p
     where p.selid in (select sel16_em_addr
                         from prvsel
                        where prv15_em_addr = P_AggregateRuleID);
  
    delete from prb_w p
     where p.selid in (select sel16_em_addr
                         from prvsel
                        where prv15_em_addr = P_AggregateRuleID);
  
    --delete AggregateNode condition
    delete cdt
     where sel11_em_addr in
           (select sel16_em_addr
              from prvsel
             where prv15_em_addr = P_AggregateRuleID);
  
    --delete Selection or AggregateNode to AggregateNode attribute link
    delete selcrt
     where sel53_em_addr in
           (select sel16_em_addr
              from prvsel
             where prv15_em_addr = P_AggregateRuleID);
    --delete all datas link to bdg
    --don_supplier
    delete from don_supplier d
     where exists
     (select s.supplier118_em_addr
              from serie_supplier2 s
             where d.serie_supplier2119_em_addr = s.supplier118_em_addr
               and exists
             (select 1
                      from (select sp.supplier_em_addr
                              from supplier sp
                             where exists
                             (select b.bdg_em_addr
                                      from bdg b
                                     where b.id_bdg = 71
                                       and sp.pere_bdg = b.bdg_em_addr
                                       and exists
                                     (select s.sel_cle
                                              from sel s, prvsel l
                                             where s.sel_em_addr =
                                                   l.sel16_em_addr
                                               and l.prv15_em_addr =
                                                   P_AggregateRuleID
                                               and b.b_cle = s.sel_cle))
                            
                            union all
                            select sp.supplier_em_addr
                              from supplier sp
                             where exists
                             (select b.bdg_em_addr
                                      from bdg b
                                     where b.id_bdg = 71
                                       and sp.fils_bdg = b.bdg_em_addr
                                       and exists
                                     (select s.sel_cle
                                              from sel s, prvsel l
                                             where s.sel_em_addr =
                                                   l.sel16_em_addr
                                               and l.prv15_em_addr =
                                                   P_AggregateRuleID
                                               and b.b_cle = s.sel_cle))) t
                     where s.supplier118_em_addr = t.supplier_em_addr));
  
    --serie_supplier2
    delete from serie_supplier2 s
     where exists
     (select 1
              from (select sp.supplier_em_addr
                      from supplier sp
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and sp.pere_bdg = b.bdg_em_addr
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle))
                    
                    union all
                    select sp.supplier_em_addr
                      from supplier sp
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and sp.fils_bdg = b.bdg_em_addr
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle))) t
             where s.supplier118_em_addr = t.supplier_em_addr);
    --supplier
    delete from supplier sp
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and sp.pere_bdg = b.bdg_em_addr
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle));
    delete from supplier sp
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and sp.fils_bdg = b.bdg_em_addr
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle));
  
    --serinote
  
    delete from serinote t
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and t.bdg3_em_addr = b.bdg_em_addr
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle));
  
    --modscl
    delete modscl l
     where exists
     (select 1
              from mod m
             where exists (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and m.bdg30_em_addr = b.bdg_em_addr)
               and l.mod42_em_addr = m.mod_em_addr);
  
    --mod
    delete from mod m
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and m.bdg30_em_addr = b.bdg_em_addr);
  
    ----bud_M
  
    delete from bud_m m
     where exists
     (select 1
              from bgc c
             where exists (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and c.bdg31_em_addr = b.bdg_em_addr)
               and m.bdgid = c.bgc_em_addr);
    ----bud_W
    delete from bud_w m
     where exists
     (select 1
              from bgc c
             where exists (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and c.bdg31_em_addr = b.bdg_em_addr)
               and m.bdgid = c.bgc_em_addr);
  
    --bgc
    delete from bgc c
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and c.bdg31_em_addr = b.bdg_em_addr);
  
    ----timenote
    delete from timenote n
     where exists
     (select 1
              from typenote t
             where exists (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and t.bdg47_em_addr = b.bdg_em_addr)
               and n.typenote59_em_addr = t.typenote_em_addr);
  
    --typenote
    delete from typenote t
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and t.bdg47_em_addr = b.bdg_em_addr);
    --select_sel
    delete from select_sel ss
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and ss.bdg52_em_addr = b.bdg_em_addr);
  
    ----don_budget
  
    delete from don_budget d
     where exists
     (select 1
              from serie_budget sb
             where exists
             (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and sb.bdg70_em_addr = b.bdg_em_addr)
               and d.serie_budget71_em_addr = sb.serie_budget_em_addr);
  
    --serie_budget
    delete from serie_budget sb
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and sb.bdg70_em_addr = b.bdg_em_addr);
  
    --------bdg_task
  
    delete from bdg_task bt
     where exists
     (select 1
              from oftask s
             where exists
             (select 1
                      from ofdetail d
                     where exists
                     (select 1
                              from oftype o
                             where exists
                             (select b.bdg_em_addr
                                      from bdg b
                                     where b.id_bdg = 71
                                       and exists
                                     (select s.sel_cle
                                              from sel s, prvsel l
                                             where s.sel_em_addr =
                                                   l.sel16_em_addr
                                               and l.prv15_em_addr =
                                                   P_AggregateRuleID
                                               and b.b_cle = s.sel_cle)
                                       and o.bdg104_em_addr = b.bdg_em_addr)
                               and d.oftype105_em_addr = o.oftype_em_addr)
                       and s.ofdetail106_em_addr = d.ofdetail_em_addr)
               and bt.oftask108_em_addr = s.oftask_em_addr);
  
    ------oftask
    delete from oftask s
     where exists
     (select 1
              from ofdetail d
             where exists
             (select 1
                      from oftype o
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and o.bdg104_em_addr = b.bdg_em_addr)
                       and d.oftype105_em_addr = o.oftype_em_addr)
               and s.ofdetail106_em_addr = d.ofdetail_em_addr);
  
    ----ofdetail
  
    delete from ofdetail d
     where exists
     (select 1
              from oftype o
             where exists (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and o.bdg104_em_addr = b.bdg_em_addr)
               and d.oftype105_em_addr = o.oftype_em_addr);
  
    --oftype
    delete from oftype o
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and o.bdg104_em_addr = b.bdg_em_addr);
    ------oftask
    delete from oftask ot
     where exists
     (select 1
              from detailserie ds
             where exists
             (select 1
                      from detailtype dt
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and dt.bdg112_em_addr = b.bdg_em_addr)
                       and ds.detailtype113_em_addr = dt.detailtype_em_addr)
               and ot.detailserie110_em_addr = ds.detailserie_em_addr);
  
    ----detailserie
  
    delete from detailserie ot
     where exists
     (select 1
              from detailserie ds
             where exists
             (select 1
                      from detailtype dt
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and dt.bdg112_em_addr = b.bdg_em_addr)
                       and ds.detailtype113_em_addr = dt.detailtype_em_addr)
               and ot.detailserie114_em_addr = ds.detailserie_em_addr);
  
    commit;
  
    ------detailremark
    delete from detailremark dr
     where exists
     (select 1
              from detailserie ds
             where exists
             (select 1
                      from detailtype dt
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and dt.bdg112_em_addr = b.bdg_em_addr)
                       and ds.detailtype113_em_addr = dt.detailtype_em_addr)
               and dr.detailserie115_em_addr = ds.detailserie_em_addr);
  
    ----detailserie
  
    delete from detailserie ds
     where exists
     (select 1
              from detailtype dt
             where exists
             (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and dt.bdg112_em_addr = b.bdg_em_addr)
               and ds.detailtype113_em_addr = dt.detailtype_em_addr);
  
    --detailtype
    delete from detailtype dt
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and dt.bdg112_em_addr = b.bdg_em_addr);
  
    ------detail_sequence
    delete from detail_sequence se
     where exists
     (select 1
              from detail_sequence ds
             where exists
             (select 1
                      from type_sequence ts
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and ts.bdg134_em_addr = b.bdg_em_addr)
                       and ds.type_sequence135_em_addr =
                           ts.type_sequence_em_addr)
               and se.detail_sequence136_em_addr =
                   ds.detail_sequence_em_addr);
  
    delete from detail_sequence se
     where exists
     (select 1
              from detail_sequence ds
             where exists
             (select 1
                      from type_sequence ts
                     where exists
                     (select b.bdg_em_addr
                              from bdg b
                             where b.id_bdg = 71
                               and exists
                             (select s.sel_cle
                                      from sel s, prvsel l
                                     where s.sel_em_addr = l.sel16_em_addr
                                       and l.prv15_em_addr = P_AggregateRuleID
                                       and b.b_cle = s.sel_cle)
                               and ts.bdg134_em_addr = b.bdg_em_addr)
                       and ds.type_sequence135_em_addr =
                           ts.type_sequence_em_addr)
               and se.detail_sequence137_em_addr =
                   ds.detail_sequence_em_addr);
  
    ----detail_sequence
  
    delete from detail_sequence ds
     where exists
     (select 1
              from type_sequence ts
             where exists
             (select b.bdg_em_addr
                      from bdg b
                     where b.id_bdg = 71
                       and exists
                     (select s.sel_cle
                              from sel s, prvsel l
                             where s.sel_em_addr = l.sel16_em_addr
                               and l.prv15_em_addr = P_AggregateRuleID
                               and b.b_cle = s.sel_cle)
                       and ts.bdg134_em_addr = b.bdg_em_addr)
               and ds.type_sequence135_em_addr = ts.type_sequence_em_addr);
    --type_sequence
    delete from type_sequence ts
     where exists
     (select b.bdg_em_addr
              from bdg b
             where b.id_bdg = 71
               and exists (select s.sel_cle
                      from sel s, prvsel l
                     where s.sel_em_addr = l.sel16_em_addr
                       and l.prv15_em_addr = P_AggregateRuleID
                       and b.b_cle = s.sel_cle)
               and ts.bdg134_em_addr = b.bdg_em_addr);
  
    --bdg
    delete from bdg b
     where b.id_bdg = 71
       and b.b_cle in
           (select s.sel_cle
              from sel s, prvsel l
             where s.sel_em_addr = l.sel16_em_addr
               and l.prv15_em_addr = P_AggregateRuleID);
  
    --delete AggregateNode
    delete sel
     where sel_em_addr in
           (select sel16_em_addr
              from prvsel
             where prv15_em_addr = P_AggregateRuleID);
  
    --delete AggregateRule to AggregateNode link
    delete prvsel where prv15_em_addr = P_AggregateRuleID;
  
    delete from aggregatenode_fullid a
     where a.aggregationid = P_AggregateRuleID;
    delete from prvselpvt t where t.prvid = P_AggregateRuleID;
    commit;
  exception
    when others then
      p_SqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_Constant.e_oraerr, p_sqlcode || sqlerrm);
    
  end;

  procedure SP_GetAggregateNodes(P_AggregateRuleID  in number,
                                 pIn_bNeedCreateTab in boolean default false,
                                 P_Sequence         in varchar2, --Sort sequence
                                 p_AggregateNode    out sys_refcursor,
                                 pOut_vTabName      out varchar2,
                                 p_SqlCode          out number) as
  begin
    FMP_GetAggregateNodes.FMCSP_GetAggregateNodes(pIn_nAggregationID   => P_AggregateRuleID,
                                                  pIn_bNeedCreateTab   => pIn_bNeedCreateTab,
                                                  pIn_vSequence        => P_Sequence,
                                                  pOut_rAggregateNodes => p_AggregateNode,
                                                  pOut_vTabName        => pOut_vTabName,
                                                  pOut_nSqlCode        => p_SqlCode);
  end;

  procedure SP_GetAggNodesByConditions(P_AggregateRuleID  in number,
                                       pIn_bNeedCreateTab in boolean default false,
                                       P_Conditions       in varchar2,
                                       P_Sequence         in varchar2, --Sort sequence
                                       p_AggregateNode    out nocopy sys_refcursor,
                                       pOut_vTabName      out varchar2,
                                       p_SqlCode          out number) as
  
  begin
    FMP_GetAggregateNodes.FMCSP_GetAggNodesByConditions(pIn_nAggregationID   => P_AggregateRuleID,
                                                        pIn_bNeedCreateTab   => pIn_bNeedCreateTab,
                                                        pIn_vConditions      => P_Conditions,
                                                        pIn_vSequence        => P_Sequence,
                                                        pOut_rAggregateNodes => p_AggregateNode,
                                                        pOut_vTabName        => pOut_vTabName,
                                                        pOu8t_nSqlCode       => p_SqlCode);
  end;

  procedure spprv_updateAggregation(p_TimeSeriesNoForUoM in varchar2,
                                    p_TimeSeriesNoNomal  in varchar2,
                                    p_UoMConfig          in varchar2,
                                    p_Config             in varchar2,
                                    p_Startid            in number,
                                    P_Endid              in number) is
    v_sqlcode number;
  begin
    for x in (select p.prv_em_addr
                from prv p
               where prv_em_addr between p_Startid and P_Endid) loop
      p_aggregation.SPPRV_BuildAggregateRule(P_AggregateRuleID     => x.prv_em_addr,
                                             p_TimeSeriesNoForUoM  => p_TimeSeriesNoForUoM,
                                             p_TimeSeriesNoNomal   => p_TimeSeriesNoNomal,
                                             p_UoMConfig           => p_UoMConfig,
                                             p_Config              => p_Config,
                                             p_IsUpdateAggregation => 1,
                                             p_SqlCode             => v_sqlcode);
    end loop;
  end;

  procedure sp_GenerateDetailNodeOfTheRule(p_AggRuleID  in number,
                                           pIn_vObjId   in varchar2,
                                           pIn_nObjType in number,
                                           p_Sqlcode    out number) as
  
    v_p_level number;
    v_s_level number;
    v_t_level number;
  
    v_AttrID   number;
    v_Ope      number;
    v_Sql_Part varchar2(2000);
    --sql for select all the pvt of the aggregation rule
    v_sql                 clob;
    v_Sql_BetweenPart     clob;
    v_sql_product         clob := ' (select fam_em_addr from v_productattrvalue where isleaf =1  and id_fam=80';
    v_sql_saleterritory   clob := ' (select geo_em_addr from v_SaleTerritoryAttrValue where isleaf =1 ';
    v_sql_tradechannel    clob := ' (select dis_em_addr from v_tradechannelattrvalue where isleaf =1 ';
    v_sql_detailnode      clob;
    v_sql_detailnode_Norm clob := ' insert /*+ append */ into aggregation_detailnode(aggregationid,DetailNodeID) select ' ||
                                  p_AggRuleID ||
                                  ', pvt_em_addr
                                            from v_detailnodeattrvalue d where 1=1 ';
  
    v_sql_detailnode_OBJ clob := ' insert /*+ append */ into AGG_DETAILNODE_info(aggregationid,DetailNodeID) select ' ||
                                 p_AggRuleID ||
                                 ', pvt_em_addr
                                            from v_detailnodeattrvalue d where 1=1 ';
  
    v_p_key_cdt_cnt number; -- key specified in the rule
    v_s_key_cdt_cnt number;
    v_t_key_cdt_cnt number;
  
    v_Begin varchar2(200);
    v_End   varchar2(200);
  
    v_IsBalance_flag number; --identify the children is balance or not
    v_Is2DDefined    number := 0;
    v_Is3DDefined    number := 0;
    v_IsWhere        varchar2(100);
  
  begin
    if pIn_vObjId is not null then
      if pIn_nObjType = 1 then
        v_IsWhere := ' and d.fam4_em_addr= ' || pIn_vObjId;
      elsif (pIn_nObjType = 2) then
        v_IsWhere := ' and d.geo5_em_addr= ' || pIn_vObjId;
      elsif (pIn_nObjType = 3) then
        v_IsWhere := ' and d.dis6_em_addr= ' || pIn_vObjId;
      elsif (pIn_nObjType = 4) then
        v_IsWhere := ' and d.pvt_em_addr = ' || pIn_vObjId;
      end if;
    
      v_sql_detailnode := v_sql_detailnode_OBJ || v_IsWhere;
    else
      v_sql_detailnode := v_sql_detailnode_Norm;
    end if;
    --target  select all the requried detail node.
    --the level defined in the aggregation rule.
    select p.regroup_pro, p.regroup_geo, p.regroup_dis
      into v_p_level, v_s_level, v_t_level
      from prv p
     where p.prv_em_addr = p_AggRuleID;
  
    delete from aggregation_detailnode where aggregationid = p_AggRuleID;
    commit;
  
    --- proces cst
    for i in (select c.rcd_cdt tabid, c.n0_cdt + 49 attrno
                from cdt c
               where c.operant = 4
                 and c.prv12_em_addr = p_AggRuleID
               order by decode(c.rcd_cdt, 10055, 20055, c.rcd_cdt)) loop
      case i.tabid
        when 20007 then
          v_sql_product := v_sql_product || ' and c' || i.attrno ||
                           ' in  (select vct_em_addr from vct where id_crt=80 and num_crt=' ||
                           i.attrno || ')';
        when 20008 then
          v_Is2DDefined       := 1;
          v_sql_saleterritory := v_sql_saleterritory || ' and c' ||
                                 i.attrno ||
                                 ' in  (select vct_em_addr from vct where id_crt=71 and num_crt=' ||
                                 i.attrno || ')';
        when 20009 then
          v_Is3DDefined      := 1;
          v_sql_tradechannel := v_sql_tradechannel || ' and c' || i.attrno ||
                                ' in  (select vct_em_addr from vct where id_crt=68 and num_crt=' ||
                                i.attrno || ')';
        when 10055 then
          v_sql_detailnode := v_sql_detailnode || ' and d.c' || i.attrno ||
                              ' in  (select crtserie_em_addr from crtserie where id_crt_serie=83 and
                        num_crt_serie=' ||
                              i.attrno || ')';
      end case;
    end loop;
    -- process the product ,salte territory ,trade channel one by one
    --if the level is any.and consider the key is specified or not
  
    ---------<<Process product>>-----------------
    --loop all the operator (=,<>,<<)defined in product's attributes .as the cdt to find the required level product
    for k in (select distinct t.n0_cdt
                from cdt t
               where t.prv12_em_addr = p_AggRuleID
                 and t.operant <> 4
                 and t.rcd_cdt = 20007) loop
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_AttrID, v_Ope, v_Sql_Part
        from (select c.n0_cdt + 49 attrordno,
                     c.operant ope,
                     c.adr_cdt addr,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) - 1 prev
                from cdt c
               where c.prv12_em_addr = p_AggRuleID
                 and c.operant <> 4
                 and c.rcd_cdt = 20007
                 and c.n0_cdt = k.n0_cdt) t
       group by t.attrordno, t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
    
      case v_Ope
        when 1 then
          v_sql_product := v_sql_product || ' and c' || v_AttrID || ' in ' ||
                           v_Sql_Part;
        when 2 then
          v_sql_product := v_sql_product || ' and c' || v_AttrID ||
                           ' not in ' || v_Sql_Part;
        when 3 then
          begin
            select '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Sql_BetweenPart
              from (select v.vct_em_addr addr,
                           v.num_crt,
                           row_number() over(partition by v.num_crt order by v.val) cur,
                           row_number() over(partition by v.num_crt order by v.val) - 1 prev
                      from vct v
                     where v.id_crt = 80
                       and v.num_crt = v_AttrID
                       and v.val between
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, '(', ',')) and
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, ',', ')'))) t
             group by t.num_crt
             start with t.cur = 1
            connect by prior t.cur = t.prev;
            v_sql_product := v_sql_product || ' and c' || v_AttrID ||
                             '  in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null; ---- >>>>ToDo record the error
            when others then
              p_sqlcode := sqlcode; ---- >>>>ToDo record the error
          end;
      end case;
    end loop;
    --didnot specfy level in the rule
    if v_p_level = 0 then
      select count(*)
        into v_p_key_cdt_cnt
        from cdt c
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10000;
      if v_p_key_cdt_cnt > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10000) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case v_Ope
          when 1 then
            v_sql_product := v_sql_product || ' start with fam_em_addr in ' ||
                             v_Sql_Part ||
                             ' connect by prior fam_em_addr=fam0_em_addr ';
          when 2 then
            v_sql_product := v_sql_product ||
                             ' start with nlevel =(select max(nlevel) from v_fam_tree where fam_em_addr in ' ||
                             v_Sql_Part || ')
                                and  fam_em_addr not in ' ||
                             v_Sql_Part ||
                             ' connect by prior fam_em_addr=fam0_em_addr ';
          when 3 then
            select f_cle
              into v_Begin
              from fam f
             where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select f_cle
              into v_End
              from fam f
             where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_product := v_sql_product ||
                             ' start with f_cle between ''' || v_Begin ||
                             ''' and ''' || v_End ||
                             ''' and nlevel=(select nlevel from v_fam_tree where f_cle= ''' ||
                             v_Begin ||
                             ''')  connect by prior fam_em_addr=fam0_em_addr';
        end case;
      end if;
    else
      -- if the level specified in the rule is not any.(1,2,3....)
      -- if key is specified .and must assume that the level of the key specified is higher then the level in the rule
      select count(*)
        into v_p_key_cdt_cnt
        from cdt c, v_fam_tree v
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10000
         and v.fam_em_addr = c.adr_cdt
         and v.nlevel >= v_p_level
         and rownum < 2;
      if v_p_key_cdt_cnt > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10000) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
      
        --execute immediate 'truncate table tmp_fam_tree';
        delete from tmp_fam_tree where ruleid = p_AggRuleID;
        if v_p_level <> 1 then
          --case 1 when the level is not 1 and the tree is balance ,process it as normal
          --case 2 when the level is not 1 and the tree is not balance raise exception
          case v_ope
            when 1 then
              execute immediate 'select count(distinct nlevel) from  v_fam_tree f  where isleaf=1 start with f.fam_em_addr in ' ||
                                v_Sql_Part || '
                    connect by prior f.fam_em_addr = f.fam0_em_addr'
                into v_IsBalance_flag;
            
            when 2 then
              execute immediate 'select count(distinct nlevel) from  v_fam_tree f  where isleaf=1
               start with f.nlevel =(select max(nlevel) from v_fam_tree where fam_em_addr in ' ||
                                v_Sql_Part || ') and f.fam_em_addr not in ' ||
                                v_Sql_Part || '
                    connect by prior f.fam_em_addr = f.fam0_em_addr'
                into v_IsBalance_flag;
            when 3 then
              select f_cle
                into v_Begin
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select f_cle
                into v_End
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
              execute immediate 'select count(distinct nlevel) from  v_fam_tree f   where isleaf=1  start with f_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_fam_tree where f_cle= ''' ||
                                v_Begin ||
                                ''')  connect by prior fam_em_addr=fam0_em_addr'
                into v_IsBalance_flag;
            
          end case;
          if v_IsBalance_flag <> 1 then
            raise_application_error(-20001,
                                    'Unbalance tree Of Product.Only level 1 supported');
          end if;
        end if;
      
        -- case 3 specify level =1  ignore whether the tree is balance or not (the same process case 1)
        if v_p_level = 1 or (v_p_level <> 1 and v_IsBalance_flag = 1) then
          case v_ope
            when 1 then
              execute immediate 'insert into tmp_fam_tree (ruleid,fam_em_addr, id_fam, f_cle, f_desc, fam0_em_addr, grp, isleaf, nlevel)
          select ' || p_AggRuleID ||
                                ',f.fam_em_addr, f.id_fam, f.f_cle, f.f_desc, f.fam0_em_addr, grp, isleaf, nlevel
            from v_fam_tree f start with f.fam_em_addr in ' ||
                                v_Sql_Part || '
          connect by prior f.fam_em_addr = f.fam0_em_addr';
            when 2 then
              execute immediate 'insert into tmp_fam_tree (ruleid,fam_em_addr, id_fam, f_cle, f_desc, fam0_em_addr, grp, isleaf, nlevel)
          select ' || p_AggRuleID ||
                                ',f.fam_em_addr, f.id_fam, f.f_cle, f.f_desc, f.fam0_em_addr, grp, isleaf, nlevel
            from v_fam_tree f start with f.nlevel =(select max(nlevel) from v_fam_tree where  fam_em_addr in ' ||
                                v_Sql_Part ||
                                ' ) and  f.fam_em_addr not in ' ||
                                v_Sql_Part ||
                                ' connect by prior f.fam_em_addr=f.fam0_em_addr';
            when 3 then
              select f_cle
                into v_Begin
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select f_cle
                into v_End
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
              execute immediate 'insert into tmp_fam_tree (ruleid,fam_em_addr, id_fam, f_cle, f_desc, fam0_em_addr, grp, isleaf, nlevel)
          select ' || p_AggRuleID ||
                                ',f.fam_em_addr, f.id_fam, f.f_cle, f.f_desc, f.fam0_em_addr, grp, isleaf, nlevel
            from v_fam_tree f start with f_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_fam_tree where  f_cle= ''' ||
                                v_Begin ||
                                ''')    connect by prior fam_em_addr=fam0_em_addr ';
          end case;
          v_sql_product := v_sql_product ||
                           ' and fam_em_addr in (select fam_em_addr from tmp_fam_tree where ruleid = ' ||
                           p_AggRuleID || ' and isleaf=1)';
        end if;
      
      else
        -- no key
        -- if the tree is not balance .only process level 1
        v_IsBalance_flag := 0;
        sp_isbalancetree(p_Demension => 1,
                         p_Rootid    => 1,
                         P_IsBalance => v_IsBalance_flag,
                         p_sqlcode   => p_sqlcode);
        if v_IsBalance_flag = 1 then
          v_sql_product := v_sql_product || ' start with  nlevel=' ||
                           v_p_level ||
                           ' connect by prior fam_em_addr=fam0_em_addr ';
        else
          if v_p_level <= 1 then
            null;
          else
            raise_application_error(-20001,
                                    'Unbalance tree Of Product.Only level 1 supported');
          end if;
        end if;
      end if;
    end if;
  
    ---------<<Process sale territory>>-----------------
    for k in (select distinct t.n0_cdt
                from cdt t
               where t.prv12_em_addr = p_AggRuleID
                 and t.operant <> 4
                 and t.rcd_cdt = 20008) loop
      v_Is2DDefined := 1;
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_AttrID, v_Ope, v_Sql_Part
        from (select c.n0_cdt + 49 attrordno,
                     c.operant ope,
                     c.adr_cdt addr,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) - 1 prev
                from cdt c
               where c.prv12_em_addr = p_AggRuleID
                 and c.operant <> 4
                 and c.rcd_cdt = 20008
                 and c.n0_cdt = k.n0_cdt) t
       group by t.attrordno, t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
    
      case v_Ope
        when 1 then
          v_sql_saleterritory := v_sql_saleterritory || ' and c' ||
                                 v_AttrID || ' in ' || v_Sql_Part;
        when 2 then
          v_sql_saleterritory := v_sql_saleterritory || ' and c' ||
                                 v_AttrID || ' not in ' || v_Sql_Part;
        when 3 then
          begin
            select '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Sql_BetweenPart
              from (select v.vct_em_addr addr,
                           v.num_crt,
                           row_number() over(partition by v.num_crt order by v.val) cur,
                           row_number() over(partition by v.num_crt order by v.val) - 1 prev
                      from vct v
                     where v.id_crt = 71
                       and v.num_crt = v_AttrID
                       and v.val between
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, '(', ',')) and
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, ',', ')'))) t
             group by t.num_crt
             start with t.cur = 1
            connect by prior t.cur = t.prev;
            v_sql_saleterritory := v_sql_saleterritory || ' and c' ||
                                   v_AttrID || '  in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
            when others then
              p_sqlcode := sqlcode;
              --record it;
          end;
      end case;
    end loop;
  
    if v_s_level = 0 then
      select count(*)
        into v_s_key_cdt_cnt
        from cdt c
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10001;
      if v_s_key_cdt_cnt > 0 then
        v_Is2DDefined := 1;
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10001) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
      
        case v_Ope
          when 1 then
            v_sql_saleterritory := v_sql_saleterritory ||
                                   ' start with geo_em_addr in ' ||
                                   v_Sql_Part ||
                                   ' connect by prior geo_em_addr=geo1_em_addr ';
          when 2 then
            v_sql_saleterritory := v_sql_saleterritory ||
                                   ' start with nlevel =(select max(nlevel) from v_geo_tree where geo_em_addr in ' ||
                                   v_Sql_Part || ')
                                      and  geo_em_addr not in ' ||
                                   v_Sql_Part ||
                                   ' connect by prior geo_em_addr=geo1_em_addr ';
          when 3 then
            select g_cle
              into v_Begin
              from geo g
             where g.geo_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select g_cle
              into v_End
              from geo g
             where g.geo_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_saleterritory := v_sql_saleterritory ||
                                   ' start with g_cle between ''' ||
                                   v_Begin || ''' and ''' || v_End ||
                                   ''' and nlevel=(select nlevel from v_geo_tree where g_cle= ''' ||
                                   v_Begin ||
                                   ''')  connect by prior geo_em_addr=geo1_em_addr';
        end case;
      end if;
    else
      v_Is2DDefined := 1;
      select count(*)
        into v_s_key_cdt_cnt
        from cdt c, v_geo_tree v
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10001
         and v.geo_em_addr = c.adr_cdt
         and v.nlevel >= v_s_level
         and rownum < 2;
      if v_s_key_cdt_cnt > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10001) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
      
        delete from tmp_geo_tree where ruleid = p_AggRuleID;
        v_IsBalance_flag := 0;
        if v_s_level <> 1 then
          --case 1 when the level is not 1 and the tree is balance ,process it al normal
          --case 2 when the level is not 1 and the tree is not balance raise exception
          case v_ope
            when 1 then
              execute immediate 'select count(distinct nlevel) from  v_geo_tree g  where isleaf=1
              start with g.geo_em_addr in ' ||
                                v_Sql_Part || '
                    connect by prior g.geo_em_addr = g.geo1_em_addr'
                into v_IsBalance_flag;
            
            when 2 then
              execute immediate 'select count(distinct nlevel) from  v_geo_tree g where isleaf=1
               start with g.nlevel =(select max(nlevel) from v_geo_tree where geo_em_addr in ' ||
                                v_Sql_Part || ') and g.geo_em_addr not in ' ||
                                v_Sql_Part || '
                    connect by prior g.geo_em_addr = g.geo1_em_addr'
                into v_IsBalance_flag;
            when 3 then
              select f_cle
                into v_Begin
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select f_cle
                into v_End
                from fam f
               where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
              execute immediate 'select count(distinct nlevel) from  v_geo_tree g  where isleaf=1  start with g_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_geo_tree where g_cle= ''' ||
                                v_Begin ||
                                ''')  connect by prior g.geo_em_addr=g.geo1_em_addr'
                into v_IsBalance_flag;
            
          end case;
          if v_IsBalance_flag <> 1 then
            raise_application_error(-20002,
                                    'Unbalance tree Of SaleTerritory.Only level 1 supported');
          end if;
        end if;
        if v_s_level = 1 or (v_s_level <> 1 and v_IsBalance_flag = 1) then
          case v_ope
            when 1 then
              execute immediate 'insert into tmp_geo_tree(ruleid,geo_em_addr, g_cle, g_desc, geo1_em_addr, grp, isleaf, nlevel)
               select ' || p_AggRuleID ||
                                ', g.geo_em_addr, g.g_cle, g.g_desc, g.geo1_em_addr, grp, isleaf, nlevel
               from v_geo_tree g start with g.geo_em_addr in ' ||
                                v_Sql_Part ||
                                ' connect by prior g.geo_em_addr = g.geo1_em_addr';
            when 2 then
              execute immediate 'insert into tmp_geo_tree(ruleid,geo_em_addr, g_cle, g_desc, geo1_em_addr, grp, isleaf, nlevel)
               select ' || p_AggRuleID ||
                                ', g.geo_em_addr, g.g_cle, g.g_desc, g.geo1_em_addr, grp, isleaf, nlevel
               from v_geo_tree g start with g.nlevel =(select max(nlevel) from v_geo_tree where  geo_em_addr in ' ||
                                v_Sql_Part ||
                                ' ) and  g.geo_em_addr not in ' ||
                                v_Sql_Part ||
                                ' connect by prior g.geo_em_addr=g.geo1_em_addr';
            when 3 then
              select g_cle
                into v_Begin
                from geo f
               where f.geo_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select g_cle
                into v_End
                from geo f
               where f.geo_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            
              execute immediate ' insert into tmp_geo_tree(ruleid,geo_em_addr, g_cle, g_desc, geo1_em_addr, grp, isleaf, nlevel)
               select ' || p_AggRuleID ||
                                ', g.geo_em_addr, g.g_cle, g.g_desc, g.geo1_em_addr, grp, isleaf, nlevel
               from v_geo_tree g start with g_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_geo_tree where  g_cle= ''' ||
                                v_Begin ||
                                ''')    connect by prior geo_em_addr=geo1_em_addr ';
          end case;
          v_sql_saleterritory := v_sql_saleterritory ||
                                 ' and geo_em_addr in (select geo_em_addr from tmp_geo_tree where ruleid=' ||
                                 p_AggRuleID || ' and  nlevel=1)';
        end if;
      else
        -- no key
        v_IsBalance_flag := 0;
        sp_isbalancetree(p_Demension => 2,
                         p_Rootid    => 1,
                         P_IsBalance => v_IsBalance_flag,
                         p_sqlcode   => p_sqlcode);
        if v_IsBalance_flag = 1 then
          v_sql_saleterritory := v_sql_saleterritory ||
                                 ' start with  nlevel=' || v_s_level ||
                                 ' connect by prior geo_em_addr=geo1_em_addr ';
        else
          if v_s_level <= 1 then
            null;
          else
            raise_application_error(-20002,
                                    'Unbalance tree of SaleTerritory.Only level 1 supported');
          end if;
        end if;
      
      end if;
    end if;
  
    ---------<<Process trade channel>>-----------------
    for k in (select distinct t.n0_cdt
                from cdt t
               where t.prv12_em_addr = p_AggRuleID
                 and t.operant <> 4
                 and t.rcd_cdt = 20009) loop
      v_Is3DDefined := 1;
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_AttrID, v_Ope, v_Sql_Part
        from (select c.n0_cdt + 49 attrordno,
                     c.operant ope,
                     c.adr_cdt addr,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) - 1 prev
                from cdt c
               where c.prv12_em_addr = p_AggRuleID
                 and c.operant <> 4
                 and c.rcd_cdt = 20009
                 and c.n0_cdt = k.n0_cdt) t
       group by t.attrordno, t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
      case v_Ope
        when 1 then
          v_sql_tradechannel := v_sql_tradechannel || ' and c' || v_AttrID ||
                                ' in ' || v_Sql_Part;
        when 2 then
          v_sql_tradechannel := v_sql_tradechannel || ' and c' || v_AttrID ||
                                ' not in ' || v_Sql_Part;
        when 3 then
          begin
            select '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Sql_BetweenPart
              from (select v.vct_em_addr addr,
                           v.num_crt,
                           row_number() over(partition by v.num_crt order by v.val) cur,
                           row_number() over(partition by v.num_crt order by v.val) - 1 prev
                      from vct v
                     where v.id_crt = 68
                       and v.num_crt = v_AttrID
                       and v.val between
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, '(', ',')) and
                           (select val
                              from vct
                             where vct_em_addr =
                                   f_GetStr(v_Sql_Part, ',', ')'))) t
             group by t.num_crt
             start with t.cur = 1
            connect by prior t.cur = t.prev;
            v_sql_tradechannel := v_sql_tradechannel || ' and c' ||
                                  v_AttrID || '  in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
            when others then
              p_sqlcode := sqlcode;
              --record it;
          end;
      end case;
    end loop;
  
    if v_t_level = 0 then
      select count(*)
        into v_s_key_cdt_cnt
        from cdt c
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10002;
      if v_t_key_cdt_cnt > 0 then
        v_Is3DDefined := 1;
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10002) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
      
        case v_Ope
          when 1 then
            v_sql_tradechannel := v_sql_tradechannel ||
                                  ' start with dis_em_addr in ' ||
                                  v_Sql_Part ||
                                  ' connect by prior dis_em_addr=dis2_em_addr ';
          when 2 then
            v_sql_tradechannel := v_sql_tradechannel ||
                                  ' start with nlevel =(select max(nlevel) from v_dis_tree where dis_em_addr in ' ||
                                  v_Sql_Part || ')
                                      and  dis_em_addr not in ' ||
                                  v_Sql_Part ||
                                  ' connect by prior dis_em_addr=dis2_em_addr ';
          when 3 then
            select d_cle
              into v_Begin
              from dis g
             where g.dis_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select d_cle
              into v_End
              from dis g
             where g.dis_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_tradechannel := v_sql_tradechannel ||
                                  ' start with g_cle between ''' || v_Begin ||
                                  ''' and ''' || v_End ||
                                  ''' and nlevel=(select nlevel from v_dis_tree where g_cle= ''' ||
                                  v_Begin ||
                                  ''')  connect by prior dis_em_addr=dis2_em_addr';
        end case;
      end if;
    else
      v_Is3DDefined := 1;
      select count(*)
        into v_t_key_cdt_cnt
        from cdt c, v_dis_tree v
       where c.prv12_em_addr = p_AggRuleID
         and c.rcd_cdt = 10002
         and v.dis_em_addr = c.adr_cdt
         and v.nlevel >= v_t_level
         and rownum < 2;
      if v_t_key_cdt_cnt > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = p_AggRuleID
                   and c.rcd_cdt = 10002) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        delete from tmp_dis_tree where ruleid = p_AggRuleID;
        v_IsBalance_flag := 0;
        if v_t_level <> 1 then
          --case 1 when the level is not 1 and the tree is balance ,process it al normal
          --case 2 when the level is not 1 and the tree is not balance raise exception
          case v_ope
            when 1 then
              execute immediate 'select count(distinct nlevel) from  v_dis_tree d  where isleaf=1
               start with d.dis_em_addr in ' ||
                                v_Sql_Part || '
                    connect by prior d.dis_em_addr = d.dis2_em_addr'
                into v_IsBalance_flag;
            
            when 2 then
              execute immediate 'select count(distinct nlevel) from  v_dis_tree d where isleaf=1
               start with d.nlevel =(select max(nlevel) from v_dis_tree where dis_em_addr in ' ||
                                v_Sql_Part || ') and d.dis_em_addr not in ' ||
                                v_Sql_Part || '
                    connect by prior d.dis_em_addr = d.dis2_em_addr'
                into v_IsBalance_flag;
            when 3 then
              select d_cle
                into v_Begin
                from dis f
               where f.dis_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select d_cle
                into v_End
                from dis f
               where f.dis_em_addr = f_GetStr(v_Sql_Part, ',', ')');
              execute immediate 'select count(distinct nlevel) from  v_dis_tree d  where isleaf=1  start with d_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_dis_tree where d_cle= ''' ||
                                v_Begin ||
                                ''')  connect by prior d.dis_em_addr=d.dis2_em_addr'
                into v_IsBalance_flag;
            
          end case;
          if v_IsBalance_flag <> 1 then
            raise_application_error(-20003,
                                    'Unbalance tree Of TradeChannel.Only level 1 supported');
          end if;
        end if;
        if v_t_level = 1 or (v_t_level <> 1 and v_IsBalance_flag = 1) then
          case v_ope
            when 1 then
              execute immediate 'insert into tmp_dis_tree(ruleid,dis_em_addr, d_cle, d_desc, dis2_em_addr, grp, isleaf, nlevel)
               select ' || p_AggRuleID ||
                                ', g.dis_em_addr, g.d_cle, g.d_desc, g.dis2_em_addr, grp, isleaf, nlevel
               from v_dis_tree g start with g.dis_em_addr in ' ||
                                v_Sql_Part ||
                                ' connect by prior g.dis_em_addr = g.dis2_em_addr';
            when 2 then
              execute immediate 'insert into tmp_dis_tree(ruleid,dis_em_addr, d_cle, d_desc, dis2_em_addr, grp, isleaf, nlevel)
               select ' || p_AggRuleID ||
                                ', g.dis_em_addr, g.d_cle, g.d_desc, g.dis2_em_addr, grp, isleaf, nlevel
               from v_dis_tree g start with g.nlevel =(select max(nlevel) from v_dis_tree where  dis_em_addr in ' ||
                                v_Sql_Part ||
                                ' ) and  g.dis_em_addr not in ' ||
                                v_Sql_Part ||
                                ' connect by prior g.dis_em_addr=g.dis2_em_addr';
            when 3 then
              select d_cle
                into v_Begin
                from dis f
               where f.dis_em_addr = f_GetStr(v_Sql_Part, '(', ',');
              select d_cle
                into v_End
                from dis f
               where f.dis_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            
              execute immediate 'insert into tmp_dis_tree(ruleid,dis_em_addr, d_cle, d_desc, dis2_em_addr, grp, isleaf, nlevel)
                 select ' || p_AggRuleID ||
                                ', d.dis_em_addr,d.d_cle, d.d_desc, d.dis2_em_addr, grp, isleaf, nlevel
            from v_dis_tree d start with d_cle between ''' ||
                                v_Begin || ''' and ''' || v_End ||
                                ''' and nlevel=(select nlevel from v_dis_tree where  d_cle= ''' ||
                                v_Begin ||
                                ''')    connect by prior dis_em_addr=dis2_em_addr ';
          end case;
          v_sql_tradechannel := v_sql_tradechannel ||
                                ' and dis_em_addr in (select dis_em_addr from tmp_dis_tree where ruleid=' ||
                                p_AggRuleID || ' and  nlevel=1)';
        end if;
      else
        -- no key
        v_IsBalance_flag := 0;
        sp_isbalancetree(p_Demension => 3,
                         p_Rootid    => 1,
                         P_IsBalance => v_IsBalance_flag,
                         p_sqlcode   => p_sqlcode);
        if v_IsBalance_flag = 1 then
          v_sql_tradechannel := v_sql_tradechannel ||
                                ' start with  nlevel=' || v_t_level ||
                                ' connect by prior dis_em_addr=dis2_em_addr ';
        else
          if v_t_level <= 1 then
            null;
          else
            raise_application_error(-20003,
                                    'Unbalance tree of TradeChannel.Only level 1 supported');
          end if;
        end if;
      end if;
    end if;
    ---------<<Process conditions of detail node>>-----------------
    for k in (select distinct t.n0_cdt
                from cdt t
               where t.prv12_em_addr = p_AggRuleID
                 and t.operant <> 4
                 and t.rcd_cdt = 10055) loop
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_AttrID, v_Ope, v_Sql_Part
        from (select c.n0_cdt + 49 attrordno,
                     c.operant ope,
                     c.adr_cdt addr,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) cur,
                     row_number() over(partition by c.operant order by c.n0_val_cdt) - 1 prev
                from cdt c
               where c.prv12_em_addr = p_AggRuleID
                 and c.operant <> 4
                 and c.rcd_cdt = 10055
                 and c.n0_cdt = k.n0_cdt) t
       group by t.attrordno, t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
      case v_Ope
        when 1 then
          v_sql_detailnode := v_sql_detailnode || ' and c' || v_AttrID ||
                              ' in ' || v_Sql_Part;
        when 2 then
          v_sql_detailnode := v_sql_detailnode || ' and c' || v_AttrID ||
                              ' not in ' || v_Sql_Part;
        when 3 then
          begin
            select '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Sql_BetweenPart
              from (select v.crtserie_em_addr addr,
                           v.num_crt_serie,
                           row_number() over(partition by v.num_crt_serie order by v.val_crt_serie) cur,
                           row_number() over(partition by v.num_crt_serie order by v.val_crt_serie) - 1 prev
                      from crtserie v
                     where v.id_crt_serie = 83
                       and v.num_crt_serie = v_AttrID
                       and v.val_crt_serie between
                           (select val_crt_serie
                              from crtserie c
                             where c.crtserie_em_addr =
                                   f_GetStr(v_Sql_Part, '(', ',')) and
                           (select val_crt_serie
                              from crtserie c
                             where c.crtserie_em_addr =
                                   f_GetStr(v_Sql_Part, ',', ')'))) t
             group by t.num_crt_serie
             start with t.cur = 1
            connect by prior t.cur = t.prev;
            v_sql_detailnode := v_sql_detailnode || ' and c' || v_AttrID ||
                                '  in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
            when others then
              p_sqlcode := sqlcode;
              --record it;
          end;
      end case;
    end loop;
    v_sql_product       := v_sql_product || ') ';
    v_sql_saleterritory := v_sql_saleterritory || ') ';
    v_sql_tradechannel  := v_sql_tradechannel || ') ';
  
    v_sql := v_sql_detailnode || ' and fam4_em_addr in ' || v_sql_product;
    if v_Is2DDefined = 1 then
      v_sql := v_sql || ' and geo5_em_addr in ' || v_sql_saleterritory;
    end if;
    if v_Is3DDefined = 1 then
      v_sql := v_sql || ' and dis6_em_addr in ' || v_sql_tradechannel;
    end if;
    fmp_log.LOGDEBUG(pIn_vModules => 'aggregation', pIn_cSqlText => v_sql);
    sp_execsql(p_Sql => v_sql);
    p_Sqlcode := 0;
  exception
    when others then
      p_Sqlcode := sqlcode;
      raise_application_error(p_Constant.e_oraerr, p_sqlcode || sqlerrm);
  end sp_GenerateDetailNodeOfTheRule;

  procedure spprv_GetColsSQLOfRule(P_AggregateRuleID in number,
                                   p_tab_cols        out varchar2,
                                   p_distinct_cols   out varchar2,
                                   p_ids_cols        out varchar2,
                                   p_names_cols      out varchar2,
                                   p_desc_cols       out varchar2,
                                   p_sqlcode         out number) as
  
    v_p_level  number; --product level
    v_st_level number;
    v_tc_level number;
    v_pid      number; --product id
    v_stid     number; --sale territory id
    v_tcid     number; --trade channel id
    v_operator number; --operator = <> <<
    v_level    number;
  
    v_tab_cols      clob; --sql: table cols for table pvt_xxxx  detail node with all of its cdt
    v_distinct_cols clob; --sql :table cols for table sel_xxx  aggregate node with all of its cdt
    v_ids_cols      clob;
    v_names_cols    clob; --sql: cols to generate aggregate node name
    v_desc_cols     clob; --sql: cols to generate description
  
    v_attr_tab_cols      clob;
    v_attr_distinct_cols clob;
    v_attr_ids_cols      clob;
    v_attr_names_cols    clob;
    v_attr_desc_cols     clob;
  
    v_nodeids  varchar2(200);
    v_nodename varchar2(200);
    v_desc     varchar2(400);
  
    v_cnt   number;
    vTmpVar varchar2(100);
  begin
    --initial global again
    v_column_flag := -1;
  
    select p.regroup_pro, p.regroup_geo, p.regroup_dis
      into v_p_level, v_st_level, v_tc_level
      from prv p
     where p.prv_em_addr = P_AggregateRuleID;
  
    --product level and key
    if v_p_level = 0 then
      select nvl(max(c.adr_cdt), 0)
        into v_pid
        from cdt c
       where c.prv12_em_addr = P_AggregateRuleID
         and c.rcd_cdt = p_constant.PRODUCT
         and c.n0_val_cdt = 0;
      if v_pid <> 0 then
        select nlevel
          into v_level
          from v_fam_tree
         where fam_em_addr = v_pid;
        v_tab_cols      := v_tab_cols || ';9999,' || v_level ||
                           ',0,C00_10000_' || v_level || '_ID,C00_10000_' ||
                           v_level || '_K,C00_10000_' || v_level || '_D';
        v_distinct_cols := v_distinct_cols || ',C00_10000_' || v_level ||
                           '_ID,C00_10000_' || v_level || '_K,C00_10000_' ||
                           v_level || '_D';
      
        select c.operant
          into v_operator
          from cdt c
         where c.prv12_em_addr = P_AggregateRuleID
           and c.rcd_cdt = p_constant.PRODUCT
           and c.n0_val_cdt = 0;
        case v_operator
          when 1 then
            select ltrim(max(sys_connect_by_path(t.fam_em_addr, '-')), '-') || '-',
                   ltrim(max(sys_connect_by_path(t.f_cle, '-')), '-') || '-',
                   ltrim(max(sys_connect_by_path(t.f_desc, '-')), '-') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select f.fam_em_addr,
                           f.f_cle,
                           f.f_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, fam f
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 1
                       and c.rcd_cdt = p_constant.PRODUCT
                       and c.adr_cdt = f.fam_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          when 3 then
            select ltrim(max(sys_connect_by_path(t.fam_em_addr, '-')), '-') || '-',
                   ltrim(max(sys_connect_by_path(t.f_cle, '<.<')), '<.<') || '-',
                   ltrim(max(sys_connect_by_path(t.f_desc, '<.<')), '<.<') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select f.fam_em_addr,
                           f.f_cle,
                           f.f_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, fam f
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 3
                       and c.rcd_cdt = p_constant.PRODUCT
                       and c.adr_cdt = f.fam_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          else
            null;
        end case;
      
        v_ids_cols    := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodeids);
        v_names_cols  := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodename);
        v_desc_cols   := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_desc);
        v_column_flag := 0;
      end if;
    else
      v_tab_cols      := v_tab_cols || ';9999,' || v_p_level ||
                         ',0,C00_10000_' || v_p_level || '_ID,C00_10000_' ||
                         v_p_level || '_K,C00_10000_' || v_p_level || '_D';
      v_distinct_cols := v_distinct_cols || ',C00_10000_' || v_p_level ||
                         '_ID,C00_10000_' || v_p_level || '_K,C00_10000_' ||
                         v_p_level || '_D';
      v_nodeids       := 'C00_10000_' || v_p_level || '_ID';
      v_nodename      := 'C00_10000_' || v_p_level || '_K';
      v_desc          := 'C00_10000_' || v_p_level || '_D';
    
      v_ids_cols    := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodeids);
      v_names_cols  := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodename);
      v_desc_cols   := f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_desc);
      v_column_flag := 1;
    end if;
  
    --sale territory level and key
    if v_st_level = 0 then
      select nvl(max(c.adr_cdt), 0)
        into v_stid
        from cdt c
       where c.prv12_em_addr = P_AggregateRuleID
         and c.rcd_cdt = p_constant.SALE_TERRITORY
         and c.n0_val_cdt = 0;
      if v_stid <> 0 then
        select nlevel
          into v_level
          from v_geo_tree
         where geo_em_addr = v_stid;
        v_tab_cols      := v_tab_cols || ';10001,' || v_level ||
                           ',0,C00_10001_' || v_level || '_ID,C00_10001_' ||
                           v_level || '_K,C00_10001_' || v_level || '_D';
        v_distinct_cols := v_distinct_cols || ',C00_10001_' || v_level ||
                           '_ID,C00_10001_' || v_level || '_K,C00_10001_' ||
                           v_level || '_D';
      
        select c.operant
          into v_operator
          from cdt c
         where c.prv12_em_addr = P_AggregateRuleID
           and c.rcd_cdt = p_constant.SALE_TERRITORY
           and c.n0_val_cdt = 0;
        case v_operator
          when 1 then
            select ltrim(max(sys_connect_by_path(t.geo_em_addr, '-')), '-') || '-',
                   ltrim(max(sys_connect_by_path(t.g_cle, '-')), '-') || '-',
                   
                   ltrim(max(sys_connect_by_path(t.g_desc, '-')), '-') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select g.geo_em_addr,
                           g.g_cle,
                           g.g_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, geo g
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 1
                       and c.rcd_cdt = p_constant.SALE_TERRITORY
                       and c.adr_cdt = g.geo_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          when 3 then
            select ltrim(max(sys_connect_by_path(t.geo_em_addr, '<.<')),
                         '<.<') || '-',
                   ltrim(max(sys_connect_by_path(t.g_cle, '<.<')), '<.<') || '-',
                   
                   ltrim(max(sys_connect_by_path(t.g_desc, '<.<')), '<.<') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select g.geo_em_addr,
                           g.g_cle,
                           g.g_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, geo g
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 3
                       and c.rcd_cdt = p_constant.SALE_TERRITORY
                       and c.adr_cdt = g.geo_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          else
            null;
        end case;
      
        v_ids_cols := v_ids_cols ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 0,
                                       f_CurrentStr     => v_nodeids);
      
        v_names_cols  := v_names_cols ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodename);
        v_desc_cols   := v_desc_cols ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_desc);
        v_column_flag := 0;
      end if;
    else
      v_tab_cols      := v_tab_cols || ';10001,' || v_st_level ||
                         ',0,C00_10001_' || v_st_level || '_ID,C00_10001_' ||
                         v_st_level || '_K,C00_10001_' || v_st_level || '_D';
      v_distinct_cols := v_distinct_cols || ',C00_10001_' || v_st_level ||
                         '_ID,C00_10001_' || v_st_level || '_K,C00_10001_' ||
                         v_st_level || '_D';
      v_nodeids       := 'C00_10001_' || v_st_level || '_ID';
      v_nodename      := 'C00_10001_' || v_st_level || '_K';
      v_desc          := 'C00_10001_' || v_st_level || '_D';
    
      v_ids_cols    := v_ids_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodeids);
      v_names_cols  := v_names_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodename);
      v_desc_cols   := v_desc_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_desc);
      v_column_flag := 1;
    end if;
    --trade channel level and key
    if v_tc_level = 0 then
      select nvl(max(c.adr_cdt), 0)
        into v_tcid
        from cdt c
       where c.prv12_em_addr = P_AggregateRuleID
         and c.rcd_cdt = p_constant.TRADE_CHANNEL
         and c.n0_val_cdt = 0;
      if v_tcid <> 0 then
        select nlevel
          into v_level
          from v_dis_tree
         where dis_em_addr = v_tcid;
        v_tab_cols      := v_tab_cols || ';10002,' || v_level ||
                           ',0,C00_10002_' || v_level || '_ID,C00_10002_' ||
                           v_level || '_K,C00_10002_' || v_level || '_D';
        v_distinct_cols := v_distinct_cols || ',C00_10002_' || v_level ||
                           '_ID,C00_10002_' || v_level || '_K,C00_10002_' ||
                           v_level || '_D';
      
        select c.operant
          into v_operator
          from cdt c
         where c.prv12_em_addr = P_AggregateRuleID
           and c.rcd_cdt = p_constant.TRADE_CHANNEL
           and c.n0_val_cdt = 0;
        case v_operator
          when 1 then
            select ltrim(max(sys_connect_by_path(t.dis_em_addr, '-')), '-') || '-',
                   ltrim(max(sys_connect_by_path(t.d_cle, '-')), '-') || '-',
                   
                   ltrim(max(sys_connect_by_path(t.d_desc, '-')), '-') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select d.dis_em_addr,
                           d.d_cle,
                           d.d_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, dis d
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 1
                       and c.rcd_cdt = p_constant.TRADE_CHANNEL
                       and c.adr_cdt = d.dis_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          when 3 then
            select ltrim(max(sys_connect_by_path(t.dis_em_addr, '<.<')),
                         '<.<') || '-',
                   ltrim(max(sys_connect_by_path(t.d_cle, '<.<')), '<.<') || '-',
                   
                   ltrim(max(sys_connect_by_path(t.d_desc, '<.<')), '<.<') || '-'
              into v_nodeids, v_nodename, v_desc
              from (select d.dis_em_addr,
                           d.d_cle,
                           d.d_desc,
                           c.n0_val_cdt cur,
                           c.n0_val_cdt - 1 prev
                      from cdt c, dis d
                     where c.prv12_em_addr = P_AggregateRuleID
                       and c.operant = 3
                       and c.rcd_cdt = p_constant.TRADE_CHANNEL
                       and c.adr_cdt = d.dis_em_addr) t
             start with t.cur = 0
            connect by prior t.cur = t.prev;
          else
            null;
        end case;
      
        v_ids_cols := v_ids_cols ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 0,
                                       f_CurrentStr     => v_nodeids);
      
        v_names_cols  := v_names_cols ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodename);
        v_desc_cols   := v_desc_cols ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_desc);
        v_column_flag := 0;
      end if;
    else
      v_tab_cols      := v_tab_cols || ';10002,' || v_tc_level ||
                         ',0,C00_10002_' || v_tc_level || '_ID,C00_10002_' ||
                         v_tc_level || '_K,C00_10002_' || v_tc_level || '_D';
      v_distinct_cols := v_distinct_cols || ',C00_10002_' || v_tc_level ||
                         '_ID,C00_10002_' || v_tc_level || '_K,C00_10002_' ||
                         v_tc_level || '_D';
      v_nodeids       := 'C00_10002_' || v_tc_level || '_ID';
      v_nodename      := 'C00_10002_' || v_tc_level || '_K';
      v_desc          := 'C00_10002_' || v_tc_level || '_D';
    
      v_ids_cols    := v_ids_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodeids);
      v_names_cols  := v_names_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_nodename);
      v_desc_cols   := v_desc_cols ||
                       f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                        f_CurrentStrType => 1,
                                        f_CurrentStr     => v_desc);
      v_column_flag := 1;
    end if;
  
    -- process 3D 's attributes
    for i in 20007 .. 20009 loop
      select count(*)
        into v_cnt
        from cdt c
       where c.prv12_em_addr = P_AggregateRuleID
         and c.rcd_cdt = i;
      if v_cnt > 0 then
        spprv_GetColsSQL3DAttrs(p_P_AggregateRuleID => P_AggregateRuleID,
                                p_TableType         => i,
                                p_tab_cols          => v_attr_tab_cols,
                                p_distinct_cols     => v_attr_distinct_cols,
                                p_NodeIDs           => v_attr_ids_cols,
                                p_NodeName          => v_attr_names_cols,
                                p_NodeDesc          => v_attr_desc_cols,
                                p_sqlcode           => p_sqlcode);
        if p_sqlcode <> 0 then
          raise v_unknown_exception;
        end if;
        v_tab_cols      := v_tab_cols || v_attr_tab_cols;
        v_distinct_cols := v_distinct_cols || v_attr_distinct_cols;
        v_ids_cols := case
                        when v_ids_cols is null then
                         v_attr_ids_cols
                        else
                         v_ids_cols || v_attr_ids_cols
                      end;
        v_names_cols := case
                          when v_names_cols is null then
                           v_attr_names_cols
                          else
                           v_names_cols || v_attr_names_cols
                        end;
        v_desc_cols := case
                         when v_desc_cols is null then
                          v_attr_desc_cols
                         else
                          v_desc_cols || v_attr_desc_cols
                       end;
      end if;
    end loop;
    -- process data
    select count(*)
      into v_cnt
      from cdt c
     where c.prv12_em_addr = P_AggregateRuleID
       and c.rcd_cdt = p_constant.DETAIL_NODE;
    if v_cnt > 0 then
      spprv_GetColsSQLDataAttrs(p_P_AggregateRuleID => P_AggregateRuleID,
                                p_tab_cols          => v_attr_tab_cols,
                                p_distinct_cols     => v_attr_distinct_cols,
                                p_NodeIDs           => v_attr_ids_cols,
                                p_NodeName          => v_attr_names_cols,
                                p_NodeDesc          => v_attr_desc_cols,
                                pOut_nColumnFlag    => v_column_flag,
                                p_sqlcode           => p_sqlcode);
      if p_sqlcode <> 0 then
        raise v_unknown_exception;
      end if;
      v_tab_cols      := v_tab_cols || v_attr_tab_cols;
      v_distinct_cols := v_distinct_cols || v_attr_distinct_cols;
      v_ids_cols := case
                      when v_ids_cols is null then
                       v_attr_ids_cols
                      else
                       v_ids_cols || v_attr_ids_cols
                    end;
      v_names_cols := case
                        when v_names_cols is null then
                         v_attr_names_cols
                        else
                         v_names_cols || v_attr_names_cols
                      end;
      v_desc_cols := case
                       when v_desc_cols is null then
                        v_attr_desc_cols
                       else
                        v_desc_cols || v_attr_desc_cols
                     end;
    end if;
    p_tab_cols      := substr(v_tab_cols, 2);
    p_distinct_cols := substr(v_distinct_cols, 2);
    if v_column_flag = 1 then
      p_ids_cols   := v_ids_cols;
      p_names_cols := v_names_cols;
      p_desc_cols  := v_desc_cols;
    else
      p_ids_cols   := substr(v_ids_cols, 1, length(v_ids_cols) - 2) ||
                      v_single_quote;
      p_names_cols := substr(v_names_cols, 1, length(v_names_cols) - 2) ||
                      v_single_quote;
      p_desc_cols  := substr(v_desc_cols, 1, length(v_desc_cols) - 2) ||
                      v_single_quote;
    end if;
    /*    vTmpVar := substr(p_names_cols, length(p_names_cols) - 5 + 1);
    if vTmpVar = v_str_ping_at_col then
      p_ids_cols   := substr(p_ids_cols, 1, length(p_ids_cols) - 5);
      p_names_cols := substr(p_names_cols, 1, length(p_names_cols) - 5);
      p_desc_cols  := substr(p_desc_cols, 1, length(p_desc_cols) - 5);
    end if;*/
    v_column_flag := -1;
  exception
    when v_unknown_exception then
      v_column_flag := -1;
      p_sqlcode     := sqlcode;
    when others then
      v_column_flag := -1;
      p_sqlcode     := sqlcode;
      raise;
  end;

  procedure spprv_GetColsSQL3DAttrs(p_P_AggregateRuleID in number,
                                    p_TableType         in number,
                                    p_tab_cols          out varchar2,
                                    p_distinct_cols     out varchar2,
                                    p_NodeIDs           out varchar2,
                                    p_NodeName          out varchar2,
                                    p_NodeDesc          out varchar2,
                                    p_sqlcode           out number) as
  
    v_nodeid    varchar2(200);
    v_nodename  varchar2(200);
    v_nodedesc  varchar2(400);
    v_tab_type  number;
    v_seqenceid number;
  
    v_tab_cols      varchar2(4000);
    v_distinct_cols varchar2(4000);
    v_val_cnt       number;
    v_1stValue      varchar2(120); --vct.val%type;
    v_2ndValue      varchar2(120); --vct.val%type;
    v_1stDesc       varchar2(120);
    v_2ndDesc       varchar2(120);
  
  begin
    p_sqlcode := 0;
    -- attribute and cst
    v_tab_type := case p_TableType
                    when 20007 then
                     30050
                    when 20008 then
                     30100
                    when 20009 then
                     30150
                    when 10055 then
                     27000
                  end;
    for i in (select distinct c.n0_cdt, c.operant
                from cdt c
               where c.prv12_em_addr = p_P_AggregateRuleID
                 and c.rcd_cdt = p_TableType
                 and c.operant <> 0
               order by c.n0_cdt) loop
      v_seqenceid := v_tab_type + i.n0_cdt;
      if i.operant = 4 then
        v_tab_cols := v_tab_cols || ';' || v_seqenceid || ',0,0,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_ID,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_K,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_D';
      
        v_distinct_cols := v_distinct_cols || ',' || 'C' ||
                           lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                           '_ID,C' || lpad(i.n0_cdt, 2, 0) || '_' ||
                           p_TableType || '_K,C' || lpad(i.n0_cdt, 2, 0) || '_' ||
                           p_TableType || '_D';
      
        v_nodeid   := 'C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_ID';
        v_nodename := 'C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_K';
        v_nodedesc := 'nvl(C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_D,C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_K)';
        p_nodeids  := p_nodeids ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodeid);
        p_nodename := p_nodename ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodename);
        p_NodeDesc := p_NodeDesc ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodedesc);
      
        v_column_flag := 1;
      else
        select count(*)
          into v_val_cnt
          from cdt c
         where c.prv12_em_addr = p_P_AggregateRuleID
           and c.rcd_cdt = p_TableType
           and c.operant = i.operant
           and c.n0_cdt = i.n0_cdt;
      
        case i.operant
          when 1 then
            if v_val_cnt = 1 then
              -- only one value specified
              for k in (select c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt) loop
                select v.val || '-', nvl(v.lib_crt, v.val) || '-'
                  into v_nodename, v_nodedesc
                  from vct v
                 where v.vct_em_addr = k.val;
                v_nodeid := k.val || '-';
              end loop;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val || '-', nvl(v.lib_crt, v.val) || '-'
                    into v_1stValue, v_1stDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := k.val || '-';
                else
                  select v.val || '-', nvl(v.lib_crt, v.val) || '-'
                    into v_2ndValue, v_2ndDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := v_nodeid || k.val || '-';
                end if;
              end loop;
              v_nodename := v_1stValue || '-' || v_2ndValue || '-';
              v_nodedesc := v_1stDesc || '-' || v_2ndDesc || '-';
            end if;
          when 2 then
            if v_val_cnt = 1 then
              -- only one value specified
              for k in (select c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt) loop
                select '<>' || v.val || '-',
                       '<>' || nvl(v.lib_crt, v.val) || '-'
                  into v_nodename, v_nodedesc
                  from vct v
                 where v.vct_em_addr = k.val;
                v_nodeid := '<>' || k.val || '-';
              end loop;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val || '-', nvl(v.lib_crt, v.val) || '-'
                    into v_1stValue, v_1stDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := k.val || '-';
                else
                  select v.val || '-', nvl(v.lib_crt, v.val) || '-'
                    into v_2ndValue, v_2ndDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := v_nodeid || k.val || '-';
                end if;
              end loop;
              v_nodeid   := '<>' || v_nodeid;
              v_nodename := '<>' || v_1stValue || '-' || v_2ndValue || '-';
              v_nodedesc := '<>' || v_1stDesc || '-' || v_2ndDesc || '-';
            end if;
          when 3 then
            if v_val_cnt = 1 then
              null;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val, nvl(v.lib_crt, v.val)
                    into v_1stValue, v_1stDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := k.val;
                else
                  select v.val, nvl(v.lib_crt, v.val)
                    into v_2ndValue, v_2ndDesc
                    from vct v
                   where v.vct_em_addr = k.val;
                  v_nodeid := v_nodeid || '<.<' || v_2ndValue || '-';
                end if;
              end loop;
              v_nodename := v_1stValue || '<.<' || v_2ndValue || '-';
              v_nodedesc := v_1stDesc || '<.<' || v_2ndDesc || '-';
            end if;
        end case;
        p_NodeIDs     := p_NodeIDs ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodeid);
        p_nodename    := p_nodename ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodename);
        p_NodeDesc    := p_NodeDesc ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodedesc);
        v_column_flag := 0;
      end if;
    end loop;
    p_tab_cols      := v_tab_cols;
    p_distinct_cols := v_distinct_cols;
  exception
    when others then
      null;
  end;
  procedure spprv_GetColsSQLDataAttrs(p_P_AggregateRuleID in number,
                                      p_TableType         in number default 10055,
                                      p_tab_cols          out varchar2,
                                      p_distinct_cols     out varchar2,
                                      p_NodeIDs           out varchar2,
                                      p_NodeName          out varchar2,
                                      p_NodeDesc          out varchar2,
                                      pOut_nColumnFlag    out number,
                                      p_sqlcode           out number) as
  
    v_nodeid    varchar2(200);
    v_nodename  varchar2(200);
    v_nodedesc  varchar2(400);
    v_tab_type  number;
    v_seqenceid number;
  
    v_tab_cols      varchar2(4000);
    v_distinct_cols varchar2(4000);
    v_val_cnt       number;
    v_1stValue      varchar2(120); --vct.val%type;
    v_2ndValue      varchar2(120); --vct.val%type;
    v_1stDesc       varchar2(120);
    v_2ndDesc       varchar2(120);
    --v_column_flag   number := -1; --flag the current  var is col name or const var
  
  begin
    p_sqlcode := 0;
    -- attribute and cst
    v_tab_type := case p_TableType
                    when 10055 then
                     27000
                  end;
    for i in (select distinct c.n0_cdt, c.operant
                from cdt c
               where c.prv12_em_addr = p_P_AggregateRuleID
                 and c.rcd_cdt = p_TableType
                 and c.operant <> 0
               order by c.n0_cdt) loop
      v_seqenceid := v_tab_type + i.n0_cdt;
      if i.operant = 4 then
        v_tab_cols := v_tab_cols || ';' || v_seqenceid || ',0,0,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_ID,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_K,C' ||
                      lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_D';
      
        v_distinct_cols := v_distinct_cols || ',' || 'C' ||
                           lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                           '_ID,C' || lpad(i.n0_cdt, 2, 0) || '_' ||
                           p_TableType || '_K,C' || lpad(i.n0_cdt, 2, 0) || '_' ||
                           p_TableType || '_D';
      
        v_nodeid   := 'C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_ID';
        v_nodename := 'C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType || '_K';
        v_nodedesc := 'nvl(C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_D,C' || lpad(i.n0_cdt, 2, 0) || '_' || p_TableType ||
                      '_K)';
        p_nodeids  := p_nodeids ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodeid);
        p_nodename := p_nodename ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodename);
        p_NodeDesc := p_NodeDesc ||
                      f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                       f_CurrentStrType => 1,
                                       f_CurrentStr     => v_nodedesc);
      
        v_column_flag := 1;
      else
        select count(*)
          into v_val_cnt
          from cdt c
         where c.prv12_em_addr = p_P_AggregateRuleID
           and c.rcd_cdt = p_TableType
           and c.operant = i.operant
           and c.n0_cdt = i.n0_cdt;
      
        case i.operant
          when 1 then
            if v_val_cnt = 1 then
              -- only one value specified
              for k in (select c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt) loop
                select v.val_crt_serie || '-',
                       nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                  into v_nodename, v_nodedesc
                  from crtserie v
                 where v.crtserie_em_addr = k.val;
                v_nodeid := k.val || '-';
              end loop;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val_crt_serie || '-',
                         nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                    into v_1stValue, v_1stDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := k.val || '-';
                else
                  select v.val_crt_serie || '-',
                         nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                    into v_2ndValue, v_2ndDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := v_nodeid || k.val || '-';
                end if;
              end loop;
              v_nodename := v_1stValue || '-' || v_2ndValue || '-';
              v_nodedesc := v_1stDesc || '-' || v_2ndDesc || '-';
            end if;
          when 2 then
            if v_val_cnt = 1 then
              -- only one value specified
              for k in (select c.adr_cdt val
                          into v_val_cnt
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt) loop
                select '<>' || v.val_crt_serie || '-',
                       '<>' || nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                  into v_nodename, v_nodedesc
                  from crtserie v
                 where v.crtserie_em_addr = k.val;
                v_nodeid := '<>' || k.val || '-';
              end loop;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val_crt_serie || '-',
                         nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                    into v_1stValue, v_1stDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := k.val || '-';
                else
                  select v.val_crt_serie || '-',
                         nvl(v.lib_crt_serie, v.val_crt_serie) || '-'
                    into v_2ndValue, v_2ndDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := v_nodeid || k.val || '-';
                end if;
              end loop;
              v_nodeid   := '<>' || v_nodeid;
              v_nodename := '<>' || v_1stValue || '-' || v_2ndValue || '-';
              v_nodedesc := '<>' || v_1stDesc || '-' || v_2ndDesc || '-';
            end if;
          when 3 then
            if v_val_cnt = 1 then
              null;
            else
              v_1stValue := '';
              v_2ndValue := '';
              v_1stDesc  := '';
              v_2ndDesc  := '';
              for k in (select c.n0_val_cdt, c.adr_cdt val
                          from cdt c
                         where c.prv12_em_addr = p_P_AggregateRuleID
                           and c.rcd_cdt = p_TableType
                           and c.operant = i.operant
                           and c.n0_cdt = i.n0_cdt
                         order by c.n0_val_cdt) loop
                if k.n0_val_cdt = 0 then
                  select v.val_crt_serie,
                         nvl(v.lib_crt_serie, v.val_crt_serie)
                    into v_1stValue, v_1stDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := k.val;
                else
                  select v.val_crt_serie,
                         nvl(v.lib_crt_serie, v.val_crt_serie)
                    into v_2ndValue, v_2ndDesc
                    from crtserie v
                   where v.crtserie_em_addr = k.val;
                  v_nodeid := v_nodeid || '<.<' || v_2ndValue || '-';
                end if;
              end loop;
              v_nodename := v_1stValue || '<.<' || v_2ndValue || '-';
              v_nodedesc := v_1stDesc || '<.<' || v_2ndDesc || '-';
            end if;
        end case;
        p_NodeIDs     := p_NodeIDs ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodeid);
        p_nodename    := p_nodename ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodename);
        p_NodeDesc    := p_NodeDesc ||
                         f_GetStrToConcat(f_LastStrType    => v_column_flag,
                                          f_CurrentStrType => 0,
                                          f_CurrentStr     => v_nodedesc);
        v_column_flag := 0;
      end if;
    end loop;
    pOut_nColumnFlag := v_column_flag;
    p_tab_cols       := v_tab_cols;
    p_distinct_cols  := v_distinct_cols;
  exception
    when others then
      null;
  end;
  procedure spprv_ParallelBuildNormalSEL(P_startid in number,
                                         p_endid   in number) as
    v_sqlcode number;
  begin
    for i in (select s.sel_em_addr
                from sel s
               where s.sel_bud = 0
                 and s.operant_geo = 0
                 and s.sel_em_addr between p_startid and p_endid) loop
      p_selection.SP_ReBuildSelection(p_SelectionID => i.sel_em_addr,
                                      p_SqlCode     => v_sqlcode);
    end loop;
  exception
    when others then
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_CreateOneNodeOnly(pIn_nAggRuleID  in number,
                                   pOut_nIsCreated out number)
  --*****************************************************************
    -- Description: call this procedure when defined a aggregation rule without any condition.
    --              otherwise ,return immediately
    -- Parameters:
    --       pIn_nAggRuleID
    --       pOut_nIsCreated:1 created
    -- Error Conditions Raised:
    --
    -- Author:      JYLiu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        24-JAN-1997     JY.Liu     Created.
    -- **************************************************************
   as
    nConditionFlag number;
    vName          prv.prv_cle%type;
    nNewSelID      sel.sel_em_addr%type;
  begin
    pOut_nIsCreated := 0;
    select nvl(p.regroup_pro + p.regroup_geo + p.regroup_dis, 0), p.prv_cle
      into nConditionFlag, vName
      from prv p
     where p.prv_em_addr = pIn_nAggRuleID;
    if nConditionFlag <> 0 then
      return;
    end if;
    select count(*)
      into nConditionFlag
      from cdt c
     where c.prv12_em_addr = pIn_nAggRuleID;
    if nConditionFlag <> 0 then
      return;
    end if;
    --sel
    insert into sel
      (sel_em_addr, sel_bud, sel_cle)
      select seq_sel.nextval, 71, vName
        from dual
       where not exists (select 1 from sel s where s.sel_cle = vName);
  
    select s.sel_em_addr
      into nNewSelID
      from sel s
     where s.sel_cle = vName
       and s.sel_bud = 71;
    --bdg
    insert into bdg
      (bdg_em_addr, id_bdg, b_cle, bdg_parent_node)
      select seq_bdg.nextval, 71, vName, '00000000000000000000000000000000'
        from dual
       where not exists (select 1
                from bdg b
               where b.id_bdg = 71
                 and b.b_cle = vName);
    --prvsel
    insert into prvsel
      (prvsel_em_addr, prv15_em_addr, sel16_em_addr)
      select seq_prvsel.nextval, pIn_nAggRuleID, nNewSelID
        from dual
       where not exists (select 1
                from prvsel l
               where l.prv15_em_addr = pIn_nAggRuleID
                 and l.sel16_em_addr = nNewSelID);
    --prvselpvt
    --delete from prvselpvt t where t.prvid = pIn_nAggRuleID;
    insert /*+ append */
    into prvselpvt
      (prvid, selid, pvtid)
      select pIn_nAggRuleID, nNewSelID, p.pvt_em_addr
        from pvt p
       where not exists (select 1
                from prvselpvt l
               where l.prvid = pIn_nAggRuleID
                 and l.selid = nNewSelID
                 and l.pvtid = p.pvt_em_addr);
    --rsp
    insert /*+append */
    into rsp
      (rsp_em_addr, sel13_em_addr, pvt14_em_addr)
      select seq_rsp.nextval, nNewSelID, p.pvt_em_addr from pvt p;
    commit;
    --aggregatenode_fullid
    delete from aggregatenode_fullid a
     where a.aggregationid = pIn_nAggRuleID;
    insert into aggregatenode_fullid
      (aggregationid, aggregatenodeid, name)
    values
      (pIn_nAggRuleID, nNewSelID, vName);
    --
    update sel s
       set s.effectif    =
           (select count(*) from pvt),
           s.annee_select = to_char(sysdate, 'yyyy'),
           s.mois_select  = to_char(sysdate, 'mm'),
           s.jour_select  = to_char(sysdate, 'dd');
    commit;
    pOut_nIsCreated := 1;
  exception
    when others then
      raise;
  end;

  procedure spprv_ProduceAggNodeAndLinks(P_AggregateRuleID in number,
                                         pIn_nType         in number,
                                         p_sqlcode         out number) as
  
    v_tablename_detail           varchar2(30);
    v_tablename_aggregate        varchar2(30);
    v_sql_create_table_detail    clob;
    v_sql_text                   clob;
    v_sql_create_table_aggregate clob;
    v_sql_alter_tab_add_cols     clob;
  
    v_tab_cols      clob;
    v_distinct_cols clob;
    v_ids_cols      clob;
    v_names_cols    clob;
    v_desc_cols     clob;
    v_SqlRSP_clause clob;
    v_isNULLRule    number := 0;
    v_cnt           number;
  
    v_aggregationrulename     varchar2(60);
    v_nAggregation_Detailnode varchar2(100);
    nCreated                  number;
    vOuterTableCols           clob;
    vWhere                    clob;
  begin
    if pIn_nType = 0 then
      v_nAggregation_Detailnode := 'AGG_DETAILNODE_info';
    
      select count(*)
        into v_cnt
        from AGG_DETAILNODE_info a
       where a.aggregationid = P_AggregateRuleID
         and rownum < 2;
    else
      v_nAggregation_Detailnode := 'aggregation_detailnode';
      FMSP_CreateOneNodeOnly(pIn_nAggRuleID  => P_AggregateRuleID,
                             pOut_nIsCreated => nCreated);
      if nCreated = 1 then
        return;
      end if;
      select count(*)
        into v_cnt
        from aggregation_detailnode a
       where a.aggregationid = P_AggregateRuleID
         and rownum < 2;
    end if;
  
    if v_cnt = 0 then
      return;
    end if;
  
    select p.prv_cle
      into v_aggregationrulename
      from prv p
     where p.prv_em_addr = P_AggregateRuleID;
  
    v_tablename_detail := fmf_gettmptablename();
  
    spprv_GetColsSQLOfRule(P_AggregateRuleID => P_AggregateRuleID,
                           p_tab_cols        => v_tab_cols,
                           p_distinct_cols   => v_distinct_cols,
                           p_ids_cols        => v_ids_cols,
                           p_names_cols      => v_names_cols,
                           p_desc_cols       => v_desc_cols,
                           p_sqlcode         => p_Sqlcode);
  
    --call procedure
    sp_RuleIDToSequenceSQL(P_AggregateRuleID => P_AggregateRuleID,
                           P_Sequence        => v_tab_cols,
                           pIn_nType         => pIn_nType,
                           p_Strsql          => v_sql_text,
                           p_SqlCode         => p_Sqlcode);
  
    v_sql_create_table_detail := 'create table ' || v_tablename_detail ||
                                 ' nologging as ' || v_sql_text;
  
    sp_ExecSql(v_sql_create_table_detail);
    v_tablename_aggregate := fmf_gettmptablename();
  
    select count(*)
      into v_isNULLRule
      from prvsel l
     where l.prv15_em_addr = P_AggregateRuleID
       and rownum < 2;
    if v_distinct_cols is null then
      --no key and level and cst specified in aggregation ,and only = <> <.<,in this case we should specify one node
      vOuterTableCols := ' pvt_em_addr ';
      v_distinct_cols := '-1 pvt_em_addr';
      vWhere          := 'where rownum=1';
    else
      vOuterTableCols := v_distinct_cols;
      v_distinct_cols := ' distinct ' || v_distinct_cols || ' ';
    end if;
    if v_isNULLRule = 0 then
      v_sql_create_table_aggregate := 'create table ' ||
                                      v_tablename_aggregate ||
                                      ' as select
                                    seq_sel.nextval nodeid ,' ||
                                      vOuterTableCols || ' from   (select ' ||
                                      v_distinct_cols || ' from ' ||
                                      v_tablename_detail || ' ' || vWhere || ')';
      sp_ExecSql(v_sql_create_table_aggregate);
    else
      v_sql_create_table_aggregate := 'create table ' ||
                                      v_tablename_aggregate ||
                                      ' as select
                                    0 nodeid ,' ||
                                      vOuterTableCols || ' from   (select ' ||
                                      v_distinct_cols || ' from ' ||
                                      v_tablename_detail || ' ' || vWhere || ')';
    
      sp_ExecSql(v_sql_create_table_aggregate);
    
    end if;
    v_sql_alter_tab_add_cols := 'alter table ' || v_tablename_aggregate ||
                                ' add (fullids varchar2(200),fullname varchar2(200),fulldesc varchar2(200))';
    sp_ExecSql(v_sql_alter_tab_add_cols);
  
    v_sql_text := ' update ' || v_tablename_aggregate || ' set fullids = ' ||
                  v_ids_cols;
    sp_ExecSql(v_sql_text);
  
    v_sql_text := ' update ' || v_tablename_aggregate || ' set fullname = ' ||
                  'decode(' || v_names_cols || ',null,''' ||
                  v_aggregationrulename || ''',substr(' || v_names_cols ||
                  ',1,92))';
    sp_ExecSql(v_sql_text);
  
    v_sql_text := ' update ' || v_tablename_aggregate || ' set fulldesc = ' ||
                  'substr(' || v_desc_cols || ',1,60)';
    sp_ExecSql(v_sql_text);
    --process  the length of name is too long or existed~!!
  
    delete from aggregatenode_fullid t
     where t.aggregationid = P_AggregateRuleID
       and not exists
     (select 1
              from prvsel l
             where l.prv15_em_addr = P_AggregateRuleID
               and t.aggregatenodeid = l.sel16_em_addr);
    commit;
    v_sql_text := 'update ' || v_tablename_aggregate ||
                  ' t set t.fullname = substr(''' || v_aggregationrulename ||
                  ''' ||''-''|| t.fullname, 1, 92) where exists (select 1 from aggregatenode_fullid s where s.aggregationid <> ' ||
                  P_AggregateRuleID || '  and t.fullname = s.name)';
    sp_ExecSql(v_sql_text);
    --if the node name is the smae as selection
    v_sql_text := 'update ' || v_tablename_aggregate ||
                  ' t set t.fullname = substr(''' || v_aggregationrulename ||
                  ''' ||''-''|| t.fullname, 1, 92) where exists (select 1 from sel s where s.sel_bud=0  and t.fullname = s.sel_cle)';
    fmsp_execsql(pIn_cSql => v_sql_text);
    if v_isNULLRule <> 0 then
      /* v_sql_text := 'update ' || v_tablename_aggregate ||
       ' t set t.nodeid =(select p.aggregatenodeid from aggregatenode_fullid p
      where  p.aggregationid=' ||
       P_AggregateRuleID ||
       ' and p.aggregatefullid=t.fullids)';*/
      v_sql_text := 'merge into  ' || v_tablename_aggregate ||
                    ' va using (select p.aggregatenodeid,p.aggregatefullid  from aggregatenode_fullid p ' ||
                    ' where  p.aggregationid= ' || P_AggregateRuleID ||
                    ' )  t on( va.fullids =t.aggregatefullid) ' ||
                    ' when matched then ' ||
                    ' update  set    va.nodeid = t.aggregatenodeid   ';
      sp_ExecSql(v_sql_text);
      v_sql_text := 'update ' || v_tablename_aggregate ||
                    ' set nodeid=seq_sel.nextval where nodeid =0 or nodeid is null';
      sp_ExecSql(v_sql_text);
    end if;
    -- process order  rsp,cdt,sel,aggregatenode_fullid,prvsel,bdg,rbp
    --update the aggregate node id to tmp detial table
    if pIn_nType = 1 then
      v_SqlRSP_clause := 'insert /*+append */ into rsp (rsp_em_addr,pvt14_em_addr,sel13_em_addr)
                        select seq_rsp.nextval,a.pvt_em_addr, b.nodeid from ' ||
                         v_tablename_detail || ' a, ' ||
                         v_tablename_aggregate || ' b where 1=1 ';
      for k in (select t.COLUMN_NAME
                  from user_tab_cols t
                 where table_name = upper(v_tablename_aggregate)
                   AND COLUMN_NAME LIKE '%\_ID' escape '\'
                 ORDER BY COLUMN_ID) loop
        v_SqlRSP_clause := v_SqlRSP_clause || ' and a.' || k.column_name ||
                           '=b.' || k.column_name;
      
        v_sql_text := 'insert /*+ append */ into cdt  (cdt_em_addr, n0_cdt,  rcd_cdt, operant, n0_val_cdt, adr_cdt, sel11_em_addr)
                     select  seq_cdt.nextval,to_number(substr(''' ||
                      k.column_name || ''',2,2)) n0_cdt,substr(''' ||
                      k.column_name ||
                      ''',5,5) rcd_cdt,1 operant ,0 n0_val_cdt,' ||
                      k.column_name ||
                      ' adr_cdt ,nodeid sel11_em_addr from    ' ||
                      v_tablename_aggregate ||
                      ' a where not exists
                    (select 1 from cdt c, prvsel l where l.sel16_em_addr = c.sel11_em_addr and l.prv15_em_addr = ' ||
                      P_AggregateRuleID ||
                      ' and a.nodeid = c.sel11_em_addr)';
        sp_ExecSql(v_sql_text);
      end loop;
      sp_ExecSql(v_SqlRSP_clause);
      --in the case: when sel commit but prvsel exception .rebuild aggregation  next time will cause ora-00001
    end if;
  
    v_sql_text := 'merge into sel s ' ||
                  ' using (select fullname, fulldesc, t.nodeid from ' ||
                  v_tablename_aggregate || ' t, aggregatenode_fullid p ' ||
                  ' where t.fullids = p.aggregatefullid ' ||
                  ' and p.aggregationid =  ' || P_AggregateRuleID ||
                  ' ) s2 on (s.sel_em_addr = s2.nodeid)' ||
                  ' when matched then ' ||
                  ' update set s.sel_cle = s2.fullname, s.sel_desc = s2.fulldesc ';
  
    fmsp_execsql(v_sql_text);
  
    v_sql_text := 'insert /*+append */ into  sel(sel_em_addr,sel_bud,sel_cle,sel_desc) select nodeid,71,fullname,fulldesc from ' ||
                  v_tablename_aggregate ||
                  ' t where not exists (select 1 from aggregatenode_fullid p
         where p.aggregationid = ' || P_AggregateRuleID || '
           and t.fullids = p.aggregatefullid)';
    begin
      execute immediate v_sql_text;
    exception
      when Dup_val_on_index then
        delete from sel s
         where s.sel_bud = 71
           and exists (select 1
                  from prvsel l
                 where l.prv15_em_addr = P_AggregateRuleID
                   and l.sel16_em_addr = s.sel_em_addr);
        execute immediate v_sql_text;
    end;
    commit;
    --update table aggregatenode_fullid to store the full-ids of the aggregate node of the aggregation rule.
  
    v_sql_text := 'merge into aggregatenode_fullid t
    using (select ' || P_AggregateRuleID || ' ruleid, s.nodeid, s.fullids, s.fullname, s.fulldesc
    from ' || v_tablename_aggregate ||
                  ' s) p
    on (t.aggregationid = p.ruleid and t.aggregatenodeid = p.nodeid and t.aggregatefullid = p.fullids)
    when not matched then
    insert (aggregationid, aggregatenodeid, aggregatefullid, name, descriptions)
    values (p.ruleid, p.nodeid, p.fullids, p.fullname, p.fulldesc)
    when matched then
    update set t.name = p.fullname, t.descriptions = p.fulldesc';
  
    sp_ExecSql(v_sql_text);
    insert /*+ append */
    into prvsel
      (prvsel_em_addr, prv15_em_addr, sel16_em_addr)
      select seq_prvsel.nextval, P_AggregateRuleID, p.aggregatenodeid
        from aggregatenode_fullid p
       where p.aggregationid = P_AggregateRuleID
         and not exists
       (select 1
                from prvsel t
               where t.prv15_em_addr = P_AggregateRuleID
                 and t.sel16_em_addr = p.aggregatenodeid);
  
    commit;
    insert /*+ append */
    into cdt
      (cdt_em_addr,
       sel11_em_addr,
       n0_cdt,
       rcd_cdt,
       operant,
       n0_val_cdt,
       adr_cdt)
      select seq_cdt.nextval, t1.aggregatenodeid, t2.*
        from (select a.aggregatenodeid
                from aggregatenode_fullid a
               where a.aggregationid = P_AggregateRuleID
                 and not exists
               (select 1
                        from prvsel t
                       where t.prv15_em_addr = P_AggregateRuleID
                         and t.sel16_em_addr = a.aggregatenodeid)) t1,
             (select c.n0_cdt, c.rcd_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
                from cdt c
               where c.prv12_em_addr = P_AggregateRuleID
                 and c.operant <> 4
                 and c.rcd_cdt not in (10000, 10001, 10002)) t2;
  
    insert /*+ append */
    into bdg
      (bdg_em_addr, id_bdg, b_cle, bdg_desc, bdg_parent_node)
      select seq_bdg.nextval,
             71 id,
             s.sel_cle,
             s.sel_desc,
             '00000000000000000000000000000000' parent_node
        from sel s, prvsel l
       where l.prv15_em_addr = P_AggregateRuleID
         and s.sel_em_addr = l.sel16_em_addr
         and not exists (select 1
                from bdg b
               where b.id_bdg = 71
                 and b.b_cle = s.sel_cle);
  
    commit;
    --delete from prvselpvt t where t.prvid = P_AggregateRuleID;
    insert /*+ append */
    into prvselpvt
      (prvid, selid, pvtid)
      select P_AggregateRuleID, r.sel13_em_addr, r.pvt14_em_addr
        from rsp r, prvsel l
       where l.prv15_em_addr = P_AggregateRuleID
         and l.sel16_em_addr = r.sel13_em_addr
         and not exists (select 1
                from prvselpvt t
               where t.prvid = P_AggregateRuleID
                 and t.selid = l.sel16_em_addr
                 and t.pvtid = r.pvt14_em_addr);
    commit;
    merge into sel s
    using (select selid, count(*) cnt
             from prvselpvt
            where prvid = P_AggregateRuleID
            group by selid) t
    on (s.sel_em_addr = t.selid)
    when matched then
      update
         set s.effectif     = t.cnt,
             s.annee_select = to_char(sysdate, 'yyyy'),
             s.mois_select  = to_char(sysdate, 'mm'),
             s.jour_select  = to_char(sysdate, 'dd');
    commit;
    sp_ExecSql('drop table ' || v_tablename_detail || ' purge');
    sp_ExecSql('drop table ' || v_tablename_aggregate || ' purge');
  exception
    when others then
      rollback;
      p_sqlcode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure sp_RuleIDToSequenceSQL(P_AggregateRuleID in number,
                                   P_Sequence        in varchar2, --Sort sequence
                                   pIn_nType         in number,
                                   p_Strsql          out varchar2,
                                   p_SqlCode         out number) as
    v_strfield   varchar2(3000);
    v_strwhere   varchar2(3000);
    v_nTableName varchar2(100);
  begin
    p_sqlcode := 0;
    P_SortSequence.sp_SequencetoAgg(P_Sequence => P_Sequence,
                                    p_strfield => v_strfield,
                                    p_strwhere => v_strwhere,
                                    p_sqlcode  => p_sqlcode);
    if p_SqlCode <> 0 then
      return;
    end if;
    if pIn_nType = 0 then
      v_nTableName := 'agg_detailnode_info';
    else
      v_nTableName := 'aggregation_detailnode';
    end if;
    p_Strsql := 'select t.*' || v_strfield ||
                ' from (select detailnodeID pvt_em_addr from ' ||
                v_nTableName || ' a where aggregationid=' ||
                P_AggregateRuleID || ') t ' || v_strwhere;
  
  exception
    when others then
      p_sqlcode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
  procedure sp_AsyncExecTask(p_TaskName in varchar2,
                             p_sqlcode  out number,
                             p_JobNo    out number) as
    v_job binary_integer;
  begin
    p_sqlcode := 0;
    DBMS_JOB.SUBMIT(v_job, p_taskname, sysdate, NULL, FALSE);
    p_JobNo := v_job;
    commit;
  exception
    when others then
      rollback;
      p_sqlcode := sqlcode;
  end;
  procedure sp_PutTaskIntoPipe(p_PipeName in varchar2) as
    v_ChunkSQL varchar2(400) := 'select start_id, end_id from (select tile, min(id) start_id, max(id) end_id
          from (select t.sel_em_addr id, ntile(' ||
                                v_parallel_cnt ||
                                ') over(order by t.sel_em_addr desc) tile
                  from sel t where t.sel_bud = 0) group by tile)';
    v_SQLStmt  varchar2(200);
    v_status   number;
    v_sqlcode  number;
  begin
    dbms_pipe.pack_message(item => v_ACTIVE);
    v_status  := dbms_pipe.send_message(pipename => p_PipeName);
    v_SQLStmt := 'begin p_aggregation.spprv_ParallelBuildNormalSEL(:start_id,:end_id); end;';
    p_ParallelTaskMgr.sp_ParallelExecTaskBySQL(p_TaskName => 'ReBuildNormalSel',
                                               p_ChunkSQL => v_ChunkSQL,
                                               p_SQLStmt  => v_SQLStmt,
                                               p_sqlcode  => v_sqlcode);
    if v_sqlcode <> 0 then
      raise e_rebuildNormalSel_err;
    end if;
    dbms_pipe.purge(pipename => p_PipeName);
  end;
  function f_GetStrToConcat(f_LastStrType    in number, --  -1 begin 1 column 0 const varible
                            f_CurrentStrType in number, --1 column 0 const varible
                            f_CurrentStr     in varchar2) return varchar2 as
    v_result varchar2(200);
  begin
    case f_LastStrType
      when -1 then
        --begin
        if f_CurrentStrType = 1 then
          v_result := f_CurrentStr;
        else
          v_result := v_single_quote || f_CurrentStr || v_single_quote;
        end if;
      when 1 then
        --column
        if f_CurrentStrType = 1 then
          v_result := v_str_ping_at_col || f_CurrentStr;
        else
          v_result := v_str_ping_at_col || v_single_quote || f_CurrentStr ||
                      v_single_quote;
        end if;
      when 0 then
        --const varible
        if f_CurrentStrType = 1 then
          v_result := '||' || f_CurrentStr;
        else
          v_result := v_str_ping_at_col || v_single_quote || f_CurrentStr ||
                      v_single_quote;
        end if;
    end case;
    return v_result;
  exception
    when others then
      null;
  end;

  procedure FMSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  in number,
                                      pIn_vSequence   in varchar2 default null,
                                      pIn_vConditions in varchar2,
                                      pOut_Nodes      out sys_refcursor,
                                      pOut_nSqlCode   out number)
  --*****************************************************************
    -- Description: get nodes by rule or|and cdt
    --
    -- Parameters:
    --       pIn_nAggRuleID
    --       pIn_vSequence
    --       pIn_nAggRuleID
    --       pOut_Nodes
    --       pOut_nSqlCode
  
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-JAN-2013     JY.Liu     Created.
    --  V7.0        21-MAR-2013     JY.Liu     add input parameter pIn_vSequence.
    -- **************************************************************
   is
  begin
    FMP_GetAggregateNodes.FMCSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  => pIn_nAggRuleID,
                                                     pIn_vSequence   => pIn_vSequence,
                                                     pIn_vConditions => pIn_vConditions,
                                                     pOut_Nodes      => pOut_Nodes,
                                                     pOut_nSqlCode   => pOut_nSqlCode);
  exception
    when others then
      rollback;
      fmp_log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode);
  end;

  procedure FMISP_GetAggregateNodes(pIn_nAggregationID in number,
                                    PIn_vSequence      in varchar2,
                                    pOut_AggregateNode out sys_refcursor,
                                    pOut_vTabName      out varchar2,
                                    pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: get agg nodes
    --
    -- Parameters:
    --       pIn_nAggregationID:
    --       PIn_vSequence
    --       pOut_AggregateNode
    --       pOut_vTabName
    --       pOut_SqlCode
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
    FMP_GetAggregateNodes.FMCSP_GetAggregateNodes(pIn_nAggregationID   => pIn_nAggregationID,
                                                  pIn_vSequence        => PIn_vSequence,
                                                  pOut_rAggregateNodes => pOut_AggregateNode,
                                                  pOut_vTabName        => pOut_vTabName,
                                                  pOut_nSqlCode        => pOut_nSqlCode);
  
  end;

  procedure FMISP_GetAggNodesByConditions(pIn_nAggregationID in number,
                                          pIn_vConditions    in varchar2,
                                          pIn_vSequence      in varchar2,
                                          pOut_AggregateNode out sys_refcursor,
                                          pOut_vTabName      out varchar2,
                                          pOut_SqlCode       out number)
  --*****************************************************************
    -- Description: get agg nodes
    --
    -- Parameters:
    --       pIn_nAggregationID:
    --       pIn_vConditions
    --       PIn_vSequence
    --       pOut_AggregateNode
    --       pOut_vTabName
    --       pOut_SqlCode
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
    FMP_GetAggregateNodes.FMCSP_GetAggNodesByConditions(pIn_nAggregationID   => pIn_nAggregationID,
                                                        pIn_vConditions      => pIn_vConditions,
                                                        pIn_vSequence        => pIn_vSequence,
                                                        pOut_rAggregateNodes => pOut_AggregateNode,
                                                        pOut_vTabName        => pOut_vTabName,
                                                        pOu8t_nSqlCode       => pOut_SqlCode);
  end;

  procedure FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID in number,
                                       pIn_vObjId           in varchar2,
                                       pIn_nType            in number,
                                       pIn_nObjType         in number,
                                       pOut_nSqlCode        out number) as
  
  begin
    pOut_nSqlCode := 0;
    FMSP_BuildAggregate_Tmp(pIn_nAggregateRuleID => PIn_nAggregateRuleID,
                            pIn_vObjId           => Pin_vObjId,
                            pIn_nType            => pIn_nType,
                            pIn_nObjType         => pIn_nObjType,
                            pOut_nSqlCode        => pOut_nSqlCode);
  
    pOut_nSqlCode := 0;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_log.LOGERROR;
      raise;
  end;

begin
  select nvl(max(value), 1)
    into v_parallel_cnt
    from fm_config
   where id = 1;

  select count(*)
    into v_Is2DExisted
    from geo
   where geo_em_addr <> 1
     and rownum < 2;
  select count(*)
    into v_Is3DExisted
    from dis
   where dis_em_addr <> 1
     and rownum < 2;

end p_Aggregation;
/
