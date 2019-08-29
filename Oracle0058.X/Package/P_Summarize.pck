create or replace package P_Summarize is
  /********
  Created by JYLiu on 2012/11/20  create package to summarize aggregate nodes time series
  ********/

  --p_TimeSeriesNo  format [M];[W];[D];[BDG-M];[BDG-W];[BDG-D] time series separeated by ','
  --example (49,51);(49,51);(49,51);(49,51);(49,51);(49,51)
  procedure SPPRV_SummarizeAggregateNodes(P_AggregateRuleID in number,
                                          p_TimeSeriesNo    in varchar2,
                                          p_UoMConfig       in varchar2,
                                          p_Config          in varchar2 default '0',
                                          p_SqlCode         out number);

  procedure FMSP_Summarize(p_TimeSeriesNo in varchar2,
                           p_UoMConfig    in varchar2,
                           p_Config       in varchar2 default '0',
                           p_SqlCode      out number);
  --the function return a sql clause which belongs to merge select part
  function f_GetSumClause(p_MorWorD    in varchar2,
                          p_Expression in varchar2 default null) return clob;
  --the function return a sql cluase which belongs to merge when matched insert part
  function f_GetInsertClause(p_MorWorD in varchar2) return clob;
  --the function return a sql cluase which belongs to merge when matched values part
  function f_GetValuesClause(p_MorWorD in varchar2) return clob;
  --the function return a sql cluase which belongs to merge when not matched update part
  function f_GetUpdateClause(p_MorWorD    in varchar2,
                             P_IsAddExist number default 0) return clob;
  --return the periods of monthly or weekly of daily
  function f_ConvertTcnt(p_MorWorD in char) return number;

  procedure spprv_ExecSql(p_Sql in CLOB, p_pars1 in number);

end P_Summarize;
/
create or replace package body P_Summarize is

  --the p_MorWorD is monthly weekly or daily
  --the 1st bit of p_Options is 0 or 1 0-all hist value 1- if hist value is null then exist value end if
  procedure SPPRV_SumTSToAggNode(P_AggregateRuleID in number,
                                 p_TimeSeriesIDs   in varchar2,
                                 p_MorWorD         in char default 'M',
                                 p_IsAddExist      in number,
                                 p_SqlCode         out number);

  procedure spprv_SumAggNodeBaseON(P_AggregateRuleID  in number,
                                   p_TimeseriesNO     in varchar2,
                                   p_TargetTimeSeries in number,
                                   p_MorWorD          in char default 'M',
                                   p_expression       in varchar2 default null,
                                   pIn_vCols          in varchar2 default null,
                                   p_sqlcode          out number);

  procedure spprv_SumAggNodeOnUoM(P_AggregateRuleID in number,
                                  p_TimeseriesNO    in varchar2,
                                  p_MorWorD         in char default 'M',
                                  p_expression      in varchar2,
                                  pIn_vCols         in varchar2,
                                  p_IsAddExist      in number default 0,
                                  p_sqlcode         out number);

  procedure spprv_CalculateCurrency(P_AggregateRuleID in number,
                                    p_TimeseriesID    in number,
                                    p_MorWorD         in char default 'M',
                                    p_sqlcode         out number);
  procedure FMSP_SumTSID(pIn_nAggregationID in number,
                         pIn_nTSID          in number,
                         pIn_nIsAddExist    in number,
                         pIn_cMorWorD       in char default 'M',
                         pOut_nSqlCode      out number);

  procedure FMSP_Summarize(p_TimeSeriesNo in varchar2,
                           p_UoMConfig    in varchar2,
                           p_Config       in varchar2 default '0',
                           p_SqlCode      out number)

    --*****************************************************************
    -- Description: summarize the specified time series data.
    --
    -- Parameters:
    --       p_TimeSeriesNo:
    --       p_UoMConfig:
    --       p_Config:
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        28-JAN-2013     JY.Liu      Created.
    -- **************************************************************
   is
  begin
    for j in (select prv_em_addr from prv order by prv_em_addr) loop
      SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => j.prv_em_addr,
                                    p_TimeSeriesNo    => p_TimeSeriesNo,
                                    p_UoMConfig       => p_UoMConfig,
                                    p_Config          => p_Config,
                                    p_SqlCode         => p_sqlcode);
    end loop;
  exception
    when others then
      fmp_log.LOGERROR;
      raise;
  end;

  procedure SPPRV_SummarizeAggregateNodes(P_AggregateRuleID in number,
                                          p_TimeSeriesNo    in varchar2,
                                          p_UoMConfig       in varchar2,
                                          p_Config          in varchar2 default '0',
                                          p_SqlCode         out number) as
    v_Unite              number := 0;
    v_AggregationBasedon number;
    --  v_Convert_Currency   number;
    v_TimeseriesNO     varchar2(200);
    v_TargetTimeSeries number;
    v_N0_QTE           number := 49;
    v_N0_Hist_NMC      number := 71; --22+49

    v_N0_VAL          number := 59; --10+49
    v_N0_HIST_NMC_VAL number := 72; --23+49;

    v_N0_MENS_HISTO number := 69; --10+49;

    v_N0_HEBDO_HISTO    number := 49; --0+49;
    v_N0_HEBDO_HIST_NMC number := 83; --34+49;

    v_N0_JOUR_HISTO    number := 50; --1+49;
    v_N0_JOUR_HIST_NMC number := 82; --33+49

    V_N0_HIST_KIT       NUMBER := 193; --144 + 49;
    V_N0_HEBDO_HIST_KIT NUMBER := 195; --146 + 49;
    v_N0_JOUR_HIST_KIT  NUMBER := 194; --145 + 49;

    v_Expression     varchar2(200);
    vCols            varchar2(60);
    v_UNo            number;
    v_CurrencyFlag   number := -1;
    v_CurrencyUMFlag number := -1;

    v_IsAddExist          number;
    v_MorWorD             char(1) := 'M'; -- monthly or weekly or daily
    v_TimeSeriesIDS       varchar2(2000);
    v_CurrentSemicolonIdx number;
    v_NextSemicolonIdx    number;
    e_FormatErr exception;
  begin
    p_sqlcode := 0;
    Fmp_log.FMP_SetValue(P_AggregateRuleID);
    Fmp_log.FMP_SetValue(p_TimeSeriesNo);
    Fmp_log.FMP_SetValue(p_UoMConfig);
    Fmp_log.FMP_SetValue(p_Config);
    Fmp_log.LOGBEGIN;
    select nvl(p.unite_prv, 0), nvl(p.nb_prevision, 0)
      into v_unite, v_AggregationBasedon
      from prv p
     where p.prv_em_addr = P_AggregateRuleID;

    --convert historical val Using UM.using UM  ox3E8=1000
    v_CurrencyUMFlag := case
                          when bitand(v_AggregationBasedon, 1000) = 1000 then
                           0
                          else
                           -1
                        end;
    --convert all values to base currency  0x800=2048
    if v_CurrencyUMFlag <> 0 then
      v_CurrencyFlag := case
                          when bitand(v_AggregationBasedon, 2048) = 2048 then
                           0
                          else
                           -1
                        end;
    end if;

    v_UNo := v_unite + 1;

    if v_UNo = 3 then
      vCols := 'f.unite_3 * f.unite_2';
    elsif v_UNo = 1 then
      vCols := '1';
    else
      vCols := 'f.unite_' || v_UNo;
    end if;

    if substr(p_UoMConfig, v_Unite, 1) = '0' then
      v_Expression := '*u.value';
    else
      v_Expression := '/ decode(u.value, 0, 1,u.value)';
    end if;

    v_IsAddExist := substr(nvl(p_config, '0'), 1, 1);

    for i_Nth in 1 .. 6 loop
      begin
        if i_Nth in (3, 6) then
          --not support daily right now!
          raise e_FormatErr;
        end if;

        v_MorWorD := case
                       when i_Nth in (1, 4) then
                        'M'
                       when i_Nth in (2, 5) then
                        'W'
                       when i_Nth in (3, 6) then
                        'D'
                     end;
        -- sum hist val
        FMSP_SumTSID(pIn_nAggregationID => P_AggregateRuleID,
                     pIn_nTSID          => v_N0_VAL,
                     pIn_nIsAddExist    => v_IsAddExist,
                     pIn_cMorWorD       => v_MorWorD,
                     pOut_nSqlCode      => p_sqlcode);
        if i_Nth = 1 then
          v_CurrentSemicolonIdx := instr(p_TimeSeriesNo, ';', 1, 1);
          if v_CurrentSemicolonIdx = 0 then
            raise e_FormatErr;
          end if;
          v_TimeSeriesIDS := substr(p_TimeSeriesNo,
                                    1,
                                    v_CurrentSemicolonIdx - 1);
        else
          v_CurrentSemicolonIdx := instr(p_TimeSeriesNo, ';', 1, i_Nth - 1);
          v_NextSemicolonIdx    := instr(p_TimeSeriesNo, ';', 1, i_Nth);
          if v_CurrentSemicolonIdx = 0 or v_NextSemicolonIdx = 0 then
            raise e_FormatErr;
          end if;
          v_TimeSeriesIDS := substr(p_TimeSeriesNo,
                                    v_CurrentSemicolonIdx + 1,
                                    (v_NextSemicolonIdx -
                                    v_CurrentSemicolonIdx) - 1);
        end if;

        if v_TimeSeriesIDS = '()' then
          raise e_FormatErr;
        end if;

        --process  UoM  Option
        if v_Unite = 0 then
          SPPRV_SumTSToAggNode(P_AggregateRuleID => P_AggregateRuleID,
                               p_TimeSeriesIDs   => v_TimeSeriesIDS,
                               p_IsAddExist      => v_IsAddExist,
                               p_MorWorD         => v_MorWorD,
                               p_SqlCode         => p_sqlcode);
        elsif v_Unite between 1 and 9 then
          spprv_SumAggNodeOnUoM(P_AggregateRuleID => P_AggregateRuleID,
                                p_TimeseriesNO    => v_TimeSeriesIDS,
                                p_expression      => v_Expression,
                                pIn_vCols         => vCols,
                                p_MorWorD         => v_MorWorD,
                                p_IsAddExist      => v_IsAddExist,
                                p_sqlcode         => p_sqlcode);
          update sel s
             set s.unite_sel = v_Unite
           where exists (select 1
                    from prvsel l
                   where l.prv15_em_addr = P_AggregateRuleID
                     and l.sel16_em_addr = s.sel_em_addr);
        elsif v_Unite = 80 then
          --group
          null;
        elsif v_Unite = 22 then
          --without
          /*Without: nodes values in the aggregation are stored and
          consolidated in product UM1 but the corresponding aggregate nodes have no unit*/
          null;
        end if;

        --process Aggregate based on option
        case v_AggregationBasedon
          when 0 then
            -- sum history Qty
            null;
          when 1 then
            --historic+historic of component
            case upper(v_MorWorD)
              when 'M' then
                ---monthly
                --N0_QTE+N0_HIST_NMC
                -- add time series N0_HIST_NMC to N0_QTE
                --time series N0_QTE has been calculated
                v_TimeseriesNO     := '(' || v_N0_Hist_NMC || ')';
                v_TargetTimeSeries := v_N0_QTE;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_expression       => v_Expression,
                                       pIn_vCols          => vCols,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);

                if v_CurrencyFlag = 0 or v_CurrencyUMFlag = 0 then
                  spprv_CalculateCurrency(P_AggregateRuleID => P_AggregateRuleID,
                                          p_TimeseriesID    => v_N0_VAL,
                                          p_MorWorD         => v_MorWorD,
                                          p_sqlcode         => p_SqlCode);
                end if;
                -- N0_VAL+N0_HIST_NMC_VAL
                v_TimeseriesNO     := '(' || v_N0_HIST_NMC_VAL || ')';
                v_TargetTimeSeries := v_N0_VAL;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_sqlcode);
                --N0_MENS_HISTO+N0_HIST_NMC
                v_TimeseriesNO     := '(' || v_N0_HIST_NMC || ')';
                v_TargetTimeSeries := v_N0_MENS_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
              when 'W' then
                --N0_HEBDO_HISTO+N0_HEBDO_HIST_NMC
                v_TimeseriesNO     := '(' || v_N0_HEBDO_HIST_NMC || ')';
                v_TargetTimeSeries := v_N0_HEBDO_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
              when 'D' then
                --N0_JOUR_HISTO+N0_JOUR_HIST_NMC
                v_TimeseriesNO     := '(' || v_N0_JOUR_HIST_NMC || ')';
                v_TargetTimeSeries := v_N0_JOUR_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
            end case;
          when 2 then
            --No historic at Aggregate level
            --hist qty time series is 49 in monthly weekly daily version.
            case upper(v_MorWorD)
              when 'M' THEN
                delete from prb_m p
                 where exists (select 1
                          from prvsel l
                         where l.prv15_em_addr = P_AggregateRuleID
                           and l.sel16_em_addr = p.selid)
                   and p.tsid = 49;
              when 'W' THEN
                delete from prb_w p
                 where exists (select 1
                          from prvsel l
                         where l.prv15_em_addr = P_AggregateRuleID
                           and l.sel16_em_addr = p.selid)
                   and p.tsid = 49;
              when 'D' THEN
                null;
            end case;
            commit;
          when 3 then
            --Historic + Historic of component + Historic of kit
            case upper(v_MorWorD)
              when 'M' then
                --N0_QTE+N0_HIST_NMC+N0_HIST_KIT
                v_TimeseriesNO     := '(' || v_N0_HIST_NMC || ',' ||
                                      v_N0_HIST_KIT || ')';
                v_TargetTimeSeries := v_N0_QTE;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_expression       => v_Expression,
                                       pIn_vCols          => vCols,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);

                if v_CurrencyFlag = 0 or v_CurrencyUMFlag = 0 then
                  spprv_CalculateCurrency(P_AggregateRuleID => P_AggregateRuleID,
                                          p_TimeseriesID    => v_N0_VAL,
                                          p_MorWorD         => v_MorWorD,
                                          p_sqlcode         => p_SqlCode);
                end if;
                --  N0_VAL+N0_HIST_NMC_VAL
                v_TimeseriesNO     := '(' || v_N0_HIST_NMC_VAL || ')';
                v_TargetTimeSeries := v_N0_VAL;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
                --N0_MENS_HISTO+N0_HIST_NMC+N0_HIST_KIT
                v_TimeseriesNO     := '(' || v_N0_HIST_NMC || ',' ||
                                      v_N0_HIST_KIT || ')';
                v_TargetTimeSeries := v_N0_MENS_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);

              when 'W' then
                --N0_HEBDO_HISTO+N0_HEBDO_HIST_NMC+N0_HEBDO_HIST_KIT
                v_TimeseriesNO     := '(' || v_N0_HEBDO_HIST_NMC || ',' ||
                                      v_N0_HEBDO_HIST_KIT || ')';
                v_TargetTimeSeries := v_N0_HEBDO_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
              when 'D' then
                --N0_JOUR_HISTO+N0_JOUR_HIST_NMC+N0_JOUR_HIST_KIT
                v_TimeseriesNO     := '(' || v_N0_JOUR_HIST_NMC || ',' ||
                                      v_N0_JOUR_HIST_KIT || ')';
                v_TargetTimeSeries := v_N0_JOUR_HISTO;
                spprv_SumAggNodeBaseON(P_AggregateRuleID  => P_AggregateRuleID,
                                       p_TimeseriesNO     => v_TimeseriesNO,
                                       p_TargetTimeSeries => v_TargetTimeSeries,
                                       p_MorWorD          => v_MorWorD,
                                       p_sqlcode          => p_SqlCode);
            end case;
          else
            null;
        end case;

      exception
        when e_FormatErr then
          null;
      end;
    end loop;
    Fmp_Log.LOGEND;
  exception
    when others then
      rollback;
      p_sqlcode := p_constant.e_oraerr;
      Fmp_Log.LOGERROR;
      raise_application_error(p_sqlcode, sqlcode || sqlerrm);
  end;

  procedure SPPRV_SumTSToAggNode(P_AggregateRuleID in number,
                                 p_TimeSeriesIDs   in varchar2,
                                 p_MorWorD         in char default 'M',
                                 p_IsAddExist      in number, --total  of hist data(0) or total  of hist data +exist data
                                 p_SqlCode         out number) as
    v_sql  clob;
    v_sql1 clob;
    v_sql2 clob;
    v_sql3 clob;
    v_sql4 clob;
  begin
    p_sqlcode := 0;
    v_sql1    := f_GetSumClause(p_MorWorD => p_MorWorD);
    v_sql2    := f_GetInsertClause(p_MorWorD => p_MorWorD);
    v_sql3    := f_GetValuesClause(p_MorWorD => p_MorWorD);
    v_sql4    := f_GetUpdateClause(p_MorWorD    => p_MorWorD,
                                   P_IsAddExist => p_IsAddExist);
    v_sql     := 'merge /*+ ordered use_hash(p) */ into prb_' || p_MorWorD || ' p
              using (select p.selid, d.tsid, d.yy' ||
                 v_sql1 || ' from don_' || p_MorWorD ||
                 ' d, prvselpvt p
                     where p.prvid = :1 and d.tsid in ' ||
                 p_TimeSeriesIDs || '  and d.version = 0  and p.pvtid = d.pvtid
                  group by p.selid, d.tsid, d.yy) t
              on (p.selid = t.selid and p.tsid = t.tsid and p.version = 0 and p.yy = t.yy)
              when not matched then
                insert (prb_' || p_MorWorD ||
                 'id,selid,tsid,version,yy' || v_sql2 || ')
                values(seq_prb_' || p_MorWorD ||
                 '.nextval,t.selid,t.tsid,0,t.yy' || v_sql3 || ')
              when matched then   update set ' || v_sql4;
    spprv_ExecSql(p_Sql => v_sql, p_pars1 => P_AggregateRuleID);
  exception
    when others then
      p_sqlcode := p_constant.e_oraerr;
      raise_application_error(p_sqlcode, sqlcode || sqlerrm);
  end;

  procedure spprv_SumAggNodeBaseON(P_AggregateRuleID  in number,
                                   p_TimeseriesNO     in varchar2,
                                   p_TargetTimeSeries in number,
                                   p_MorWorD          in char default 'M',
                                   p_Expression       in varchar2 default null,
                                   pIn_vCols          in varchar2,
                                   p_sqlcode          out number) as
    v_sql  clob;
    v_sql1 clob;
    v_sql2 clob;

  begin
    p_sqlcode := 0;
    v_sql1    := f_GetSumClause(p_MorWorD    => p_MorWorD,
                                p_Expression => p_Expression);
    select substr(replace(sys_connect_by_path('p.t' || level || '=p.t' ||
                                              level || '+nvl(t.t' || level ||
                                              ',0)',
                                              '#'),
                          '#',
                          ','),
                  2)
      into v_sql2
      from dual
     where level = f_ConvertTcnt(p_MorWorD)
    connect by level <= f_ConvertTcnt(p_MorWorD);
    if p_Expression is null then
      v_sql := 'merge into prb_' || p_MorWorD || '
      p using (select /*+ordered*/ p.selid, d.yy' || v_sql1 || '
                 from don_' || p_MorWorD ||
               ' d, prvselpvt p
                where p.prvid = :1 and d.tsid in ' ||
               p_TimeseriesNO ||
               '  and d.version = 0  and p.pvtid = d.pvtid
             group by p.selid,d.yy) t
                   on (p.version = 0 and p.yy=t.yy and p.tsid=' ||
               p_TargetTimeSeries || ' and p.selid=t.selid)
                when matched then
                  update set ' || v_sql2;
    else
      v_sql := 'merge into prb_' || p_MorWorD || ' p
         using (select d.selid, d.yy' || v_sql1 ||
               ' from (select /*+no_merge use_hash(d p)*/ d.*, p.selid
                 from don_' || p_MorWorD ||
               ' d, prvselpvt p
                where p.prvid = :1
                  and d.tsid in ' || p_TimeseriesNO || '
                  and d.version = 0
                  and p.pvtid = d.pvtid) d,
              (select /*+no_merge use_hash(p f)*/
                p.pvt_em_addr detailnodeid, ' || pIn_vCols ||
               ' value
                 from pvt p, fam f
                where p.fam4_em_addr = f.fam_em_addr) u
        where u.detailnodeid = d.pvtid
        group by d.selid, d.yy) t
                   on (p.version = 0 and p.yy=t.yy and p.tsid=' ||
               p_TargetTimeSeries || ' and p.selid=t.selid)
                when matched then
                  update set ' || v_sql2;
    end if;
    spprv_ExecSql(p_Sql => v_sql, p_pars1 => P_AggregateRuleID);
  exception
    when others then
      p_sqlcode := p_constant.e_oraerr;
      raise_application_error(p_sqlcode, sqlcode || sqlerrm);
  end;

  procedure spprv_SumAggNodeOnUoM(P_AggregateRuleID in number,
                                  p_TimeseriesNO    in varchar2,
                                  p_MorWorD         in char default 'M',
                                  p_expression      in varchar2,
                                  pIn_vCols         in varchar2,
                                  p_IsAddExist      in number default 0,
                                  p_sqlcode         out number) as
    v_sql  clob;
    v_sql1 clob;
    v_sql2 clob;
    v_sql3 clob;
    v_sql4 clob;
  begin
    p_sqlcode := 0;
    v_sql1    := f_GetSumClause(p_MorWorD    => p_MorWorD,
                                p_Expression => p_expression);
    v_sql2    := f_GetInsertClause(p_MorWorD => p_MorWorD);
    v_sql3    := f_GetValuesClause(p_MorWorD => p_MorWorD);
    v_sql4    := f_GetUpdateClause(p_MorWorD    => p_MorWorD,
                                   P_IsAddExist => p_IsAddExist);
    v_sql     := 'merge into prb_' || p_MorWorD ||
                 ' p using (select  d.selid, d.tsid, d.yy' || v_sql1 ||
                 ' from   (select /*+no_merge use_hash(d p)*/  d.*, p.selid from don_' ||
                 p_MorWorD || ' d, prvselpvt p  where p.prvid = :1
                  and d.tsid in ' || p_TimeseriesNO || '
                  and d.version = 0
                  and p.pvtid = d.pvtid) d,
              (select /*+no_merge use_hash(p f)*/
                p.pvt_em_addr detailnodeid, ' || pIn_vCols ||
                 ' value
                 from pvt p, fam f
                where p.fam4_em_addr = f.fam_em_addr) u
        where u.detailnodeid = d.pvtid
        group by d.selid, d.tsid, d.yy) t
              on (p.selid = t.selid and p.tsid = t.tsid and p.version = 0 and p.yy = t.yy)
              when not matched then
                insert (prb_' || p_MorWorD ||
                 'id,selid,tsid,version,yy' || v_sql2 || ')
                values(seq_prb_' || p_MorWorD ||
                 '.nextval,t.selid,t.tsid,0,t.yy' || v_sql3 || ')
              when matched then update set ' || v_sql4;

    spprv_ExecSql(p_Sql => v_sql, p_pars1 => P_AggregateRuleID);
  exception
    when others then
      p_sqlcode := p_constant.e_oraerr;
      raise_application_error(p_sqlcode, sqlcode || sqlerrm);
  end;

  procedure spprv_CalculateCurrency(P_AggregateRuleID in number,
                                    p_TimeseriesID    in number,
                                    p_MorWorD         in char default 'M',
                                    p_sqlcode         out number) as

    cSql       clob;
    cSql1      clob;
    cSql2      clob;
    cSql3      clob;
    cSql4      clob;
    nPeriodCnt number;
  begin
    p_sqlcode := 0;

    --covnert all values to base currency
    -- first calculate   N0_VAL*NUM_DVS
    -- then calculate N0_VAL+N0_HIST_NMC_VAL
    nPeriodCnt := case upper(p_MorWorD)
                    when 'M' then
                     12
                    when 'W' then
                     53
                  end;
    for i in 1 .. nPeriodCnt loop
      cSql1 := cSql1 || ',sum(m.t' || i || '*v.t' || i || ') t' || i;
      cSql2 := cSql2 || ',t' || i;
      cSql3 := cSql3 || ',t.t' || i;
      cSql4 := cSql4 || ',p.t' || i || '=decode(t.t' || i || ',null,p.t' || i ||
               ',t.t' || i || ')';
    end loop;
    cSql4 := substr(cSql4, 2); --remove comma

    cSql := 'merge into prb_' || p_MorWorD ||
            ' p using (select s.selid, m.tsid, m.yy' || cSql1 ||
            ' from dvs_' || p_MorWorD || ' v,pvt t,prvselpvt s,don_' ||
            p_MorWorD || ' m where s.prvid = :1
              and s.pvtid = t.pvt_em_addr
              and v.nodeid = t.geo5_em_addr
              and m.pvtid = s.pvtid
              and m.tsid = ' || p_TimeseriesID || '
              and v.yy = m.yy
            group by s.selid, m.tsid, m.yy) t
    on (p.selid = t.selid and p.yy = t.yy and p.tsid = t.tsid)
    when not matched then
      insert  (prb_' || p_MorWorD || 'id,selid,tsid,version,yy' ||
            cSql2 || ')
      values(seq_prb_' || p_MorWorD ||
            '.nextval,t.selid,t.tsid,0,t.yy' || cSql3 ||
            ') when matched then update set ' || csql4;
    spprv_ExecSql(p_Sql => csql, p_pars1 => P_AggregateRuleID);
  exception
    when others then
      p_sqlcode := p_constant.e_oraerr;
      raise_application_error(p_sqlcode, sqlcode || sqlerrm);
  end;

  procedure FMSP_SumTSID(pIn_nAggregationID in number,
                         pIn_nTSID          in number,
                         pIn_nIsAddExist    in number,
                         pIn_cMorWorD       in char default 'M',
                         pOut_nSqlCode      out number) is
  begin
    SPPRV_SumTSToAggNode(P_AggregateRuleID => pIn_nAggregationID,
                         p_TimeSeriesIDs   => '(' || to_char(pIn_nTSID) || ')',
                         p_IsAddExist      => pIn_nIsAddExist,
                         p_MorWorD         => pIn_cMorWorD,
                         p_SqlCode         => pOut_nSqlCode);
  end;
  function f_GetSumClause(p_MorWorD    in varchar2,
                          p_Expression in varchar2 default null) return clob as
    v_SqlClause clob;
    v_Tcnt      number := 12;
  begin
    v_Tcnt := f_ConvertTcnt(p_MorWorD => p_MorWorD);
    for i in 1 .. v_Tcnt loop
      v_SqlClause := v_SqlClause || ',' || ' sum(d.t' || i || p_Expression ||
                     ')  t' || i;
    end loop;

    return v_SqlClause;
  exception
    when others then
      raise;
  end;

  function f_GetInsertClause(p_MorWorD in varchar2) return clob as
    v_SqlClause clob;
    v_Tcnt      number := 12;
  begin

    v_Tcnt := f_ConvertTcnt(p_MorWorD => p_MorWorD);
    for i in 1 .. v_Tcnt loop
      v_SqlClause := v_SqlClause || ',t' || i;
    end loop;
    return v_SqlClause;
  exception
    when others then
      raise;
  end;

  function f_GetValuesClause(p_MorWorD in varchar2) return clob as
    v_SqlClause clob;
    v_Tcnt      number := 12;
  begin
    v_Tcnt := f_ConvertTcnt(p_MorWorD => p_MorWorD);
    for i in 1 .. v_Tcnt loop
      v_SqlClause := v_SqlClause || ',t.t' || i;
    end loop;
    return v_SqlClause;
  exception
    when others then
      raise;
  end;

  function f_GetUpdateClause(p_MorWorD    in varchar2,
                             P_IsAddExist number default 0) return clob as
    v_SqlClause clob;
    v_Tcnt      number := 12;
  begin
    v_Tcnt := f_ConvertTcnt(p_MorWorD => p_MorWorD);
    if P_IsAddExist = 0 then
      for i in 1 .. v_Tcnt loop
        v_SqlClause := v_SqlClause || ',p.t' || i || '=t.t' || i;
      end loop;
    elsif P_IsAddExist = 1 then
      for i in 1 .. v_Tcnt loop
        v_SqlClause := v_SqlClause || ',p.t' || i || '=decode(t.t' || i ||
                       ',null,p.t' || i || ',t.t' || i || ')';
      end loop;
    end if;
    return substr(v_SqlClause, 2);
  exception
    when others then
      raise;
  end;

  procedure spprv_ExecSql(p_Sql in clob, p_pars1 in number) as
  begin
    execute immediate p_Sql
      using p_pars1;
    commit;
  exception
    when others then
      rollback;
      Fmp_Log.LOGERROR;
      raise;
  end;

  function f_ConvertTcnt(p_MorWorD in char) return number as
    v_Tcnt number := 12;
  begin
    select decode(upper(p_MorWorD), 'M', 12, 'W', 53, 'D', 366)
      into v_Tcnt
      from dual;
    return v_Tcnt;
  end;
end P_Summarize;
/
