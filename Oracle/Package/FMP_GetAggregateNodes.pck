create or replace package FMP_GetAggregateNodes is

  procedure FMCSP_GetAggregateNodes(pIn_nAggregationID   in number,
                                    pIn_bNeedCreateTab   in boolean default false,
                                    pIn_vSequence        in varchar2,
                                    pOut_rAggregateNodes out sys_refcursor,
                                    pOut_vTabName        out varchar2,
                                    pOut_nSqlCode        out number);

  procedure FMCSP_GetAggNodesByConditions(pIn_nAggregationID   in number,
                                          pIn_bNeedCreateTab   in boolean default false,
                                          pIn_vConditions      in varchar2,
                                          pIn_vSequence        in varchar2,
                                          pOut_rAggregateNodes out nocopy sys_refcursor,
                                          pOut_vTabName        out varchar2,
                                          pOu8t_nSqlCode       out number);
  procedure FMCSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  in number,
                                       pIn_vSequence   in varchar2 default null,
                                       pIn_vConditions in varchar2,
                                       pOut_Nodes      out sys_refcursor,
                                       pOut_nSqlCode   out number);
end FMP_GetAggregateNodes;
/
create or replace package body FMP_GetAggregateNodes is
  --*****************************************************************
  -- Description: get aggregate nodes

  -- Error Conditions Raised:
  --
  -- Author:      JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        20-MAR-2013     JY.Liu       moved from package p_aggregation
  -- **************************************************************

  procedure FMCSP_GetAggregateNodes(pIn_nAggregationID   in number,
                                    pIn_bNeedCreateTab   in boolean default false,
                                    pIn_vSequence        in varchar2,
                                    pOut_rAggregateNodes out sys_refcursor,
                                    pOut_vTabName        out varchar2,
                                    pOut_nSqlCode        out number)
  --*****************************************************************
    -- Description: get nodes by aggregation ID
    --
    -- Parameters:
    --       pIn_nAggregationID
    --       pIn_vConditions
    --       pIn_vSequence
    --       pOut_rAggregateNodes
    --       pOut_vTabName
    --       pOu8t_nSqlCode

    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-MAR-2013     JY.Liu       moved from package p_aggregation
    -- **************************************************************
   as
    v_Strsql   CLOB;
    v_strfield CLOB;
    v_strwhere CLOB;
    cSQL       CLOB;
  begin

    pOut_nSqlCode := 0;
    --add log
    Fmp_log.FMP_SetValue(pIn_nAggregationID);
    Fmp_log.FMP_SetValue(pIn_vSequence);
    Fmp_log.LOGBEGIN;
    --wfq 2013.1.30 --------------------------
    IF pIn_nAggregationID < 1 THEN
      pOut_nSqlCode := -20006;
      return;
    END IF;
    if pIn_vSequence is null then
      v_Strsql := 'select sel_em_addr id, sel_cle, sel_desc  from v_AggregateNode
             where prv_em_addr =' || pIn_nAggregationID;
    else
      P_SortSequence.sp_sequence(P_Sequence  => pIn_vSequence,
                                 P_AggruleID => pIn_nAggregationID,
                                 p_strfield  => v_strfield,
                                 p_strwhere  => v_strwhere,
                                 p_sqlcode   => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
      v_Strsql := ' select /*+ no_parallel */ t.sel_em_addr id ,sel_cle,sel_desc' ||
                  v_strfield || '
          from (select s.sel_em_addr, s.sel_cle, sel_desc,fam4_em_addr,geo5_em_addr,dis6_em_addr
             from v_AggregateNode s left join v_aggnodetodimension d
                  on s.sel_em_addr=d.sel_em_addr
             where prv_em_addr=' || pIn_nAggregationID ||
                  ') t ' || v_strwhere;
    end if;

    pOut_vTabName := fmf_gettmptablename;
    cSQL          := 'create table ' || pOut_vTabName || ' as ' || v_Strsql;
    fmsp_execsql(pIn_cSql => csql);
    execute immediate 'truncate table TB_TS_AggregateNode';
    cSQL := 'INSERT into TB_TS_AggregateNode select id from ' ||
            pOut_vTabName;
    fmsp_execsql(pIn_cSql => cSQL);
    open pOut_rAggregateNodes for 'select * from ' || pOut_vTabName;
    Fmp_Log.LogEnd;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_Constant.e_oraerr,
                              pOut_nSqlCode || sqlerrm);
  end;

  procedure FMCSP_GetAggNodesByConditions(pIn_nAggregationID   in number,
                                          pIn_bNeedCreateTab   in boolean default false,
                                          pIn_vConditions      in varchar2,
                                          pIn_vSequence        in varchar2,
                                          pOut_rAggregateNodes out nocopy sys_refcursor,
                                          pOut_vTabName        out varchar2,
                                          pOu8t_nSqlCode       out number)
  --*****************************************************************
    -- Description: get nodes by condition
    --
    -- Parameters:
    --       pIn_nAggregationID
    --       pIn_bNeedCreateTab
    --       pIn_vConditions
    --       pIn_vSequence
    --       pOut_rAggregateNodes
    --       pOut_vTabName
    --       pOu8t_nSqlCode

    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-MAR-2013     JY.Liu       moved from package p_aggregation
    -- **************************************************************
   as
    v_P_Level         number;
    v_ST_Level        number;
    v_TC_Level        number;
    v_DatasCounts     number;
    v_Ope             number;
    v_Sql_Part        clob;
    v_Sql_BetweenPart clob;
    v_F_Begin         VARCHAR2(60);
    v_F_End           VARCHAR2(60);
    v_AttrID          number;

    v_TmpCdtCount  number;
    v_p_loop_times number;
    v_AggNodeCount number;
    v_Cstcount     number;
    v_Sql          varchar2(2000);

    v_Strsql   clob;
    v_strfield clob;
    v_strwhere clob;

    -------16/10/2012
    v_sql_p           clob := ' select fam_em_addr from v_productattrvalue p ';
    v_sql_p_where     clob := ' where 1=1 ';
    v_sql_p_connectby clob;

    v_sql_st           clob := ' select geo_em_addr from v_saleterritoryattrvalue s ';
    v_sql_st_where     clob := ' where 1=1 ';
    v_sql_st_connectby clob;

    v_sql_tc           clob := ' select dis_em_addr from v_tradechannelattrvalue t ';
    v_sql_tc_where     clob := ' where 1=1 ';
    v_sql_tc_connectby clob;
    -------16/10/2012
    v_Str_Temp_sql clob := '';
    cSql           clob;

  begin
    pOu8t_nSqlCode := 0;

    --add log
    Fmp_Log.FMP_SetValue(pIn_nAggregationID);
    Fmp_Log.FMP_SetValue(pIn_vConditions);
    Fmp_Log.FMP_SetValue(pIn_vSequence);
    Fmp_Log.LOGBEGIN;
    if pOu8t_nSqlCode <> 0 then
      return;
    end if;
    --wfq 2013.1.30 --------------------------
    IF pIn_nAggregationID < 1 THEN
      pOu8t_nSqlCode := -20006;
      return;
    END IF;

    SP_GetcdtByConditions(P_Conditions => pIn_vConditions,
                          p_SqlCode    => pOu8t_nSqlCode);
    if pOu8t_nSqlCode <> 0 then
      return;
    end if;

    --clear tmp table
    delete from tmp_agg_node;

    select p.regroup_pro, p.regroup_geo, p.regroup_dis
      into v_P_Level, v_ST_Level, v_TC_Level
      from prv p
     where p.prv_em_addr = pIn_nAggregationID;

    if v_P_Level <> 0 then
      --have  level defined in aggregation.so use all of the attributes and key set in the condition of DDA.
      --delete the conditions:when operator is 3 and only one value spefified!
      v_sql_p_where := v_sql_p_where || ' and p.nlevel= ' || v_P_Level;
      delete from tmp_cdt t
       where exists (select tabid, attrordno, ope, val_idx, addr
                from (select tabid,
                             attrordno,
                             ope,
                             val_idx,
                             addr,
                             count(ope) over(partition by tabid, ope) valcount
                        from tmp_cdt) c
               where c.ope = 3
                 and c.valcount = 1
                 and t.tabid = c.tabid
                 and t.attrordno = c.attrordno
                 and t.ope = c.ope
                 and t.val_idx = c.val_idx
                 and t.addr = c.addr);

      select count(*)
        into v_Datascounts
        from tmp_cdt c
       where c.tabid = p_Constant.PRODUCT;
      if v_Datascounts <> 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.tabid,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.PRODUCT) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;

        case v_Ope
          when 1 then
            v_sql_p_connectby := ' start with p.fam_em_addr in ' ||
                                 v_Sql_Part ||
                                 ' connect by prior p.fam_em_addr=p.fam0_em_addr';
          when 2 then
            v_sql_p_where := v_sql_p_where ||
                             ' and not exists (select 1  from v_fam_tree x  where p.fam_em_addr = x.fam_em_addr and x.nlevel = ' ||
                             v_P_Level || ' start with x.fam_em_addr in ' ||
                             v_Sql_Part ||
                             ' connect by prior x.fam_em_addr=x.fam0_em_addr)';
          when 3 then
            select f_cle
              into v_F_Begin
              from fam f
             where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select f_cle
              into v_F_End
              from fam f
             where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_p_connectby := ' start with p.f_cle between ''' ||
                                 v_F_Begin || ''' and ''' || v_F_End ||
                                 ''' and p.nlevel=(select nlevel from v_fam_tree where f_cle=''' ||
                                 v_F_Begin || ''')
                               connect by prior fam_em_addr=fam0_em_addr';
          else
            null;
        end case;
      else
        null; --all of products
      end if;

      for k in (select distinct c.attrordno
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_ProductAttr
                   and c.ope <> 0) loop
        select t.attrordno,
               t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_AttrID, v_Ope, v_Sql_Part
          from (select c.attrordno + 49 attrordno,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_ProductAttr
                   and c.attrordno = k.attrordno) t
         group by t.attrordno, t.ope
         start with t.cur = 1
        connect by prior t.cur = t.prev;
        case v_Ope
          when 1 then
            v_sql_P_where := v_sql_P_where || ' and  p.c' || v_AttrID ||
                             ' in ' || v_Sql_Part;
          when 2 then
            v_sql_P_where := v_sql_P_where || '  and (p.c' || v_AttrID ||
                             ' not in ' || v_Sql_Part || ' or p.c' ||
                             v_AttrID || ' is null )';
          when 3 then
            begin
              select '(' ||
                     ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
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
              v_sql_P_where := v_sql_P_where || '   and p.c' || v_AttrID ||
                               '  in ' || v_Sql_BetweenPart;
            exception
              when no_data_found then
                null;
            end;
          else
            null;
        end case;
      end loop;
    else
      --process cst of product
      --4 represent cst
      select count(*)
        into v_Cstcount
        from cdt c
       where c.prv12_em_addr = pIn_nAggregationID
         and c.operant = 4
         and c.rcd_cdt = p_Constant.v_ProductAttr;
      if v_Cstcount = 0 then
        null;
      else
        for i in (select c.n0_cdt attrno
                    from cdt c
                   where c.prv12_em_addr = pIn_nAggregationID
                     and c.operant = 4
                     and c.rcd_cdt = p_Constant.v_ProductAttr) loop

          select count(*)
            into v_TmpCdtCount
            from tmp_cdt d
           where d.tabid = p_Constant.v_ProductAttr
             and d.attrordno = i.attrno;
          --condition match with aggregation rule 's cst condition
          if v_TmpCdtCount <> 0 then
            select t.ope,
                   '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Ope, v_Sql_Part
              from (select c.tabid,
                           c.ope,
                           c.addr,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                      from tmp_cdt c
                     where c.tabid = p_Constant.v_ProductAttr
                       and c.attrordno = i.attrno) t
             group by t.ope
             start with t.cur = 1
            connect by prior t.prev = t.cur;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            v_p_loop_times := v_p_loop_times + 1;
            case v_Ope
              when 1 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                               select t.sel11_em_addr
                                 from cdt t ,prvsel p
                                where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                             (select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_Part || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 2 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr || '
                                and t.n0_cdt= ' ||
                                    i.attrno || '
                                and t.adr_cdt not in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid  not in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr ||
                                    ' and t.n0_cdt=' || i.attrno || '
                                      and t.adr_cdt not in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 3 then
                --parse a string like this '(124,125)' to 2 string like this '124' and '125'
                select '(' ||
                       ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
                  into v_Sql_BetweenPart
                  from (select v.vct_em_addr addr,
                               v.num_crt,
                               row_number() over(partition by v.num_crt order by v.val) cur,
                               row_number() over(partition by v.num_crt order by v.val) - 1 prev
                          from vct v
                         where v.id_crt = p_Constant.v_productdata
                           and v.num_crt = i.attrno + 49
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
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                               select t.sel11_em_addr
                                 from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                             (select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_ProductAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              else
                null;
            end case;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            if v_AggNodecount = 0 then
              return;
            end if;
          end if;
        end loop;
      end if;
    end if;
    -- sale territory
    if v_ST_Level <> 0 then
      v_sql_st_where := v_sql_st_where || ' and s.nlevel =  ' || v_ST_Level;
      select count(*)
        into v_Datascounts
        from tmp_cdt c
       where c.tabid = p_Constant.SALE_TERRITORY;

      if v_Datascounts <> 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.tabid,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.SALE_TERRITORY) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case v_Ope
          when 1 then
            v_sql_st_connectby := ' start with s.geo_em_addr in ' ||
                                  v_Sql_Part ||
                                  ' connect by prior s.geo_em_addr=s.geo1_em_addr';

          when 2 then
            v_sql_st_where := v_sql_st_where ||
                              ' and not exists (select 1  from v_geo_tree x where s.geo_em_addr=x.geo_em_addr and  x.nlevel = ' ||
                              v_ST_Level || ' start with x.geo_em_addr in ' ||
                              v_Sql_Part || '
                               connect by prior x.geo_em_addr=x.geo1_em_addr )';
          when 3 then
            --parse a string like this '(124,125)' to 2 string like this '124' and '125'
            select g.g_cle
              into v_F_Begin
              from geo g
             where g.geo_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select g.g_cle
              into v_F_End
              from geo g
             where g.geo_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_st_connectby := ' start with s.g_cle  between ''' ||
                                  v_F_Begin || ''' and ''' || v_F_End ||
                                  '''   and g.nlevel =(select nlevel from v_geo_tree where g_cle=''' ||
                                  v_F_Begin ||
                                  ''' ) connect by prior geo_em_addr=geo1_em_addr';
          else
            null;
        end case;

      else
        null;
      end if;

      for k in (select distinct c.attrordno
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_STAttr
                   and c.ope <> 0) loop
        select t.attrordno,
               t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_AttrID, v_Ope, v_Sql_Part
          from (select c.attrordno + 49 attrordno,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_STAttr
                   and c.attrordno = k.attrordno) t
         group by t.attrordno, t.ope
         start with t.cur = 1
        connect by prior t.cur = t.prev;

        case v_Ope
          when 1 then
            v_sql_st_where := v_sql_st_where || '   and s.c' || v_attrid ||
                              ' in ' || v_Sql_Part;
          when 2 then
            v_sql_st_where := v_sql_st_where || '   and (s.c' || v_attrid ||
                              ' not in ' || v_Sql_Part || ' or s.c' ||
                              v_AttrID || ' is null )';
          when 3 then
            begin
              select '(' ||
                     ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
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
              v_sql_st_where := v_sql_st_where || '  and s.c' || v_attrid ||
                                ' in ' || v_Sql_BetweenPart;
            exception
              when no_data_found then
                null;
            end;
          else
            null;
        end case;
      end loop;
    else
      --process cst of sale territory
      select count(*)
        into v_Cstcount
        from cdt c
       where c.prv12_em_addr = pIn_nAggregationID
         and c.operant = 4
         and c.rcd_cdt = p_Constant.v_STAttr;
      if v_Cstcount = 0 then
        null;
      else
        for i in (select c.n0_cdt attrno
                    from cdt c
                   where c.prv12_em_addr = pIn_nAggregationID
                     and c.operant = 4
                     and c.rcd_cdt = p_Constant.v_STAttr) loop

          select count(*)
            into v_TmpCdtCount
            from tmp_cdt d
           where d.tabid = p_Constant.v_STAttr
             and d.attrordno = i.attrno;
          if v_TmpCdtCount <> 0 then
            select t.ope,
                   '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Ope, v_Sql_Part
              from (select c.tabid,
                           c.ope,
                           c.addr,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                      from tmp_cdt c
                     where c.tabid = p_Constant.v_STAttr
                       and c.attrordno = i.attrno) t
             group by t.ope
             start with t.cur = 1
            connect by prior t.prev = t.cur;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            v_p_loop_times := v_p_loop_times + 1;
            case v_Ope
              when 1 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                     and t.adr_cdt in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                               (select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                       and t.adr_cdt in ' ||
                                    v_Sql_Part || ')';

                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 2 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt not in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid  in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt not in ' ||
                                    v_Sql_Part || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 3 then
                select '(' ||
                       ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
                  into v_Sql_BetweenPart
                  from (select v.vct_em_addr addr,
                               v.num_crt,
                               row_number() over(partition by v.num_crt order by v.val) cur,
                               row_number() over(partition by v.num_crt order by v.val) - 1 prev
                          from vct v
                         where v.id_crt = p_Constant.v_STData
                           and v.num_crt = i.attrno + 49
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
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                               select t.sel11_em_addr
                                 from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                             (select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_STAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              else
                null;
            end case;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            if v_AggNodecount = 0 then
              return;
            end if;
          end if;

        end loop;
      end if;
    end if;

    -- trade channel level
    if v_TC_level <> 0 then
      v_sql_tc_where := v_sql_tc_where || ' and t.nlevel=' || v_TC_level;
      select count(*)
        into v_Datascounts
        from tmp_cdt c
       where c.tabid = p_Constant.TRADE_CHANNEL;

      if v_Datascounts <> 0 then

        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_Ope, v_Sql_Part
          from (select c.tabid,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.TRADE_CHANNEL) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;

        case v_Ope
          when 1 then
            v_sql_tc_connectby := ' start with t.dis_em_addr in ' ||
                                  v_Sql_Part ||
                                  ' connect by prior t.dis_em_addr=t.dis2_em_addr';
          when 2 then
            v_sql_tc_where := v_sql_tc_where ||
                              ' and not exists (select 1  from v_dis_tree x   where t.dis_em_addr = x.dis_em_addr and x.nlevel = ' ||
                              v_TC_Level || ' start with x.dis_em_addr in ' ||
                              v_Sql_Part ||
                              ' connect by prior x.dis_em_addr=x.dis2_em_addr)';
            select d.d_cle
              into v_F_Begin
              from dis d
             where d.dis_em_addr = f_GetStr(v_Sql_Part, '(', ',');
            select d.d_cle
              into v_F_End
              from dis d
             where d.dis_em_addr = f_GetStr(v_Sql_Part, ',', ')');
            v_sql_tc_connectby := ' start with t.d_cle between ''' ||
                                  v_F_Begin || ''' and ''' || v_F_End ||
                                  '''  and t.nlevel-(select nlevel from v_dis_tree where d_cle= ''' ||
                                  v_F_Begin ||
                                  ''') connect by prior t.dis_em_addr=t.dis2_em_addr';

          else
            null;
        end case;
      else
        null;
      end if;

      --trade channel 's attributes
      for k in (select distinct c.attrordno
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_TCAttr
                   and c.ope <> 0) loop
        select t.attrordno,
               t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into v_AttrID, v_Ope, v_Sql_Part
          from (select c.attrordno + 49 attrordno,
                       c.ope,
                       c.addr,
                       row_number() over(partition by c.ope order by c.val_idx) cur,
                       row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                  from tmp_cdt c
                 where c.tabid = p_Constant.v_TCAttr
                   and c.attrordno = k.attrordno) t
         group by t.attrordno, t.ope
         start with t.cur = 1
        connect by prior t.cur = t.prev;

        case v_Ope
          when 1 then
            v_sql_tc_where := v_sql_tc_where || ' and t.c' || v_AttrID ||
                              ' in ' || v_Sql_Part;
          when 2 then
            v_sql_tc_where := v_sql_tc_where || ' and (t.c' || v_AttrID ||
                              ' not in ' || v_Sql_Part || ' or t.c' ||
                              v_AttrID || ' is null )';
          when 3 then
            begin
              select '(' ||
                     ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
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
              v_sql_tc_where := v_sql_tc_where || ' and t.c' || v_AttrID ||
                                ' in ' || v_Sql_BetweenPart;
            exception
              when no_data_found then
                null;
            end;
          else
            null;

        end case;
      end loop;
    else
      --process cst of trade channel
      select count(*)
        into v_Cstcount
        from cdt c
       where c.prv12_em_addr = pIn_nAggregationID
         and c.operant = 4
         and c.rcd_cdt = p_Constant.v_TCAttr;
      if v_Cstcount = 0 then
        null;
      else
        for i in (select c.n0_cdt attrno
                    from cdt c
                   where c.prv12_em_addr = pIn_nAggregationID
                     and c.operant = 4
                     and c.rcd_cdt = p_Constant.v_TCAttr) loop

          select count(*)
            into v_TmpCdtCount
            from tmp_cdt d
           where d.tabid = p_Constant.v_TCAttr
             and d.attrordno = i.attrno;
          if v_TmpCdtCount <> 0 then
            select t.ope,
                   '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into v_Ope, v_Sql_Part
              from (select c.tabid,
                           c.ope,
                           c.addr,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                           row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                      from tmp_cdt c
                     where c.tabid = p_Constant.v_TCAttr
                       and c.attrordno = i.attrno) t
             group by t.ope
             start with t.cur = 1
            connect by prior t.prev = t.cur;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            v_p_loop_times := v_p_loop_times + 1;
            case v_Ope
              when 1 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                     and t.adr_cdt in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                       and t.adr_cdt in ' ||
                                    v_Sql_Part || ')';

                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 2 then
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_Part;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid  in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt not in ' ||
                                    v_Sql_Part || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              when 3 then
                select '(' ||
                       ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
                  into v_Sql_BetweenPart
                  from (select v.vct_em_addr addr,
                               v.num_crt,
                               row_number() over(partition by v.num_crt order by v.val) cur,
                               row_number() over(partition by v.num_crt order by v.val) - 1 prev
                          from vct v
                         where v.id_crt = p_Constant.v_TCData
                           and v.num_crt = i.attrno + 49
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
                if v_AggNodecount = 0 and v_p_loop_times = 1 then
                  execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                               select t.sel11_em_addr
                                 from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart;
                elsif v_AggNodecount > 0 then
                  execute immediate 'delete from tmp_agg_node where aggnodeid not in
                             (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                    pIn_nAggregationID ||
                                    ' and p.sel16_em_addr=t.sel11_em_addr and t.rcd_cdt=' ||
                                    p_Constant.v_TCAttr ||
                                    ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                    v_Sql_BetweenPart || ')';
                elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                  return;
                end if;
              else
                null;

            end case;
            select count(*) into v_AggNodecount from tmp_Agg_Node;
            if v_AggNodecount = 0 then
              return;
            end if;
          end if;

        end loop;
      end if;
    end if;
    --process cst of detail node
    select count(*)
      into v_Cstcount
      from cdt c
     where c.prv12_em_addr = pIn_nAggregationID
       and c.operant = 4
       and c.rcd_cdt = p_Constant.DETAIL_NODE;
    if v_Cstcount <> 0 then
      for i in (select c.n0_cdt attrno
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggregationID
                   and c.operant = 4
                   and c.rcd_cdt = p_Constant.DETAIL_NODE) loop

        select count(*)
          into v_TmpCdtCount
          from tmp_cdt d
         where d.tabid = p_Constant.DETAIL_NODE
           and d.attrordno = i.attrno;
        if v_TmpCdtCount <> 0 then
          select t.ope,
                 '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
            into v_Ope, v_Sql_Part
            from (select c.tabid,
                         c.ope,
                         c.addr,
                         row_number() over(partition by c.tabid, c.ope order by c.val_idx) cur,
                         row_number() over(partition by c.tabid, c.ope order by c.val_idx) + 1 prev
                    from tmp_cdt c
                   where c.tabid = p_Constant.DETAIL_NODE
                     and c.attrordno = i.attrno) t
           group by t.ope
           start with t.cur = 1
          connect by prior t.prev = t.cur;
          select count(*) into v_AggNodecount from tmp_Agg_Node;
          v_p_loop_times := v_p_loop_times + 1;
          case v_Ope
            when 1 then
              if v_AggNodecount = 0 and v_p_loop_times = 1 then
                execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                     and t.adr_cdt in ' ||
                                  v_Sql_Part;
              elsif v_AggNodecount > 0 then
                execute immediate 'delete from tmp_agg_node where aggnodeid not in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                       and t.adr_cdt in ' ||
                                  v_Sql_Part || ')';
              elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                return;
              end if;
            when 2 then
              if v_AggNodecount = 0 and v_p_loop_times = 1 then
                execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                                select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                minus
                                select t.sel11_em_addr
                                from cdt t
                                where t.prv12_em_addr=' ||
                                  pIn_nAggregationID || ' and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                  v_Sql_Part;
              elsif v_AggNodecount > 0 then
                execute immediate 'delete from tmp_agg_node where aggnodeid  in
                               (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                  v_Sql_Part || ')';
              elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                return;
              end if;
            when 3 then
              select '(' ||
                     ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
                into v_Sql_BetweenPart
                from (select v.crtserie_em_addr addr,
                             v.num_crt_serie,
                             row_number() over(partition by v.num_crt_serie order by v.val_crt_serie) cur,
                             row_number() over(partition by v.num_crt_serie order by v.val_crt_serie) - 1 prev
                        from crtserie v
                       where v.id_crt_serie = p_Constant.v_DetailNodeData
                         and v.num_crt_serie = i.attrno + 49 --note here when 101 99 if need to do ?
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
              if v_AggNodecount = 0 and v_p_loop_times = 1 then
                execute immediate 'insert into tmp_Agg_Node(aggnodeid)
                               select t.sel11_em_addr
                                 from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                  v_Sql_BetweenPart;
              elsif v_AggNodecount > 0 then
                execute immediate 'delete from tmp_agg_node where aggnodeid not in
                             (select t.sel11_em_addr
                                from cdt t  ,prvsel p
                              where p.prv15_em_addr=' ||
                                  pIn_nAggregationID ||
                                  ' and p.sel16_em_addr=t.sel11_em_addr  and t.rcd_cdt=' ||
                                  p_Constant.DETAIL_NODE ||
                                  ' and t.n0_cdt= ' || i.attrno || '
                                      and t.adr_cdt in ' ||
                                  v_Sql_BetweenPart || ')';
              elsif v_AggNodecount = 0 and v_p_loop_times <> 1 then
                return;
              end if;
            else
              null;

          end case;
          select count(*) into v_AggNodecount from tmp_Agg_Node;
          if v_AggNodecount = 0 then
            return;
          end if;
        end if;

      end loop;
    end if;

    select count(*) into v_AggNodecount from tmp_Agg_Node where rownum < 2;

    v_sql_p  := v_sql_p || v_sql_P_where || v_sql_P_connectby;
    v_sql_st := v_sql_st || v_sql_st_where || v_sql_st_connectby;
    v_sql_tc := v_sql_tc || v_sql_tc_where || v_sql_tc_connectby;
    if v_P_Level <> 0 then
      v_Sql := ' and a.pid in (' || v_sql_p || ') ';
    end if;
    if v_st_Level <> 0 then
      v_Sql := v_Sql || ' and a.stid in (' || v_sql_st || ') ';
    end if;
    if v_tc_Level <> 0 then
      v_Sql := v_Sql || ' and a.tcid in (' || v_sql_tc || ') ';
    end if;
    if v_AggNodecount > 0 then
      v_Sql := v_Sql ||
               ' and a.aggnodeid in (select aggnodeid from tmp_agg_node)';
    end if;

    if pIn_vSequence is null then
      v_Strsql := 'select a.aggnodeid id,a.name,a.description from v_aggnodewithlevel a where a.aggregationid=' ||
                  pIn_nAggregationID || v_sql;
    else

      P_SortSequence.sp_sequence(P_Sequence  => pIn_vSequence,
                                 P_AggruleID => pIn_nAggregationID,
                                 p_strfield  => v_strfield,
                                 p_strwhere  => v_strwhere,
                                 p_sqlcode   => pOu8t_nSqlCode);
      if pOu8t_nSqlCode <> 0 then
        return;
      end if;
      v_Strsql := ' select /*+ no_parallel */ t.sel_em_addr id,t.sel_cle,t.description' ||
                  v_strfield || '
          from (select a.aggnodeid sel_em_addr,a.name sel_cle,a.description
          ,a.pid fam4_em_addr,stid geo5_em_addr ,tcid dis6_em_addr
          from v_aggnodewithlevel a
          where a.aggregationid=' || pIn_nAggregationID ||
                  v_sql || ') t ' || v_strwhere;
    end if;
    pOut_vTabName := fmf_gettmptablename;
    cSql          := ' create table ' || pOut_vTabName || ' as ' ||
                     v_Strsql;
    fmsp_execsql(pIn_cSql => cSql);
    execute immediate 'truncate table TB_TS_AggregateNodeCon';
    v_Str_Temp_sql := ' insert into TB_TS_AggregateNodeCon  select id from ' ||
                      pOut_vTabName;
    fmsp_execsql(pIn_cSql => v_Str_Temp_sql);
    open pOut_rAggregateNodes for ' select * from ' || pOut_vTabName;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOu8t_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_Constant.e_oraerr,
                              pOu8t_nSqlCode || sqlerrm);
  end;

  procedure FMCSP_GetAggNodesByRuleCdt(pIn_nAggRuleID  in number,
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
    --  V7.0        20-MAR-2013     JY.Liu       moved from package p_aggregation
    --  V7.0        21-MAR-2013     JY.Liu       add input parameter pIn_vSequence.
    -- **************************************************************
   is
    vTabName      varchar2(30);
    nNum          number := 1;
    v1stTableName varchar2(30);
  begin

    if pIn_nAggRuleID <> 0 then
      FMCSP_GetAggNodesByConditions(pIn_nAggregationID   => pIn_nAggRuleID,
                                    pIn_vConditions      => pIn_vConditions,
                                    pIn_vSequence        => pIn_vSequence,
                                    pOut_rAggregateNodes => pOut_Nodes,
                                    pOut_vTabName        => vTabName,
                                    pOu8t_nSqlCode       => pOut_nSqlCode);
    else
      for id in (select p.prv_em_addr id from prv p) loop
        FMCSP_GetAggNodesByConditions(pIn_nAggregationID   => id.id,
                                      pIn_vConditions      => pIn_vConditions,
                                      pIn_vSequence        => pIn_vSequence,
                                      pOut_rAggregateNodes => pOut_Nodes,
                                      pOut_vTabName        => vTabName,
                                      pOu8t_nSqlCode       => pOut_nSqlCode);
        if nNum = 1 then
          --union all other datas into this table and return at last
          v1stTableName := vTabName;
        else
          fmsp_execsql(pIn_cSql => 'insert into ' || v1stTableName ||
                                   ' select * from ' || vTabName);
          fmsp_execsql(pIn_cSql => 'drop table ' || vTabName || ' purge ');
        end if;
        nNum := nNum + 1;
      end loop;
      fmsp_execsql(pIn_cSql => 'truncate table TB_TS_AggregateNodeCon');
      fmsp_execsql(pIn_cSql => 'insert into  TB_TS_AggregateNodeCon select id from ' ||
                               v1stTableName);
      open pOut_Nodes for 'select * from ' || v1stTableName;
    end if;
  exception
    when others then
      fmp_log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode);
  end;

end FMP_GetAggregateNodes;
/
