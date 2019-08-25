create or replace package p_Selection Authid Current_User is
  /********
  Created by JYLiu on 6/7/2012 package contains all of the procedures about selection module
  ********/
  -- cursor of detail node
  type r_Pvt is record(
    pvt_em_addr pvt.pvt_em_addr%type,
    pvt_cle     pvt.pvt_cle%type,
    pvt_desc    pvt.pvt_desc%type);
  type t_DetailNode is ref cursor return r_Pvt;
  --  type t_DetailNode is ref cursor return pvt%rowtype;
  --build the relationship between one selection and multi-detailnodes
  procedure SP_BuildSelection(p_SelectionID in number,
                              p_SqlCode     out number);

  --Destroy Selection to detail nodes link
  procedure SP_DestroySelection(p_SelectionID in number,
                                p_SqlCode     out number);

  --retrive detail nodes by a group of conditions
  procedure SP_GetDetailNodeByConditions(P_Conditions       in varchar2,
                                         pIn_bNeedCreateTab in boolean default false,
                                         P_Sequence         in varchar2, --Sort sequence
                                         p_DetailNode       out sys_refcursor,
                                         pOut_vTabName      out varchar2,
                                         p_SqlCode          out number);
  --retrive detail nodes by selection
  procedure SP_GetDetailNodeBySelectionID(P_SelectionID      in number,
                                          pIn_bNeedCreateTab in boolean default false,
                                          p_IsDynamic        in number,
                                          P_Sequence         in varchar2, --Sort sequence
                                          p_DetailNode       out sys_refcursor,
                                          pOut_vTabName      out varchar2,
                                          p_SqlCode          out number);
  --retrive detail nodes by selection or|and conditions
  procedure SP_GetDetailNodeBySelCdt(P_SelectionID      in number,
                                     pIn_bNeedCreateTab in boolean default false,
                                     P_Conditions       in varchar2,
                                     P_Sequence         in varchar2, --Sort sequence
                                     p_DetailNode       out sys_refcursor,
                                     pOut_vTabName      out varchar2,
                                     p_SqlCode          out number);

  --private
  --get detail node by 3D
  procedure SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode out varchar2,
                                         p_SqlCode                out number);

  /*  Return all selections which have id, key, descriptions
  Kind(1 :Selection, 2 :AggreGateNode, 3 :Selection and AggreGateNode)*/
  procedure SP_GetAllSelections(P_Kind      in number,
                                p_Selection out sys_refcursor,
                                p_SqlCode   out number);

  --get detail node by detail node's attributes
  --  procedure SP_GetDetailNodeByData(P_SelectionID in number,p_SqlCode out number);

  --get a substring from a string.for example to get sub-string '12' from string '(12,34)', using f_GetStr('(12,34)','(',',')

  procedure SP_ReBuildSelection(p_SelectionID in number,
                                p_SqlCode     out number);

  procedure FMISP_GetDetailNodeByCdt(PIn_vConditions in varchar2,
                                     PIn_vSequence   in varchar2,
                                     pOut_DetailNode out sys_refcursor,
                                     pOut_vTabName   out varchar2,
                                     pOut_SqlCode    out number);
  procedure FMISP_GetDetailNodeBySelID(PIn_nSelectionID in number,
                                       pIn_nIsDynamic   in number,
                                       PIn_vSequence    in varchar2,
                                       pOut_DetailNode  out sys_refcursor,
                                       pOut_vTabName    out varchar2,
                                       pOut_SqlCode     out number);

  procedure FMISP_GetDetailNodeBySelCdt(PIn_nSelectionID in number,
                                        PIn_vConditions  in varchar2,
                                        PIn_vSequence    in varchar2,
                                        pOut_DetailNode  out sys_refcursor,
                                        pOut_vTabName    out varchar2,
                                        pOut_SqlCode     out number);

  function f_GetStr(f_String   in varchar2,
                    f_BeginStr in varchar2,
                    f_EndStr   in varchar2) return varchar2;

end p_Selection;
/
create or replace package body p_Selection is

  procedure SP_BuildSelection(p_SelectionID in number,
                              p_SqlCode     out number) as
    /********
    Created by JYLiu on 6/7/2012 Build the relationship between selection and detai nodes.
    ********/
    v_FromClause clob;
    v_cnt        number;
  begin
    Fmp_Log.FMP_SetValue(p_SelectionID);
    Fmp_Log.LOGBEGIN;
    delete from rsp where sel13_em_addr = p_SelectionID;
    delete from tmp_cdt;
    insert into tmp_cdt
      (tabid, attrordno, ope, val_idx, addr)
      select c.rcd_cdt, c.n0_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
        from cdt c
       where c.sel11_em_addr = P_SelectionID
         and c.operant <> 0;
  
    SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode => v_FromClause,
                                 p_SqlCode                => p_SqlCode);
  
    if p_SqlCode <> 0 then
      return;
    end if;
    if v_FromClause is null then
      p_SqlCode := 0;
      return;
    end if;
    select s.sel_em_addr
      into v_cnt
      from sel s
     where s.sel_em_addr = p_SelectionID
       for update;
    select count(*)
      into v_cnt
      from rsp r
     where r.sel13_em_addr = p_SelectionID
       and rownum < 2;
    if v_cnt = 0 then
      execute immediate 'insert   into rsp(rsp_em_addr,sel13_em_addr,pvt14_em_addr)
     select seq_rsp.nextval,' || p_SelectionID ||
                        ',pvt_em_addr   ' || v_FromClause;
      commit;
      update sel s
         set s.effectif    =
             (select count(*)
                from rsp r
               where r.sel13_em_addr = p_SelectionID),
             s.annee_select = to_char(sysdate, 'yyyy'),
             s.mois_select  = to_char(sysdate, 'mm'),
             s.jour_select  = to_char(sysdate, 'dd')
       where s.SEL_EM_ADDR = p_SelectionID;
      p_SqlCode := 0;
    end if;
    commit;
    Fmp_log.LOGEND;
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      Fmp_log.LOGERROR;
      --raise_application_error(SQLCODE,SQLERRM);
  end;
  procedure SP_GetDetailNodeByConditions(P_Conditions       in varchar2,
                                         pIn_bNeedCreateTab in boolean default false,
                                         P_Sequence         in varchar2, --Sort sequence
                                         p_DetailNode       out sys_refcursor,
                                         pOut_vTabName      out varchar2,
                                         p_SqlCode          out number)
  --*****************************************************************
    -- Description: get detail nodes by conditions
    --
    -- Parameters:
    --       P_Conditions:
    --       pIn_bNeedCreateTab
    --       P_Sequence
    --       p_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
    --       p_SqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JY.Liu     add a new put parameter pOut_vTabName.
    -- **************************************************************
   as
  
    v_FromClause varchar2(2000);
  
    cSql   clob;
    cfield clob;
    cWhere clob;
  
  begin
  
    p_SqlCode := 0;
  
    --add log
    Fmp_log.FMP_SetValue(P_Conditions);
    Fmp_log.FMP_SetValue(P_Sequence);
    Fmp_Log.LOGBEGIN;
    if p_SqlCode <> 0 then
      return;
    end if;
  
    SP_GetcdtByConditions(P_Conditions => P_Conditions,
                          p_SqlCode    => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
  
    SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode => v_FromClause,
                                 p_SqlCode                => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
    --open p_DetailNode for ' select pvt_em_addr,pvt_cle,pvt_desc  ' || v_FromClause ||' order by pvt_cle ';
  
    -- sort sequence===============================================================
    if P_Sequence is null then
      cSql := 'select pvt_em_addr id,pvt_cle,pvt_desc  ' || v_FromClause;
    else
      -- Call the SortSequence===============================================================
      P_SortSequence.sp_sequence(P_Sequence  => P_Sequence,
                                 P_AggruleID => 0, --if P_AggruleID=0 then DetailNode else AggNode
                                 p_strfield  => cfield,
                                 p_strwhere  => cWhere,
                                 p_sqlcode   => p_sqlcode);
      if p_SqlCode <> 0 then
        return;
      end if;
    
      cSql := ' select t.pvt_em_addr id, t.pvt_cle, t.pvt_desc' || cfield || '
          from (select pvt_em_addr , pvt_cle, pvt_desc
             ' || v_FromClause || ') t ' || cWhere;
    end if;
  
    --get a new tmp table to store the detail node id temporarily.this table will be destroyed by external application
    pOut_vTabName := fmf_gettmptablename;
    cSql          := ' create table ' || pOut_vTabName || ' as  ' || cSql;
    fmsp_execsql(pIn_cSql => cSql);
    execute immediate 'truncate table TB_TS_DetailNodeCondition';
    execute immediate 'insert into TB_TS_DetailNodeCondition select id from ' ||
                      pOut_vTabName;
    open p_detailnode for 'select * from ' || pOut_vTabName || ' order by pvt_cle';
    Fmp_Log.LOGEND;
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      Fmp_Log.LOGERROR;
      raise;
  end;

  --Destroy Selection to detail nodes link
  procedure SP_DestroySelection(p_SelectionID in number,
                                p_SqlCode     out number) as
  
  begin
    p_SqlCode := 0;
    --delete selection to DetailNode link
    delete rsp where sel13_em_addr = p_SelectionID;
  
    /*--delete selection condition
        delete cdt where sel11_em_addr = p_SelectionID;
        update sel s
           set s.effectif     = 0,
               s.annee_select = to_char(sysdate, 'yyyy'),
               s.mois_select  = to_char(sysdate, 'mm'),
               s.jour_select  = to_char(sysdate, 'dd')
         where s.SEL_EM_ADDR = p_SelectionID;
    */
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      --raise_application_error(SQLCODE,SQLERRM);
  end;

  procedure SP_GetDetailNodeBySelectionID(P_SelectionID      in number,
                                          pIn_bNeedCreateTab in boolean default false,
                                          p_IsDynamic        in number, --1:true,0:false
                                          P_Sequence         in varchar2, --Sort sequence
                                          p_DetailNode       out sys_refcursor,
                                          pOut_vTabName      out varchar2,
                                          p_SqlCode          out number)
  --*****************************************************************
    -- Description: get detail nodes by conditions
    --
    -- Parameters:
    --       P_SelectionID:
    --       pIn_bNeedCreateTab
    --       p_IsDynamic:
    --       P_Sequence
    --       p_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
    --       p_SqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JY.Liu     add a new put parameter pOut_vTabName.
    -- **************************************************************
   as
    v_FromClause varchar2(2000);
  
    cSql   clob;
    cField clob;
    cWhere clob;
  
  begin
    p_SqlCode := 0;
    --add log==================================================================
    Fmp_log.FMP_SetValue(P_SelectionID);
    Fmp_log.FMP_SetValue(p_IsDynamic);
    Fmp_log.FMP_SetValue(P_Sequence);
    Fmp_log.LOGBEGIN;
  
    if p_IsDynamic = 1 then
      --1:true
      if P_SelectionID <> 0 then
        --selection id =0 mean all detail nodes
        insert into tmp_cdt
          (tabid, attrordno, ope, val_idx, addr)
          select c.rcd_cdt, c.n0_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
            from cdt c
           where c.sel11_em_addr = P_SelectionID
             and c.operant <> 0;
      end if;
    
      SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode => v_FromClause,
                                   p_SqlCode                => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
      --open p_DetailNode for ' select pvt_em_addr,pvt_cle,pvt_desc  ' || v_FromClause ||' order by pvt_cle ';
      -- sort sequence===============================================================
      if P_Sequence is null then
        cSql := 'select pvt_em_addr id,pvt_cle,pvt_desc  ' || v_FromClause;
      
      else
        -- Call the SortSequence===============================================================
        P_SortSequence.sp_sequence(P_Sequence  => P_Sequence,
                                   P_AggruleID => 0, --if P_AggruleID=0 then DetailNode else AggNode
                                   p_strfield  => cField,
                                   p_strwhere  => cWhere,
                                   p_sqlcode   => p_sqlcode);
        if p_SqlCode <> 0 then
          return;
        end if;
        cSql := ' select t.pvt_em_addr id, t.pvt_cle, t.pvt_desc' || cField || '
          from (select pvt_em_addr, pvt_cle, pvt_desc
             ' || v_FromClause || ') t ' || cWhere;
      end if;
    
    elsif p_IsDynamic = 0 then
      --0:false
      -- sort sequence===============================================================
      if P_Sequence is null then
      
        cSql := ' select pvt_em_addr id, pvt_cle, pvt_desc from v_Sel_DetailNode  where sel_em_addr = ' ||
                P_SelectionID;
      else
        -- Call the SortSequence===============================================================
        P_SortSequence.sp_sequence(P_Sequence  => P_Sequence,
                                   P_AggruleID => 0, --if P_AggruleID=0 then DetailNode else AggNode
                                   p_strfield  => cField,
                                   p_strwhere  => cWhere,
                                   p_sqlcode   => p_sqlcode);
        if p_SqlCode <> 0 then
          return;
        end if;
        cSql := ' select t.pvt_em_addr id, t.pvt_cle, t.pvt_desc' || cField || '
          from (select pvt_em_addr , pvt_cle, pvt_desc
            from v_Sel_DetailNode
           where sel_em_addr = ' || P_SelectionID || '
            ) t ' || cWhere;
      
      end if;
    
    end if;
  
    pOut_vTabName := fmf_gettmptablename;
    cSql          := ' create table ' || pOut_vTabName || ' as  ' || cSql;
    fmsp_execsql(pIn_cSql => cSql);
    execute immediate 'truncate table TB_TS_DetailNodeCondition';
    execute immediate 'insert into TB_TS_DetailNodeCondition select id from ' ||
                      pOut_vTabName;
    open p_detailnode for 'select * from ' || pOut_vTabName || ' order by pvt_cle';
    Fmp_Log.LOGEND;
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      Fmp_log.LOGERROR;
      raise;
  end;

  procedure SP_GetDetailNodeBySelCdt(P_SelectionID      in number,
                                     pIn_bNeedCreateTab in boolean default false,
                                     P_Conditions       in varchar2,
                                     P_Sequence         in varchar2, --Sort sequence
                                     p_DetailNode       out sys_refcursor,
                                     pOut_vTabName      out varchar2,
                                     p_SqlCode          out number)
  
    --*****************************************************************
    -- Description: get detail nodes by conditions
    --
    -- Parameters:
    --       P_SelectionID:
    --       pIn_bNeedCreateTab
    --       P_Conditions:
    --       P_Sequence
    --       p_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
    --       p_SqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        21-FEB-2013     JY.Liu     add a new put parameter pOut_vTabName.
    -- **************************************************************
   as
    v_FromClause varchar2(2000);
  
    cSQL       CLOB;
    v_strfield CLOB;
    v_strwhere CLOB;
  begin
  
    --add log
    Fmp_Log.FMP_SetValue(P_SelectionID);
    Fmp_Log.FMP_SetValue(P_Conditions);
    Fmp_Log.FMP_SetValue(P_Sequence);
    Fmp_Log.LOGBEGIN;
    if p_SqlCode <> 0 then
      return;
    end if;
  
    if P_SelectionID <> 0 then
      --selection id =0 mean all detail nodes
      insert into tmp_cdt
        (tabid, attrordno, ope, val_idx, addr)
        select c.rcd_cdt, c.n0_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
          from cdt c
         where c.sel11_em_addr = P_SelectionID
           and c.operant <> 0;
    end if;
  
    if P_Conditions is not null then
      SP_GetcdtByConditions(P_Conditions => P_Conditions,
                            p_SqlCode    => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    end if;
  
    SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode => v_FromClause,
                                 p_SqlCode                => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
    --open p_DetailNode for ' select pvt_em_addr,pvt_cle,pvt_desc  ' || v_FromClause ||' order by pvt_cle ';
    -- sort sequence===============================================================
    if P_Sequence is null then
      cSQL := 'select pvt_em_addr id ,pvt_cle,pvt_desc  ' || v_FromClause;
    else
      -- Call the SortSequence===============================================================
      P_SortSequence.sp_sequence(P_Sequence  => P_Sequence,
                                 P_AggruleID => 0, --if P_AggruleID=0 then DetailNode else AggNode
                                 p_strfield  => v_strfield,
                                 p_strwhere  => v_strwhere,
                                 p_sqlcode   => p_sqlcode);
      if p_SqlCode <> 0 then
        return;
      end if;
      cSQL := ' select t.pvt_em_addr id, t.pvt_cle, t.pvt_desc' ||
              v_strfield || '
          from (select pvt_em_addr , pvt_cle, pvt_desc
             ' || v_FromClause || ') t ' || v_strwhere;
    end if;
    pOut_vTabName := fmf_gettmptablename;
    cSQL          := ' create table ' || pOut_vTabName || ' as ' || cSQL;
    fmsp_execsql(pIn_cSql => cSQL);
    execute immediate 'truncate table TB_TS_DetailNodeSelCdt';
    cSQL := 'insert into TB_TS_DetailNodeSelCdt select id from ' ||
            pOut_vTabName;
    fmsp_execsql(pIn_cSql => cSQL);
    open p_detailnode for 'select * from ' || pOut_vTabName || ' order by pvt_cle ';
    Fmp_Log.LOGEND;
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      Fmp_Log.LOGERROR;
      raise;
  end;

  procedure SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode out varchar2,
                                         p_SqlCode                out number) as
    --insert conditions of the selection into tmp_cdt.
    --if no condition of product ,select all products
    --if no record by conditon of products.return.
    v_P_CondtionCount  number := 0; --condition count of product
    v_ST_CondtionCount number := 0; --condition count of sale territory
    v_TC_CondtionCount number := 0; --condition count of trade channel
    v_N_CondtionCount  number := 0; --condition count of detail node
  
    v_Ope      number; --operation 1 equal;2 not equal;3 between
    v_Sql_Part clob; --
    v_Sql      clob; --part of sql for 3D.may 1D,2D or null
  
    v_Sql_BetweenPart varchar2(400); --part of sql for creating the KEY scope when meeting 'between' operation
    v_F_Begin         VARCHAR2(60);
    v_F_End           VARCHAR2(60);
  
    --v_Flag   number := 0;
    v_AttrID number;
  
    --v_DataConditionCount number := 0;
    v_Datascounts         number := 0;
    v_BoM_addr            number;
    v_BoM_count           number;
    v_BoS_Count           number;
    v_ProcessOfData_Count number;
  
    v_sql_p            clob := ' select p.fam_em_addr  from v_productattrvalue p ';
    v_sql_P_where      clob := 'where p.isleaf=1 and p.id_fam=80 ';
    v_sql_P_connectby  clob;
    v_sql_st           clob := ' select s.geo_em_addr  from v_saleterritoryattrvalue s ';
    v_sql_st_where     clob := ' where s.isleaf=1 ';
    v_sql_st_connectby clob;
    v_sql_tc           clob := ' select t.dis_em_addr  from v_tradechannelattrvalue  t ';
    v_sql_tc_where     clob := '  where t.isleaf=1 ';
    v_sql_tc_connectby clob;
    v_sql_data         clob := ' select p.pvt_em_addr  from pvt p  ';
  begin
    p_SqlCode                := 0;
    p_SqlClauseForDetailNode := ' from pvt p  where 1=2 '; --no data found
    --clear tmp table
    /*    delete from  tmp_product;
    delete from  tmp_sales_territory;
    delete from tmp_trade_channel;
    delete from tmp_detail_node;*/
  
    --replace begin
    fmp_getdetailnodesql.FMSP_GetDetailNodeSql(pInOut_vProduct      => v_sql_p,
                                               pInOut_vProductWhere => v_sql_P_where,
                                               pInOut_vSaleT        => v_sql_st,
                                               pInOut_vSaleTWhere   => v_sql_st_where,
                                               pInOut_vTradeC       => v_sql_tc,
                                               pInOut_vTradeCWhere  => v_sql_tc_where,
                                               pInOut_nProductCount => v_P_CondtionCount,
                                               pInOut_nSalesCount   => v_ST_CondtionCount,
                                               pInOut_nTradeCount   => v_TC_CondtionCount,
                                               pOut_nSqlCode        => p_SqlCode);
    ---replace end
  
    /*    execute immediate 'insert into tmp_trade_channel(channelid) '||v_sql_tc;
    select 1 into v_T_Count from tmp_trade_channel where rownum<2;
    if v_T_Count = 0 then
      return;
    end if;*/
    ----detal node
    select count(*)
      into v_N_CondtionCount
      from tmp_cdt c
     where c.tabid in (p_Constant.DETAIL_NODE,
                       p_Constant.BoS,
                       p_Constant.ProcessOfData,
                       9962,
                       9979)
       and c.ope <> 0;
  
    for i in (select distinct c.attrordno
                from tmp_cdt c
               where c.tabid in (p_Constant.DETAIL_NODE, 9962, 9979)
                 and c.ope <> 0) loop
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_AttrID, v_Ope, v_Sql_Part
        from (select case c.tabid
                       when 9979 then
                        100 --promotion attribute number,in pvtcrt table this value is differentfrom crtserie.add 1 .
                       when 9962 then
                        102 --launch as above.
                       else
                        c.attrordno + 49
                     end attrordno,
                     c.ope,
                     c.addr,
                     row_number() over(partition by c.ope order by c.val_idx) cur,
                     row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                from tmp_cdt c
               where c.tabid in (p_Constant.DETAIL_NODE, 9962, 9979)
                 and c.attrordno = i.attrordno) t
       group by t.attrordno, t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
      case v_Ope
        when 1 then
          v_sql_data := v_sql_data ||
                        ' intersect select c.pvt35_em_addr from pvtcrt c where c.numero_crt_pvt = ' ||
                        v_AttrID || ' and  c.crtserie36_em_addr in ' ||
                        v_Sql_Part;
        when 2 then
          v_sql_data := v_sql_data ||
                        ' intersect select c.pvt35_em_addr from pvtcrt c where c.numero_crt_pvt =' ||
                        v_AttrID || ' and c.crtserie36_em_addr  not in ' ||
                        v_Sql_Part;
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
                       and v.num_crt_serie = v_AttrID - 1
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
            v_sql_data := v_sql_data ||
                          ' intersect select c.pvt35_em_addr from pvtcrt c where c.numero_crt_pvt = ' ||
                          v_AttrID || ' and  c.crtserie36_em_addr in ' ||
                          v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
          end;
      end case;
    end loop;
    --process BoS
    select count(*)
      into v_BoS_Count
      from tmp_cdt c
     where c.tabid = p_constant.BoS
       and c.ope <> 0;
    if v_BoS_Count <> 0 then
      select t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_Ope, v_Sql_Part
        from (select c.ope,
                     c.addr,
                     row_number() over(partition by c.ope order by c.val_idx) cur,
                     row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                from tmp_cdt c
               where c.tabid = p_Constant.BoS) t
       group by t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
      case v_Ope
        when 1 then
          --component
          if v_Sql_Part = '(1)' or v_Sql_Part = '(0)' then
            v_sql_data := v_sql_data ||
                          ' intersect select distinct p.pvt_em_addr from
                      pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                      where s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle';
          else
            v_sql_data := v_sql_data ||
                          ' intersect select distinct p.pvt_em_addr from
                      pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                      where s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle
                       start with s.pere_bdg in ' ||
                          v_Sql_Part ||
                          ' connect by nocycle s.pere_bdg = prior s.fils_bdg';
          end if;
        when 2 then
          --Assembly
          if v_Sql_Part = '(1)' or v_Sql_Part = '(0)' then
            v_sql_data := v_sql_data ||
                          ' intersect select distinct p.pvt_em_addr from
                         pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                         where s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle';
          else
            v_sql_data := v_sql_data ||
                          ' intersect select  distinct p.pvt_em_addr from
                         pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                         where s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle
                         start with s.fils_bdg in ' ||
                          v_Sql_Part ||
                          ' connect by nocycle prior s.pere_bdg =  s.fils_bdg';
          end if;
        when 3 then
          --not a componet.ignore the val1 and val2 (v_Sql_Part)
          v_sql_data := v_sql_data ||
                        ' minus select p.pvt_em_addr from
                   pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                   where  s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle';
        when 4 then
          --not an assembly  ignore val1 and val2 too.
          v_sql_data := v_sql_data ||
                        ' minus select p.pvt_em_addr from
                   pvt p,(select *  from supplier s where s.id_supplier = 78) s,bdg b
                   where  s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle';
      end case;
    end if;
    --process Data's process
    select count(*)
      into v_ProcessOfData_Count
      from tmp_cdt c
     where c.tabid = p_constant.ProcessOfData
       and c.ope <> 0;
    if v_ProcessOfData_Count <> 0 then
      select t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into v_Ope, v_Sql_Part
        from (select c.ope,
                     c.addr,
                     row_number() over(partition by c.ope order by c.val_idx) cur,
                     row_number() over(partition by c.ope order by c.val_idx) - 1 prev
                from tmp_cdt c
               where c.tabid = p_Constant.ProcessOfData) t
       group by t.ope
       start with t.cur = 1
      connect by prior t.cur = t.prev;
      case v_Ope
        when 1 then
          --Is a Manufacturing Process:find the child process of  the node (agg or detail node) specified in Val1 and val2
        
          if v_Sql_Part = '(1,1)' or v_Sql_Part = '(0)' or
             v_Sql_Part = '(1)' then
            v_sql_data := v_sql_data ||
                          ' intersect select distinct p.pvt_em_addr from
                       pvt p,(select *  from supplier s where s.id_supplier = 71)  s,bdg b
                       where s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle ';
          else
            v_sql_data := v_sql_data ||
                          ' intersect  select distinct p.pvt_em_addr from
                       pvt p,(select *  from supplier s where s.id_supplier = 71) s,bdg b
                       where s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle
                       start with  s.pere_bdg in ' ||
                          v_Sql_Part ||
                          'connect by nocycle s.pere_bdg = prior s.fils_bdg     ';
          end if;
        when 2 then
          --Has Manufacturing Process :find the detail node  has the process of the val1 and val2 specified
          if v_Sql_Part = '(1,1)' or v_Sql_Part = '(0)' or
             v_Sql_Part = '(1)' then
            v_sql_data := v_sql_data ||
                          ' intersect select distinct p.pvt_em_addr from
                      pvt p,(select *  from supplier s where s.id_supplier = 71) s,bdg b
                      where s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle ';
          else
            v_sql_data := v_sql_data ||
                          ' intersect  select distinct p.pvt_em_addr from
                       pvt p,(select *  from supplier s where s.id_supplier = 71) s,bdg b
                       where s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle
                       start with s.fils_bdg in ' ||
                          v_Sql_Part ||
                          ' connect by nocycle prior s.pere_bdg =  s.fils_bdg  ';
          end if;
        when 3 then
          --is not a Manufacturing Process
          v_sql_data := v_sql_data ||
                        ' minus select p.pvt_em_addr from
                   pvt p,(select *  from supplier s where s.id_supplier = 71) s,bdg b
                   where s.fils_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle';
        when 4 then
          --has not any Manufacturing Process
          v_sql_data := v_sql_data ||
                        ' minus select p.pvt_em_addr from
                   pvt p,(select *  from supplier s where s.id_supplier = 71) s,bdg b
                    where  s.pere_bdg=b.bdg_em_addr and b.b_cle=p.pvt_cle ';
      end case;
      /*    select count(*) into v_N_Count from tmp_Detail_Node where rownum<2;
      if v_N_Count =0 and v_N_CondtionCount>0 then
        return;
      end if;*/
    end if;
  
    if v_P_CondtionCount > 0 then
      v_Sql := ' and p.fam4_em_addr in ( ' || v_sql_p || ' ) ';
    end if;
    if v_ST_CondtionCount > 0 then
      v_Sql := v_Sql || ' and p.geo5_em_addr in (' || v_sql_st || ') ';
    end if;
    if v_TC_CondtionCount > 0 then
      v_Sql := v_Sql || ' and p.dis6_em_addr in (' || v_sql_tc || ') ';
    end if;
    if v_N_CondtionCount > 0 then
      v_Sql := v_Sql || ' and p.pvt_em_addr in (' || v_sql_data || ') ';
    end if;
  
    p_SqlClauseForDetailNode := ' from pvt p  where 1=1 ' || v_Sql;
  exception
    when others then
      p_SqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise;
  end;

  /*  Return all selections which have id, key, descriptions
  Kind(1 :Selection, 2 :AggreGateNode, 3 :Selection and AggreGateNode)*/
  procedure SP_GetAllSelections(P_Kind      in number,
                                p_Selection out sys_refcursor,
                                p_SqlCode   out number) as
  
  begin
    p_SqlCode := 0;
    if P_Kind = 1 then
      --1 :Selection
      open p_Selection for
        select sel_em_addr, sel_cle, sel_desc from sel where sel_bud = 0;
    elsif P_Kind = 2 then
      --2 :AggreGateNode
      open p_Selection for
        select sel_em_addr, sel_cle, sel_desc from sel where sel_bud = 71;
    
    elsif P_Kind = 3 then
      --3 :Selection and AggreGateNode
      open p_Selection for
        select sel_em_addr, sel_cle, sel_desc
          from sel
         where sel_bud = 0
            or sel_bud = 71;
    
    end if;
  
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
  end;

  procedure SP_ReBuildSelection(p_SelectionID in number,
                                p_SqlCode     out number) as
    v_FromClause clob;
  begin
    Fmp_log.FMP_SetValue(p_SelectionID);
    Fmp_log.LOGBEGIN;
    delete from rsp where sel13_em_addr = p_SelectionID;
    delete from tmp_cdt;
    insert into tmp_cdt
      (tabid, attrordno, ope, val_idx, addr)
      select c.rcd_cdt, c.n0_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
        from cdt c
       where c.sel11_em_addr = P_SelectionID
         and c.operant <> 0;
    SPPrv_GetDetailNodeSqlClause(p_SqlClauseForDetailNode => v_FromClause,
                                 p_SqlCode                => p_SqlCode);
    if p_SqlCode <> 0 then
      return;
    end if;
    if v_FromClause is null then
      p_SqlCode := 0;
      return;
    end if;
    execute immediate 'insert   into rsp(rsp_em_addr,sel13_em_addr,pvt14_em_addr)
     select seq_rsp.nextval,' || p_SelectionID ||
                      ',pvt_em_addr   ' || v_FromClause;
    commit;
    update sel s
       set s.effectif    =
           (select count(*) from rsp r where r.sel13_em_addr = p_SelectionID),
           s.annee_select = to_char(sysdate, 'yyyy'),
           s.mois_select  = to_char(sysdate, 'mm'),
           s.jour_select  = to_char(sysdate, 'dd')
     where s.SEL_EM_ADDR = p_SelectionID;
    p_SqlCode := 0;
    commit;
    Fmp_log.LOGEND;
  exception
    when others then
      rollback;
      p_SqlCode := SQLCODE;
      Fmp_log.LOGERROR;
  end;

  procedure FMISP_GetDetailNodeByCdt(PIn_vConditions in varchar2,
                                     PIn_vSequence   in varchar2,
                                     pOut_DetailNode out sys_refcursor,
                                     pOut_vTabName   out varchar2,
                                     pOut_SqlCode    out number)
  --*****************************************************************
    -- Description: get detail nodes by conditions
    --
    -- Parameters:
    --       PIn_vConditions:
    --       PIn_vSequence
    --       pOut_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
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
    SP_GetDetailNodeByConditions(P_Conditions       => PIn_vConditions,
                                 pIn_bNeedCreateTab => true,
                                 P_Sequence         => PIn_vSequence,
                                 p_DetailNode       => pOut_DetailNode,
                                 pOut_vTabName      => pOut_vTabName,
                                 p_SqlCode          => pOut_SqlCode);
  end;
  procedure FMISP_GetDetailNodeBySelID(PIn_nSelectionID in number,
                                       pIn_nIsDynamic   in number,
                                       PIn_vSequence    in varchar2,
                                       pOut_DetailNode  out sys_refcursor,
                                       pOut_vTabName    out varchar2,
                                       pOut_SqlCode     out number)
  --*****************************************************************
    -- Description: get detail nodes by selection
    --
    -- Parameters:
    --       PIn_nSelectionID:
    --       pIn_nIsDynamic
    --       PIn_vSequence
    --       pOut_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
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
    SP_GetDetailNodeBySelectionID(P_SelectionID      => PIn_nSelectionID,
                                  pIn_bNeedCreateTab => true,
                                  p_IsDynamic        => pIn_nIsDynamic,
                                  P_Sequence         => PIn_vSequence,
                                  p_DetailNode       => pOut_DetailNode,
                                  pOut_vTabName      => pOut_vTabName,
                                  p_SqlCode          => pOut_SqlCode);
  end;

  procedure FMISP_GetDetailNodeBySelCdt(PIn_nSelectionID in number,
                                        PIn_vConditions  in varchar2,
                                        PIn_vSequence    in varchar2,
                                        pOut_DetailNode  out sys_refcursor,
                                        pOut_vTabName    out varchar2,
                                        pOut_SqlCode     out number)
  --*****************************************************************
    -- Description: get detail nodes by selection and condition
    --
    -- Parameters:
    --       PIn_nSelectionID:
    --       PIn_vConditions
    --       PIn_vSequence
    --       pOut_DetailNode
    --       pOut_vTabName mod_forecast.num_mod
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
    SP_GetDetailNodeBySelCdt(P_SelectionID      => PIn_nSelectionID,
                             pIn_bNeedCreateTab => true,
                             P_Conditions       => PIn_vConditions,
                             P_Sequence         => PIn_vSequence,
                             p_DetailNode       => pOut_DetailNode,
                             pOut_vTabName      => pOut_vTabName,
                             p_SqlCode          => pOut_SqlCode);
  end;

  /**************************************
  Function
  **************************************/

  function f_GetStr(f_String   in varchar2,
                    f_BeginStr in varchar2,
                    f_EndStr   in varchar2) return varchar2 is
    v_Return   varchar2(400);
    v_BeginIdx number;
    v_EndIdx   number;
  begin
    v_BeginIdx := instr(f_String, f_BeginStr);
    v_EndIdx   := instr(f_String, f_EndStr);
    v_Return   := substr(f_String,
                         v_BeginIdx + 1,
                         v_EndIdx - (v_BeginIdx + 1));
    return v_Return;
  exception
    when others then
      raise_application_error(sqlcode, sqlerrm);
  end;
end p_Selection;
/
