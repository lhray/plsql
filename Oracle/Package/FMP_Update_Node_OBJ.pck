create or replace package FMP_Update_Node_OBJ is

  procedure FMSP_Update_Node_obj(pIn_nObjtype     in number, -- 1 :fam   2: geo ;3:dis ; 4:detail node  5: agg node
                                 pIn_vObjId       in varchar2, -- Object id
                                 pIn_vNewVal      in varchar2, -- new value
                                 pIn_nIsSelection in number, -- 1:selection 2: no selection
                                 pIn_vName        in varchar2, -- old key name
                                 pIn_vDesc        in varchar2, -- old key desc
                                 pIn_vShortDesc   in varchar2,
                                 pIn_nFatherID    in varchar2,
                                 pOut_nSqlCode    out number -- return sqlcode
                                 );
  procedure FMSP_Update_Continuation_Node(pIn_nProduct             in number,
                                          pIn_nContinuationProduct in number,
                                          pOut_nSqlCode            out number);
  procedure FMSP_FindSelection(pIn_nDetailNodeID in number,
                               pOut_nSqlCode     out number);
end FMP_Update_Node_OBJ;
/
create or replace package body FMP_Update_Node_OBJ is

  procedure FMSP_Update_Fam(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number);

  procedure FMSP_Update_Geo(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number);

  procedure FMSP_Update_Dis(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number);

  procedure FMSP_Update_Detail_Node(pIn_vObjId       in varchar2,
                                    pIn_vNewVal      in varchar2,
                                    pIn_nIsSelection in varchar2,
                                    pIn_vName        in varchar2,
                                    pIn_vDesc        in varchar2,
                                    pIn_vShortDesc   in varchar2,
                                    pOut_nSqlCode    out number);

  procedure FMSP_Update_Aggregation_Node(pIn_vObjId       in varchar2,
                                         pIn_vNewVal      in varchar2,
                                         pIn_nIsSelection in varchar2,
                                         pIn_vName        in varchar2,
                                         pIn_vDesc        in varchar2,
                                         pIn_vShortDesc   in varchar2,
                                         pOut_nSqlCode    out number);
  procedure FMSP_ReBuildSelection(pIn_vNewVal   in varchar2,
                                  pIn_nObjType  in number,
                                  pOut_nSqlCode out number);

  procedure FMSP_ReBuildAggRule(pIn_vNewVal   in varchar2,
                                pIn_nObjType  in number,
                                pOut_nSqlCode out number);

  procedure FMSP_Update_Node_obj(pIn_nObjtype     in number,
                                 pIn_vObjId       in varchar2,
                                 pIn_vNewVal      in varchar2,
                                 pIn_nIsSelection in number,
                                 pIn_vName        in varchar2,
                                 pIn_vDesc        in varchar2,
                                 pIn_vShortDesc   in varchar2,
                                 pIn_nFatherID    in varchar2,
                                 pOut_nSqlCode    out number)
  
   as
    --*****************************************************************
    -- Description: this procedure  is update node info .
    --
    -- Parameters:
    --            pIn_nObjtype
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
  begin
    Fmp_Log.FMP_SetValue(pIn_nObjtype);
    Fmp_Log.FMP_SetValue(pIn_vObjId);
    Fmp_Log.FMP_SetValue(pIn_vNewVal);
    Fmp_Log.FMP_SetValue(pIn_nIsSelection);
    Fmp_Log.FMP_SetValue(pIn_vName);
    Fmp_Log.FMP_SetValue(pIn_vDesc);
    Fmp_Log.FMP_SetValue(pIn_vShortDesc);
    Fmp_Log.FMP_SetValue(pIn_nFatherID);
    Fmp_Log.LOGBEGIN;
  
    if pIn_nObjtype is null or pIn_vObjId is null then
      pOut_nSqlCode := 0;
      return;
    end if;
  
    if pIn_nObjtype = 1 then
    
      FMSP_Update_Fam(pIn_vObjId       => pIn_vObjId,
                      pIn_vNewVal      => pIn_vNewVal,
                      pIn_nIsSelection => pIn_nIsSelection,
                      pIn_vName        => pIn_vName,
                      pIn_vDesc        => pIn_vDesc,
                      pIn_vShortDesc   => pIn_vShortDesc,
                      pIn_nFatherID    => pIn_nFatherID,
                      pOut_nSqlCode    => pOut_nSqlCode);
    
    elsif pIn_nObjtype = 2 then
      FMSP_Update_Geo(pIn_vObjId       => pIn_vObjId,
                      pIn_vNewVal      => pIn_vNewVal,
                      pIn_nIsSelection => pIn_nIsSelection,
                      pIn_vName        => pIn_vName,
                      pIn_vDesc        => pIn_vDesc,
                      pIn_vShortDesc   => pIn_vShortDesc,
                      pIn_nFatherID    => pIn_nFatherID,
                      pOut_nSqlCode    => pOut_nSqlCode);
    elsif pIn_nObjtype = 3 then
      FMSP_Update_Dis(pIn_vObjId       => pIn_vObjId,
                      pIn_vNewVal      => pIn_vNewVal,
                      pIn_nIsSelection => pIn_nIsSelection,
                      pIn_vName        => pIn_vName,
                      pIn_vDesc        => pIn_vDesc,
                      pIn_vShortDesc   => pIn_vShortDesc,
                      pIn_nFatherID    => pIn_nFatherID,
                      pOut_nSqlCode    => pOut_nSqlCode);
    elsif pIn_nObjtype = 4 then
      FMSP_Update_Detail_Node(pIn_vObjId       => pIn_vObjId,
                              pIn_vNewVal      => pIn_vNewVal,
                              pIn_nIsSelection => pIn_nIsSelection,
                              pIn_vName        => pIn_vName,
                              pIn_vDesc        => pIn_vDesc,
                              pIn_vShortDesc   => pIn_vShortDesc,
                              pOut_nSqlCode    => pOut_nSqlCode);
    elsif pIn_nObjtype = 5 then
      FMSP_Update_Aggregation_Node(pIn_vObjId       => pIn_vObjId,
                                   pIn_vNewVal      => pIn_vNewVal,
                                   pIn_nIsSelection => pIn_nIsSelection,
                                   pIn_vName        => pIn_vName,
                                   pIn_vDesc        => pIn_vDesc,
                                   pIn_vShortDesc   => pIn_vShortDesc,
                                   pOut_nSqlCode    => pOut_nSqlCode);
    
    end if;
    Fmp_Log.LOGEND;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_Update_Fam(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number) as
    --*****************************************************************
    -- Description: this procedure  is update fam  info .
    --
    -- Parameters:
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nFamCount    number;
    nSelCount    number;
    nFatherID    varchar2(100);
  
  begin
  
    if pIn_vObjId is null then
      return;
    end if;
    select count(*)
      into nFamCount
      from fam f
     where f.fam_em_addr = pIn_vObjId;
  
    if nFamCount = 0 then
      return;
    end if;
  
    vNewVal := pIn_vNewVal;
  
    select count(*) into nPvtCount from pvt p where p.adr_pro = pIn_vObjId;
  
    select count(*)
      into nSelCount
      from cdt c
     where c.adr_cdt = pIn_vObjId
       and c.rcd_cdt = p_constant.PRODUCT;
  
    /* select count(*)
     into nSelCount
     from v_aggnodetodimension s
    where s.fam4_em_addr = pIn_vObjId;*/
  
    select f.fam0_em_addr
      into nFatherID
      from fam f
     where f.fam_em_addr = pIn_vObjId;
  
    -- update key
    if nPvtCount > 0 and pIn_vName is not null then
    
      update pvt p
         set p.pvt_cle =
             (select pIn_vName || (case
                       when g.g_cle is null then
                        ''
                       else
                        '-'
                     end) || g.g_cle || (case
                       when d.d_cle is null then
                        ''
                       else
                        '-'
                     end) || d.d_cle
                from fam f, geo g, dis d
               where p.adr_pro = pIn_vObjId
                 and p.adr_pro = f.fam_em_addr(+)
                 and p.adr_geo = g.geo_em_addr(+)
                 and p.adr_dis = d.dis_em_addr(+))
       where exists (select 1
                from pvt p1
               where p1.adr_pro = pIn_vObjId
                 and p1.pvt_em_addr = p.pvt_em_addr);
    
    end if;
  
    -- rebuild selection
    if nFamCount > 0 and pIn_vName is null and pIn_vDesc is null and
       pIn_vObjId is not null and nFatherID != pIn_nFatherID then
    
      for k in (select distinct c.sel11_em_addr
                  from sel s,
                       cdt c,
                       (SELECT f.fam_em_addr
                          FROM fam f
                         START WITH f.fam_em_addr = pIn_vObjId
                        CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr
                        union
                        SELECT f.fam_em_addr
                          FROM fam f
                         START WITH f.fam_em_addr = pIn_vObjId
                        CONNECT BY PRIOR f.fam0_em_addr = f.fam_em_addr) t
                 where s.sel_em_addr = c.sel11_em_addr
                   and t.fam_em_addr = c.adr_cdt
                   and c.rcd_cdt = p_constant.PRODUCT
                   and s.sel_bud = 0
                union
                select r.sel13_em_addr
                  from pvt p,
                       rsp r,
                       sel s,
                       (SELECT f.fam_em_addr, f.id_fam
                          FROM fam f
                         START WITH f.fam_em_addr = pIn_vObjId
                        CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) t
                 where p.adr_pro = t.fam_em_addr
                   and t.id_fam = 80
                   and r.pvt14_em_addr = p.pvt_em_addr
                   and s.sel_em_addr = r.sel13_em_addr
                   and s.sel_bud = 0) loop
        p_selection.SP_BuildSelection(p_SelectionID => k.sel11_em_addr,
                                      p_SqlCode     => pOut_nSqlCode);
      
      end loop;
    
      -- rebuild agg node
      for k1 in (select distinct af.aggregationid
                   from v_aggnodetodimension va,
                        aggregatenode_fullid af,
                        (SELECT f.fam_em_addr
                           FROM fam f
                          START WITH f.fam_em_addr = pIn_vObjId
                         CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr
                         union
                         SELECT f.fam_em_addr
                           FROM fam f
                          START WITH f.fam_em_addr = pIn_vObjId
                         CONNECT BY PRIOR f.fam0_em_addr = f.fam_em_addr) t
                  where va.fam4_em_addr = t.fam_em_addr
                    and va.sel_em_addr = af.aggregatenodeid
                 union (select p.prv_em_addr
                         from cdt c,
                              prv p,
                              (SELECT f.fam_em_addr
                                 FROM fam f
                                START WITH f.fam_em_addr = pIn_vObjId
                               CONNECT BY f.fam0_em_addr = PRIOR
                                          f.fam_em_addr
                               union
                               SELECT f.fam_em_addr
                                 FROM fam f
                                START WITH f.fam_em_addr = pIn_vObjId
                               CONNECT BY PRIOR f.fam0_em_addr = f.fam_em_addr) t
                        where t.fam_em_addr = c.adr_cdt
                          and c.rcd_cdt = p_constant.PRODUCT
                          and p.prv_em_addr = c.prv12_em_addr)
                 union (select distinct ad.aggregationid
                         from pvt p,
                              aggregation_detailnode ad,
                              prv pp,
                              (SELECT f.fam_em_addr
                                 FROM fam f
                                START WITH f.fam_em_addr = pIn_vObjId
                               CONNECT BY f.fam0_em_addr = PRIOR
                                          f.fam_em_addr
                               union
                               SELECT f.fam_em_addr
                                 FROM fam f
                                START WITH f.fam_em_addr = pIn_vObjId
                               CONNECT BY PRIOR f.fam0_em_addr = f.fam_em_addr) t
                        where p.adr_pro = t.fam_em_addr
                          and ad.detailnodeid = p.pvt_em_addr
                          and ad.aggregationid = pp.prv_em_addr)) loop
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k1.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 1,
                                                 pIn_nObjType         => 1,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    
    end if;
  
    -- update desc
    if nPvtCount > 0 and pIn_vDesc is not null then
      update pvt p
         set p.pvt_desc =
             (select pIn_vDesc || (case
                       when g.g_desc is null then
                        ''
                       else
                        '-'
                     end) || g.g_desc || (case
                       when d.d_desc is null then
                        ''
                       else
                        '-'
                     end) || d.d_desc
                from fam f, geo g, dis d
               where p.adr_pro = pIn_vObjId
                 and p.adr_pro = f.fam_em_addr(+)
                 and p.adr_geo = g.geo_em_addr(+)
                 and p.adr_dis = d.dis_em_addr(+))
       where exists (select 1
                from pvt p1
               where p1.adr_pro = pIn_vObjId
                 and p.pvt_em_addr = p1.pvt_em_addr);
    
    end if;
  
    if pIn_nIsSelection = 1 and nPvtCount > 0 and vNewVal is not null then
      --rebuild selection
      FMSP_ReBuildSelection(pIn_vNewVal   => vNewVal,
                            pIn_nObjType  => 1,
                            pOut_nSqlCode => pOut_nSqlCode);
    end if;
  
    if pIn_nIsSelection = 1 and nSelCount > 0 and vNewVal is not null then
      --rebuild selection
      FMSP_ReBuildAggRule(pIn_vNewVal   => vNewVal,
                          pIn_nObjType  => 1,
                          pOut_nSqlCode => pOut_nSqlCode);
    end if;
  
    /*if pIn_nIsSelection = 1 and vNewVal is null then
      select f.id_fam
        into nFamCount
        from fam f
       where f.fam_em_addr = pIn_vObjId
         and f.id_fam = p_constant.product_group_id;
    
      if nFamCount > 0 then
        for k in (select distinct c.sel11_em_addr
                    from cdt c, sel s
                   where c.rcd_cdt = p_constant.PRODUCT
                     and c.adr_cdt = pIn_vObjId) loop
          p_selection.SP_BuildSelection(p_SelectionID => k.sel11_em_addr,
                                        p_SqlCode     => pOut_nSqlCode);
    
        end loop;
    
      end if;
    
    end if;*/
  
    -- update agg node key
    if nSelCount > 0 and pIn_vName is not null then
    
      for k in (select distinct af.aggregationid
                  from aggregatenode_fullid af
                 where af.aggregatenodeid in
                       (select va.sel_em_addr
                          from v_aggnodetodimension va
                         where va.fam4_em_addr = pIn_vObjId)
                union
                select distinct c.prv12_em_addr
                  from cdt c
                 where c.rcd_cdt = p_constant.PRODUCT
                   and c.adr_cdt = pIn_vObjId
                   and c.prv12_em_addr is not null) loop
      
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 0,
                                                 pIn_nObjType         => 1,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    
    end if;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
    
  end;

  procedure FMSP_Update_Geo(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number) as
    --*****************************************************************
    -- Description: this procedure  is update geo  info .
    --
    -- Parameters:
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nSelCount    number;
    nGeoCount    number;
    nFatherID    varchar2(100);
  
  begin
    if pIn_vObjId is null then
      return;
    end if;
    vNewVal := pIn_vNewVal;
    select count(*) into nPvtCount from pvt p where p.adr_geo = pIn_vObjId;
    select count(*)
      into nGeoCount
      from geo g
     where g.geo_em_addr = pIn_vObjId;
  
    if nGeoCount = 0 then
      return;
    end if;
  
    select count(*)
      into nSelCount
      from v_aggnodetodimension s
     where s.geo5_em_addr = pIn_vObjId;
  
    select g.geo1_em_addr
      into nFatherID
      from geo g
     where g.geo_em_addr = pIn_vObjId;
  
    if nPvtCount > 0 and pIn_vName is not null then
      update pvt p
         set p.pvt_cle =
             (select f.f_cle || '-' || pIn_vName || (case
                       when d.d_cle is null then
                        ''
                       else
                        '-'
                     end) || d.d_cle
                from fam f, geo g, dis d
               where p.adr_pro = f.fam_em_addr(+)
                 and p.adr_geo = g.geo_em_addr(+)
                 and p.adr_dis = d.dis_em_addr(+))
       where exists (select 1
                from pvt p1
               where p1.adr_geo = pIn_vObjId
                 and p1.pvt_em_addr = p.pvt_em_addr);
    
    end if;
  
    if pIn_vDesc is not null then
      update pvt p
         set p.pvt_desc =
             (select f.f_cle || '-' || pIn_vDesc || '-' || d.d_desc
                from fam f, geo g, dis d
               where p.adr_geo = pIn_vObjId
                 and p.adr_pro = f.fam_em_addr(+)
                 and p.adr_geo = g.geo_em_addr(+)
                 and p.adr_dis = d.dis_em_addr(+))
       where exists (select 1
                from pvt p1
               where p1.adr_geo = pIn_vObjId
                 and p1.pvt_em_addr = p.pvt_em_addr);
    
    end if;
  
    --rebuild selection
    if nGeoCount > 0 and pIn_vName is null and pIn_vDesc is null and
       pIn_vObjId is not null and nFatherID != pIn_nFatherID then
    
      for k in (select distinct c.sel11_em_addr
                  from sel s,
                       cdt c,
                       (SELECT g.geo_em_addr
                          FROM geo g
                         START WITH g.geo_em_addr = pIn_vObjId
                        CONNECT BY g.geo1_em_addr = PRIOR g.geo_em_addr
                        union
                        SELECT g.geo_em_addr
                          FROM geo g
                         START WITH g.geo_em_addr = pIn_vObjId
                        CONNECT BY PRIOR g.geo1_em_addr = g.geo_em_addr) t
                 where s.sel_em_addr = c.sel11_em_addr
                   and t.geo_em_addr = c.adr_cdt
                   and c.rcd_cdt = p_constant.SALE_TERRITORY
                   and s.sel_bud = 0
                union
                select r.sel13_em_addr
                  from pvt p,
                       rsp r,
                       sel s,
                       (SELECT g.geo_em_addr
                          FROM geo g
                         START WITH g.geo_em_addr = pIn_vObjId
                        CONNECT BY g.geo1_em_addr = PRIOR g.geo_em_addr) t
                 where p.adr_geo = t.geo_em_addr
                   and r.pvt14_em_addr = p.pvt_em_addr
                   and s.sel_em_addr = r.sel13_em_addr
                   and s.sel_bud = 0) loop
        p_selection.SP_BuildSelection(p_SelectionID => k.sel11_em_addr,
                                      p_SqlCode     => pOut_nSqlCode);
      
      end loop;
      -- rebuild agg node
      for k1 in (select distinct af.aggregationid
                   from v_aggnodetodimension va,
                        aggregatenode_fullid af,
                        (SELECT g.geo_em_addr
                           FROM geo g
                          START WITH g.geo_em_addr = pIn_vObjId
                         CONNECT BY g.geo1_em_addr = PRIOR g.geo_em_addr
                         union
                         SELECT g.geo_em_addr
                           FROM geo g
                          START WITH g.geo_em_addr = pIn_vObjId
                         CONNECT BY PRIOR g.geo1_em_addr = g.geo_em_addr) t
                  where va.geo5_em_addr = t.geo_em_addr
                    and va.sel_em_addr = af.aggregatenodeid
                 union (select p.prv_em_addr
                         from cdt c,
                              prv p,
                              (SELECT g.geo_em_addr
                                 FROM geo g
                                START WITH g.geo_em_addr = pIn_vObjId
                               CONNECT BY g.geo1_em_addr = PRIOR
                                          g.geo_em_addr
                               union
                               SELECT g.geo_em_addr
                                 FROM geo g
                                START WITH g.geo_em_addr = pIn_vObjId
                               CONNECT BY PRIOR g.geo1_em_addr = g.geo_em_addr) t
                        where t.geo_em_addr = c.adr_cdt
                          and c.rcd_cdt = p_constant.SALE_TERRITORY
                          and p.prv_em_addr = c.prv12_em_addr)
                 union (select distinct ad.aggregationid
                         from pvt p,
                              aggregation_detailnode ad,
                              prv pp,
                              (SELECT g.geo_em_addr
                                 FROM geo g
                                START WITH g.geo_em_addr = pIn_vObjId
                               CONNECT BY g.geo1_em_addr = PRIOR
                                          g.geo_em_addr
                               union
                               SELECT g.geo_em_addr
                                 FROM geo g
                                START WITH g.geo_em_addr = pIn_vObjId
                               CONNECT BY PRIOR g.geo1_em_addr = g.geo_em_addr) t
                        where p.adr_geo = t.geo_em_addr
                          and ad.detailnodeid = p.pvt_em_addr
                          and ad.aggregationid = pp.prv_em_addr)) loop
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k1.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 1,
                                                 pIn_nObjType         => 2,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    
    end if;
  
    if pIn_nIsSelection = 1 and nPvtCount > 0 and vNewVal is not null then
      --- rebuild selection
      FMSP_ReBuildSelection(pIn_vNewVal   => vNewVal,
                            pIn_nObjType  => 2,
                            pOut_nSqlCode => pOut_nSqlCode);
    
    end if;
  
    if pIn_nIsSelection = 1 and nSelCount > 0 and vNewVal is not null then
      --rebuild selection
      FMSP_ReBuildAggRule(pIn_vNewVal   => vNewVal,
                          pIn_nObjType  => 1,
                          pOut_nSqlCode => pOut_nSqlCode);
    end if;
  
    if nSelCount > 0 and pIn_vName is not null then
    
      for k in (select distinct af.aggregationid
                  from aggregatenode_fullid af
                 where af.aggregatenodeid in
                       (select va.sel_em_addr
                          from v_aggnodetodimension va
                         where va.geo5_em_addr = pIn_vObjId)
                union
                select distinct c.prv12_em_addr
                  from cdt c
                 where c.rcd_cdt = p_constant.SALE_TERRITORY
                   and c.adr_cdt = pIn_vObjId
                   and c.prv12_em_addr is not null) loop
      
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 0,
                                                 pIn_nObjType         => 2,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    
    end if;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_Update_Dis(pIn_vObjId       in varchar2,
                            pIn_vNewVal      in varchar2,
                            pIn_nIsSelection in varchar2,
                            pIn_vName        in varchar2,
                            pIn_vDesc        in varchar2,
                            pIn_vShortDesc   in varchar2,
                            pIn_nFatherID    in varchar2,
                            pOut_nSqlCode    out number) as
    --*****************************************************************
    -- Description: this procedure  is update dis  info .
    --
    -- Parameters:
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nSelCount    number;
    nDisCount    varchar2(100);
    nFatherID    varchar2(100);
  
  begin
  
    if pIn_vObjId is null then
      return;
    end if;
    vNewVal := pIn_vNewVal;
    select count(*) into nPvtCount from pvt p where p.adr_dis = pIn_vObjId;
  
    select count(*)
      into nDisCount
      from dis d
     where d.dis_em_addr = pIn_vObjId;
  
    if nDisCount = 0 then
      return;
    end if;
  
    select d.dis2_em_addr
      into nFatherID
      from dis d
     where d.dis_em_addr = pIn_vObjId;
  
    select count(*)
      into nSelCount
      from v_aggnodetodimension s
     where s.dis6_em_addr = pIn_vObjId;
  
    if nPvtCount > 0 and pIn_vName is not null then
      update pvt p
         set p.pvt_cle =
             (select f.f_cle || (case
                       when g.g_cle is null then
                        ''
                       else
                        '-'
                     end) || g.g_cle || '-' || pIn_vName
                from fam f, geo g, dis d
               where p.adr_pro = f.fam_em_addr(+)
                 and p.adr_geo = g.geo_em_addr(+)
                 and p.adr_dis = d.dis_em_addr(+))
       where exists (select 1
                from pvt p1
               where p1.adr_dis = pIn_vObjId
                 and p1.pvt_em_addr = p.pvt_em_addr);
    
    end if;
  
    if pIn_vDesc is not null then
      update pvt p
         set p.pvt_desc =
             (select f.f_cle || (case
                       when g.g_desc is null then
                        ''
                       else
                        '-'
                     end) || g.g_desc || '-' || pIn_vDesc
                from fam f, geo g, dis d
               where p.adr_dis = pIn_vObjId
                 and p.adr_pro = f.fam_em_addr
                 and p.adr_geo = g.geo_em_addr
                 and p.adr_dis = d.dis_em_addr)
       where exists (select 1
                from pvt p1
               where p.adr_dis = pIn_vObjId
                 and p1.pvt_em_addr = p.pvt_em_addr);
    
    end if;
  
    if nDisCount > 0 and pIn_vName is null and pIn_vObjId is not null and
       nFatherID != pIn_nFatherID then
      for k in (select distinct c.sel11_em_addr
                  from sel s,
                       cdt c,
                       (SELECT d.dis_em_addr
                          FROM dis d
                         START WITH d.dis_em_addr = pIn_vObjId
                        CONNECT BY d.dis2_em_addr = PRIOR d.dis_em_addr
                        union
                        SELECT d.dis_em_addr
                          FROM dis d
                         START WITH d.dis_em_addr = pIn_vObjId
                        CONNECT BY PRIOR d.dis2_em_addr = d.dis_em_addr) t
                 where s.sel_em_addr = c.sel11_em_addr
                   and t.dis_em_addr = c.adr_cdt
                   and c.rcd_cdt = p_Constant.SALE_TERRITORY
                   and s.sel_bud = 0
                union
                select r.sel13_em_addr
                  from pvt p,
                       rsp r,
                       sel s,
                       (SELECT d.dis_em_addr
                          FROM dis d
                         START WITH d.dis_em_addr = pIn_vObjId
                        CONNECT BY d.dis2_em_addr = PRIOR d.dis_em_addr) t
                 where p.adr_dis = t.dis_em_addr
                   and r.pvt14_em_addr = p.pvt_em_addr
                   and s.sel_em_addr = r.sel13_em_addr
                   and s.sel_bud = 0) loop
        p_selection.SP_BuildSelection(p_SelectionID => k.sel11_em_addr,
                                      p_SqlCode     => pOut_nSqlCode);
      
      end loop;
    
      for k1 in (select distinct af.aggregationid
                   from v_aggnodetodimension va,
                        aggregatenode_fullid af,
                        (SELECT d.dis_em_addr
                           FROM Dis d
                          START WITH d.dis_em_addr = pIn_vObjId
                         CONNECT BY d.dis2_em_addr = PRIOR d.dis_em_addr
                         union
                         SELECT d.dis_em_addr
                           FROM Dis d
                          START WITH d.dis_em_addr = pIn_vObjId
                         CONNECT BY PRIOR d.dis2_em_addr = d.dis_em_addr) t
                  where va.dis6_em_addr = t.dis_em_addr
                    and va.sel_em_addr = af.aggregatenodeid
                 union (select p.prv_em_addr
                         from cdt c,
                              prv p,
                              (SELECT d.dis_em_addr
                                 FROM dis d
                                START WITH d.dis_em_addr = pIn_vObjId
                               CONNECT BY d.dis2_em_addr = PRIOR
                                          d.dis_em_addr
                               union
                               SELECT d.dis_em_addr
                                 FROM dis d
                                START WITH d.dis_em_addr = pIn_vObjId
                               CONNECT BY PRIOR d.dis2_em_addr = d.dis_em_addr) t
                        where t.dis_em_addr = c.adr_cdt
                          and c.rcd_cdt = p_constant.TRADE_CHANNEL
                          and p.prv_em_addr = c.prv12_em_addr)
                 union (select distinct ad.aggregationid
                         from pvt p,
                              aggregation_detailnode ad,
                              prv pp,
                              (SELECT d.dis_em_addr
                                 FROM dis d
                                START WITH d.dis_em_addr = pIn_vObjId
                               CONNECT BY d.dis2_em_addr = PRIOR
                                          d.dis_em_addr
                               union
                               SELECT d.dis_em_addr
                                 FROM dis d
                                START WITH d.dis_em_addr = pIn_vObjId
                               CONNECT BY PRIOR d.dis2_em_addr = d.dis_em_addr) t
                        where p.adr_dis = t.dis_em_addr
                          and ad.detailnodeid = p.pvt_em_addr
                          and ad.aggregationid = pp.prv_em_addr)) loop
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k1.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 1,
                                                 pIn_nObjType         => 3,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    end if;
  
    if pIn_nIsSelection = 1 and nPvtCount > 0 and vNewVal is not null then
    
      FMSP_ReBuildSelection(pIn_vNewVal   => vNewVal,
                            pIn_nObjType  => 3,
                            pOut_nSqlCode => pOut_nSqlCode);
    end if;
  
    if pIn_nIsSelection = 1 and nSelCount > 0 and vNewVal is not null then
      --rebuild selection
      FMSP_ReBuildAggRule(pIn_vNewVal   => vNewVal,
                          pIn_nObjType  => 1,
                          pOut_nSqlCode => pOut_nSqlCode);
    end if;
  
    if nSelCount > 0 and pIn_vName is not null then
    
      for k in (select distinct af.aggregationid
                  from aggregatenode_fullid af
                 where af.aggregatenodeid in
                       (select va.sel_em_addr
                          from v_aggnodetodimension va
                         where va.dis6_em_addr = pIn_vObjId)
                union
                select distinct c.prv12_em_addr
                  from cdt c
                 where c.rcd_cdt = p_constant.TRADE_CHANNEL
                   and c.adr_cdt = pIn_vObjId
                   and c.prv12_em_addr is not null) loop
      
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k.aggregationid,
                                                 pIn_vObjId           => pIn_vObjId,
                                                 pIn_nType            => 0,
                                                 pIn_nObjType         => 3,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      
      end loop;
    
    end if;
  
  end;

  procedure FMSP_Update_Detail_Node(pIn_vObjId       in varchar2,
                                    pIn_vNewVal      in varchar2,
                                    pIn_nIsSelection in varchar2,
                                    pIn_vName        in varchar2,
                                    pIn_vDesc        in varchar2,
                                    pIn_vShortDesc   in varchar2,
                                    pOut_nSqlCode    out number) as
    --*****************************************************************
    -- Description: this procedure  is update detail node  info .
    --
    -- Parameters:
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
  
  begin
  
    if pIn_nIsSelection = 1 and pIn_vNewVal is not null then
      FMSP_ReBuildSelection(pIn_vNewVal   => pIn_vNewVal,
                            pIn_nObjType  => 4,
                            pOut_nSqlCode => pOut_nSqlCode);
    
    end if;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_Update_Aggregation_Node(pIn_vObjId       in varchar2,
                                         pIn_vNewVal      in varchar2,
                                         pIn_nIsSelection in varchar2,
                                         pIn_vName        in varchar2,
                                         pIn_vDesc        in varchar2,
                                         pIn_vShortDesc   in varchar2,
                                         pOut_nSqlCode    out number) as
  
    --*****************************************************************
    -- Description: this procedure  is update Aggregation Node  info .
    --
    -- Parameters:
    --            pIn_vObjId
    --            pIn_vNewVal
    --            pIn_nIsSelection
    --            pIn_vName
    --            pIn_vDesc
    --            pIn_vShortDesc
    --            pOut_vSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  begin
  
    /*p_Aggregation.spprv_ProduceAggNodeAndLinks(P_AggregateRuleID => pIn_vObjId,
    p_sqlcode         => pOut_nSqlCode);*/
    return;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_ReBuildSelection(pIn_vNewVal   in varchar2,
                                  pIn_nObjType  in number,
                                  pOut_nSqlCode out number)
  
   as
  
    --*****************************************************************
    -- Description: this procedure  is ReBuild Selection  .
    --
    -- Parameters:
    --            pIn_vNewVal
    --            pIn_nObjType
    --            pIn_nIsSelection
    --            pOut_nSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;
  
  begin
  
    if pIn_vNewVal is null or pIn_nObjType is null then
      return;
    end if;
  
    vNewVal := pIn_vNewVal;
  
    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;
  
    for i in 1 .. nValCount loop
    
      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);
    
      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;
    
      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);
    
      if pIn_nObjType = 1 then
        nObjType := p_constant.v_ProductAttr;
      elsif pIn_nObjType = 2 then
        nObjType := p_constant.v_STAttr;
      elsif pIn_nObjType = 3 then
        nObjType := p_constant.v_TCAttr;
      elsif pIn_nObjType = 4 then
        nObjType := p_constant.DETAIL_NODE;
      end if;
    
      for k in (select distinct c.sel11_em_addr
                  from cdt c, sel s
                 where c.rcd_cdt = nObjType
                   and c.n0_cdt = nNodeAttNum - 49
                   and s.sel_em_addr = c.sel11_em_addr
                   and s.sel_bud = 0) loop
        p_selection.SP_BuildSelection(p_SelectionID => k.sel11_em_addr,
                                      p_SqlCode     => pOut_nSqlCode);
      end loop;
    end loop;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_ReBuildAggRule(pIn_vNewVal   in varchar2,
                                pIn_nObjType  in number,
                                pOut_nSqlCode out number) as
  
    --*****************************************************************
    -- Description: this procedure  is ReBuild AggRule  .
    --
    -- Parameters:
    --            pIn_vNewVal
    --            pIn_nObjType
    --            pOut_nSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        25-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;
  
  begin
  
    if pIn_vNewVal is null or pIn_nObjType is null then
      return;
    end if;
  
    vNewVal := pIn_vNewVal;
  
    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;
  
    for i in 1 .. nValCount loop
    
      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);
    
      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;
    
      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);
    
      if pIn_nObjType = 1 then
        nObjType := p_constant.v_ProductAttr;
      elsif pIn_nObjType = 2 then
        nObjType := p_constant.v_STAttr;
      elsif pIn_nObjType = 3 then
        nObjType := p_constant.v_TCAttr;
      elsif pIn_nObjType = 4 then
        nObjType := p_constant.DETAIL_NODE;
      end if;
    
      for k in (select p.prv_em_addr
                  from cdt c, prv p
                 where c.rcd_cdt = nObjType
                   and c.n0_cdt = nNodeAttNum - 49
                   and c.prv12_em_addr = p.prv_em_addr) loop
        p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => k.prv_em_addr,
                                                 pIn_vObjId           => null,
                                                 pIn_nType            => 1,
                                                 pIn_nObjType         => null,
                                                 pOut_nSqlCode        => pOut_nSqlCode);
      end loop;
    end loop;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;
  procedure FMSP_Update_Continuation_Node(pIn_nProduct             in number,
                                          pIn_nContinuationProduct in number,
                                          pOut_nSqlCode            out number) as
    vProduct   fam.f_cle%type;
    v_sqlStr   clob;
    vtb_tmppvt varchar2(8) default 'tmp_pvt';
    vtb_tmpsel varchar2(8) default 'tmp_sel';
    bAggflg    boolean;
    cursor cur_pvt is
      select distinct t.pvt_em_addr from tmp_pvt t;
    cursor cur_sel is
      select distinct t.id from tmp_tselaggid t where t.type = 'sel';
    cursor cur_agg is
    --select distinct t.id from tmp_tselaggid t where t.type = 'agg';
      select p.prv_em_addr id from prv p;
    cursor cur_aggregation is
      select distinct p.prv_em_addr from prv p;
  begin
    FMP_LOG.FMP_SETVALUE(pIn_nProduct);
    FMP_LOG.FMP_SETVALUE(pIn_nContinuationProduct);
    FMP_log.logBegin;
    select f.f_cle
      into vProduct
      from fam f
     where f.fam_em_addr = pIn_nProduct;
  
    --
    BEGIN
      v_sqlStr := 'truncate table ' || vtb_tmppvt;
      fmsp_execsql(v_sqlStr);
    EXCEPTION
      WHEN OTHERS THEN
        v_sqlStr := 'create global temporary table ' || vtb_tmppvt ||
                    '(PVT_EM_ADDR integer,PVT_CLE varchar2(400),
        PVT_DESC varchar(400),ADR_PRO number,ADR_GEO integer,ADR_DIS integer,USER_CREATE_PVT varchar2(60),
        DATE_CREATE_PVT number,DATE_MODIFY_PVT number,PVT_PARENT_BDG number,FAM4_EM_ADDR number,
        GEO5_EM_ADDR integer,DIS6_EM_ADDR integer) on commit preserve rows ';
        fmsp_execsql(v_sqlStr);
    END;
    v_sqlStr := 'insert into  ' || vtb_tmppvt || '
     select seq_pvt.nextval pvt_em_addr,
             ''' || vProduct || ''' || ''-'' || g.g_cle || ''-'' || d.d_cle pvt_cle,
             g.g_desc ||decode(g_desc,null,'''',decode(d.d_desc, null, '''', ''-'')) || d.d_desc pvt_desc,
             ''' || pIn_nProduct || ''' adr_pro,
             p.adr_geo,
             p.adr_dis,
             p.user_create_pvt,
             F_ConvertDateToOleDateTime(sysdate) date_create_pvt,
             p.date_modify_pvt,
             p.pvt_parent_bdg,
             ''' || pIn_nProduct ||
                ''' fam4_em_addr,
             p.geo5_em_addr,
             p.dis6_em_addr
        from (select p.*
                from pvt p
                left outer join (select b.adr_pro, b.adr_geo, b.adr_dis
                                  from pvt a, pvt b
                                 where a.adr_geo = b.adr_geo
                                   and a.adr_dis = b.adr_dis
                                   and a.adr_pro = ' ||
                pIn_nProduct || '
                                   and b.adr_pro =' ||
                pIn_nContinuationProduct || ') c
                  on p.adr_geo = c.adr_geo
                 and p.adr_dis = c.adr_dis
               where p.adr_pro = ' ||
                pIn_nContinuationProduct || '
                 and c.adr_dis is null) p,
             fam f,
             geo g,
             dis d
       where p.adr_pro = ' || pIn_nContinuationProduct || '
         and p.adr_pro = f.fam_em_addr(+)
         and p.adr_geo = g.geo_em_addr(+)
         and p.adr_dis = d.dis_em_addr(+)';
  
    fmsp_execsql(v_sqlStr);
  
    --insert continuation 's detail node
    v_sqlStr := 'insert into pvt
      (pvt_em_addr,
       pvt_cle,
       pvt_desc,
       adr_pro,
       adr_geo,
       adr_dis,
       user_create_pvt,
       date_create_pvt,
       date_modify_pvt,
       pvt_parent_bdg,
       fam4_em_addr,
       geo5_em_addr,
       dis6_em_addr)
      select * from ' || vtb_tmppvt;
    fmsp_execsql(v_sqlStr);
  
    --insert into bdg table
    v_sqlStr := 'insert into bdg
      (bdg_em_addr, id_bdg, b_cle, bdg_desc)
      select seq_bdg.nextval, 80, p.pvt_cle, p.pvt_desc from ' ||
                vtb_tmppvt || ' p';
    fmsp_execsql(v_sqlStr);
  
    --update selection
    delete from tmp_tselaggid;
    for sel in cur_pvt loop
      FMP_Update_Node_OBJ.FMSP_FindSelection(pIn_nDetailNodeID => sel.pvt_em_addr,
                                             pOut_nSqlCode     => pOut_nSqlCode);
      merge into tmp_tselaggid t
      using (select * from tmp_SEL_ADDR_TODETAIL) s
      on (t.id = s.sel_em_addr)
      when not matched then
        insert (type, id) values ('sel', s.sel_em_addr);
    
    end loop;
    for c in cur_sel loop
      p_selection.SP_BuildSelection(p_SelectionID => c.id,
                                    p_SqlCode     => pOut_nSqlCode);
    end loop;
  
    --agg
    /*for agg in cur_aggregation loop
      for c in cur_pvt loop
        fmp_createaggnode.FMSP_IsBelongToAggRule(pIn_nDetailNodeID => c.pvt_em_addr,
                                                 pIn_nAggRuleID    => agg.prv_em_addr,
                                                 pOut_bIsBelong    => bAggflg,
                                                 pOut_nSqlCode     => pOut_nSqlCode);
        if bAggflg then
          insert into tmp_tselaggid values ('agg', agg.prv_em_addr);
        end if;
      
      end loop;
    end loop;*/
    for c in cur_agg loop
      p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => c.id,
                                               pIn_vObjId           => null,
                                               pIn_nType            => 1,
                                               pIn_nObjType         => null,
                                               pOut_nSqlCode        => pOut_nSqlCode);
    end loop;
    pOut_nSqlCode := 0;
  
    FMP_LOG.LOGEND;
  exception
    when others then
      FMP_LOG.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_FindSelection(pIn_nDetailNodeID in number,
                               pOut_nSqlCode     out number)
  
    --*****************************************************************
    -- Description: this procedure  is use detail node id find selection   .
    --
    -- Parameters:
    --            pIn_nDetailNodeID
    --            pOut_nSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        15-Mar-2013     LiSang         Created.
    -- **************************************************************
   as
    nFAM         number;
    nGEO         number;
    nDIS         number;
    vSQL_SEL     varchar2(4000);
    vSQL_ATT     Clob;
    vSQL_SEL_DIS varchar2(4000);
    nDETAIL      number;
    nAttcount    number;
    nKeycount    number;
    ndetacount   number;
    cSQl         clob := 'insert into TMP_SEL_ADDR_TODETAIL ';
    vSQLAtt      varchar2(1000);
    vSQLKey      varchar2(1000);
    vSQLDeta     varchar2(1000);
  begin
  
    select p.fam4_em_addr, p.geo5_em_addr, p.dis6_em_addr
      into nFAM, nGEO, nDIS
      from pvt p
     where p.pvt_em_addr = pIn_nDetailNodeID;
  
    if nFAM is null and nGEO is null and nDIS is null then
      pOut_nSqlCode := 0;
      return;
    end if;
  
    -- this is FAM
  
    vSQL_ATT := 'insert into TMP_Detail_attribute  ' || ' select ' ||
                pIn_nDetailNodeID ||
                ', 20007,r.numero_crt,r.vct10_em_addr , ' ||
                ' (select v.val from vct v where v.vct_em_addr = r.vct10_em_addr  )' ||
                ' from rfc r where r.fam7_em_addr = ' || nFAM ||
                ' and r.numero_crt >= 49 and r.numero_crt < 49+19';
    vSQL_ATT := vSQL_ATT || '  union SELECT ' || pIn_nDetailNodeID ||
                ',10000,49,f.fam_em_addr ,f.f_cle ' ||
                '  FROM fam f where f.fam_em_addr !=1 ' ||
                ' START WITH f.fam_em_addr = ' || nFAM ||
                ' CONNECT BY PRIOR f.fam0_em_addr = f.fam_em_addr ';
    vSQL_ATT := vSQL_ATT || ' union select ' || pIn_nDetailNodeID ||
                ', 10055,pc.numero_crt_pvt,c.crtserie_em_addr,c.val_crt_serie ' ||
                ' from pvtcrt pc,crtserie c where pc.pvt35_em_addr =  ' ||
                pIn_nDetailNodeID ||
                ' and pc.crtserie36_em_addr = c.crtserie_em_addr ';
    ---this is get GEO
    if nGEO is not null then
    
      vSQL_ATT := vSQL_ATT || ' union select ' || pIn_nDetailNodeID ||
                  ', 20008,r.numero_crt,r.vct10_em_addr , ' ||
                  ' (select v.val from vct v where v.vct_em_addr =r.vct10_em_addr  )  ' ||
                  ' from rfc r where r.geo8_em_addr = ' || nGEO ||
                  ' and r.numero_crt >= 49 and r.numero_crt <49+19 ' ||
                  ' union select ' || pIn_nDetailNodeID ||
                  ', 10001,49,g.geo_em_addr ,g.g_cle  from geo g where g.geo_em_addr = ' || nGEO;
    
    end if;
    -- this is get DIS
    if nDIS is not null then
    
      vSQL_ATT := vSQL_ATT || ' union select ' || pIn_nDetailNodeID ||
                  ', 20009,r.numero_crt,r.vct10_em_addr ,' ||
                  ' (select v.val from vct v where v.vct_em_addr =r.vct10_em_addr  ) ' ||
                  ' from rfc r where r.dis9_em_addr = ' || nDIS ||
                  ' and r.numero_crt >= 49 and r.numero_crt <49+19 ' ||
                  ' union select ' || pIn_nDetailNodeID ||
                  ', 10002,49,d.dis_em_addr ,d.d_cle  from dis d where d.dis_em_addr =' || nDIS;
    end if;
  
    fmsp_execsql('truncate table TMP_SEL_ADDR_TODETAIL');
    fmsp_execsql('truncate table TMP_Detail_attribute');
    fmsp_execsql('truncate table TMP_attributeinfo');
    fmsp_execsql('truncate table TMP_detailinfo');
    fmsp_execsql('truncate table TMP_keyinfo');
    fmsp_execsql(vSQL_ATT);
  
    select count(*) into nDETAIL from TMP_Detail_attribute;
  
    if nDETAIL = 0 then
      return;
    end if;
  
    --insert into SEL_ADDR_TODETAIL
    insert into TMP_attributeinfo
      select t.sel11_em_addr
        from TMP_Detail_attribute da,
             (select tt.rcd_cdt,
                     tt.sel11_em_addr,
                     tt.numero_crt,
                     tt.ma,
                     (select v.val from vct v where v.vct_em_addr = tt.ma) maxval,
                     tt.mi,
                     (select v.val from vct v where v.vct_em_addr = tt.mi) minval
                from fmv_getselectionobject tt
               where tt.rcd_cdt in (20007, 20008, 20009)) t
       where da.val between t.minval and maxval
         and da.n0_cdt = t.rcd_cdt
         and da.numero_crt = t.numero_crt;
  
    insert into TMP_detailinfo
      select t.sel11_em_addr
        from TMP_Detail_attribute da,
             (select tt.rcd_cdt,
                     tt.sel11_em_addr,
                     tt.numero_crt,
                     tt.ma,
                     (select c.val_crt_serie
                        from crtserie c
                       where c.crtserie_em_addr = tt.ma) maxval,
                     tt.mi,
                     (select c.val_crt_serie
                        from crtserie c
                       where c.crtserie_em_addr = tt.mi) minval
                from fmv_getselectionobject tt
               where tt.rcd_cdt = 10055) t
       where da.val between t.minval and maxval
         and da.n0_cdt = t.rcd_cdt
         and da.numero_crt = t.numero_crt;
  
    -- insert into keyinfo
    insert into TMP_keyinfo
      select t.sel11_em_addr sel
        from TMP_Detail_attribute da,
             (select tt.rcd_cdt,
                     tt.sel11_em_addr,
                     tt.numero_crt,
                     tt.ma,
                     (case
                       when tt.rcd_cdt = 10000 then
                        (select f.f_cle from fam f where f.fam_em_addr = tt.ma)
                       when tt.rcd_cdt = 10001 then
                        (select g.g_cle from geo g where g.geo_em_addr = tt.ma)
                       when tt.rcd_cdt = 10002 then
                        (select d.d_cle from dis d where d.dis_em_addr = tt.ma)
                     end) maxval,
                     tt.mi,
                     (case
                       when tt.rcd_cdt = 10000 then
                        (select f.f_cle from fam f where f.fam_em_addr = tt.mi)
                       when tt.rcd_cdt = 10001 then
                        (select g.g_cle from geo g where g.geo_em_addr = tt.mi)
                       when tt.rcd_cdt = 10002 then
                        (select d.d_cle from dis d where d.dis_em_addr = tt.mi)
                     end) minval
                from fmv_getselectionobject tt
               where tt.rcd_cdt in (10000, 10001, 10002)) t
       where 1 = 1
         and da.val between t.minval and maxval
         and da.n0_cdt = t.rcd_cdt
         and da.numero_crt = t.numero_crt
      union
      select c.sel11_em_addr
        from cdt c, TMP_Detail_attribute tt
       where c.operant = 2
         and c.rcd_cdt = tt.n0_cdt
         and c.adr_cdt = tt.vct10_em_addr
         and tt.n0_cdt in (10000, 10001, 10002)
      union
      select c.sel11_em_addr
        from cdt c, TMP_Detail_attribute tt
       where c.operant = 2
         and c.rcd_cdt = tt.n0_cdt
         and c.adr_cdt = tt.vct10_em_addr
         and tt.n0_cdt in (20007, 20008, 20009);
  
    insert into TMP_detailinfo
      select distinct c.sel11_em_addr
        from cdt c
       where c.sel11_em_addr in (select a.sel
                                   from TMP_attributeinfo a, TMP_keyinfo k
                                  where a.sel = k.sel)
         and c.sel11_em_addr not in
             (select distinct c.sel11_em_addr
                from cdt c, sel s
               where c.rcd_cdt = 10055
                 and c.sel11_em_addr = s.sel_em_addr
                 and s.sel_bud = 0);
  
    insert into TMP_attributeinfo
      select k.sel
        from tmp_keyinfo k
       where k.sel not in
             (select t.sel11_em_addr
                from fmv_getselectionobject t
               where t.rcd_cdt in (20007, 20008, 20009));
  
    select count(*) into nAttcount from TMP_attributeinfo;
    select count(*) into nKeycount from TMP_keyinfo;
    select count(*) into nDetacount from TMP_detailinfo;
  
    if nAttcount = 0 and nKeycount = 0 and nDetacount = 0 then
      return;
    end if;
  
    if nAttcount != 0 then
      vSQLAtt := ' select * from TMP_attributeinfo   ';
    end if;
  
    if nKeycount != 0 then
      vSQLKey := '  select * from TMP_keyinfo  ';
    end if;
  
    if nDetacount != 0 then
      vSQLDeta := ' select * from TMP_detailinfo ';
    end if;
  
    if nAttcount != 0 then
      cSQl := cSQl || vSQLAtt;
    else
      cSQl := cSQl;
    end if;
  
    if nKeycount != 0 and nAttcount != 0 then
      cSQl := cSQl || ' intersect ' || vSQLKey;
    elsif nKeycount != 0 and nAttcount = 0 then
      cSQl := cSQl || vSQLKey;
    else
      cSQl := cSQl;
    end if;
  
    if nDetacount != 0 and nKeycount != 0 then
      cSQl := cSQl || ' intersect ' || vSQLDeta;
    elsif nDetacount != 0 and nKeycount = 0 then
      cSQl := cSQl || vSQLDeta;
    else
      cSQl := cSQl;
    end if;
  
    if nDetacount = 0 then
      delete from TMP_keyinfo k
       where k.sel in (select c.sel11_em_addr
                         from cdt c
                        where c.sel11_em_addr = k.sel
                          and c.rcd_cdt = 10055);
    
    end if;
  
    delete from TMP_keyinfo k
     where k.sel in (select c.sel11_em_addr
                       from cdt c, TMP_Detail_attribute tt
                      where c.operant = 2
                        and c.rcd_cdt = tt.n0_cdt
                        and c.adr_cdt != tt.vct10_em_addr
                        and tt.n0_cdt in (10000, 10001, 10002));
  
    delete from TMP_attributeinfo k
     where k.sel in (select c.sel11_em_addr
                       from cdt c, TMP_Detail_attribute tt
                      where c.operant = 2
                        and c.rcd_cdt = tt.n0_cdt
                        and c.adr_cdt = tt.vct10_em_addr
                        and tt.n0_cdt in (20007, 20008, 20009));
  
    fmsp_execsql(cSQl);
  
    insert into TMP_SEL_ADDR_TODETAIL
      select s.sel_em_addr
        from sel s
       where not exists
       (select 1 from cdt c where s.sel_em_addr = c.sel11_em_addr);
  
    insert into TMP_SEL_ADDR_TODETAIL
      select a.sel11_em_addr
        from fmv_getselectionobject a, tmp_attributeinfo b
       where a.rcd_cdt in (20007, 20008, 20009)
         and a.sel11_em_addr = b.sel
       having(count(a.sel11_em_addr)) = 1
       group by a.sel11_em_addr;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
    
  end;

end FMP_Update_Node_OBJ;
/
