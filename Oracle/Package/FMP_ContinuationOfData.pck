create or replace package FMP_ContinuationOfData is

  -- Author  : YZHU
  -- Created : 3/5/2013 5:27:04 PM
  -- Purpose :

  procedure FMISP_GetContinuationOfData(pIn_vTabName  in varchar2,
                                        pIn_nNodeType in number, --0 detail node ; 1 aggregate node
                                        pIn_nScenario in integer,
                                        pOutDatas     out sys_refcursor,
                                        pOut_nSqlCode out number,
                                        pOut_vSqlMsg  out varchar2);

  procedure FMSP_GetContinuationOfData(pIn_cNodeList in clob,
                                       pIn_nNodeType in number, --0 detail node ; 1 aggregate node
                                       pIn_nScenario in integer,
                                       pOutDatas     out sys_refcursor,
                                       pOut_nSqlCode out number,
                                       pOut_vSqlMsg  out varchar2);

end FMP_ContinuationOfData;
/
create or replace package body FMP_ContinuationOfData is

  procedure FMISP_GetContinuationOfData(pIn_vTabName  in varchar2,
                                        pIn_nNodeType in number, --0 detail node ; 1 aggregate node
                                        pIn_nScenario in integer,
                                        pOutDatas     out sys_refcursor,
                                        pOut_nSqlCode out number,
                                        pOut_vSqlMsg  out varchar2) is
    --*****************************************************************
    -- Description: return the specified nodes continuation of node list
    --
    -- Parameters:
    --       pIn_vTabName  in varchar2, --Table name of nodelist data.
    --       pIn_nNodeType in number, --0 detail node ; 1 aggregate node
    --       pOutDatas     out sys_refcursor,
    --       pOut_nSqlCode out number,
    --       pOut_vSqlMsg  out varchar2
    -- Error Conditions Raised:
    --
    -- Author:      Yi.Zhu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0  18-FEB-2013     Yi.Zhu     Created.
    --  V7.0  26-FEB-2013     Yi.Zhu     Performance Tuning.
    --  V7.0  5-MAR-2013      Yi.Zhu     create package FMP_ContinuationOfData.
    --  V7.0  3-APR-2013      Yi.Zhu     Fix bug for mod forcast type.    
    -- **************************************************************
    --
    cSQL varchar2(8000);
  begin
    pOut_nSqlCode := 0;
    case pIn_nNodeType
      when 0 then
        --        open pOutDatas for
        cSQL := 'with sup as
           (select p.pvt_em_addr pere_nodeid,
                   p2.pvt_em_addr fils_nodeid,
                   b2.bdg_em_addr bdgid,
                   cast(b2.b_cle as varchar2(255)) b_cle,
                   f.decalage,
                   nvl(s.coeff, 100) ratio,
                   s.startyear,
                   s.startperiod,
                   s.endyear,
                   s.endperiod
              from supplier s, bdg b, pvt p, pvt p2, bdg b2, mod_forecast f
             where b.b_cle = p.pvt_cle
               and b2.b_cle = p2.pvt_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.pere_bdg = b.bdg_em_addr
               and s.fils_bdg = b2.bdg_em_addr
               and s.id_supplier = 83
               and s.pere_bdg > 1
               and s.fils_bdg > 1
               and b.id_bdg = 80
               and b2.id_bdg = 80
               and f.num_mod = :pIn_nScenario
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select pro.pere_nodeid,
                   p2.pvt_em_addr  fils_nodeid,
                   b.bdg_em_addr   bdgid,
                   '' b_cle,
                   0               decalage,
                   pro.ratio,
                   0               startyear,
                   0               startperiod,
                   0               endyear,
                   0               endperiod
              from (select p.pvt_em_addr pere_nodeid,
                           n.fils_pro_nmc,
                           p.fam4_em_addr,
                           p.geo5_em_addr,
                           p.dis6_em_addr,
                           nvl(n.qute, 100) ratio
                      from pvt p, nmc n
                     where p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) pro,
                   pvt p2,
                   bdg b
             where pro.fils_pro_nmc = p2.fam4_em_addr
               and nvl(pro.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(pro.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.pvt_cle = b.b_cle
               and b.id_bdg = 80)
          select CONNECT_BY_ROOT cf.pere_nodeid headnodeid,
                 cf.fils_nodeid  nodeid,
                 0               type,
                 bdgid,
                 b_cle,
                 decalage,
                 ratio,
                 startyear,
                 startperiod,
                 endyear,
                 endperiod
            from (select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from sup
                  union all
                  select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from pro
                   where not exists
                   (select null
                            from sup
                           where pro.pere_nodeid = sup.pere_nodeid)) cf
           start with exists (select null
                         from ' || pIn_vTabName || ' t
                        where cf.pere_nodeid = t.id)
          connect by nocycle prior cf.fils_nodeid = cf.pere_nodeid';
      
      when 1 then
        --   open pOutDatas for
        cSQL := 'with sup as
           (select p.sel_em_addr pere_nodeid,
                   p2.sel_em_addr fils_nodeid,
                   b2.bdg_em_addr bdgid,
                   cast(b2.b_cle as varchar2(255)) b_cle,
                   f.decalage,
                   nvl(s.coeff, 100) ratio,
                   s.startyear,
                   s.startperiod,
                   s.endyear,
                   s.endperiod
              from supplier s, bdg b, sel p, sel p2, bdg b2, mod_forecast f
             where b.b_cle = p.sel_cle
               and b2.b_cle = p2.sel_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.pere_bdg = b.bdg_em_addr
               and s.fils_bdg = b2.bdg_em_addr
               and s.id_supplier = 83
               and s.pere_bdg > 1
               and s.fils_bdg > 1
               and b.id_bdg = 71
               and b2.id_bdg = 71
               and f.num_mod = :pIn_nScenario
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select pro.pere_nodeid,
                   p2.sel_em_addr  fils_nodeid,
                   b.bdg_em_addr   bdgid,
                   '' b_cle,
                   0               decalage,
                   pro.ratio,
                   0               startyear,
                   0               startperiod,
                   0               endyear,
                   0               endperiod
              from (select p.sel_em_addr pere_nodeid,
                           n.fils_pro_nmc,
                           p.fam4_em_addr,
                           p.geo5_em_addr,
                           p.dis6_em_addr,
                           nvl(n.qute, 100) ratio
                      from v_aggnodetodimension p, nmc n
                     where p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) pro,
                   v_aggnodetodimension p2,
                   bdg b
             where pro.fils_pro_nmc = p2.fam4_em_addr
               and nvl(pro.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(pro.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.sel_cle = b.b_cle
               and b.id_bdg = 71)
          select CONNECT_BY_ROOT cf.pere_nodeid headnodeid,
                 cf.fils_nodeid  nodeid,
                 1               type,
                 bdgid,
                 b_cle,
                 decalage,
                 ratio,
                 startyear,
                 startperiod,
                 endyear,
                 endperiod
            from (select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from sup
                  union all
                  select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from pro
                   where not exists
                   (select null
                            from sup
                           where pro.pere_nodeid = sup.pere_nodeid)) cf
           start with exists (select null
                         from  ' || pIn_vTabName || ' t
                        where cf.pere_nodeid = t.id)
          connect by nocycle prior cf.fils_nodeid = cf.pere_nodeid';
    end case;
    open pOutDatas for csql
      using pIn_nScenario;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      pOut_vSqlMsg  := sqlerrm;
      raise_application_error(-20004, sqlcode || '--' || sqlerrm);
    
  end FMISP_GetContinuationOfData;

  procedure FMSP_GetContinuationOfData(pIn_cNodeList in clob,
                                       pIn_nNodeType in number, --0 detail node ; 1 aggregate node
                                       pIn_nScenario in integer,
                                       pOutDatas     out sys_refcursor,
                                       pOut_nSqlCode out number,
                                       pOut_vSqlMsg  out varchar2) is
    --*****************************************************************
    -- Description: return the specified nodes continuation of node list
    --
    -- Parameters:
    --       pIn_cNodeList in clob,
    --       pIn_nNodeType in number, --0 detail node ; 1 aggregate node
    --       pOutDatas     out sys_refcursor,
    --       pOut_nSqlCode out number,
    --       pOut_vSqlMsg  out varchar2
    -- Error Conditions Raised:
    --
    -- Author:      Yi.Zhu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0  18-FEB-2013     Yi.Zhu     Created.
    --  V7.0  26-FEB-2013     Yi.Zhu     Performance Tuning.
    --  V7.0  5-MAR-2013      Yi.Zhu     create package FMP_ContinuationOfData.
    --  V7.0  3-APR-2013      Yi.Zhu     Fix bug for mod forcast type.    
    -- **************************************************************
    tNestedTabNodeids fmt_nest_tab_nodeid := fmt_nest_tab_nodeid();
    --
  begin
    pOut_nSqlCode := 0;
    FMSP_clobtonestedtable(pIn_cClob     => pIn_cNodeList,
                           pOut_tNestTab => tNestedTabNodeids,
                           pOut_nSqlCode => pOut_nSqlCode);
  
    case pIn_nNodeType
      when 0 then
        open pOutDatas for
          with sup as
           (select p.pvt_em_addr pere_nodeid,
                   p2.pvt_em_addr fils_nodeid,
                   b2.bdg_em_addr bdgid,
                   cast(b2.b_cle as varchar2(255)) b_cle,
                   f.decalage,
                   nvl(s.coeff, 100) ratio,
                   s.startyear,
                   s.startperiod,
                   s.endyear,
                   s.endperiod
              from supplier s, bdg b, pvt p, pvt p2, bdg b2, mod_forecast f
             where b.b_cle = p.pvt_cle
               and b2.b_cle = p2.pvt_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.pere_bdg = b.bdg_em_addr
               and s.fils_bdg = b2.bdg_em_addr
               and s.id_supplier = 83
               and s.pere_bdg > 1
               and s.fils_bdg > 1
               and b.id_bdg = 80
               and b2.id_bdg = 80
               and f.num_mod = pIn_nScenario
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select pro.pere_nodeid,
                   p2.pvt_em_addr fils_nodeid,
                   b.bdg_em_addr bdgid,
                   '' b_cle,
                   0 decalage,
                   pro.ratio,
                   0 startyear,
                   0 startperiod,
                   0 endyear,
                   0 endperiod
              from (select p.pvt_em_addr pere_nodeid,
                           n.fils_pro_nmc,
                           p.fam4_em_addr,
                           p.geo5_em_addr,
                           p.dis6_em_addr,
                           nvl(n.qute, 100) ratio
                      from pvt p, nmc n
                     where p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) pro,
                   pvt p2,
                   bdg b
             where pro.fils_pro_nmc = p2.fam4_em_addr
               and nvl(pro.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(pro.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.pvt_cle = b.b_cle
               and b.id_bdg = 80)
          select CONNECT_BY_ROOT cf.pere_nodeid headnodeid,
                 cf.fils_nodeid  nodeid,
                 0               type,
                 bdgid,
                 b_cle,
                 decalage,
                 ratio,
                 startyear,
                 startperiod,
                 endyear,
                 endperiod
            from (select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from sup
                  union all
                  select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from pro
                   where not exists
                   (select null
                            from sup
                           where pro.pere_nodeid = sup.pere_nodeid)) cf
           start with exists (select null
                         from table(tNestedTabNodeids) t
                        where cf.pere_nodeid = t.id)
          connect by nocycle prior cf.fils_nodeid = cf.pere_nodeid;
      
      when 1 then
        open pOutDatas for
          with sup as
           (select p.sel_em_addr pere_nodeid,
                   p2.sel_em_addr fils_nodeid,
                   b2.bdg_em_addr bdgid,
                   cast(b2.b_cle as varchar2(255)) b_cle,
                   f.decalage,
                   nvl(s.coeff, 100) ratio,
                   s.startyear,
                   s.startperiod,
                   s.endyear,
                   s.endperiod
              from supplier s, bdg b, sel p, sel p2, bdg b2, mod_forecast f
             where b.b_cle = p.sel_cle
               and b2.b_cle = p2.sel_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.pere_bdg = b.bdg_em_addr
               and s.fils_bdg = b2.bdg_em_addr
               and s.id_supplier = 83
               and s.pere_bdg > 1
               and s.fils_bdg > 1
               and b.id_bdg = 71
               and b2.id_bdg = 71
               and f.num_mod = pIn_nScenario
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select pro.pere_nodeid,
                   p2.sel_em_addr fils_nodeid,
                   b.bdg_em_addr bdgid,
                   '' b_cle,
                   0 decalage,
                   pro.ratio,
                   0 startyear,
                   0 startperiod,
                   0 endyear,
                   0 endperiod
              from (select p.sel_em_addr pere_nodeid,
                           n.fils_pro_nmc,
                           p.fam4_em_addr,
                           p.geo5_em_addr,
                           p.dis6_em_addr,
                           nvl(n.qute, 100) ratio
                      from v_aggnodetodimension p, nmc n
                     where p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) pro,
                   v_aggnodetodimension p2,
                   bdg b
             where pro.fils_pro_nmc = p2.fam4_em_addr
               and nvl(pro.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(pro.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.sel_cle = b.b_cle
               and b.id_bdg = 71)
          select CONNECT_BY_ROOT cf.pere_nodeid headnodeid,
                 cf.fils_nodeid  nodeid,
                 1               type,
                 bdgid,
                 b_cle,
                 decalage,
                 ratio,
                 startyear,
                 startperiod,
                 endyear,
                 endperiod
            from (select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from sup
                  union all
                  select pere_nodeid,
                         fils_nodeid,
                         bdgid,
                         b_cle,
                         decalage,
                         ratio,
                         startyear,
                         startperiod,
                         endyear,
                         endperiod
                    from pro
                   where not exists
                   (select null
                            from sup
                           where pro.pere_nodeid = sup.pere_nodeid)) cf
           start with exists (select null
                         from table(tNestedTabNodeids) t
                        where cf.pere_nodeid = t.id)
          connect by nocycle prior cf.fils_nodeid = cf.pere_nodeid;
      
    end case;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      pOut_vSqlMsg  := sqlerrm;
      raise_application_error(-20004, sqlcode || '--' || sqlerrm);
    
  end FMSP_GetContinuationOfData;

end FMP_ContinuationOfData;
/
