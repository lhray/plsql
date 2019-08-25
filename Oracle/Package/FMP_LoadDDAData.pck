create or replace package FMP_LoadDDAData is

  -- type t_tNesttab_TimeSeriesWithTime is table of FMT_tTimeSeriesWithTime ;

  procedure FMSP_BasicData(pIn_nNodeType         in number,
                           pIn_cNodeList         in clob,
                           pIn_nAttributeNumber  in number,
                           pIn_nNumMOD           in number,
                           pIn_nNeedContinuation in number default 0,
                           pIn_nNeedNotes        in number default 1,
                           pIn_nNeedHasBos       in number default 0,
                           pIn_nNeedHasBoM       in number default 0,
                           pOut_rResult          out sys_refcursor,
                           pOut_nSqlCode         out number);

  procedure FMSP_GetNoteArrayByPeriod(pIn_nNodeType in number,
                                      pIn_cNodeList in clob,
                                      pIn_MorWorD   in number,
                                      pIn_vStart    in varchar2,
                                      pIn_vEnd      in varchar2,
                                      pOut_rResult  out sys_refcursor,
                                      pOut_nSqlCode out number);

  procedure FMSP_GetDimesionValue(pIn_nNodeType in number,
                                  pIn_cNodeList in clob,
                                  pOut_rResult  out sys_refcursor,
                                  pOut_nSqlCode out number);

  procedure FMISP_BasicData(pIn_nNodeType         in number,
                            pIn_vTabName          in varchar2,
                            pIn_nAttributeNumber  in number,
                            pIn_nNumMOD           in number,
                            pIn_nNeedContinuation in number default 0,
                            pIn_nNeedNotes        in number default 1,
                            pIn_nNeedHasBos       in number default 0,
                            pIn_nNeedHasBoM       in number default 0,
                            pOut_rResult          out sys_refcursor,
                            pOut_nSqlCode         out number);

  procedure FMISP_GetNoteArrayByPeriod(pIn_nNodeType in number,
                                       pIn_vTabName  in varchar2,
                                       pIn_MorWorD   in number,
                                       pIn_vStart    in varchar2,
                                       pIn_vEnd      in varchar2,
                                       pOut_rResult  out sys_refcursor,
                                       pOut_nSqlCode out number);

  procedure FMISP_GetDimesionValue(pIn_nNodeType in number,
                                   pIn_vTabName  in varchar2,
                                   pOut_rResult  out sys_refcursor,
                                   pOut_nSqlCode out number);

  procedure FMSP_ContinuationOfandData(pIn_nNodeType         in number,
                                       pIn_cNodeList         in clob default null,
                                       pIn_vTabName          in varchar2 default null,
                                       pIn_nAttributeNumber  in number,
                                       pIn_nNumMOD           in number,
                                       pIn_nNeedContinuation in number default 0,
                                       pIn_nNeedHasBos       in number default 0,
                                       pIn_nNeedHasBoM       in number default 0,
                                       pIn_nScenario         in number,
                                       pOut_rBasicData       out sys_refcursor,
                                       pOut_Datas            out sys_refcursor,
                                       pOut_nSqlCode         out number);
end FMP_LoadDDAData;
/
create or replace package body FMP_LoadDDAData is
  --*****************************************************************
  -- Description: load DDA datas
  --
  -- Author:      JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        19-DEC-2012     JY.Liu     Created.
  -- **************************************************************

  procedure FMSP_DDABasicData(pIn_nNodeType         in number,
                              pIn_cNodeList         in clob default null,
                              pIn_vTabName          in varchar2 default null,
                              pIn_nAttributeNumber  in number,
                              pIn_nNumMOD           in number,
                              pIn_nNeedContinuation in number default 0,
                              pIn_nNeedNotes        in number default 1,
                              pIn_nNeedHasBos       in number default 0,
                              pIn_nNeedHasBoM       in number default 0,
                              pOut_rResult          out sys_refcursor,
                              pOut_nSqlCode         out number);

  procedure FMSP_GetDDADimesionValue(pIn_nNodeType in number,
                                     pIn_cNodeList in clob default null,
                                     pIn_vTabName  in varchar2 default null,
                                     pOut_nSqlCode out number);

  procedure FMP_CreateContinuationOf(pIn_vTabName in varchar2,
                                     pIn_NodeType in number,
                                     pIn_nNumMOD  in number)
  --*****************************************************************
    -- Description: Building whether the specified node have continuation of data
    --
    -- Parameters:
    --       pIn_vTabName: table sored nodes ID
    --       pIn_NodeType: 0 detail node ; 1 aggregate node
  
    -- Error Conditions Raised:
    --
    -- Author:      Yi.Zhu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0  2/18/2013 3:31:30 PM     Yi.Zhu     Created.
    --  V7.0        21-FEB-2013     JY.Liu        modify the input parameter
    --  V7.0        25-FEB-2013     Yi.Zhu     Fix bug --product continuation of err.
    --  V7.0        26-FEB-2013     Yi.Zhu     Fix bug --sql err.
    -- **************************************************************
   as
    cSQL clob;
  begin
    --
    execute immediate 'truncate table tmp_node_continuation';
    case pIn_NodeType
      when 0 then
        cSQL := 'insert into tmp_node_continuation
          (nodeid, continuationof)
          with sup as
           (select c.id nodeid
              from supplier s, bdg b, pvt p, ' ||
                pIn_vTabName || ' c, mod_forecast f
             where s.pere_bdg = b.bdg_em_addr
               and p.pvt_em_addr = c.id
               and b.b_cle = p.pvt_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.id_supplier = 83
               and s.fils_bdg <> 1
               and b.id_bdg = 80
               and f.num_mod = :pIn_nNumMOD
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select a.pvt_em_addr nodeid
              from (select p.pvt_em_addr,
                           p.fam4_em_addr,
                           n.fils_pro_nmc,
                           p.geo5_em_addr,
                           p.dis6_em_addr
                      from pvt p, nmc n, ' || pIn_vTabName || ' c
                     where c.id = p.pvt_em_addr
                       and p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) a,
                   pvt p2,
                   bdg b
             where a.fils_pro_nmc = p2.fam4_em_addr
               and nvl(a.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(a.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.pvt_cle = b.b_cle(+)
               and b.id_bdg = 80)
          select sup.nodeid, 1 continuationof
            from sup
          union all
          select pro.nodeid, 1 continuationof
            from pro
           where not exists
           (select sup.nodeid from sup where pro.nodeid = sup.nodeid)';
      when 1 then
        -- aggregate node
        cSQL := 'insert into tmp_node_continuation
         (nodeid, continuationof)
          with sup as
           (select c.id nodeid
              from supplier s, bdg b, sel p, ' ||
                pIn_vTabName || ' c, mod_forecast f
             where s.pere_bdg = b.bdg_em_addr
               and p.sel_em_addr = c.id
               and b.b_cle = p.sel_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.id_supplier = 83
               and s.fils_bdg <> 1
               and b.id_bdg = 71
               and f.num_mod = :pIn_nNumMOD
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select a.sel_em_addr nodeid
              from (select p.sel_em_addr,
                           p.fam4_em_addr,
                           n.fils_pro_nmc,
                           p.geo5_em_addr,
                           p.dis6_em_addr
                      from v_aggnodetodimension p,
                           nmc n,
                           ' || pIn_vTabName || ' c
                     where c.id = p.sel_em_addr
                       and p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) a,
                   v_aggnodetodimension p2,
                   bdg b
             where a.fils_pro_nmc = p2.fam4_em_addr
               and nvl(a.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(a.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.sel_cle = b.b_cle(+)
               and b.id_bdg = 71)
          select sup.nodeid, 1 continuationof
            from sup
          union all
          select pro.nodeid, 1 continuationof
            from pro
           where not exists
           (select sup.nodeid from sup where pro.nodeid = sup.nodeid)';
    end case;
    execute immediate cSQL
      using pIn_nNumMOD;
  
  end;

  procedure FMP_CreateContinuationOf(pIn_NestedTabNodeids in fmt_nest_tab_nodeid,
                                     pIn_NodeType         in number,
                                     pIn_nNumMOD          in number)
  --*****************************************************************
    -- Description: Building whether the specified node have continuation of data
    --
    -- Parameters:
    --       pIn_NestedTabNodeids: type of t_nest_tab_nodeid
    --       pIn_NodeType:           0 detail node ; 1 aggregate node
  
    -- Error Conditions Raised:
    --
    -- Author:      Yi.Zhu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0  2/18/2013 3:31:30 PM     Yi.Zhu     Created.
    --  V7.0  25-FEB-2013              Yi.Zhu     Fix bug --product continuation of err.
    --  V7.0  26-FEB-2013              Yi.Zhu     Fix bug --sql err.
    --  V7.0  03-MAR-2013              JY.Liu     deprecated
    --  V7.0  3-APR-2013               Yi.Zhu     Fix bug for mod forcast type.
    -- **************************************************************
   as
  begin
    --
    execute immediate 'truncate table tmp_node_continuation';
    case pIn_NodeType
      when 0 then
        insert into tmp_node_continuation
          (nodeid, continuationof)
          with sup as
           (select c.id nodeid
              from supplier s,
                   bdg b,
                   pvt p,
                   table(pIn_NestedTabNodeids) c,
                   mod_forecast f
             where s.pere_bdg = b.bdg_em_addr
               and p.pvt_em_addr = c.id
               and b.b_cle = p.pvt_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.id_supplier = 83
               and s.fils_bdg <> 1
               and b.id_bdg = 80
               and f.num_mod = pIn_nNumMOD
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select a.pvt_em_addr nodeid
              from (select p.pvt_em_addr,
                           p.fam4_em_addr,
                           n.fils_pro_nmc,
                           p.geo5_em_addr,
                           p.dis6_em_addr
                      from pvt p, nmc n, table(pIn_NestedTabNodeids) c
                     where c.id = p.pvt_em_addr
                       and p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) a,
                   pvt p2,
                   bdg b
             where a.fils_pro_nmc = p2.fam4_em_addr
               and nvl(a.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(a.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.pvt_cle = b.b_cle(+)
               and b.id_bdg = 80)
          select sup.nodeid, 1 continuationof
            from sup
          union all
          select pro.nodeid, 1 continuationof
            from pro
           where not exists
           (select sup.nodeid from sup where pro.nodeid = sup.nodeid);
      
      when 1 then
        -- aggregate node
        insert into tmp_node_continuation
          with sup as
           (select c.id nodeid
              from supplier s,
                   bdg b,
                   sel p,
                   table(pIn_NestedTabNodeids) c,
                   mod_forecast f
             where s.pere_bdg = b.bdg_em_addr
               and p.sel_em_addr = c.id
               and b.b_cle = p.sel_cle
               and b.bdg_em_addr = f.bdg_em_addr
               and s.id_supplier = 83
               and s.fils_bdg <> 1
               and b.id_bdg = 71
               and f.num_mod = pIn_nNumMOD
               and f.type_param in (8, 17, 18, 19, 21, 25)),
          pro as
           (select a.sel_em_addr nodeid
              from (select p.sel_em_addr,
                           p.fam4_em_addr,
                           n.fils_pro_nmc,
                           p.geo5_em_addr,
                           p.dis6_em_addr
                      from v_aggnodetodimension p,
                           nmc n,
                           table(pIn_NestedTabNodeids) c
                     where c.id = p.sel_em_addr
                       and p.fam4_em_addr = n.pere_pro_nmc
                       and n.nmc_field = 83) a,
                   v_aggnodetodimension p2,
                   bdg b
             where a.fils_pro_nmc = p2.fam4_em_addr
               and nvl(a.geo5_em_addr, 0) = nvl(p2.geo5_em_addr, 0)
               and nvl(a.dis6_em_addr, 0) = nvl(p2.dis6_em_addr, 0)
               and p2.sel_cle = b.b_cle(+)
               and b.id_bdg = 71)
          select sup.nodeid, 1 continuationof
            from sup
          union all
          select pro.nodeid, 1 continuationof
            from pro
           where not exists
           (select sup.nodeid from sup where pro.nodeid = sup.nodeid);
      
    end case;
  end;

  procedure FMSP_DDABasicData(pIn_nNodeType         in number,
                              pIn_cNodeList         in clob default null,
                              pIn_vTabName          in varchar2 default null,
                              pIn_nAttributeNumber  in number,
                              pIn_nNumMOD           in number,
                              pIn_nNeedContinuation in number default 0,
                              pIn_nNeedNotes        in number default 1,
                              pIn_nNeedHasBos       in number default 0,
                              pIn_nNeedHasBoM       in number default 0,
                              pOut_rResult          out sys_refcursor,
                              pOut_nSqlCode         out number) as
    tNestedTabNodeids fmt_nest_tab_nodeid;
    vTableName        varchar2(30) := 'tb_node';
    vTable            varchar2(30);
  
    vContinuationCols varchar2(100);
    vNotesCols        varchar2(200);
  
    vContinuationSQL varchar2(200);
    vNotesSQL        varchar2(300);
  
    cSQL           clob;
    vHasBosSqlCols varchar2(200);
    vHasBomSqlCols varchar2(200);
    vHasBosSqlJoin varchar2(400);
    vHasBomSqlJoin varchar2(400);
  
  begin
  
    pOut_nSqlCode := 0;
    fmsp_execsql(pIn_cSql => 'truncate table  TMP_DDABASICDATA');
  
    fmsp_execsql(' truncate table  ' || vTableName);
    if pIn_cNodeList is not null then
      FMSP_ClobToTable(pIn_cClob     => pIn_cNodeList,
                       pOut_nSqlCode => pOut_nSqlCode);
    else
      fmsp_execsql(pIn_cSql => 'insert into ' || vTableName ||
                               ' select id from ' || pIn_vTabName);
    end if;
  
    vTable := case pIn_nNodeType
                when 0 then
                 'pvt'
                when 1 then
                 'sel'
              end;
  
    if pIn_nNeedContinuation = 1 then
      --initial table tmp_node_continuation
      FMP_CreateContinuationOf(pIn_vTabName => vTableName,
                               pIn_NodeType => pIn_nNodeType,
                               pIn_nNumMOD  => pIn_nNumMOD);
      vContinuationCols := ' nvl(cf.ContinuationOf, 0) isContinuationof, ';
      vContinuationSQL  := ' left join tmp_node_continuation cf
              on a.' || vTable ||
                           '_em_addr = cf.NodeId ';
    else
      vContinuationCols := ' NULL isContinuationof, ';
      vContinuationSQL  := ' ';
    end if;
  
    if pIn_nNeedNotes = 1 then
      vNotesCols := 'sf.texte fnotes,sd.texte dnotes, ';
      vNotesSQL  := ' left join serinote sf  on sf.bdg3_em_addr = b.bdg_em_addr and sf.nopage = 0 and sf.num_mod = ' ||
                    pIn_nNumMOD || '
                      left join serinote sd  on sd.bdg3_em_addr = b.bdg_em_addr and sd.nopage = 1000 and sf.num_mod = ' ||
                    pIn_nNumMOD;
    else
      vNotesCols := 'null fnotes,null dnotes, ';
      vNotesSQL  := ' ';
    end if;
  
    if pIn_nNeedHasBos = 1 then
      vHasBosSqlCols := ',sign(nvl(hasbos.supplier_em_addr, 0)) hasbos';
      vHasBosSqlJoin := '
            left join (select distinct s.pere_bdg, s.supplier_em_addr
                         from supplier s
                        where s.id_supplier = 78) hasbos
              on hasbos.pere_bdg = b.bdg_em_addr';
    else
      vHasBosSqlCols := ',null hasbos';
      vHasBosSqlJoin := ' ';
    
    end if;
    if pIn_nNeedHasBoM = 1 then
      vHasBoMSqlCols := ',sign(nvl(hasbom.nmc_em_addr, 0)) hasBom';
      vHasBoMSqlJoin := '      left join (select distinct n.pere_pro_nmc, n.nmc_em_addr
                         from nmc n
                        where n.nmc_field = 80) hasbom
              on hasbom.pere_pro_nmc = ';
      if pIn_nNodeType = 0 then
        vHasBoMSqlJoin := vHasBoMSqlJoin || ' f.fam_em_addr';
      else
        vHasBoMSqlJoin := vHasBoMSqlJoin || '  c.adr_cdt';
      end if;
    else
      vHasBoMSqlCols := ',null hasBom';
      vHasBomSqlJoin := ' ';
    end if;
  
    if pIn_nNodeType = 0 then
      --detail  node
      cSQL := ' select a.pvt_cle key,
                 a.pvt_desc description,
                 a.pvt_em_addr nodeid,
                 b.bdg_em_addr bdgid,
                 null uom,
                 m.date_deb_prev_annee sdateyy,
                 m.date_deb_prev_periode sdatemm,
                 m.date_fin_prev_annee edateyy,
                 m.date_fin_prev_periode edatemm,' ||
              vNotesCols || '
                 p.crtserie36_em_addr attributeID,
                 c.val_crt_serie attributeval,
                 c.lib_crt_serie attributedesc,
               ' || vContinuationCols || '
                 sign(nvl(n.nmc_em_addr, 0)) isBom,
                 sign(nvl(s2.supplier_em_addr, 0)) isBos,
                 0 nb_prevision,
                 f.fam_em_addr pid,
                 f.f_cle pval,
                 f.f_desc pdesc,
                 v.vct_em_addr pvalunitid,
                 v.val pvalunitkey,
                 g.geo_em_addr stid,
                 g.g_cle stval,
                 g.g_desc stdesc,
                 d.dis_em_addr tcid,
                 d.d_cle tcval,
                 d.d_desc tcdesc,1,1,1' || vHasBosSqlCols ||
              vHasBomSqlCols || '
            from (select p.pvt_cle,
                         p.pvt_desc,
                         p.pvt_em_addr,
                         p.fam4_em_addr,
                         p.geo5_em_addr,
                         p.dis6_em_addr
                    from pvt p, ' || vTableName || ' t
                   where t.id = p.pvt_em_addr) a
            left join fam f
              on a.fam4_em_addr = f.fam_em_addr
            left join (select r.fam7_em_addr, c.vct_em_addr, c.val
                         from rfc r, vct c
                        where r.vct10_em_addr = c.vct_em_addr
                          and c.id_crt = 80
                          and c.num_crt = 68) v
              on v.fam7_em_addr = f.fam_em_addr
            left join geo g
              on a.geo5_em_addr = g.geo_em_addr
            left join dis d
              on a.dis6_em_addr = d.dis_em_addr
            left join bdg b
              on b.id_bdg = 80
             and b.b_cle = a.pvt_cle
            left join mod_forecast m
              on m.num_mod = ' || pIn_nNumMOD || '
             and m.bdg_em_addr = b.bdg_em_addr ' || vNotesSQL || '
            left join pvtcrt p
              on p.pvt35_em_addr = a.pvt_em_addr
             and p.numero_crt_pvt = ' || pIn_nAttributeNumber || '
            left join crtserie c
              on c.crtserie_em_addr = p.crtserie36_em_addr
            left join (select distinct n.fils_pro_nmc, n.nmc_em_addr
                         from nmc n
                        where n.nmc_field = 80) n
              on n.fils_pro_nmc = a.fam4_em_addr
            left join (select distinct s.fils_bdg, s.supplier_em_addr
                         from supplier s
                        where s.id_supplier = 78) s2
              on s2.fils_bdg = b.bdg_em_addr
            ' || vContinuationSQL || vHasBoSSqlJoin ||
              vHasBoMSqlJoin;
    
    elsif pIn_nNodeType = 1 then
      --aggregate node
      csql := '  select a.sel_cle               key,
                 a.sel_desc              description,
                 a.sel_em_addr           nodeid,
                 b.bdg_em_addr           bdgid,
                 a.unite_sel             uom,
                 m.date_deb_prev_annee   sdateyy,
                 m.date_deb_prev_periode sdatemm,
                 m.date_fin_prev_annee   edateyy,
                 m.date_fin_prev_periode edatemm,' ||
              vNotesCols || '
                 s.crtserie54_em_addr    attributeID,
                 c.val_crt_serie         attributeval,
                 c.lib_crt_serie         attributedesc,' ||
              vContinuationCols || '
                 sign(nvl(n.nmc_em_addr, 0)) isBom,
                 sign(nvl(s2.supplier_em_addr, 0)) isBos,
                 r.nb_prevision nb_prevision,
                 f.fam_em_addr pid,
                 f.f_cle pval,
                 f.f_desc pdesc,
                 v.vct_em_addr pvalunitid,
                 v.val pvalunitkey,
                 g.geo_em_addr stid,
                 g.g_cle stval,
                 g.g_desc stdesc,
                 d.dis_em_addr tcid,
                 d.d_cle tcval,
                 d.d_desc tcdesc,
                 o.poperator,
                 o.stoperator,
                 o.tcoperator' || vHasBosSqlCols ||
              vHasBomSqlCols || '
            from (select s.sel_cle, s.sel_desc, s.sel_em_addr, s.unite_sel
                    from sel s, ' || vTableName || ' t
                   where t.id = s.sel_em_addr
                     and s.sel_bud = 71) a
            left join v_aggnodewithlevel l
              on a.sel_em_addr = l.aggnodeid
            left join fmv_aggregatenodewithoperator o
              on a.sel_em_addr = o.aggnodeid
            left join fam f
              on f.fam_em_addr = l.PID
            left join (select r.fam7_em_addr, c.vct_em_addr, c.val
                         from rfc r, vct c
                        where r.vct10_em_addr = c.vct_em_addr
                          and c.id_crt = 80
                          and c.num_crt = 68) v
              on v.fam7_em_addr = f.fam_em_addr
            left join geo g
              on g.geo_em_addr = l.STID
            left join dis d
              on d.dis_em_addr = l.tcid

            left join cdt c
              on c.rcd_cdt = 10000
             and c.operant = 1
             and c.sel11_em_addr = a.sel_em_addr
            left join bdg b
              on b.id_bdg = 71
             and b.b_cle = a.sel_cle
            left join mod_forecast m
              on m.num_mod = ' || pIn_nNumMOD || '
             and m.bdg_em_addr = b.bdg_em_addr ' || vNotesSQL || '

            left join selcrt s
              on s.sel53_em_addr = a.sel_em_addr
             and s.numero_crt_sel = ' || pIn_nAttributeNumber || '
            left join crtserie c
              on c.crtserie_em_addr = s.crtserie54_em_addr
            left join (select distinct s.fils_bdg, s.supplier_em_addr
                         from supplier s
                        where s.id_supplier = 83) s1
              on s1.fils_bdg = b.bdg_em_addr
            left join (select distinct n.fils_pro_nmc, n.nmc_em_addr
                         from nmc n
                        where n.nmc_field = 80) n
              on n.fils_pro_nmc = c.adr_cdt
            left join (select distinct s.fils_bdg, s.supplier_em_addr
                         from supplier s
                        where s.id_supplier = 78) s2
              on s2.fils_bdg = b.bdg_em_addr
            left join prvsel v
              on v.sel16_em_addr = a.sel_em_addr
            left join prv r
              on v.prv15_em_addr = r.prv_em_addr
            ' || vContinuationSQL || vHasBoSSqlJoin ||
              vHasBoMSqlJoin;
    
    end if;
    csql := 'insert into TMP_DDABASICDATA
             (KEY,DESCRIPTION,NODEID,BDGID,UOM,SDATEYY,SDATEMM,EDATEYY,EDATEMM,
             FNOTES,DNOTES, ATTRIBUTEID,ATTRIBUTEVAL,ATTRIBUTEDESC,ISCONTINUATIONOF,ISBOM,ISBOS,NB_PREVISION,
             PID,PVAL, PDESC,PVALUNITID,PVALUNITKEY,STID,STVAL,STDESC,TCID,TCVAL,TCDESC,poperator,stoperator,tcoperator,hasBos,HasBom)' || csql;
    fmp_log.LogCrucInfo(pIn_cSqlText => csql);
    fmsp_execsql(pIn_cSql => csql);
    if pIn_nNeedNotes = 1 then
      open pOut_rResult for
        select KEY,
               DESCRIPTION,
               NODEID,
               BDGID,
               UOM,
               SDATEYY,
               SDATEMM,
               EDATEYY,
               EDATEMM,
               FNOTES,
               DNOTES,
               ATTRIBUTEID,
               ATTRIBUTEVAL,
               ATTRIBUTEDESC,
               ISCONTINUATIONOF,
               ISBOM,
               ISBOS,
               NB_PREVISION,
               PID,
               PVAL,
               PDESC,
               PVALUNITID,
               PVALUNITKEY,
               STID,
               STVAL,
               STDESC,
               TCID,
               TCVAL,
               TCDESC,
               1                poperator,
               1                stoperator,
               1                tcoperator,
               hasBos,
               HasBom
          from TMP_DDABASICDATA;
    else
      open pOut_rResult for
        select KEY,
               DESCRIPTION,
               NODEID,
               BDGID,
               UOM,
               SDATEYY,
               SDATEMM,
               EDATEYY,
               EDATEMM,
               null             FNOTES,
               null             DNOTES,
               ATTRIBUTEID,
               ATTRIBUTEVAL,
               ATTRIBUTEDESC,
               ISCONTINUATIONOF,
               ISBOM,
               ISBOS,
               NB_PREVISION,
               PID,
               PVAL,
               PDESC,
               PVALUNITID,
               PVALUNITKEY,
               STID,
               STVAL,
               STDESC,
               TCID,
               TCVAL,
               TCDESC,
               poperator,
               stoperator,
               tcoperator,
               hasBos,
               HasBom
          from TMP_DDABASICDATA;
    end if;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_BasicData(pIn_nNodeType         in number,
                           pIn_cNodeList         in clob,
                           pIn_nAttributeNumber  in number,
                           pIn_nNumMOD           in number,
                           pIn_nNeedContinuation in number default 0,
                           pIn_nNeedNotes        in number default 1,
                           pIn_nNeedHasBos       in number default 0,
                           pIn_nNeedHasBoM       in number default 0,
                           pOut_rResult          out sys_refcursor,
                           pOut_nSqlCode         out number)
  --*****************************************************************
    -- Description: load data:key desc,address,BDG address,UoM type,forecast start /end date,note forecast,note DRP
    --              node attribute
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_vNodeList
    --       pIn_nAttributeNumber
    --       pIn_nNumMOD mod_forecast.num_mod
    --       pIn_nNeedContinuation:1 return continuation of ;0 not return continuation of
    --       pIn_nNeedNotes:whether return forecast and drp notes
    --       pIn_nNeedHasBos:hasbos
    --       pIn_nNeedHasBom:hasbom
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        19-DEC-2012     JY.Liu     Created.
    --  V7.0        04-JAN-2013     JY.Liu     SQL Tuning
    --  V7.0        30-JAN-2013     JY.Liu     add parameter pIn_nNeedContinuation
    --  V7.0        2/18/2013       Yi.Zhu     SQL Tuning
    --  V7.0        08-MAR-2013     JY.Liu     add dimension value
    --  v&.0        26-APR-2013     JY.Liu     add HasBoS HasBoM
    -- **************************************************************
   as
  begin
    -- add log
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_cNodeList);
    Fmp_Log.FMP_SetValue(pIn_nAttributeNumber);
    Fmp_Log.FMP_SetValue(pIn_nNumMOD);
    Fmp_Log.FMP_SetValue(pIn_nNeedContinuation);
    fmp_log.FMP_SetValue(pIn_nNeedNotes);
    Fmp_Log.LOGBEGIN;
    FMSP_DDABasicData(pIn_nNodeType         => pIn_nNodeType,
                      pIn_cNodeList         => pIn_cNodeList,
                      pIn_nAttributeNumber  => pIn_nAttributeNumber,
                      pIn_nNumMOD           => pIn_nNumMOD,
                      pIn_nNeedContinuation => pIn_nNeedContinuation,
                      pIn_nNeedNotes        => pIn_nNeedNotes,
                      pIn_nNeedHasBos       => pIn_nNeedHasBos,
                      pIn_nNeedHasBoM       => pIn_nNeedHasBoM,
                      pOut_rResult          => pOut_rResult,
                      pOut_nSqlCode         => pOut_nSqlCode);
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_GetNoteArrayByPeriod(pIn_nNodeType in number,
                                      pIn_cNodeList in clob,
                                      pIn_MorWorD   in number,
                                      pIn_vStart    in varchar2,
                                      pIn_vEnd      in varchar2,
                                      pOut_rResult  out sys_refcursor,
                                      pOut_nSqlCode out number)
  --*****************************************************************
    -- Description: load data:note by period
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_vNodeList
    --       pIn_MorWorD:1-monthly;2-weekly;4-day
    --       pIn_vStart:srart time .format is yyyy-pp-dd  pp month or week.(2012-12-0)
    --       pIn_vEnd :end time
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        25-DEC-2012     JY.Liu     Created.
    -- **************************************************************
   as
    tNestedTabNodeids fmt_nest_tab_nodeid;
    nPeroidLimit      number;
    nSYYYY            number; --start year
    nSPP              number; --start month or week
    nSDD              number; --start day
    nEYYYY            number; --end year
    nEPP              number; --end month or week
    nEDD              number; --end day
  
  begin
    pOut_nSqlCode := 0;
    -- add log
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_cNodeList);
    Fmp_Log.FMP_SetValue(pIn_MorWorD);
    Fmp_Log.FMP_SetValue(pIn_vStart);
    Fmp_Log.FMP_SetValue(pIn_vEnd);
    Fmp_Log.LOGBEGIN;
    nPeroidLimit := case pIn_MorWorD
                      when 1 then
                       12
                      when 2 then
                       53
                      when 4 then
                       31
                    end;
    --parsing start time .for example 2012-12-12
    nSYYYY := to_number(substr(pIn_vStart,
                               1,
                               instr(pIn_vStart, '-', 1, 1) - 1));
    nSPP   := to_number(substr(pIn_vStart,
                               (instr(pIn_vStart, '-', 1, 1) + 1),
                               (instr(pIn_vStart, '-', 1, 2) -
                               instr(pIn_vStart, '-', 1, 1) - 1)));
    nSDD   := to_number(substr(pIn_vStart, instr(pIn_vStart, '-', 1, 2) + 1));
    --parsing end time.for example 2013-12-12
    nEYYYY := to_number(substr(pIn_vEnd, 1, instr(pIn_vEnd, '-', 1, 1) - 1));
    nEPP   := to_number(substr(pIn_vEnd,
                               (instr(pIn_vEnd, '-', 1, 1) + 1),
                               (instr(pIn_vEnd, '-', 1, 2) -
                               instr(pIn_vEnd, '-', 1, 1) - 1)));
    nSDD   := to_number(substr(pIn_vEnd, instr(pIn_vEnd, '-', 1, 2) + 1));
    FMSP_ClobToTable(pIn_cClob     => pIn_cNodeList,
                     pOut_nSqlCode => pOut_nSqlCode);
  
    if pIn_MorWorD in (1, 2) then
      if pIn_nNodeType = 0 then
        --detail node
        open pOut_rResult for
          select p.pvt_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 pvt p,
                 bdg g,
                 (select id from tb_node) n
           where n.id = p.pvt_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = pIn_MorWorD
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 80
             and g.b_cle = p.pvt_cle
             and (((t.annee_deb_note >= nSYYYY and
                 t.annee_deb_note < nEYYYY) and
                 (t.mois_deb_note >= nSPP and t.mois_deb_note <= (case
                   when nEYYYY = nSYYYY then
                    nEPP
                   else
                    nPeroidLimit
                 end))) or (t.annee_deb_note = nEYYYY and t.mois_deb_note between case
                   when nSYYYY = nEYYYY then
                    nSPP
                   else
                    1
                 end and nEPP));
      
      elsif pIn_nNodeType = 1 then
        --aggregate node
        open pOut_rResult for
          select s.sel_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 sel s,
                 bdg g,
                 (select id from tb_node) n
           where n.id = s.sel_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = pIn_MorWorD
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 71
             and g.b_cle = s.sel_cle
             and (((t.annee_deb_note >= nSYYYY and
                 t.annee_deb_note < nEYYYY) and
                 (t.mois_deb_note >= nSPP and t.mois_deb_note <= (case
                   when nEYYYY = nSYYYY then
                    nEPP
                   else
                    nPeroidLimit
                 end))) or (t.annee_deb_note = nEYYYY and t.mois_deb_note between case
                   when nSYYYY = nEYYYY then
                    nSPP
                   else
                    1
                 end and nEPP));
      end if;
    elsif pIn_MorWorD = 4 then
      --day
      if pIn_nNodeType = 0 then
        --detail node
        open pOut_rResult for
          select p.pvt_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t, typenote n, pvt p, bdg g, tb_node i
           where i.id = p.pvt_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = pIn_MorWorD
             and t.jour_deb_note <> 0
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 80
             and g.b_cle = p.pvt_cle
             and to_char(t.annee_deb_note, 'FM0000') ||
                 to_char(t.mois_deb_note, 'FM000') ||
                 to_char(t.jour_deb_note, 'FM00') between
                 to_char(nSYYYY, 'FM0000') || to_char(nSPP, 'FM000') ||
                 to_char(nSDD, 'FM00') and
                 to_char(nEYYYY, 'FM0000') || to_char(nEPP, 'FM000') ||
                 to_char(nEDD, 'FM00');
      elsif pIn_nNodeType = 1 then
        --aggregate node
        open pOut_rResult for
          select s.sel_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t, typenote n, sel s, bdg g, tb_node i
           where i.id = s.sel_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = pIn_MorWorD
             and t.jour_deb_note <> 0
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 71
             and g.b_cle = s.sel_cle
             and to_char(t.annee_deb_note, 'FM0000') ||
                 to_char(t.mois_deb_note, 'FM000') ||
                 to_char(t.jour_deb_note, 'FM00') between
                 to_char(nSYYYY, 'FM0000') || to_char(nSPP, 'FM000') ||
                 to_char(nSDD, 'FM00') and
                 to_char(nEYYYY, 'FM0000') || to_char(nEPP, 'FM000') ||
                 to_char(nEDD, 'FM00');
      end if;
    end if;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_GetDimesionValue(pIn_nNodeType in number,
                                  pIn_cNodeList in clob,
                                  pOut_rResult  out sys_refcursor,
                                  pOut_nSqlCode out number)
  --*****************************************************************
    -- Description: load dimesion values of node
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_vNodeList
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        29-DEC-2012     JY.Liu     Created.
    --  V7.0        08-MAR-2013     JY.Liu     remove dupicate code .tuning
    -- **************************************************************
   as
  
  begin
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_cNodeList);
    Fmp_Log.LOGBEGIN;
  
    FMSP_GetDDADimesionValue(pIn_nNodeType => pIn_nNodeType,
                             pIn_cNodeList => pIn_cNodeList,
                             pOut_nSqlCode => pOut_nSqlCode);
    open pOut_rResult for
      select * from TMP_DDADimesionValue;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
  procedure FMISP_BasicData(pIn_nNodeType         in number,
                            pIn_vTabName          in varchar2,
                            pIn_nAttributeNumber  in number,
                            pIn_nNumMOD           in number,
                            pIn_nNeedContinuation in number default 0,
                            pIn_nNeedNotes        in number default 1,
                            pIn_nNeedHasBos       in number default 0,
                            pIn_nNeedHasBoM       in number default 0,
                            pOut_rResult          out sys_refcursor,
                            pOut_nSqlCode         out number)
  --*****************************************************************
    -- Description: load data:key desc,address,BDG address,UoM type,forecast start /end date,note forecast,note DRP
    --              node attribute
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_vNodeList
    --       pIn_nAttributeNumber
    --       pIn_nNumMOD mod_forecast.num_mod
    --       pIn_nNeedContinuation:1 return continuation of ;0 not return continuation of
    --       pIn_nNeedNotes:whether return forecast and drp notes
    --       pIn_nNeedHasBos:hasbos
    --       pIn_nNeedHasBom:hasbom
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        19-DEC-2012     JY.Liu     Created.
    --  V7.0        04-JAN-2013     JY.Liu     SQL Tuning
    --  V7.0        30-JAN-2013     JY.Liu     add parameter pIn_nNeedContinuation
    --  V7.0        2/18/2013       Yi.Zhu     SQL Tuning
    --  V7.0        21-FEB-2013     JY.Liu     copy code from  FMSP_BasicData
    --  V7.0        08-MAR-2013     JY.Liu     add dimension value
    --  v&.0        26-APR-2013     JY.Liu     add HasBoS HasBoM
    -- **************************************************************
   as
  begin
    -- add log
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_vTabName);
    Fmp_Log.FMP_SetValue(pIn_nAttributeNumber);
    Fmp_Log.FMP_SetValue(pIn_nNumMOD);
    Fmp_Log.FMP_SetValue(pIn_nNeedContinuation);
    fmp_log.FMP_SetValue(pIn_nNeedNotes);
    Fmp_Log.LOGBEGIN;
    FMSP_DDABasicData(pIn_nNodeType         => pIn_nNodeType,
                      pIn_vTabName          => pIn_vTabName,
                      pIn_nAttributeNumber  => pIn_nAttributeNumber,
                      pIn_nNumMOD           => pIn_nNumMOD,
                      pIn_nNeedContinuation => pIn_nNeedContinuation,
                      pIn_nNeedNotes        => pIn_nNeedNotes,
                      pIn_nNeedHasBos       => pIn_nNeedHasBos,
                      pIn_nNeedHasBoM       => pIn_nNeedHasBoM,
                      pOut_rResult          => pOut_rResult,
                      pOut_nSqlCode         => pOut_nSqlCode);
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
  procedure FMISP_GetNoteArrayByPeriod(pIn_nNodeType in number,
                                       pIn_vTabName  in varchar2,
                                       pIn_MorWorD   in number,
                                       pIn_vStart    in varchar2,
                                       pIn_vEnd      in varchar2,
                                       pOut_rResult  out sys_refcursor,
                                       pOut_nSqlCode out number)
  --*****************************************************************
    -- Description: load data:note by period
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_vTabName:table stored nodes ID
    --       pIn_MorWorD:1-monthly;2-weekly;4-day
    --       pIn_vStart:srart time .format is yyyy-pp-dd  pp month or week.(2012-12-0)
    --       pIn_vEnd :end time
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        25-DEC-2012     JY.Liu     Created.
    --  V7.0        21-FEB-2013     JY.Liu     copy code from  FMSP_GetNoteArrayByPeriod
    -- **************************************************************
   as
    nPeroidLimit number;
    nSYYYY       number; --start year
    nSPP         number; --start month or week
    nEYYYY       number; --end year
    nEPP         number; --end month or week
    cSql         clob;
  begin
    pOut_nSqlCode := 0;
    -- add log
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_vTabName);
    Fmp_Log.FMP_SetValue(pIn_MorWorD);
    Fmp_Log.FMP_SetValue(pIn_vStart);
    Fmp_Log.FMP_SetValue(pIn_vEnd);
    Fmp_Log.LOGBEGIN;
    nPeroidLimit := case pIn_MorWorD
                      when 1 then
                       12
                      when 2 then
                       53
                      when 4 then
                       31
                    end;
    --parsing start time .for example 2012-12-12
    nSYYYY := to_number(substr(pIn_vStart,
                               1,
                               instr(pIn_vStart, '-', 1, 1) - 1));
    nSPP   := to_number(substr(pIn_vStart,
                               (instr(pIn_vStart, '-', 1, 1) + 1),
                               (instr(pIn_vStart, '-', 1, 2) -
                               instr(pIn_vStart, '-', 1, 1) - 1)));
    --nsdd   := to_number(substr(pIn_vStart, instr(pIn_vStart, '-', 1, 2) + 1));
    --parsing end time.for example 2013-12-12
    nEYYYY := to_number(substr(pIn_vEnd, 1, instr(pIn_vEnd, '-', 1, 1) - 1));
    nEPP   := to_number(substr(pIn_vEnd,
                               (instr(pIn_vEnd, '-', 1, 1) + 1),
                               (instr(pIn_vEnd, '-', 1, 2) -
                               instr(pIn_vEnd, '-', 1, 1) - 1)));
    --nEdd   := to_number(substr(pIn_vStart, instr(pIn_vStart, '-', 1, 2) + 1));
  
    if pIn_MorWorD in (1, 2) then
      if pIn_nNodeType = 0 then
        --detail node
        cSQL := ' select p.pvt_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 pvt p,
                 bdg g,
                 ' || pIn_vTabName || ' n
           where n.id = p.pvt_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = ' || pIn_MorWorD || '
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 80
             and g.b_cle = p.pvt_cle
             and (((t.annee_deb_note >= ' || nSYYYY ||
                ' and
                 t.annee_deb_note < ' || nEYYYY ||
                ') and
                 (t.mois_deb_note >= ' || nSPP ||
                ' and t.mois_deb_note <= (case
                   when ' || nEYYYY || ' = ' || nSYYYY ||
                ' then
                    ' || nEPP || '
                   else
                    ' || nPeroidLimit || '
                 end))) or (t.annee_deb_note = ' || nEYYYY ||
                ' and t.mois_deb_note between case
                   when ' || nSYYYY || ' = ' || nEYYYY ||
                ' then
                    ' || nSPP || '
                   else
                    1
                 end and ' || nEPP || '))';
      
      elsif pIn_nNodeType = 1 then
        --aggregate node
        CSQL := 'select s.sel_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 sel s,
                 bdg g,
                 ' || pIn_vTabName || ' n
           where n.id = s.sel_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = ' || pIn_MorWorD || '
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 71
             and g.b_cle = s.sel_cle
             and (((t.annee_deb_note >= ' || nSYYYY ||
                ' and
                 t.annee_deb_note < ' || nEYYYY ||
                ') and
                 (t.mois_deb_note >= ' || nSPP ||
                ' and t.mois_deb_note <= (case
                   when ' || nEYYYY || ' = ' || nSYYYY ||
                ' then
                    ' || nEPP || '
                   else
                    ' || nPeroidLimit || '
                 end))) or (t.annee_deb_note = ' || nEYYYY ||
                ' and t.mois_deb_note between case
                   when ' || nSYYYY || ' = ' || nEYYYY ||
                ' then
                    ' || nSPP || '
                   else
                    1
                 end and ' || nEPP || '))';
      end if;
    elsif pIn_MorWorD = 4 then
      --day
      if pIn_nNodeType = 0 then
        --detail node
        csql := '   select p.pvt_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 pvt p,
                 bdg g,
           ' || pIn_vTabName || ' n
           where n.id = p.pvt_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = ' || pIn_MorWorD || '
             and t.jour_deb_note <> 0
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 80
             and g.b_cle = p.pvt_cle
             and to_date(t.annee_deb_note || ''-'' || t.mois_deb_note || ''-'' ||
                         t.jour_deb_note,
                         ''yyyy-mm-dd'') between
                          to_date(''' || pIn_vStart ||
                ''', ''yyyy-mm-dd'') and
                 to_date(''' || pIn_vEnd ||
                ''', ''yyyy-mm-dd'')';
      
      elsif pIn_nNodeType = 1 then
        --aggregate node
        csql := 'select s.sel_em_addr    nodeid,
                 t.annee_deb_note yyyy,
                 t.mois_deb_note  pp,
                 t.jour_deb_note  dd,
                 t.timetext       note
            from timenote t,
                 typenote n,
                 sel s,
                 bdg g,
                 ' || pIn_vTabName || ' n
           where n.id = s.sel_em_addr
             and t.typenote59_em_addr = n.typenote_em_addr
             and n.type_note = ' || pIn_MorWorD || '
             and t.jour_deb_note <> 0
             and n.bdg47_em_addr = g.bdg_em_addr
             and g.id_bdg = 71
             and g.b_cle = s.sel_cle
             and to_date(t.annee_deb_note || ''-'' || t.mois_deb_note || ''-'' ||
                         t.jour_deb_note,
                         ''yyyy-mm-dd'') between
                  to_date(''' || pIn_vStart ||
                ''', ''yyyy-mm-dd'') and
                 to_date(''' || pIn_vEnd ||
                ''', ''yyyy-mm-dd'')';
      end if;
    end if;
    open pOut_rResult for cSQL;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
  procedure FMSP_GetDDADimesionValue(pIn_nNodeType in number,
                                     pIn_cNodeList in clob default null,
                                     pIn_vTabName  in varchar2 default null,
                                     pOut_nSqlCode out number) as
    tNestedTabNodeids fmt_nest_tab_nodeid;
    cSql              clob;
    vTableName        varchar2(30) := 'tb_node';
  begin
    pOut_nSqlCode := 0;
    fmsp_execsql(' truncate table  ' || vTableName);
    if pIn_cNodeList is not null then
      FMSP_ClobToTable(pIn_cClob     => pIn_cNodeList,
                       pOut_nSqlCode => pOut_nSqlCode);
    else
      fmsp_execsql(pIn_cSql => 'insert into ' || vTableName ||
                               ' select id from ' || pIn_vTabName);
    end if;
  
    fmsp_execsql(pIn_cSql => 'truncate table TMP_DDADimesionValue');
    if pIn_nNodeType = 0 then
      -- detail node
      cSql := 'select t.id   nodeid,
               f.fam_em_addr pid,
               f.f_cle       pval,
               f.f_desc      pdesc,
               v.vct_em_addr pvalunitid,
               v.val         pvalunitkey,
               g.geo_em_addr stid,
               g.g_cle       stval,
               g.g_desc      stdesc,
               d.dis_em_addr tcid,
               d.d_cle       tcval,
               d.d_desc      tcdesc
          from ' || vTableName || ' t
          left join pvt p
            on t.id = p.pvt_em_addr
          left join fam f
            on f.fam_em_addr = p.fam4_em_addr
          left join (select r.fam7_em_addr, c.vct_em_addr, c.val
                       from rfc r, vct c
                      where r.vct10_em_addr = c.vct_em_addr
                        and c.id_crt = 80
                        and c.num_crt = 68) v
            on v.fam7_em_addr = f.fam_em_addr
          left join geo g
            on p.geo5_em_addr = g.geo_em_addr
          left join dis d
            on p.dis6_em_addr = d.dis_em_addr';
    elsif pIn_nNodeType = 1 then
      --aggregate node
      csql := ' select t.nodeid      nodeid,
               f.fam_em_addr pid,
               f.f_cle       pval,
               f.f_desc      pdesc,
               v.vct_em_addr pvalunitid,
               v.val         pvalunitkey,
               g.geo_em_addr stid,
               g.g_cle       stval,
               g.g_desc      stdesc,
               d.dis_em_addr tcid,
               d.d_cle       tcval,
               d.d_desc      tcdesc
          from (select sel11_em_addr nodeid, pid, stid, tcid
                  from (select c.sel11_em_addr, c.adr_cdt, c.rcd_cdt
                          from cdt c, ' || vTableName || ' n
                         where c.sel11_em_addr = n.id
                           and c.operant = 1) pivot(max(adr_cdt) for rcd_cdt in(10000 as pid,
                                                                                10001 as stid,
                                                                                10002 as tcid))) t
          left join fam f
            on f.fam_em_addr = t.pid
          left join (select r.fam7_em_addr, c.vct_em_addr, c.val
                       from rfc r, vct c
                      where r.vct10_em_addr = c.vct_em_addr
                        and c.id_crt = 80
                        and c.num_crt = 68) v
            on v.fam7_em_addr = f.fam_em_addr
          left join geo g
            on g.geo_em_addr = t.stid
          left join dis d
            on d.dis_em_addr = t.tcid';
    end if;
    cSql := ' insert into TMP_DDADimesionValue ' || cSql;
    fmsp_execsql(pIn_cSql => cSql);
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMISP_GetDimesionValue(pIn_nNodeType in number,
                                   pIn_vTabName  in varchar2,
                                   pOut_rResult  out sys_refcursor,
                                   pOut_nSqlCode out number)
  --*****************************************************************
    -- Description: load dimesion values of node
    --
    -- Parameters:
    --       pIn_nNodeType:detail node or aggregate node
    --       pIn_cTabName:table stored node list
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        22-FEB-2013     JY.Liu     Created.
    --  V7.0        08-MAR-2013     JY.Liu     remove dupicate code .tuning
    -- **************************************************************
   as
  begin
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_vTabName);
    Fmp_Log.LOGBEGIN;
    FMSP_GetDDADimesionValue(pIn_nNodeType => pIn_nNodeType,
                             pIn_vTabName  => pIn_vTabName,
                             pOut_nSqlCode => pOut_nSqlCode);
    open pOut_rResult for
      select * from TMP_DDADimesionValue;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_ContinuationOfandData(pIn_nNodeType         in number,
                                       pIn_cNodeList         in clob default null,
                                       pIn_vTabName          in varchar2 default null,
                                       pIn_nAttributeNumber  in number,
                                       pIn_nNumMOD           in number,
                                       pIn_nNeedContinuation in number default 0,
                                       pIn_nNeedHasBos       in number default 0,
                                       pIn_nNeedHasBoM       in number default 0,
                                       pIn_nScenario         in number,
                                       pOut_rBasicData       out sys_refcursor,
                                       pOut_Datas            out sys_refcursor,
                                       pOut_nSqlCode         out number)
  
    --*****************************************************************
    -- Description: load data:key desc,address,BDG address,UoM type,forecast start /end date,note forecast,note DRP
    --              node attribute
    --              continuation of data
    -- Parameters:
    --       pIn_nNodeType:0 detail node or 1 aggregate node
    --       pIn_vNodeList
    --       pIn_nAttributeNumber
    --       pIn_nNumMOD mod_forecast.num_mod
    --       pIn_nNeedContinuation:1 return continuation of ;0 not return continuation of
    --       pIn_nNeedHasBos:hasbos
    --       pIn_nNeedHasBom:hasbom
    --       pOut_rResult
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        6-3-2012     wfq     Created.
    --  v&.0        26-APR-2013     JY.Liu     add HasBoS HasBoM
    --*****************************************************************
   as
    pOut_vSqlMsg varchar2(5000);
    p_TableName  varchar2(50);
    v_strsql     varchar2(8000);
  begin
    -- add log
    pOut_nSqlCode := 0;
  
    Fmp_Log.FMP_SetValue(pIn_nNodeType);
    Fmp_Log.FMP_SetValue(pIn_cNodeList);
    Fmp_Log.FMP_SetValue(pIn_nAttributeNumber);
    Fmp_Log.FMP_SetValue(pIn_nNumMOD);
    Fmp_Log.FMP_SetValue(pIn_nScenario);
    Fmp_Log.LOGBEGIN;
  
    if pIn_vTabName is null then
      --NodeList
      FMSP_BasicData(pIn_nNodeType         => pIn_nNodeType,
                     pIn_cNodeList         => pIn_cNodeList,
                     pIn_nAttributeNumber  => pIn_nAttributeNumber,
                     pIn_nNumMOD           => pIn_nNumMOD,
                     pIn_nNeedContinuation => pIn_nNeedContinuation,
                     pIn_nNeedHasBos       => pIn_nNeedHasBos,
                     pIn_nNeedHasBoM       => pIn_nNeedHasBoM,
                     pOut_rResult          => pOut_rBasicData,
                     pOut_nSqlCode         => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    
    else
      --TabName
      FMISP_BasicData(pIn_nNodeType         => pIn_nNodeType,
                      pIn_vTabName          => pIn_vTabName,
                      pIn_nAttributeNumber  => pIn_nAttributeNumber,
                      pIn_nNumMOD           => pIn_nNumMOD,
                      pIn_nNeedContinuation => pIn_nNeedContinuation,
                      pIn_nNeedHasBos       => pIn_nNeedHasBos,
                      pIn_nNeedHasBoM       => pIn_nNeedHasBoM,
                      pOut_rResult          => pOut_rBasicData,
                      pOut_nSqlCode         => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    
    end if;
  
    p_TableName := fmf_gettmptablename();
    v_strsql    := 'CREATE TABLE ' || p_TableName || '(ID NUMBER)';
    execute immediate v_strsql;
  
    v_strsql := 'insert into ' || p_TableName ||
                ' select nodeid from tmp_node_continuation where continuationof=1';
    execute immediate v_strsql;
  
    --pOut_vTabName := 'tmp_node_continuation';
    FMP_ContinuationOfData.FMISP_GetContinuationOfData(pIn_vTabName  => p_TableName,
                                                       pIn_nNodeType => pIn_nNodeType, --0 detail node ; 1 aggregate node
                                                       pIn_nScenario => pIn_nScenario, --48\49(num_mod in mod_forecast )
                                                       pOutDatas     => pOut_Datas,
                                                       pOut_nSqlCode => pOut_nSqlCode,
                                                       pOut_vSqlMsg  => pOut_vSqlMsg);
  
    if pOut_nSqlCode <> 0 then
      return;
    end if;
  
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

end FMP_LoadDDAData;
/
