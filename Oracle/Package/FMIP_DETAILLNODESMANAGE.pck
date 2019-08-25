CREATE OR REPLACE PACKAGE FMIP_DETAILLNODESMANAGE IS

  -- Author  : LZHANG
  -- Created : 3/12/2013 1:45:38 PM
  -- Purpose :

  -- Public type declarations
  PROCEDURE FMISP_ModifyPvtKeys(pIn_vOldKeyP           in varchar2,
                                pIn_vOldKeyS           in varchar2,
                                pIn_vOldKeyT           in varchar2,
                                pIn_vNewKeyP           in varchar2,
                                pIn_vNewKeyS           in varchar2,
                                pIn_vNewKeyT           in varchar2,
                                pIn_nType              in number,
                                pIn_TimeSeriesNoForUoM in varchar2,
                                pIn_vUoMConfig         in varchar2,
                                pOut_nSqlCode          out number);

  PROCEDURE FMISP_CreateGroupPvts(pIn_vConditions IN VARCHAR2,
                                  pIn_vUserName   IN VARCHAR2,
                                  pOut_nSqlCode   OUT NUMBER);

  PROCEDURE FMISP_AddPvtToSel(pIn_nPVTID    IN NUMBER,
                              pOut_nSqlCode OUT NUMBER);

  PROCEDURE FMISP_DeleteOnePvt(pIn_nPVTID             IN NUMBER,
                               pIn_TimeSeriesNoForUoM in varchar2,
                               pIn_vUoMConfig         in varchar2,
                               pOut_nSqlCode          OUT NUMBER);

  PROCEDURE FMISP_DeleteGroupPvts(pIn_vConditions        IN VARCHAR2,
                                  pIn_TimeSeriesNoForUoM in varchar2,
                                  pIn_vUoMConfig         in varchar2,
                                  pOut_nSqlCode          OUT NUMBER);

END FMIP_DETAILLNODESMANAGE;
/
CREATE OR REPLACE PACKAGE BODY FMIP_DETAILLNODESMANAGE IS

  GC_ONEDETAILTSID constant number := 0; -- mark modify a detail node
  GC_GROUPTSIDS    constant number := 1; -- mark modify a group of detail nodes
  GC_NEW_PVTID     constant number := 0; -- new changed detail node
  GC_OLD_PVTID     constant number := 1; -- old must changed detail node
  TYPE AGGIDListType is TABLE OF varchar2(2000) INDEX BY BINARY_INTEGER; -- define a list Type for storing aggregation nodes
  aggIdList AGGIDListType; -- store all aggregation nodes
  TYPE BelongsAGGIDListType is TABLE OF varchar2(2000) INDEX BY BINARY_INTEGER; -- define a list Type for storing relation aggregation nodes
  NewBelongsAGGIDList BelongsAGGIDListType; -- new detail nodes relation aggregation nodes
  OldBelongsAGGIDList BelongsAGGIDListType; -- old detail nodes relation aggregation nodes

  TYPE BelongsSELIDListType is TABLE OF varchar2(2000) INDEX BY BINARY_INTEGER; -- define a list Type for storing relation SELECTION IDs
  NewBelongsSELIDList BelongsAGGIDListType; -- new detail nodes relation SELECTION IDs
  OldBelongsSELIDList BelongsAGGIDListType; -- old detail nodes relation SELECTION IDs

  PROCEDURE FMSP_DeleteTables(pIn_nPVTID      in number,
                              pIn_nDelBDGType in number, -- 0 means delete bdg 1 means not
                              pOut_nSqlCode   out number) is
  BEGIN
  
    --delete don_m
    delete from don_m m where m.pvtid = pIn_nPVTID;
    --delete don_w
    delete from don_w m where m.pvtid = pIn_nPVTID;
    --delete pvtcrt
    DELETE FROM pvtcrt B where b.pvt35_em_addr = pIn_nPVTID;
    --delete modscl
    delete from modscl s
     where exists (select *
              from (select m.mod_em_addr
                      from bdg b, pvt p, mod m
                     WHERE b.b_cle = p.pvt_cle
                       and b.bdg_em_addr = m.bdg30_em_addr
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) b
             where s.mod42_em_addr = b.mod_em_addr);
    --delete mod
    delete from mod m
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where m.bdg30_em_addr = p.bdg_em_addr);
    --delete bud
    delete from bud b
     where exists (select *
              from (select c.bgc_em_addr
                      from bdg b, pvt p, bgc c
                     WHERE b.b_cle = p.pvt_cle
                       and b.bdg_em_addr = c.bdg31_em_addr
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where b.bgc32_em_addr = p.bgc_em_addr);
    --delete bgc
    delete from bgc e
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where e.bdg31_em_addr = p.bdg_em_addr);
    --delete supplier
    delete from supplier s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where s.bdg51_em_addr = p.bdg_em_addr);
    --delete serinote
    delete from serinote s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where s.bdg3_em_addr = p.bdg_em_addr);
    --delete typenode
    delete from typenote t
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where t.bdg47_em_addr = p.bdg_em_addr);
    --delete select_sel
    delete from select_sel s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, pvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80
                       and p.pvt_em_addr = pIn_nPVTID) p
             where s.bdg52_em_addr = p.bdg_em_addr);
    if pIn_nDelBDGType = 0 then
      --delete bdg
      DELETE FROM BDG B
       WHERE exists (select *
                from (select b.b_cle
                        from bdg b, pvt p
                       WHERE b.b_cle = p.pvt_cle
                         and p.pvt_em_addr = pIn_nPVTID) p
               where b.b_cle = p.b_cle);
    end if;
  END FMSP_DeleteTables;

  PROCEDURE FMSP_DeleteAggRelated IS
  
  BEGIN
    delete from prvselpvt p
     where p.selid in (select s.sel_em_addr
                         from sel s, rsp r
                        where s.sel_em_addr = r.sel13_em_addr(+)
                          and s.sel_bud = 71
                          and r.rsp_em_addr is null);
  
    delete from prvsel p
     where p.sel16_em_addr in
           (select s.sel_em_addr
              from sel s, rsp r
             where s.sel_em_addr = r.sel13_em_addr(+)
               and s.sel_bud = 71
               and r.rsp_em_addr is null);
  
    delete from cdt c
     where c.sel11_em_addr in
           (select s.sel_em_addr
              from sel s, rsp r
             where s.sel_em_addr = r.sel13_em_addr(+)
               and s.sel_bud = 71
               and r.rsp_em_addr is null);
  
    delete from sel s
     where s.sel_em_addr in (select s.sel_em_addr
                               from sel s, rsp r
                              where s.sel_em_addr = r.sel13_em_addr(+)
                                and s.sel_bud = 71
                                and r.rsp_em_addr is null);
  END FMSP_DeleteAggRelated;

  PROCEDURE FMSP_IntoBelongsSELIDList(pIn_vSELID in varchar2,
                                      pIn_nType  in number) IS
    --*****************************************************************
    -- Description:  it suppose for insert value into  belongsSELIDList
    --
    -- Parameters:
    -- pIn_vSELID relation ID
    -- pIn_nType PVT_CHANGE_TYPE  TYPE IS NEW_PVTID OR OLD_PVTID
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nIndex number := 1;
    begin
      if pIn_nType = GC_NEW_PVTID then
        if NewBelongsSELIDList.count = 0 then
          NewBelongsSELIDList(nIndex) := pIn_vSELID;
        end if;
        for nIndex in 1 .. NewBelongsSELIDList.count loop
          if NewBelongsSELIDList(nIndex) = pIn_vSELID then
            return;
          end if;
        end loop;
        NewBelongsSELIDList(NewBelongsSELIDList.count + 1) := pIn_vSELID;
      elsif pIn_nType = GC_OLD_PVTID then
        if OldBelongsSELIDList.count = 0 then
          OldBelongsSELIDList(nIndex) := pIn_vSELID;
        end if;
        for nIndex in 1 .. OldBelongsSELIDList.count loop
          if OldBelongsSELIDList(nIndex) = pIn_vSELID then
            return;
          end if;
        end loop;
        OldBelongsSELIDList(OldBelongsSELIDList.count + 1) := pIn_vSELID;
      end if;
    end;
  End FMSP_IntoBelongsSELIDList;

  PROCEDURE FMSP_IntoBelongsAGGIDList(pIn_vAggID in varchar2,
                                      pIn_nType  in number) IS
    --*****************************************************************
    -- Description:  it suppose for insert value into  belongsAggIDList
    --
    -- Parameters:
    -- pIn_vAggID relation ID
    -- pIn_nType PVT_CHANGE_TYPE  TYPE IS NEW_PVTID OR OLD_PVTID
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nIndex number := 1;
    begin
      if pIn_nType = GC_NEW_PVTID then
        if NewBelongsAGGIDList.count = 0 then
          NewBelongsAGGIDList(nIndex) := pIn_vAGGID;
        end if;
        for nIndex in 1 .. NewBelongsAGGIDList.count loop
          if NewBelongsAGGIDList(nIndex) = pIn_vAggID then
            return;
          end if;
        end loop;
        NewBelongsAGGIDList(NewBelongsAGGIDList.count + 1) := pIn_vAggID;
      elsif pIn_nType = GC_OLD_PVTID then
        if OldBelongsAGGIDList.count = 0 then
          OldBelongsAGGIDList(nIndex) := pIn_vAGGID;
        end if;
        for nIndex in 1 .. OldBelongsAGGIDList.count loop
          if OldBelongsAGGIDList(nIndex) = pIn_vAggID then
            return;
          end if;
        end loop;
        OldBelongsAGGIDList(OldBelongsAGGIDList.count + 1) := pIn_vAggID;
      end if;
    end;
  End FMSP_IntoBelongsAGGIDList;

  PROCEDURE FMSP_IsBelongToSELRule(pIn_nDetailNodeID in number,
                                   pIn_nType         in number) IS
    --*****************************************************************
    -- Description:  it suppose for check relation SELECTION node id
    --
    -- Parameters:
    -- pIn_nDetailNodeID detail node
    -- pIn_nType PVTID_CHANGE_TYPE TYPE IS NEW_PVTID OR OLD_PVTID
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSql     clob;
      bFlag    boolean := false;
      nIndex   number;
      nSqlCode number;
      sCursor  sys_refcursor;
      nSelAddr number;
    begin
      -- Call lsang's procedure
      FMP_Update_Node_OBJ.FMSP_FindSelection(pIn_nDetailNodeID => pIn_nDetailNodeID,
                                             pOut_nSqlCode     => nSqlCode);
      -- select from tmp table
      open sCursor for
        select * from tmp_SEL_ADDR_TODETAIL;
      loop
        fetch sCursor
          into nSelAddr;
        exit when sCursor%notfound;
        FMSP_IntoBelongsSELIDList(nSelAddr, pIn_nType);
      end loop;
      -- open
      -- find
      -- end
      close sCursor;
      null;
    end;
  End FMSP_IsBelongToSELRule;

  PROCEDURE FMSP_InitAggIDList IS
    --*****************************************************************
    -- Description:  it suppose for init aggIdList
    --
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSql    clob;
      sCursor sys_refcursor;
    begin
      cSql := 'select prv.prv_em_addr from prv';
      open sCursor for cSql;
      fetch sCursor bulk collect
        into aggIdList;
      close sCursor;
    end;
  End FMSP_InitAggIDList;

  PROCEDURE FMSP_IsBelongToAggRule(pIn_nDetailNodeID in number,
                                   pIn_nType         in number) IS
    --*****************************************************************
    -- Description:  it suppose for check relation aggregation node id
    --
    -- Parameters:
    -- pIn_nDetailNodeID detail node
    -- pIn_nType PVTID_CHANGE_TYPE TYPE IS NEW_PVTID OR OLD_PVTID
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSql     clob;
      bFlag    boolean := false;
      nIndex   number;
      nSqlCode number;
    begin
      for nIndex in 1 .. aggIdList.count loop
        bFlag := false;
        FMP_CreateAggNode.FMSP_IsBelongToAggRule(pIn_nDetailNodeID => pIn_nDetailNodeID,
                                                 pIn_nAggRuleID    => to_number(aggIdList(nIndex)),
                                                 pOut_bIsBelong    => bFlag,
                                                 pOut_nSqlCode     => nSqlCode);
        if bFlag then
          FMSP_IntoBelongsAGGIDList(pIn_vAggID => to_number(aggIdList(nIndex)),
                                    pIn_nType  => pIn_nType);
        end if;
      end loop;
    end;
  End FMSP_IsBelongToAggRule;

  PROCEDURE FMSP_UpdateSel(pIn_nType in number, pOut_nSqlCode OUT number) IS
    --*****************************************************************
    -- Description:  update SELECTION
    --
    -- Parameters:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nSqlCode number;
      nSelID   number;
      --cur
      cursor cur_tmpsel is
        select s.sel_em_addr id from sel s where s.sel_bud = 0;
    begin
      --rebuild selection
      if pIn_nType = GC_GROUPTSIDS then
        for c in cur_tmpsel loop
          p_selection.SP_BuildSelection(p_SelectionID => c.id,
                                        p_SqlCode     => pOut_nSqlCode);
        end loop;
      elsif pIn_nType = GC_ONEDETAILTSID then
        for nIndex in 1 .. OldBelongsSelIDList.count loop
          --old sel
          nSelID := OldBelongsSELIDList(nIndex);
          p_selection.SP_BuildSelection(p_SelectionID => nSelID,
                                        p_SqlCode     => nSqlCode);
        end loop;
      end if;
      /*for nIndex in 1 .. NewBelongsSelIDList.count loop
        --new sel
        nSelID := NewBelongsSELIDList(nIndex);
        p_selection.SP_BuildSelection(p_SelectionID => nSelID,
                                      p_SqlCode     => nSqlCode);
      end loop;*/
    end;
  End FMSP_UpdateSel;

  PROCEDURE FMSP_UpdateAgg(pIn_TimeSeriesNoForUoM in varchar2,
                           pIn_nType              in number,
                           pIn_nSummarizeType     in number, -- 0 means call SummarizeAggregateNodes 1 means not
                           pIn_vUoMConfig         in varchar2,
                           pOut_nSqlCode          OUT NUMBER) IS
    --*****************************************************************
    -- Description:  it  suppose for update relation aggregation nodes
    --
    -- Parameters:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      nIndex   number;
      nSqlCode number;
      nAggID   number;
      -- cur
      cursor cur_tmpagg is
        select p.prv_em_addr id from prv p;
    begin
      if pIn_nType = GC_GROUPTSIDS then
        --rebuild aggregation
        for c in cur_tmpagg loop
          p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => c.id,
                                                   pIn_vObjId           => null,
                                                   pIn_nType            => 1,
                                                   pIn_nObjType         => null,
                                                   pOut_nSqlCode        => pOut_nSqlCode);
          if pIn_nSummarizeType = 0 then
            p_summarize.SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => c.id,
                                                      p_TimeSeriesNo    => pIn_TimeSeriesNoForUoM,
                                                      p_UoMConfig       => pIn_vUoMConfig,
                                                      p_Config          => '0',
                                                      p_SqlCode         => pOut_nSqlCode);
          end if;
        end loop;
      elsif pIn_nType = GC_ONEDETAILTSID then
        for nIndex in 1 .. OldBelongsAGGIDList.count loop
          --old AGG
          nAGGID := OldBelongsAGGIDList(nIndex);
          p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => nAGGID,
                                                   pIn_vObjId           => null,
                                                   pIn_nType            => 1,
                                                   pIn_nObjType         => null,
                                                   pOut_nSqlCode        => pOut_nSqlCode);
          if pIn_nSummarizeType = 0 then
            p_summarize.SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => nAGGID,
                                                      p_TimeSeriesNo    => pIn_TimeSeriesNoForUoM,
                                                      p_UoMConfig       => pIn_vUoMConfig,
                                                      p_Config          => '0',
                                                      p_SqlCode         => pOut_nSqlCode);
          end if;
        end loop;
      end if;
    end;
  End FMSP_UpdateAgg;

  PROCEDURE FMSP_UPDATEDETAILNODE(pIn_vOldKeyP  in varchar2,
                                  pIn_vOldKeyS  in varchar2,
                                  pIn_vOldKeyT  in varchar2,
                                  pIn_vNewKeyP  in varchar2,
                                  pIn_vNewKeyS  in varchar2,
                                  pIn_vNewKeyT  in varchar2,
                                  pIn_nType     in number,
                                  pOut_nSqlCode out number) IS
    --*****************************************************************
    -- Description:  it support for  update detail node
    --
    -- Parameters:
    -- NAME           IN OR OUT        DESCRIPTION
    -- pIn_vOldKeyP   IN it is old  product key . VALUES IS  product key or NULL
    -- pIn_vOldKeyS   IN it is old sales territory key . VALUES IS product key or NULL
    -- pIn_vOldKeyT   IN it is old trade channel key . VALUES IS product key or NULL
    -- pIn_vNewKeyP   IN it is new product key . VALUES IS product key or NULL
    -- pIn_vNewKeyS   IN it is new sales territory key . VALUES IS product key or NULL
    -- pIn_vNewKeyT   IN it is new trade channel key . VALUES IS product key or NULL
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    Declare
      cSql             clob;
      vSql             varchar2(4000);
      sCursor          sys_refcursor;
      sDimensionCursor sys_refcursor;
      nLength          number;
      nLength1         number;
      type selResultType is record(
        sel_em_addr sel.sel_em_addr%Type,
        sel_bud     sel.sel_bud%Type);
      TYPE sSelResultTypeList IS TABLE OF selResultType INDEX BY BINARY_INTEGER;
      sSelResultList sSelResultTypeList;
      type pvtResultType is record(
        pvt_cle      pvt.pvt_cle%Type,
        pvt_em_addr  pvt.pvt_em_addr%Type,
        FAM4_EM_ADDR pvt.fam4_em_addr%Type,
        GEO5_EM_ADDR pvt.geo5_em_addr%Type,
        DIS6_EM_ADDR pvt.dis6_em_addr%Type);
      pPvtResult   pvtResultType;
      new_f_cle    FAM.F_CLE%Type := NULL;
      new_g_cle    GEO.G_CLE%Type := NULL;
      new_d_cle    DIS.D_CLE%Type := NULL;
      new_f_desc   FAM.f_Desc%Type := NULL;
      new_g_desc   GEO.g_Desc%Type := NULL;
      new_d_desc   DIS.d_Desc%Type := NULL;
      old_f_cle    FAM.F_CLE%Type := NULL;
      old_g_cle    GEO.G_CLE%Type := NULL;
      old_d_cle    DIS.D_CLE%Type := NULL;
      old_f_desc   FAM.f_Desc%Type := NULL;
      old_g_desc   GEO.g_Desc%Type := NULL;
      old_d_desc   DIS.d_Desc%Type := NULL;
      new_pvt_cle  PVT.Pvt_Cle%Type;
      new_pvt_desc PVT.Pvt_Cle%Type;
      nIndex       number;
      vSqlCode     varchar2(200);
      bUpdateFlag  boolean := true;
    Begin
      /* if pIn_nType=MARK_ONEDETAILTSID then
        return ;
      end if;*/
      if pIn_vNewKeyP <> 1 then
        select fam.f_cle, fam.f_desc
          into new_f_cle, new_f_desc
          from fam
         where fam.fam_em_addr = pIn_vNewKeyP;
      end if;
      if pIn_vNewKeyS <> 1 then
        select geo.g_cle, geo.g_desc
          into new_g_cle, new_g_desc
          from geo
         where geo.geo_em_addr = pIn_vNewKeyS;
      end if;
      if pIn_vNewKeyT <> 1 then
        select dis.d_cle, dis.d_desc
          into new_d_cle, new_d_desc
          from dis
         where dis.dis_em_addr = pIn_vNewKeyT;
      end if;
      cSql    := 'SELECT pvt_cle,pvt_em_addr,FAM4_EM_ADDR,GEO5_EM_ADDR,DIS6_EM_ADDR FROM PVT WHERE ';
      nLength := length(cSql);
      IF pIn_vOldKeyP <> 1 THEN
        cSql := cSql ||
                ' FAM4_EM_ADDR IN (SELECT FAM_EM_ADDR
                                                 FROM FAM F
                                                      START WITH FAM_EM_ADDR = ''' ||
                pIn_vOldKeyP || '''
                                                      CONNECT BY PRIOR FAM_EM_ADDR = FAM0_EM_ADDR)';
      ELSIF pIn_nType = GC_ONEDETAILTSID THEN
        cSql := cSql || '( FAM4_EM_ADDR =1 )';
      END IF;
      IF pIn_vOldKeyS <> 1 THEN
        if length(cSql) > nLength then
          cSql := cSql || ' AND ';
        end if;
        cSql := cSql ||
                ' GEO5_EM_ADDR IN (SELECT GEO_EM_ADDR
                                                 FROM GEO
                                                      START WITH GEO_EM_ADDR = ''' ||
                pIn_vOldKeyS || '''
                                                      CONNECT BY PRIOR GEO_EM_ADDR = GEO1_EM_ADDR)';
      ELSIF pIn_nType = GC_ONEDETAILTSID THEN
        if length(cSql) > nLength then
          cSql := cSql || ' AND ';
        end if;
        cSql := cSql || '( GEO5_EM_ADDR =1 )';
      END IF;
      IF pIn_vOldKeyT <> 1 THEN
        if length(cSql) > nLength then
          cSql := cSql || ' AND ';
        end if;
        cSql := cSql ||
                ' DIS6_EM_ADDR IN (SELECT DIS_EM_ADDR
                                                      FROM DIS
                                                       START WITH DIS_EM_ADDR = ''' ||
                pIn_vOldKeyT || '''
                                                             CONNECT BY PRIOR DIS_EM_ADDR = DIS2_EM_ADDR)';
      ELSIF pIn_nType = GC_ONEDETAILTSID THEN
        if length(cSql) > nLength then
          cSql := cSql || ' AND ';
        end if;
        cSql := cSql || '( DIS6_EM_ADDR =1 )';
      END IF;
      vSql := cSql;
      IF pIn_vOldKeyP = 1 AND pIn_vOldKeyS = 1 AND pIn_vOldKeyT = 1 THEN
        cSql := 'SELECT pvt_cle,pvt_em_addr,FAM4_EM_ADDR,GEO5_EM_ADDR,DIS6_EM_ADDR FROM PVT ';
      END IF;
      open sCursor for cSql;
      vSql := cSql;
      loop
        fetch sCursor
          into pPvtResult;
        exit when sCursor%notfound;
        old_f_cle  := null;
        old_g_cle  := null;
        old_d_cle  := null;
        old_f_desc := null;
        old_g_desc := null;
        old_d_desc := null;
        if pPvtResult.FAM4_EM_ADDR <> 1 then
          select fam.f_cle, fam.f_desc
            into old_f_cle, old_f_desc
            from fam
           where fam.fam_em_addr = pPvtResult.FAM4_EM_ADDR;
        end if;
        if pPvtResult.GEO5_EM_ADDR <> 1 then
          select geo.g_cle, geo.g_desc
            into old_g_cle, old_g_desc
            from geo
           where geo.geo_em_addr = pPvtResult.GEO5_EM_ADDR;
        end if;
        if pPvtResult.DIS6_EM_ADDR <> 1 then
          select dis.d_cle, dis.d_desc
            into old_d_cle, old_d_desc
            from dis
           where dis.dis_em_addr = pPvtResult.DIS6_EM_ADDR;
        end if;
        /*        -- get aggid for old_pvtID
        FMSP_IsBelongToAggRule(pIn_nDetailNodeID => to_number(pPvtResult.pvt_em_addr),
                               pIn_nType         => OLD_PVTID);*/
        -- get selid for old_pvtID
        if pIn_nType = GC_ONEDETAILTSID then
          FMSP_IsBelongToSELRule(pIn_nDetailNodeID => to_number(pPvtResult.pvt_em_addr),
                                 pIn_nType         => GC_OLD_PVTID);
          -- get aggid for old_pvtID
          FMSP_IsBelongToAggRule(pIn_nDetailNodeID => to_number(pPvtResult.pvt_em_addr),
                                 pIn_nType         => GC_OLD_PVTID);
        end if;
        -- update action is  successful or failed
        -- default is successful
        bUpdateFlag := true;
      
        if new_f_cle is not null and new_g_cle is not null and
           new_d_cle is not null then
          --111
          new_pvt_cle  := new_f_cle || '-' || new_g_cle || '-' || new_d_cle;
          new_pvt_desc := new_f_desc || '-' || new_g_desc || '-' ||
                          new_d_desc;
          if length(new_pvt_desc) <= 2 then
            new_pvt_desc := null;
          end if;
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is null and new_g_cle is not null and
              new_d_cle is not null then
          --011
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_f_cle is not null then
                  new_pvt_cle  := old_f_cle || '-' || new_g_cle || '-' ||
                                  new_d_cle;
                  new_pvt_desc := old_f_desc || '-' || new_g_desc || '-' ||
                                  new_d_desc;
                else
                  new_pvt_cle  := new_g_cle || '-' || new_d_cle;
                  new_pvt_desc := new_g_desc || '-' || new_d_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_g_cle || '-' || new_d_cle;
                new_pvt_desc := new_g_desc || '-' || new_d_desc;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is not null and new_g_cle is not null and
              new_d_cle is null then
          --110
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_d_cle is not null then
                  new_pvt_cle  := new_f_cle || '-' || new_g_cle || '-' ||
                                  old_d_cle;
                  new_pvt_desc := new_f_desc || '-' || new_g_desc || '-' ||
                                  old_d_desc;
                else
                  new_pvt_cle  := new_f_cle || '-' || new_g_cle;
                  new_pvt_desc := new_f_desc || '-' || new_g_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_f_cle || '-' || new_g_cle;
                new_pvt_desc := new_f_desc || '-' || new_g_desc;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is not null and new_g_cle is null and
              new_d_cle is not null then
          --101
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_g_cle is not null then
                  new_pvt_cle  := new_f_cle || '-' || old_g_cle || '-' ||
                                  new_d_cle;
                  new_pvt_desc := new_f_desc || '-' || old_g_desc || '-' ||
                                  new_d_desc;
                else
                  new_pvt_cle  := new_f_cle || '-' || new_d_cle;
                  new_pvt_desc := new_f_desc || '-' || new_d_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_f_cle || '-' || new_d_cle;
                new_pvt_desc := new_f_desc || '-' || new_d_desc;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
                null;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is not null and new_g_cle is null and
              new_d_cle is null then
          --100
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_g_cle is not null and old_d_cle is not null then
                  new_pvt_cle  := new_f_cle || '-' || old_g_cle || '-' ||
                                  old_d_cle;
                  new_pvt_desc := new_f_desc || '-' || old_g_desc || '-' ||
                                  old_d_desc;
                elsif old_g_cle is not null and old_d_cle is null then
                  new_pvt_cle  := new_f_cle || '-' || old_g_cle;
                  new_pvt_desc := new_f_desc || '-' || old_g_desc;
                elsif old_g_cle is null and old_d_cle is not null then
                  new_pvt_cle  := new_f_cle || '-' || old_d_cle;
                  new_pvt_desc := new_f_desc || '-' || old_d_desc;
                elsif old_g_cle is null and old_d_cle is null then
                  new_pvt_cle  := new_f_cle;
                  new_pvt_desc := new_f_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.fam4_em_addr = pIn_vNewKeyP
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_f_cle;
                new_pvt_desc := new_f_desc;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
                null;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is null and new_g_cle is not null and
              new_d_cle is null then
          --010
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_f_cle is not null and old_d_cle is not null then
                  new_pvt_cle  := old_f_cle || '-' || new_g_cle || '-' ||
                                  old_d_cle;
                  new_pvt_desc := old_f_desc || '-' || new_g_desc || '-' ||
                                  old_d_desc;
                elsif old_f_cle is not null and old_d_cle is null then
                  new_pvt_cle  := old_f_cle || '-' || new_g_cle;
                  new_pvt_desc := old_f_desc || '-' || new_g_desc;
                elsif old_f_cle is null and old_d_cle is not null then
                  new_pvt_cle  := old_f_cle || new_d_cle;
                  new_pvt_desc := old_f_desc || new_d_desc;
                elsif old_f_cle is null and old_d_cle is null then
                  new_pvt_cle  := new_g_cle;
                  new_pvt_desc := new_g_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.geo5_em_addr = pIn_vNewKeyS
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_g_cle;
                new_pvt_desc := new_g_desc;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        elsif new_f_cle is null and new_g_cle is null and
              new_d_cle is not null then
          --001
          Begin
            begin
              if pIn_nType = GC_GROUPTSIDS then
                if old_f_cle is not null and old_g_cle is not null then
                  new_pvt_cle  := old_f_cle || '-' || old_g_cle || '-' ||
                                  new_d_cle;
                  new_pvt_desc := old_f_desc || '-' || old_g_desc || '-' ||
                                  new_d_desc;
                elsif old_f_cle is not null and old_g_cle is null then
                  new_pvt_cle  := old_f_cle || '-' || new_d_cle;
                  new_pvt_desc := old_f_desc || '-' || new_d_desc;
                elsif old_f_cle is null and old_g_cle is not null then
                  new_pvt_cle  := old_g_cle || new_d_cle;
                  new_pvt_desc := old_g_desc || new_d_desc;
                elsif old_f_cle is null and old_g_cle is null then
                  new_pvt_cle  := new_d_cle;
                  new_pvt_desc := new_d_desc;
                end if;
                if length(new_pvt_desc) <= 2 then
                  new_pvt_desc := null;
                end if;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              elsif pIn_nType = GC_ONEDETAILTSID then
                new_pvt_cle  := new_d_cle;
                new_pvt_desc := new_d_desc;
                update pvt
                   set pvt.pvt_cle      = new_pvt_cle,
                       pvt.pvt_desc     = new_pvt_desc,
                       pvt.adr_pro      = pIn_vNewKeyP,
                       pvt.adr_geo      = pIn_vNewKeyS,
                       pvt.adr_dis      = pIn_vNewKeyT,
                       pvt.fam4_em_addr = pIn_vNewKeyP,
                       pvt.geo5_em_addr = pIn_vNewKeyS,
                       pvt.dis6_em_addr = pIn_vNewKeyT
                 where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
              end if;
            end;
          Exception
            when Dup_val_on_index then
              bUpdateFlag := false;
          End;
        end if;
        if bUpdateFlag = false then
          delete from pvt where pvt.pvt_em_addr = pPvtResult.pvt_em_addr;
          delete from bdg
           where bdg.b_cle = pPvtResult.pvt_cle
             and bdg.id_bdg = 80;
        else
          -- get selid for new_pvtid  but also use old_pvtid arrary
          if pIn_nType = GC_ONEDETAILTSID then
            FMSP_IsBelongToSELRule(pIn_nDetailNodeID => to_number(pPvtResult.pvt_em_addr),
                                   pIn_nType         => GC_OLD_PVTID);
            -- get aggid for old_pvtID but also use old_pvtid arrary
            FMSP_IsBelongToAggRule(pIn_nDetailNodeID => to_number(pPvtResult.pvt_em_addr),
                                   pIn_nType         => GC_OLD_PVTID);
          end if;
          -- update BDG
          update bdg
             set bdg.b_cle = new_pvt_cle, bdg.bdg_desc = new_pvt_desc
           where bdg.b_cle = pPvtResult.pvt_cle
             and bdg.id_bdg = 80;
        end if;
        -- delete table
        FMSP_DeleteTables(pIn_nPVTID      => pPvtResult.pvt_em_addr,
                          pIn_nDelBDGType => 1,
                          pOut_nSqlCode   => pOut_nSqlCode);
      end loop;
      close sCursor;
    
    End;
  END FMSP_UPDATEDETAILNODE;

  PROCEDURE FMISP_ModifyPvtKeys(pIn_vOldKeyP           in varchar2,
                                pIn_vOldKeyS           in varchar2,
                                pIn_vOldKeyT           in varchar2,
                                pIn_vNewKeyP           in varchar2,
                                pIn_vNewKeyS           in varchar2,
                                pIn_vNewKeyT           in varchar2,
                                pIn_nType              in number,
                                pIn_TimeSeriesNoForUoM in varchar2,
                                pIn_vUoMConfig         in varchar2,
                                pOut_nSqlCode          out number) IS
    --*****************************************************************
    -- Description:  it support for  modify the keys of a group of time series
    --
    -- Parameters:
    -- NAME           IN OR OUT        DESCRIPTION
    -- pIn_vOldKeyP   IN it is old  product key . VALUES IS  product key or NULL
    -- pIn_vOldKeyS   IN it is old sales territory key . VALUES IS product key or NULL
    -- pIn_vOldKeyT   IN it is old trade channel key . VALUES IS product key or NULL
    -- pIn_vNewKeyP   IN it is new product key . VALUES IS product key or NULL
    -- pIn_vNewKeyS   IN it is new sales territory key . VALUES IS product key or NULL
    -- pIn_vNewKeyT   IN it is new trade channel key . VALUES IS product key or NULL
    -- pIn_nType      IN it is a mark for a detail tsid or a group tsids  o means a detail tsid. 1 means a group tsids
    --
    -- pOut_nSqlCode  OUT it marks successful or fail . 0 means successful others means fail.
  
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-Mar-2013     lei zhang     Created.
    -- **************************************************************
  BEGIN
    Fmp_Log.FMP_SetValue(pIn_vOldKeyP);
    Fmp_Log.FMP_SetValue(pIn_vOldKeyS);
    Fmp_Log.FMP_SetValue(pIn_vOldKeyT);
    Fmp_Log.FMP_SetValue(pIn_vNewKeyP);
    Fmp_Log.FMP_SetValue(pIn_vNewKeyS);
    Fmp_Log.FMP_SetValue(pIn_vNewKeyT);
    Fmp_Log.FMP_SetValue(pIn_nType);
    Fmp_Log.FMP_SetValue(pIn_TimeSeriesNoForUoM);
    Fmp_Log.FMP_SetValue(pIn_vUoMConfig);
    Fmp_Log.LOGBEGIN;
    IF (pIn_vOldKeyP <> 0 AND pIn_vOldKeyS <> 0 AND pIn_vOldKeyT <> 0 AND
       pIn_vOldKeyP = pIn_vNewKeyP AND pIn_vOldKeyS = pIn_vNewKeyS AND
       pIn_vOldKeyT = pIn_vNewKeyT) OR
       (pIn_vOldKeyP = 0 AND pIn_vOldKeyS = 0 AND pIn_vOldKeyT = 0) OR
       (pIn_vNewKeyP = 0 AND pIn_vNewKeyS = 0 AND pIn_vNewKeyT = 0) THEN
      -- IF All OldKeys = All NewKeys OR ALL OldKeys Is NULL OR ALL NewKeys Is NULL THEN RETURE
      pOut_nSqlCode := 0;
      return;
    END IF;
    if pIn_nType = GC_ONEDETAILTSID then
      -- Init AGGLIST
      FMSP_InitAggIDList;
    end if;
    -- Update Detail Node
    FMSP_UPDATEDETAILNODE(pIn_vOldKeyP  => pIn_vOldKeyP,
                          pIn_vOldKeyS  => pIn_vOldKeyS,
                          pIn_vOldKeyT  => pIn_vOldKeyT,
                          pIn_vNewKeyP  => pIn_vNewKeyP,
                          pIn_vNewKeyS  => pIn_vNewKeyS,
                          pIn_vNewKeyT  => pIn_vNewKeyT,
                          pIn_nType     => pIn_nType,
                          pOut_nSqlCode => pOut_nSqlCode);
    --Update Seletion
    FMSP_UpdateSel(pIn_nType => pIn_nType, pOut_nSqlCode => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
    -- Update AGG
    FMSP_UpdateAgg(pIn_TimeSeriesNoForUoM => pIn_TimeSeriesNoForUoM,
                   pIn_nType              => pIn_nType,
                   pIn_nSummarizeType     => 0,
                   pIn_vUoMConfig         => pIn_vUoMConfig,
                   pOut_nSqlCode          => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
    pOut_nSqlCode := 0;
    Fmp_Log.LOGEND;
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := sqlCode;
      Fmp_log.LOGERROR;
  END FMISP_ModifyPvtKeys;

  procedure FMSP_GetDetailNodeSqlClause(pIn_vSqlClauseForDetailNode out varchar2,
                                        pIn_nFlag                   in number,
                                        pOut_nSqlCode               out number) as
    --insert conditions of the selection into tmp_cdt.
    --if no condition of product ,select all products
    --if no record by conditon of products.return.
    v_P_CondtionCount  number := 0; --condition count of product
    v_ST_CondtionCount number := 0; --condition count of sale territory
    v_TC_CondtionCount number := 0; --condition count of trade channel
  
    v_Ope      number; --operation 1 equal;2 not equal;3 between
    v_Sql_Part varchar2(2000); --
  
    v_Sql varchar2(4000); --part of sql for 3D.may 1D,2D or null
  
    v_Sql_BetweenPart varchar2(400); --part of sql for creating the KEY scope when meeting 'between' operation
    v_F_Begin         VARCHAR2(60);
    v_F_End           VARCHAR2(60);
  
    --v_Flag   number := 0;
    v_AttrID number;
  
    --v_DataConditionCount number := 0;
    v_Datascounts number := 0;
    v_BoM_addr    number;
    v_BoM_count   number;
  
    v_sql_p            varchar2(4000) := ' select p.f_cle,p.F_DESC,p.fam_em_addr  from v_productattrvalue p ';
    v_sql_p_cle        varchar2(4000) := ' select p.fam_em_addr  from v_productattrvalue p ';
    v_sql_P_where      varchar2(4000) := ' where p.isleaf=1 and p.id_fam=80 ';
    v_sql_P_connectby  varchar2(4000);
    v_sql_st           varchar2(4000) := ' select s.g_cle,s.G_DESC,s.geo_em_addr  from v_saleterritoryattrvalue s ';
    v_sql_st_cle       varchar2(4000) := ' select s.geo_em_addr  from v_saleterritoryattrvalue s ';
    v_sql_st_where     varchar2(4000) := ' where s.isleaf=1 ';
    v_sql_st_connectby varchar2(4000);
    v_sql_tc           varchar2(4000) := ' select t.d_cle,t.D_DESC,t.dis_em_addr  from v_tradechannelattrvalue  t ';
    v_sql_tc_cle       varchar2(4000) := ' select t.dis_em_addr  from v_tradechannelattrvalue  t ';
    v_sql_tc_where     varchar2(4000) := '  where t.isleaf=1 ';
    v_sql_tc_connectby varchar2(4000);
  begin
    pOut_nSqlCode := 0;
  
    if pIn_nFlag != 0 then
      v_sql_p  := v_sql_p_cle;
      v_sql_st := v_sql_st_cle;
      v_sql_tc := v_sql_tc_cle;
    end if;
    --_Sql := ' and 1=2 '; --no data found
    --clear tmp table
    /*    delete from  tmp_product;
    delete from  tmp_sales_territory;
    delete from tmp_trade_channel;
    delete from tmp_detail_node;*/
  
    --
    fmp_getdetailnodesql.FMSP_GetDetailNodeSql(pInOut_vProduct      => v_sql_p,
                                               pInOut_vProductWhere => v_sql_P_where,
                                               pInOut_vSaleT        => v_sql_st,
                                               pInOut_vSaleTWhere   => v_sql_st_where,
                                               pInOut_vTradeC       => v_sql_tc,
                                               pInOut_vTradeCWhere  => v_sql_tc_where,
                                               pInOut_nProductCount => v_P_CondtionCount,
                                               pInOut_nSalesCount   => v_ST_CondtionCount,
                                               pInOut_nTradeCount   => v_TC_CondtionCount,
                                               pOut_nSqlCode        => pOut_nSqlCode);
    --
  
    if pIn_nFlag = 0 then
    
      if v_P_CondtionCount > 0 then
        v_Sql := ' ( ' || v_sql_p || ' ), ';
      elsif v_P_CondtionCount = 0 then
        v_Sql := '(select p.f_cle,p.F_DESC,p.fam_em_addr
                            from v_productattrvalue p
                           where p.isleaf = 1
                             and p.id_fam = 80),';
      end if;
      if v_ST_CondtionCount > 0 then
        v_Sql := v_Sql || ' (' || v_sql_st || '), ';
      elsif v_ST_CondtionCount = 0 then
        v_Sql := v_Sql || '(select s.g_cle,s.G_DESC,s.geo_em_addr
          from v_saleterritoryattrvalue s
         where s.isleaf = 1),';
      end if;
      if v_TC_CondtionCount > 0 then
        v_Sql := v_Sql || ' (' || v_sql_tc || ') ';
      elsif v_TC_CondtionCount = 0 then
        v_Sql := v_Sql ||
                 ' (select t.d_cle,t.D_DESC,t.dis_em_addr from v_tradechannelattrvalue t  where t.isleaf=1) ';
      end if;
    else
      if v_P_CondtionCount > 0 then
        v_Sql := ' and p.fam4_em_addr in ( ' || v_sql_p || ' ) ';
      end if;
      if v_ST_CondtionCount > 0 then
        v_Sql := v_Sql || ' and p.geo5_em_addr in (' || v_sql_st || ') ';
      end if;
      if v_TC_CondtionCount > 0 then
        v_Sql := v_Sql || ' and p.dis6_em_addr in (' || v_sql_tc || ') ';
      end if;
    end if;
    /* if v_N_CondtionCount > 0 then
      v_Sql := v_Sql || ' and p.pvt_em_addr in (' || v_sql_data || ') ';
    end if;*/
  
    pIn_vSqlClauseForDetailNode := v_Sql;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
  end FMSP_GetDetailNodeSqlClause;

  PROCEDURE FMISP_CreateGroupPvts(pIn_vConditions IN VARCHAR2,
                                  pIn_vUserName   IN VARCHAR2,
                                  pOut_nSqlCode   OUT NUMBER) IS
    vFromClause VARCHAR2(4000);
    vSQL        VARCHAR2(4000);
    vExecuteSql varchar2(4000);
  
    vStrSql varchar2(4000);
    ncnt    number;
  BEGIN
    --add log
    Fmp_Log.FMP_SetValue(pIn_vConditions);
    Fmp_Log.FMP_SetValue(pIn_vUserName);
    Fmp_Log.LOGBEGIN;
    --
    delete from tmp_cdt;
    if pIn_vConditions is not null then
      sp_getcdtbyconditions(P_Conditions => pIn_vConditions,
                            p_SqlCode    => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    end if;
    FMSP_GetDetailNodeSqlClause(pIn_vSqlClauseForDetailNode => vFromClause,
                                pIn_nFlag                   => 0,
                                pOut_nSqlCode               => pOut_nSqlCode);
    if pOut_nSqlCode <> 0 then
      return;
    end if;
    delete from tmp_tpvt;
    --replace chr(1) to null
    VSQL := 'select replace(f_cle || decode(g_cle, chr(1), '''', ''-'') || g_cle ||
              decode(d_cle, chr(1), '''', ''-'') || d_cle,chr(1),null) pvt_cle,
              substr(f_desc || decode(g_desc, null, '''', ''-'') || g_desc ||
                     decode(d_desc, null, '''', ''-'') || d_desc,
                     0,120) pvt_desc,
fam_em_addr adr_pro,geo_em_addr adr_geo,dis_em_addr adr_dis,''' ||
            pIn_vUserName || ''' user_create_pvt ,
F_ConvertDateToOleDateTime(sysdate) date_create_pvt
  from ' || vFromClause;
  
    vExecuteSql := 'INSERT  /*+ PARALLEL */ INTO tmp_tpvt(pvt_em_addr,pvt_cle,pvt_desc,adr_pro,adr_geo,adr_dis,user_create_pvt,
    date_create_pvt,fam4_em_addr,geo5_em_addr,dis6_em_addr)
    select seq_pvt.nextval,v.pvt_cle,v.pvt_desc,v.adr_pro,v.adr_geo,v.adr_dis,v.user_create_pvt,
    v.date_create_pvt,v.adr_pro,v.adr_geo,v.adr_dis
  from pvt p,(' || VSQL || '
  ) v
  where p.pvt_cle(+) = v.pvt_cle
  and p.pvt_cle is null';
    fmsp_execsql(vExecuteSql);
  
    --insert into pvt
    MERGE /*+ PARALLEL */
    INTO pvt P
    USING (select * from tmp_tpvt) v
    on (p.pvt_cle = v.pvt_cle)
    when not matched then
      insert
        (pvt_em_addr,
         pvt_cle,
         pvt_desc,
         adr_pro,
         adr_geo,
         adr_dis,
         user_create_pvt,
         date_create_pvt,
         fam4_em_addr,
         geo5_em_addr,
         dis6_em_addr)
      values
        (v.pvt_em_addr,
         v.pvt_cle,
         v.pvt_desc,
         v.adr_pro,
         v.adr_geo,
         v.adr_dis,
         v.user_create_pvt,
         v.date_create_pvt,
         v.adr_pro,
         v.adr_geo,
         v.adr_dis);
  
    --insert into bdg
    merge /*+ PARALLEL */
    into bdg b
    using (select 80 id_bdg, p.pvt_cle, p.pvt_desc from tmp_tpvt p) s
    on (b.id_bdg = s.id_bdg and b.b_cle = s.pvt_cle)
    when not matched then
      insert
        (bdg_em_addr, id_bdg, b_cle, bdg_desc)
      values
        (seq_bdg.nextval, s.id_bdg, s.pvt_cle, s.pvt_desc);
  
    --Update Seletion
    FMSP_UpdateSel(pIn_nType => GC_GROUPTSIDS, pOut_nSqlCode => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
    -- Update AGG
    FMSP_UpdateAgg(pIn_TimeSeriesNoForUoM => null,
                   pIn_nType              => GC_GROUPTSIDS,
                   pIn_nSummarizeType     => 1,
                   pIn_vUoMConfig         => null,
                   pOut_nSqlCode          => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
  
    Fmp_Log.LOGEND;
  
    pOut_nSqlCode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      FMP_LOG.LOGERROR;
      pOut_nSqlCode := sqlcode;
    
  END FMISP_CreateGroupPvts;

  PROCEDURE FMISP_AddPvtToSel(pIn_nPVTID    IN NUMBER,
                              pOut_nSqlCode OUT NUMBER) IS
    cursor cur_data is
      select sel_em_addr from sel s where s.sel_bud = 0;
    vSql    varchar2(4000);
    vStrSql varchar2(4000);
    cursor cur_sel is
      select t.id from tmp_tselaggid t where t.type = 'sel';
    cursor cur_agg is
      select t.id from tmp_tselaggid t where t.type = 'agg';
    cursor cur_aggregation is
      select p.prv_em_addr from prv p;
    bAggflg boolean;
  BEGIN
    --add log
    Fmp_Log.FMP_SetValue(pIn_nPVTID);
    Fmp_Log.LOGBEGIN;
    --rebuild selection
  
    delete from tmp_tselaggid;
    FMP_Update_Node_OBJ.FMSP_FindSelection(pIn_nDetailNodeID => pIn_nPVTID,
                                           pOut_nSqlCode     => pOut_nSqlCode);
    merge into tmp_tselaggid t
    using (select * from tmp_SEL_ADDR_TODETAIL) s
    on (t.id = s.sel_em_addr)
    when not matched then
      insert (type, id) values ('sel', s.sel_em_addr);
  
    for c in cur_sel loop
      p_selection.SP_BuildSelection(p_SelectionID => c.id,
                                    p_SqlCode     => pOut_nSqlCode);
    end loop;
  
    --rebuild agg
    for agg in cur_aggregation loop
      fmp_createaggnode.FMSP_IsBelongToAggRule(pIn_nDetailNodeID => pIn_nPVTID,
                                               pIn_nAggRuleID    => agg.prv_em_addr,
                                               pOut_bIsBelong    => bAggflg,
                                               pOut_nSqlCode     => pOut_nSqlCode);
      if bAggflg then
        insert into tmp_tselaggid values ('agg', agg.prv_em_addr);
      end if;
    
    end loop;
    for c in cur_agg loop
      p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => c.id,
                                               pIn_vObjId           => null,
                                               pIn_nType            => 1,
                                               pIn_nObjType         => null,
                                               pOut_nSqlCode        => pOut_nSqlCode);
    end loop;
  
    Fmp_Log.LOGEND;
  EXCEPTION
    WHEN OTHERS THEN
    
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := SQLCODE;
  END FMISP_AddPvtToSel;

  PROCEDURE FMISP_DeleteOnePvt(pIn_nPVTID             IN NUMBER,
                               pIn_TimeSeriesNoForUoM in varchar2,
                               pIn_vUoMConfig         in varchar2,
                               pOut_nSqlCode          OUT NUMBER) IS
    cursor cur_aggregation is
      select p.prv_em_addr from prv p;
    bAggflg boolean;
    cursor cur_sel is
      select distinct t.id from tmp_tselaggid t where t.type = 'sel';
    cursor cur_agg is
      select distinct t.id from tmp_tselaggid t where t.type = 'agg';
  BEGIN
    --add log
    Fmp_Log.FMP_SetValue(pIn_nPVTID);
    Fmp_Log.FMP_SetValue(pIn_TimeSeriesNoForUoM);
    Fmp_Log.FMP_SetValue(pIn_vUoMConfig);
    Fmp_Log.LOGBEGIN;
  
    FMSP_DeleteTables(pIn_nPVTID      => pIn_nPVTID,
                      pIn_nDelBDGType => 0,
                      pOut_nSqlCode   => pOut_nSqlCode);
  
    --rebuild selection
    delete from tmp_tselaggid;
    FMP_Update_Node_OBJ.FMSP_FindSelection(pIn_nDetailNodeID => pIn_nPVTID,
                                           pOut_nSqlCode     => pOut_nSqlCode);
    merge into tmp_tselaggid t
    using (select * from tmp_SEL_ADDR_TODETAIL) s
    on (t.id = s.sel_em_addr)
    when not matched then
      insert (type, id) values ('sel', s.sel_em_addr);
  
    --rebuild agg
    for agg in cur_aggregation loop
    
      fmp_createaggnode.FMSP_IsBelongToAggRule(pIn_nDetailNodeID => pIn_nPVTID,
                                               pIn_nAggRuleID    => agg.prv_em_addr,
                                               pOut_bIsBelong    => bAggflg,
                                               pOut_nSqlCode     => pOut_nSqlCode);
      if bAggflg then
        insert into tmp_tselaggid values ('agg', agg.prv_em_addr);
      end if;
    
    end loop;
    --delete pvt
    delete from pvt p where p.pvt_em_addr = pIn_nPVTID;
  
    for c in cur_sel loop
      p_selection.SP_BuildSelection(p_SelectionID => c.id,
                                    p_SqlCode     => pOut_nSqlCode);
    end loop;
  
    for c in cur_agg loop
      p_aggregation.FMSP_BuildAggregateRule_ID(pIn_nAggregateRuleID => c.id,
                                               pIn_vObjId           => null,
                                               pIn_nType            => 1,
                                               pIn_nObjType         => null,
                                               pOut_nSqlCode        => pOut_nSqlCode);
      --added 220130402
      p_summarize.SPPRV_SummarizeAggregateNodes(P_AggregateRuleID => c.id,
                                                p_TimeSeriesNo    => pIn_TimeSeriesNoForUoM,
                                                p_UoMConfig       => pIn_vUoMConfig,
                                                p_Config          => '0',
                                                p_SqlCode         => pOut_nSqlCode);
    end loop;
    --DELETE agg related begin
    --FMSP_DeleteAggRelated();
    --DELETE agg related end
  
    pOut_nSqlCode := 0;
  
    Fmp_Log.LOGEND;
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := SQLCODE;
      fmp_log.LOGERROR;
    
  END FMISP_DeleteOnePvt;

  PROCEDURE FMISP_DeleteGroupPvts(pIn_vConditions        IN VARCHAR2,
                                  pIn_TimeSeriesNoForUoM in varchar2,
                                  pIn_vUoMConfig         in varchar2,
                                  pOut_nSqlCode          OUT NUMBER) IS
    vFromClause VARCHAR2(4000);
    vExecuteSql VARCHAR2(4000);
  BEGIN
    --add log
    Fmp_Log.FMP_SetValue(pIn_vConditions);
    Fmp_Log.FMP_SetValue(pIn_TimeSeriesNoForUoM);
    Fmp_Log.FMP_SetValue(pIn_vUoMConfig);
    Fmp_Log.LOGBEGIN;
    --
    delete from tmp_cdt;
    if pIn_vConditions is not null then
      sp_getcdtbyconditions(P_Conditions => pIn_vConditions,
                            p_SqlCode    => pOut_nSqlCode);
      if pOut_nSqlCode <> 0 then
        return;
      end if;
    end if;
    --
    FMSP_GetDetailNodeSqlClause(pIn_vSqlClauseForDetailNode => vFromClause,
                                pIn_nFlag                   => 1,
                                pOut_nSqlCode               => pOut_nSqlCode);
    if pOut_nSqlCode <> 0 then
      return;
    end if;
  
    delete from tmp_tpvt;
  
    vExecuteSql := 'INSERT INTO tmp_tpvt(pvt_em_addr,pvt_cle,pvt_desc,adr_pro,adr_geo,adr_dis,user_create_pvt,
    date_create_pvt,fam4_em_addr,geo5_em_addr,dis6_em_addr)
     select p.pvt_em_addr,
         p.pvt_cle,
         p.pvt_desc,
         p.adr_pro,
         p.adr_geo,
         p.adr_dis,
         p.user_create_pvt,
         p.date_create_pvt,
         p.adr_pro,
         p.adr_geo,
         p.adr_dis
    from pvt p where 1=1 ' || vFromClause;
    EXECUTE IMMEDIATE vExecuteSql;
  
    --delete don_m
    DELETE from don_m m
     WHERE exists (select * from tmp_tpvt p where p.pvt_em_addr = m.pvtid);
    --delete don_w
    delete from don_w m
     where exists (select 1 from tmp_tpvt p where p.pvt_em_addr = m.pvtid);
    --delete pvtcrt
    DELETE FROM pvtcrt B
     WHERE exists
     (select 1 from tmp_tpvt p where p.pvt_em_addr = b.pvt35_em_addr);
    --delete modscl
    delete from modscl s
     where exists (select *
              from (select m.mod_em_addr
                      from bdg b, tmp_tpvt p, mod m
                     WHERE b.b_cle = p.pvt_cle
                       and b.bdg_em_addr = m.bdg30_em_addr
                       and b.id_bdg = 80) b
             where s.mod42_em_addr = b.mod_em_addr);
    --delete mod
    delete from mod m
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where m.bdg30_em_addr = p.bdg_em_addr);
    --delete bud
    delete from bud b
     where exists (select *
              from (select c.bgc_em_addr
                      from bdg b, tmp_tpvt p, bgc c
                     WHERE b.b_cle = p.pvt_cle
                       and b.bdg_em_addr = c.bdg31_em_addr
                       and b.id_bdg = 80) p
             where b.bgc32_em_addr = p.bgc_em_addr);
    --delete bgc
    delete from bgc e
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where e.bdg31_em_addr = p.bdg_em_addr);
    --delete supplier
    delete from supplier s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where s.bdg51_em_addr = p.bdg_em_addr);
    --delete serinote
    delete from serinote s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where s.bdg3_em_addr = p.bdg_em_addr);
    --delete typenode
    delete from typenote t
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where t.bdg47_em_addr = p.bdg_em_addr);
    --delete select_sel
    delete from select_sel s
     where exists (select *
              from (select b.bdg_em_addr
                      from bdg b, tmp_tpvt p
                     WHERE b.b_cle = p.pvt_cle
                       and b.id_bdg = 80) p
             where s.bdg52_em_addr = p.bdg_em_addr);
  
    --delete bdg
    DELETE FROM BDG B
     WHERE /*b.id_bdg = 80 and */
     exists (select 1 from tmp_tpvt p where p.pvt_cle = b.b_cle);
  
    --delete pvt
    delete from pvt p
     where exists
     (select * from tmp_tpvt t where t.pvt_em_addr = p.pvt_em_addr);
  
    --Update Seletion
    FMSP_UpdateSel(pIn_nType => GC_GROUPTSIDS, pOut_nSqlCode => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
    -- Update AGG
    FMSP_UpdateAgg(pIn_TimeSeriesNoForUoM => pIn_TimeSeriesNoForUoM,
                   pIn_nType              => GC_GROUPTSIDS,
                   pIn_nSummarizeType     => 0,
                   pIn_vUoMConfig         => pIn_vUoMConfig,
                   pOut_nSqlCode          => pOut_nSqlCode);
    if 0 <> pOut_nSqlCode then
      return;
    end if;
  
    --DELETE agg related begin
    --FMSP_DeleteAggRelated();
    --DELETE agg related end
  
    Fmp_Log.LOGEND;
  
  EXCEPTION
    WHEN OTHERS THEN
      fmp_log.LOGERROR;
      pOut_nSqlCode := sqlcode;
    
  END FMISP_DeleteGroupPvts;

END FMIP_DETAILLNODESMANAGE;
/
