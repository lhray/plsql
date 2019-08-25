create or replace package fmp_getdetailnodesql is
  PROCEDURE FMSP_GetDetailNodeSql(pInOut_vProduct      in out varchar2,
                                  pInOut_vProductWhere in out varchar2,
                                  pInOut_vSaleT        in out varchar2,
                                  pInOut_vSaleTWhere   in out varchar2,
                                  pInOut_vTradeC       in out varchar2,
                                  pInOut_vTradeCWhere  in out varchar2,
                                  pInOut_nProductCount in out number,
                                  pInOut_nSalesCount   in out number,
                                  pInOut_nTradeCount   in out number,
                                  pOut_nSqlCode        out number);

  PROCEDURE FMSP_GetProductSQLBySelection(pIn_nSelectionID   in number,
                                          pIn_bIsGroup       in boolean default false,
                                          pOut_cSQL          out clob,
                                          pOut_nConditionCnt out number,
                                          pOut_nSqlCode      out number);

  PROCEDURE FMSP_GetSaleSQLBySelection(pIn_nSelectionID   in number,
                                       pOut_cSQL          out clob,
                                       pOut_nConditionCnt out number,
                                       pOut_nSqlCode      out number);

  PROCEDURE FMSP_GetTradeSQLBySelection(pIn_nSelectionID   in number,
                                        pOut_cSQL          out clob,
                                        pOut_nConditionCnt out number,
                                        pOut_nSqlCode      out number);

  PROCEDURE FMSP_GetDetailSQLBySelection(pIn_nSelectionID   in number,
                                         pOut_cSQL          out clob,
                                         pOut_nConditionCnt out number,
                                         pOut_nSqlCode      out number);
end fmp_getdetailnodesql;
/
create or replace package body fmp_getdetailnodesql is

  PROCEDURE FMSP_GetDetailNodeSql(pInOut_vProduct      in out varchar2,
                                  pInOut_vProductWhere in out varchar2,
                                  pInOut_vSaleT        in out varchar2,
                                  pInOut_vSaleTWhere   in out varchar2,
                                  pInOut_vTradeC       in out varchar2,
                                  pInOut_vTradeCWhere  in out varchar2,
                                  pInOut_nProductCount in out number,
                                  pInOut_nSalesCount   in out number,
                                  pInOut_nTradeCount   in out number,
                                  pOut_nSqlCode        out number) IS
    v_P_CondtionCount  number := 0; --condition count of product
    v_ST_CondtionCount number := 0; --condition count of sale territory
    v_TC_CondtionCount number := 0; --condition count of trade channel
    v_Datascounts      number := 0;
    v_Ope              number; --operation 1 equal;2 not equal;3 between
    v_Sql_Part         varchar2(2000); --
    v_F_Begin          VARCHAR2(60);
    v_F_End            VARCHAR2(60);
    v_AttrID           number;
    v_Sql_BetweenPart  varchar2(400); --part of sql for creating the KEY scope when meeting 'between' operation
    v_BoM_count        number;
    v_BoM_addr         number;
    vProductConnby     varchar2(4000);
    vSaleTConnby       varchar2(4000);
    vTradeCConnby      varchar2(4000);
  BEGIN
    pOut_nSqlCode := 0;
    --delete the conditions:when operator is 3 and only one value spefified!
    delete from tmp_cdt t
     where exists (select tabid, attrordno, ope, val_idx, addr
              from (select tabid,
                           attrordno,
                           ope,
                           val_idx,
                           addr,
                           count(ope) over(partition by tabid, ope) valcount
                      from tmp_cdt
                     where tabid not in
                           (p_constant.BoM,
                            p_constant.BoS,
                            p_constant.ProcessOfData)) c
             where c.ope = 3
               and c.valcount = 1
               and t.tabid = c.tabid
               and t.attrordno = c.attrordno
               and t.ope = c.ope
               and t.val_idx = c.val_idx
               and t.addr = c.addr);
    select count(*)
      into v_P_CondtionCount
      from tmp_cdt t
     where t.tabid in
           (p_Constant.PRODUCT, p_Constant.v_ProductAttr, p_constant.BoM);
  
    select count(*)
      into v_ST_CondtionCount
      from tmp_cdt t
     where t.tabid in (p_Constant.SALE_TERRITORY, p_Constant.v_STAttr);
  
    select count(*)
      into v_TC_CondtionCount
      from tmp_cdt t
     where t.tabid in (p_Constant.TRADE_CHANNEL, p_Constant.v_TCAttr);
  
    --find prodduct,sale territory,trade channel one by one first.
    select count(*)
      into v_Datascounts
      from tmp_cdt c
     where c.tabid = p_Constant.PRODUCT;
  
    -- if equal to 0 mean that thers is no conditions.
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
          vProductConnby := ' start with p.fam_em_addr in ' || v_Sql_Part ||
                            ' connect by prior p.fam_em_addr=p.fam0_em_addr';
        when 2 then
          pInOut_vProductWhere := pInOut_vProductWhere ||
                                  ' and not exists (select 1 from v_fam_tree  f where f.fam_em_addr=p.fam_em_addr and f.isleaf = 1 and f.id_fam=80
                           start with f.fam_em_addr in ' ||
                                  v_Sql_Part ||
                                  ' connect by prior f.fam_em_addr=f.fam0_em_addr)';
        when 3 then
          --parse a string like this '(124,125)' to 2 string like this '124' and '125'
          --          v_f_begin :=f_GetStr(v_sql_part,'(',',');
          select f_cle
            into v_F_Begin
            from fam f
           where f.fam_em_addr = f_GetStr(v_Sql_Part, '(', ',');
          select f_cle
            into v_F_End
            from fam f
           where f.fam_em_addr = f_GetStr(v_Sql_Part, ',', ')');
          vProductConnby := ' start with p.f_cle between ''' || v_F_Begin ||
                            ''' and ''' || v_F_End ||
                            ''' and p.nlevel=(select nlevel from v_fam_tree where f_cle= ''' ||
                            v_F_Begin ||
                            ''')  connect by prior p.fam_em_addr=p.fam0_em_addr';
      end case;
    end if;
  
    --product's attributes
    --loop by attribute in selection
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
          --if no key condition set ,then insert the detail node retrieved by attributes into tmp_product
          pInOut_vProductWhere := pInOut_vProductWhere || ' and p.c' ||
                                  v_AttrID || ' in ' || v_Sql_Part;
        when 2 then
          pInOut_vProductWhere := pInOut_vProductWhere || ' and p.c' ||
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
            pInOut_vProductWhere := pInOut_vProductWhere || ' and p.c' ||
                                    v_AttrID || '  in ' ||
                                    v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
          end;
      end case;
    end loop;
    pInOut_vProduct := pInOut_vProduct || pInOut_vProductWhere ||
                       vProductConnby;
    -- process BoM
    select count(*)
      into v_BoM_count
      from tmp_cdt c
     where c.tabid = p_constant.BoM
       and c.ope <> 0;
    if v_BoM_count > 0 then
      --V_BoM_addr only one value could be set in the UI
      select c.ope, c.addr
        into v_ope, v_BoM_addr
        from tmp_cdt c
       where c.tabid = p_constant.BoM
         and c.ope <> 0;
      ----pere_pro_nmc  is  assembly's  address
      ----fils_pro_nmc  is  component's address
      --as discussed on 07/24/2012 we knew in BoM the val1 and val2 are not worked.only check operator presently
      case v_ope
        when 1 then
          --component
          pInOut_vProduct := pInOut_vProduct ||
                             ' intersect select n1.fils_pro_nmc from nmc n1 where n1.nmc_field=80 and n1.fils_pro_nmc is not null ';
        when 2 then
          --assembly
          pInOut_vProduct := pInOut_vProduct ||
                             ' intersect select n1.pere_pro_nmc from nmc n1 where n1.nmc_field=80 and n1.pere_pro_nmc is not null';
        when 3 then
          --not a component
          pInOut_vProduct := pInOut_vProduct ||
                             ' minus  select n1.fils_pro_nmc from nmc n1 where n1.nmc_field=80 and  n1.fils_pro_nmc is not null';
        when 4 then
          --not an assembly
          pInOut_vProduct := pInOut_vProduct ||
                             ' minus  select n1.pere_pro_nmc from nmc n1 where n1.nmc_field=80 and  n1.pere_pro_nmc is not null';
      end case;
    end if;
    --sale territory
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
          vSaleTConnby := ' start with s.geo_em_addr in ' || v_Sql_Part ||
                          ' connect by prior s.geo_em_addr=s.geo1_em_addr';
        when 2 then
          pInOut_vSaleTWhere := pInOut_vSaleTWhere ||
                                ' and not exists (select 1  from v_geo_tree g where g.geo_em_addr = s.geo_em_addr and g.isleaf = 1
                           start with g.geo_em_addr in ' ||
                                v_Sql_Part || '
                           connect by prior g.geo_em_addr=g.geo1_em_addr)';
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
          vSaleTConnby := ' start with s.g_cle between ''' || v_F_Begin ||
                          ''' and ''' || v_F_End ||
                          '''  and s.nlevel =
           (select nlevel from v_geo_tree where g_cle =''' ||
                          v_F_Begin ||
                          ''') connect by prior s.geo_em_addr=s.geo1_em_addr';
      end case;
    end if;
    -----sale territory 's attributes
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
          pInOut_vSaleTWhere := pInOut_vSaleTWhere || ' and s.c' ||
                                v_AttrID || ' in ' || v_Sql_Part;
        when 2 then
          pInOut_vSaleTWhere := pInOut_vSaleTWhere || ' and s.c' ||
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
            pInOut_vSaleTWhere := pInOut_vSaleTWhere || ' and s.c' ||
                                  v_AttrID || ' in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
          end;
      end case;
    end loop;
    pInOut_vSaleT := pInOut_vSaleT || pInOut_vSaleTWhere || vSaleTConnby;
    /*    execute immediate 'insert into tmp_sales_territory (territoryid) ' ||v_sql_st;
    select 1 into v_S_Count from tmp_sales_territory where rownum<2;
    if v_S_Count = 0  then
      return;
    end if;*/
    --trade channel
    select count(*)
      into v_Datascounts
      from tmp_cdt c
     where c.tabid = p_Constant.TRADE_CHANNEL;
    -- if equal to -1 mean that thers is no conditions.
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
          vTradeCConnby := 'start with t.dis_em_addr in ' || v_Sql_Part ||
                           ' connect by prior t.dis_em_addr=t.dis2_em_addr';
        when 2 then
          pInOut_vTradeCWhere := pInOut_vTradeCWhere ||
                                 ' and  not exists (select d.dis_em_addr  from v_dis_tree d where t.dis_em_addr=d.dis_em_addr and d.isleaf = 1
                           start with d.dis_em_addr in ' ||
                                 v_Sql_Part || '
                           connect by prior d.dis_em_addr=d.dis2_em_addr)';
        when 3 then
        
          select d.d_cle
            into v_F_Begin
            from dis d
           where d.dis_em_addr = f_GetStr(v_Sql_Part, '(', ',');
          select d.d_cle
            into v_F_End
            from dis d
           where d.dis_em_addr = f_GetStr(v_Sql_Part, ',', ')');
          vTradeCConnby := ' start with t.d_cle between ''' || v_F_Begin ||
                           ''' and ''' || v_F_End ||
                           ''' and t.nlevel=
          (select nlevel from v_dis_tree where d_cle=''' ||
                           v_F_Begin ||
                           ''')  connect by prior t.dis_em_addr=t.dis2_em_addr';
      end case;
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
          pInOut_vTradeCWhere := pInOut_vTradeCWhere || ' and t.c' ||
                                 v_AttrID || ' in ' || v_Sql_Part;
        when 2 then
          pInOut_vTradeCWhere := pInOut_vTradeCWhere || ' and t.c' ||
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
            pInOut_vTradeCWhere := pInOut_vTradeCWhere || ' and t.c' ||
                                   v_AttrID || ' in ' || v_Sql_BetweenPart;
          exception
            when no_data_found then
              null;
          end;
      end case;
    end loop;
    pInOut_vTradeC := pInOut_vTradeC || pInOut_vTradeCWhere ||
                      vTradeCConnby;
  
    pInOut_nProductCount := v_P_CondtionCount;
    pInOut_nSalesCount   := v_ST_CondtionCount;
    pInOut_nTradeCount   := v_TC_CondtionCount;
  
  EXCEPTION
    WHEN OTHERS THEN
      pOut_nSqlCode := sqlcode;
      RAISE;
  END;

  PROCEDURE FMSP_GetProductSQLBySelection(pIn_nSelectionID   in number,
                                          pIn_bIsGroup       in boolean default false,
                                          pOut_cSQL          out clob,
                                          pOut_nConditionCnt out number,
                                          pOut_nSqlCode      out number) is
    nConditionCnt number := 0;
    nOperator     number; --operation 1 equal;2 not equal;3 between
    cTmpSql       clob;
    vBegin        VARCHAR2(60);
    vEnd          VARCHAR2(60);
    nAttrID       number;
    vSqlBetween   varchar2(400);
    nBoMCnt       number;
    nBoMaddr      number;
    cConnectBy    clob;
    cWhere        clob;
    cSQL          clob;
  BEGIN
    pOut_nSqlCode := 0;
  
    delete from tmp_cdt;
    insert into tmp_cdt
      (tabid, attrordno, ope, val_idx, addr)
      select c.rcd_cdt, c.n0_cdt, c.operant, c.n0_val_cdt, c.adr_cdt
        from cdt c
       where c.sel11_em_addr = pIn_nSelectionID
         and c.operant <> 0;
    --delete the conditions:when operator is 3 and only one value spefified!
    delete from tmp_cdt t
     where exists (select tabid, attrordno, ope, val_idx, addr
              from (select tabid,
                           attrordno,
                           ope,
                           val_idx,
                           addr,
                           count(ope) over(partition by tabid, ope) valcount
                      from tmp_cdt
                     where tabid not in
                           (p_constant.BoM,
                            p_constant.BoS,
                            p_constant.ProcessOfData)) c
             where c.ope = 3
               and c.valcount = 1
               and t.tabid = c.tabid
               and t.attrordno = c.attrordno
               and t.ope = c.ope
               and t.val_idx = c.val_idx
               and t.addr = c.addr);
  
    select count(*)
      into pOut_nConditionCnt
      from tmp_cdt t
     where t.tabid in
           (p_Constant.PRODUCT, p_Constant.v_ProductAttr, p_constant.BoM);
    select count(*)
      into nConditionCnt
      from tmp_cdt c
     where c.tabid = p_Constant.PRODUCT;
  
    if pIn_bIsGroup then
      cWhere := ' where p.id_Fam=70 ';
    else
      cWhere := ' where p.isleaf=1 and p.id_fam=80 ';
    end if;
  
    if nConditionCnt <> 0 then
      select t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into nOperator, cTmpSql
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
    
      case nOperator
        when 1 then
          cConnectBy := ' start with p.fam_em_addr in ' || cTmpSql ||
                        ' connect by prior p.fam_em_addr=p.fam0_em_addr';
        when 2 then
          cWhere := ' and not exists (select 1 from v_fam_tree  f where f.fam_em_addr=p.fam_em_addr and f.isleaf = 1 and f.id_fam=80
                           start with f.fam_em_addr in ' ||
                    cTmpSql ||
                    ' connect by prior f.fam_em_addr=f.fam0_em_addr)';
        when 3 then
          select f_cle
            into vBegin
            from fam f
           where f.fam_em_addr = f_GetStr(cTmpSql, '(', ',');
          select f_cle
            into vEnd
            from fam f
           where f.fam_em_addr = f_GetStr(cTmpSql, ',', ')');
          cConnectBy := ' start with p.f_cle between ''' || vBegin ||
                        ''' and ''' || vEnd ||
                        ''' and p.nlevel=(select nlevel from v_fam_tree where f_cle= ''' ||
                        vBegin ||
                        ''')  connect by prior p.fam_em_addr=p.fam0_em_addr';
      end case;
    end if;
  
    --product's attributes
    for k in (select distinct c.attrordno
                from tmp_cdt c
               where c.tabid = p_Constant.v_ProductAttr
                 and c.ope <> 0) loop
      select t.attrordno,
             t.ope,
             '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
        into nAttrID, nOperator, cTmpSql
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
    
      case nOperator
        when 1 then
          --if no key condition set ,then insert the detail node retrieved by attributes into tmp_product
          cWhere := cWhere || ' and p.c' || nAttrID || ' in ' || cTmpSql;
        when 2 then
          cWhere := cWhere || ' and p.c' || nAttrID || ' not in ' ||
                    cTmpSql;
        when 3 then
          begin
            select '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
              into vSqlBetween
              from (select v.vct_em_addr addr,
                           v.num_crt,
                           row_number() over(partition by v.num_crt order by v.val) cur,
                           row_number() over(partition by v.num_crt order by v.val) - 1 prev
                      from vct v
                     where v.id_crt = 80
                       and v.num_crt = nAttrID
                       and v.val between
                           (select val
                              from vct
                             where vct_em_addr = f_GetStr(cTmpSql, '(', ',')) and
                           (select val
                              from vct
                             where vct_em_addr = f_GetStr(cTmpSql, ',', ')'))) t
             group by t.num_crt
             start with t.cur = 1
            connect by prior t.cur = t.prev;
            cWhere := cWhere || ' and p.c' || nAttrID || '  in ' ||
                      vSqlBetween;
          exception
            when no_data_found then
              null;
          end;
      end case;
    end loop;
    cSQL := cSQL || cWhere || cConnectBy;
    -- process BoM
    select count(*)
      into nBoMCnt
      from tmp_cdt c
     where c.tabid = p_constant.BoM
       and c.ope <> 0;
    if nBoMCnt > 0 then
      --V_BoM_addr only one value could be set in the UI
      select c.ope, c.addr
        into nOperator, nBoMaddr
        from tmp_cdt c
       where c.tabid = p_constant.BoM
         and c.ope <> 0;
      ----pere_pro_nmc  is  assembly's  address
      ----fils_pro_nmc  is  component's address
      --as discussed on 07/24/2012 we knew in BoM the val1 and val2 are not worked.only check operator presently
      case nOperator
        when 1 then
          --component
          cSQL := cSQL ||
                  ' intersect select n1.fils_pro_nmc from nmc n1 where n1.nmc_field=80 and n1.fils_pro_nmc is not null ';
        when 2 then
          --assembly
          cSQL := cSQL ||
                  ' intersect select n1.pere_pro_nmc from nmc n1 where n1.nmc_field=80 and n1.pere_pro_nmc is not null';
        when 3 then
          --not a component
          cSQL := cSQL ||
                  ' minus  select n1.fils_pro_nmc from nmc n1 where n1.nmc_field=80 and  n1.fils_pro_nmc is not null';
        when 4 then
          --not an assembly
          cSQL := cSQL ||
                  ' minus  select n1.pere_pro_nmc from nmc n1 where n1.nmc_field=80 and  n1.pere_pro_nmc is not null';
      end case;
    end if;
  
    if pOut_nConditionCnt = 0 then
      pOut_cSQL := ' select p.fam_em_addr from fam p '||cWhere;
    else
      pOut_cSQL := ' select p.fam_em_addr  from v_productattrvalue p ' || cSQL;
    end if;
    fmp_log.LOGDEBUG(pIn_vModules => 'selection',
                     pIn_cSqlText => pOut_cSQL);
  end;

  PROCEDURE FMSP_GetSaleSQLBySelection(pIn_nSelectionID   in number,
                                       pOut_cSQL          out clob,
                                       pOut_nConditionCnt out number,
                                       pOut_nSqlCode      out number) is
  begin
    null;
  end;

  PROCEDURE FMSP_GetTradeSQLBySelection(pIn_nSelectionID   in number,
                                        pOut_cSQL          out clob,
                                        pOut_nConditionCnt out number,
                                        pOut_nSqlCode      out number) is
  begin
    null;
  end;

  PROCEDURE FMSP_GetDetailSQLBySelection(pIn_nSelectionID   in number,
                                         pOut_cSQL          out clob,
                                         pOut_nConditionCnt out number,
                                         pOut_nSqlCode      out number) is
  begin
    null;
  end;

begin

  null;
end fmp_getdetailnodesql;
/
