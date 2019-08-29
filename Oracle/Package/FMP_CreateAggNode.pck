create or replace package FMP_CreateAggNode is

  procedure FMSP_CreateAggNode(pIn_nDetailNode in number,
                               pOut_nSqlCode   out number);

  procedure FMSP_IsBelongToAggRule(pIn_nDetailNodeID in number,
                                   pIn_nAggRuleID    in number,
                                   pOut_bIsBelong    out boolean,
                                   pOut_nSqlCode     out number);                               

end FMP_CreateAggNode;
/
create or replace package body FMP_CreateAggNode is
  --*****************************************************************
  -- Description: create aggregation node by detail node
  --
  -- Author:  JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        25-JAN-2013     JY.Liu      Created.
  -- **************************************************************

  procedure FMSP_IsBelongToAggRule(pIn_nDetailNodeID in number,
                                   pIn_nAggRuleID    in number,
                                   pOut_bIsBelong    out boolean,
                                   pOut_nSqlCode     out number)
  --*****************************************************************
    -- Description: whether the detail node is belong to aggregation rule
    --
    -- Parameters:
    --       pIn_nDetailNodeID:
    --       pIn_nAggRuleID:
    --       pOut_bIsBelong
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0       25-JAN-2013     JY.Liu      Created.
    -- **************************************************************
   as
    nPid        number;
    nSTid       number;
    nTCid       number;
    nPLevel     number;
    nSTLevel    number;
    nTCLevel    number;
    nOperator   number;
    cSqlPart    clob;
    nPKeyCount  number;
    nSTKeyCount number;
    mTCKeyCount number;

    nAttrValID number;
    vBegin     VARCHAR2(60);
    vEnd       VARCHAR2(60);
    nPCount    number;
    nSTCount   number;
    nTCCount   number;
    vNodeVal   vct.val%type;
    VRuleVal   vct.val%type;
  begin

    pOut_bIsBelong := false;

    select p.fam4_em_addr, p.geo5_em_addr, p.dis6_em_addr
      into nPid, nSTid, nTCid
      from pvt p
     where p.pvt_em_addr = pIn_nDetailNodeID;

    select p.regroup_pro, p.regroup_geo, p.regroup_dis
      into nPLevel, nSTLevel, nTCLevel
      from prv p
     where p.prv_em_addr = pIn_nAggRuleID;

    delete from tmp_aggnoderule;
    --step1  process level and Key of 3D.[Level must be higher than the level of key specified]
    --product key
    if nPLevel > 0 then
      select count(*)
        into nPKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.PRODUCT;
      if nPKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.PRODUCT) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            --check whether  the level of the specfied key is higher than the level.And. the detail node is own to the higher level node
            execute immediate 'select nvl(sum(t.fam_em_addr),-1) from (select fam_em_addr from v_fam_tree f where f.nlevel = ' ||
                              nPLevel || ' start with f.fam_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior f.fam_em_addr = f.fam0_em_addr
                                     intersect
                                     select fam_em_addr from v_fam_tree f where f.nlevel = ' ||
                              nPLevel || ' start with f.fam_em_addr = ' || nPid ||
                              '  connect by prior f.fam0_em_addr = f.fam_em_addr) t'
              into nAttrValID;
          when 2 then
            execute immediate 'select nvl(sum(t.fam_em_addr),-1)    from (select fam_em_addr from v_fam_tree f where f.nlevel = ' ||
                              nPLevel ||
                              '  minus select f.fam_em_addr from v_fam_tree f  where f.nlevel = ' ||
                              nPLevel || '  start with f.fam_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior f.fam_em_addr = f.fam0_em_addr
                            intersect
                             select fam_em_addr from v_fam_tree f where f.nlevel=' ||
                              nPLevel || ' start with f.fam_em_addr=' || nPid ||
                              ' connect by prior f.fam0_em_addr=f.fam_em_addr) t '
              into nAttrValID;
          when 3 then
            begin
              select f_cle
                into vBegin
                from fam f
               where f.fam_em_addr = f_GetStr(cSqlPart, '(', ',');
              select f_cle
                into vEnd
                from fam f
               where f.fam_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate 'select nvl(sum(t.fam_em_addr),-1)  from (select fam_em_addr from v_fam_tree f where f.nlevel=' ||
                                nPLevel ||
                                ' start with f.f_cle between :1 and :2  connect by prior f.fam_em_addr=f.fam0_em_addr
                              intersect   select fam_em_addr from v_fam_tree f where f.nlevel=' ||
                                nPLevel || ' start with f.fam_em_addr=' || nPid ||
                                '  connect by prior f.fam0_em_addr=f.fam_em_addr) t '
                into nAttrValID
                using vBegin, vEnd;
            exception
              when others then
                null;
            end;
        end case;
        -- if -1 ,no data met
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.PRODUCT, 0, nAttrValID);
        end if;
      else
        select nvl(sum(f.fam_em_addr), -1)
          into nAttrValID
          from v_fam_tree f
         where f.nlevel = nPLevel
         start with f.fam_em_addr = nPid
        connect by prior f.fam0_em_addr = f.fam_em_addr;
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.PRODUCT, 0, nAttrValID);
        end if;
      end if;
    else
      -- if level =0
      select count(*)
        into nPKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.PRODUCT;
      if nPKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.PRODUCT) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            --v_p_count
            execute immediate 'select count(*) from v_fam_tree f where f.fam_em_addr = ' || nPid || '
           start with f.fam_em_addr in ' ||
                              cSqlPart || '
          connect by prior f.fam_em_addr = f.fam0_em_addr'
              into nPCount;
          when 2 then
            execute immediate ' select count(*) from (select * from v_fam_tree f where f.nlevel = 1 minus
                  select * from v_fam_tree f  where f.nlevel = 1 start with f.fam_em_addr in ' ||
                              cSqlPart || '
                  connect by prior f.fam_em_addr = f.fam0_em_addr) where fam_em_addr = ' || nPid
              into nPCount;
          when 3 then
            begin
              select f_cle
                into vBegin
                from fam f
               where f.fam_em_addr = f_GetStr(cSqlPart, '(', ',');
              select f_cle
                into vEnd
                from fam f
               where f.fam_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate ' select count(*)  from v_fam_tree f where f.fam_em_addr = ' || nPid ||
                                '  start with f.f_cle between :1 and :2
          connect by prior f.fam_em_addr = f.fam0_em_addr '
                into nPCount
                using vBegin, vEnd;
            exception
              when others then
                null;
            end;
        end case;
        if nPCount = 0 then
          return;
        end if;
      end if;
    end if;
    --sale territory
    if nSTLevel > 0 then
      select count(*)
        into nSTKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.SALE_TERRITORY;
      if nSTKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.SALE_TERRITORY) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            execute immediate 'select nvl(sum(t.geo_em_addr),-1) from (select geo_em_addr from v_geo_tree g where g.nlevel = ' ||
                              nSTLevel || ' start with g.geo_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior g.geo_em_addr = g.geo1_em_addr
                                     intersect
                                     select geo_em_addr from v_geo_tree g where g.nlevel = ' ||
                              nSTLevel || ' start with g.geo_em_addr = ' ||
                              nSTid ||
                              '  connect by prior g.geo1_em_addr = g.geo_em_addr) t'
              into nAttrValID;
          when 2 then
            execute immediate 'select nvl(sum(t.geo_em_addr),-1)    from (select geo_em_addr from v_geo_tree g where g.nlevel = ' ||
                              nSTLevel ||
                              '  minus select geo_em_addr from v_geo_tree g  where g.nlevel = ' ||
                              nSTLevel || '  start with g.geo_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior g.geo_em_addr =g.geo1_em_addr
                            intersect
                             select geo_em_addr from v_geo_tree g where g.nlevel=' ||
                              nSTLevel || ' start with g.geo_em_addr=' ||
                              nSTid ||
                              ' connect by prior g.geo1_em_addr=g.geo_em_addr) t '
              into nAttrValID;
          when 3 then
            begin
              select g_cle
                into vBegin
                from geo g
               where g.geo_em_addr = f_GetStr(cSqlPart, '(', ',');
              select g_cle
                into vEnd
                from geo g
               where g.geo_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate 'select nvl(sum(t.geo_em_addr),-1)  from (select geo_em_addr from v_geo_tree g where g.nlevel=' ||
                                nSTLevel ||
                                ' start with g.g_cle between :1 and :2 connect by prior g.geo_em_addr=g.geo1_em_addr
                              intersect   select geo_em_addr from v_geo_tree g where g.nlevel=' ||
                                nSTLevel || ' start with g.geo_em_addr=' ||
                                nSTid ||
                                '  connect by prior g.geo1_em_addr=g.geo_em_addr) t '
                into nAttrValID
                using vBegin, vEnd;
            exception
              when others then
                pOut_nSqlCode := sqlcode;
                null;
            end;
        end case;
        -- if -1 ,no data met
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.SALE_TERRITORY, 0, nAttrValID);
        end if;
      else
        select nvl(sum(g.geo_em_addr), -1)
          into nAttrValID
          from v_geo_tree g
         where g.nlevel = nSTLevel
         start with g.geo_em_addr = nSTid
        connect by prior g.geo1_em_addr = g.geo_em_addr;
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.SALE_TERRITORY, 0, nAttrValID);
        end if;
      end if;
    else
      -- if level =0
      select count(*)
        into nSTKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.SALE_TERRITORY;
      if nSTKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.SALE_TERRITORY) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            --v_p_count
            execute immediate 'select count(*) from v_geo_tree g where g.geo_em_addr = ' ||
                              nSTid || '
           start with g.geo_em_addr in ' ||
                              cSqlPart || '
          connect by prior g.geo_em_addr =g.geo1_em_addr'
              into nSTCount;
          when 2 then
            execute immediate ' select count(*) from (select * from v_geo_tree g where g.nlevel = 1 minus
                  select * from v_geo_tree g  where g.nlevel = 1 start with g.geo_em_addr in ' ||
                              cSqlPart || '
                  connect by prior g.geo_em_addr = g.geo1_em_addr) where geo_em_addr = ' ||
                              nSTid
              into nSTCount;
          when 3 then
            begin
              select g_cle
                into vBegin
                from geo g
               where g.geo_em_addr = f_GetStr(cSqlPart, '(', ',');
              select g_cle
                into vEnd
                from geo g
               where g.geo_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate ' select count(*)  from v_geo_tree g where g.geo_em_addr = ' ||
                                nSTid ||
                                '  start with g.g_cle between  :1 and :2
          connect by prior g.geo_em_addr = g.geo1_em_addr '
                into nSTCount
                using vBegin, vEnd;
            exception
              when others then
                null;
            end;
        end case;
        if nSTCount = 0 then
          return;
        end if;
      end if;
    end if;
    --trade channel
    if nTCLevel > 0 then
      select count(*)
        into mTCKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.TRADE_CHANNEL;
      if mTCKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.TRADE_CHANNEL) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            execute immediate 'select nvl(sum(t.dis_em_addr),-1) from (select dis_em_addr from v_dis_tree d where d.nlevel = ' ||
                              nTCLevel || ' start with d.dis_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior d.dis_em_addr = d.dis2_em_addr
                                     intersect
                                     select dis_em_addr from v_dis_tree d where d.nlevel = ' ||
                              nTCLevel || ' start with d.dis_em_addr = ' ||
                              nTCid ||
                              '  connect by prior d.dis2_em_addr = d.dis_em_addr) t'
              into nAttrValID;
          when 2 then
            execute immediate 'select nvl(sum(t.dis_em_addr),-1)    from (select dis_em_addr from v_dis_tree d where d.nlevel = ' ||
                              nTCLevel ||
                              '  minus select dis_em_addr from v_dis_tree d  where d.nlevel = ' ||
                              nTCLevel || '  start with d.dis_em_addr in ' ||
                              cSqlPart ||
                              ' connect by prior d.dis_em_addr =d.dis2_em_addr
                            intersect
                             select dis_em_addr from v_dis_tree d where d.nlevel=' ||
                              nTCLevel || ' start with d.dis_em_addr=' ||
                              nTCid ||
                              ' connect by prior d.dis2_em_addr=d.dis_em_addr) t '
              into nAttrValID;
          when 3 then
            begin
              select d_cle
                into vBegin
                from dis d
               where d.dis_em_addr = f_GetStr(cSqlPart, '(', ',');
              select d_cle
                into vEnd
                from dis d
               where d.dis_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate 'select nvl(sum(t.dis_em_addr),-1)  from (select dis_em_addr from v_dis_tree d where d.nlevel=' ||
                                nTCLevel ||
                                ' start with d.d_cle between :1 and :2 connect by prior d.dis_em_addr=d.dis2_em_addr
                            intersect   select dis_em_addr from v_dis_tree d where d.nlevel=' ||
                                nTCLevel || ' start with d.dis_em_addr=' ||
                                nTCid ||
                                '  connect by prior d.dis2_em_addr=d.dis_em_addr) t '
                into nAttrValID
                using vBegin, vEnd;
            exception
              when others then
                pOut_nSqlCode := sqlcode;
                null;
            end;
        end case;
        -- if -1 ,no data met
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.TRADE_CHANNEL, 0, nAttrValID);
        end if;
      else
        select nvl(sum(d.dis_em_addr), -1)
          into nAttrValID
          from v_dis_tree d
         where d.nlevel = nTCLevel
         start with d.dis_em_addr = nTCid
        connect by prior d.dis2_em_addr = d.dis_em_addr;
        if nAttrValID = -1 then
          return;
        else
          insert into tmp_aggnoderule
            (attributetype, attributeno, attributevalid)
          values
            (p_Constant.TRADE_CHANNEL, 0, nAttrValID);
        end if;
      end if;
    else
      select count(*)
        into mTCKeyCount
        from cdt c
       where c.prv12_em_addr = pIn_nAggRuleID
         and c.rcd_cdt = p_Constant.TRADE_CHANNEL;
      if mTCKeyCount > 0 then
        select t.ope,
               '(' || ltrim(max(sys_connect_by_path(t.addr, ',')), ',') || ')' val
          into nOperator, cSqlPart
          from (select c.operant ope,
                       c.adr_cdt addr,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) cur,
                       row_number() over(partition by c.rcd_cdt, c.operant order by c.n0_val_cdt) + 1 prev
                  from cdt c
                 where c.prv12_em_addr = pIn_nAggRuleID
                   and c.rcd_cdt = p_Constant.TRADE_CHANNEL) t
         group by t.ope
         start with t.cur = 1
        connect by prior t.prev = t.cur;
        case nOperator
          when 1 then
            execute immediate 'select count(*) from v_dis_tree d where d.dis_em_addr = ' ||
                              nTCid || '
           start with d.dis_em_addr in ' ||
                              cSqlPart || '
          connect by prior d.dis_em_addr =d.dis2_em_addr'
              into nTCCount;
          when 2 then
            execute immediate ' select count(*) from (select * from v_dis_tree d where d.nlevel = 1 minus
                  select * from v_dis_tree d where d.nlevel = 1 start with d.dis_em_addr in ' ||
                              cSqlPart || '
                  connect by prior d.dis_em_addr = d.dis2_em_addr) where dis_em_addr = ' ||
                              nTCid
              into nTCCount;
          when 3 then
            begin
              select d_cle
                into vBegin
                from dis d
               where d.dis_em_addr = f_GetStr(cSqlPart, '(', ',');
              select d_cle
                into vEnd
                from dis d
               where d.dis_em_addr = f_GetStr(cSqlPart, ',', ')');
              execute immediate ' select count(*)  from v_dis_tree d where d.dis_em_addr = ' ||
                                nTCid ||
                                '  start with d.d_cle between :1 and :2
          connect by prior  d.dis_em_addr = d.dis2_em_addr '
                into nTCCount
                using vBegin, vEnd;
            exception
              when others then
                null;
            end;
        end case;
        if nTCCount = 0 then
          return;
        end if;
      end if;
    end if;

    --step2 process Cst
    --Cst of product
    for i in (select c.n0_cdt attrno
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant = 4
                 and c.rcd_cdt = p_constant.v_ProductAttr) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.fam7_em_addr = nPid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        insert into tmp_aggnoderule
          (attributetype, attributeno, attributevalid)
        values
          (p_Constant.v_ProductAttr, i.attrno, nAttrValID);
      end if;
    end loop;
    --Cst of sale territory
    for i in (select c.n0_cdt attrno
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant = 4
                 and c.rcd_cdt = p_constant.v_STAttr) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.geo8_em_addr = nSTid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        insert into tmp_aggnoderule
          (attributetype, attributeno, attributevalid)
        values
          (p_Constant.v_STAttr, i.attrno, nAttrValID);
      end if;
    end loop;
    --Cst of trade channel
    for i in (select c.n0_cdt attrno
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant = 4
                 and c.rcd_cdt = p_constant.v_TCAttr) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.dis9_em_addr = nTCid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        insert into tmp_aggnoderule
          (attributetype, attributeno, attributevalid)
        values
          (p_Constant.v_TCAttr, i.attrno, nAttrValID);
      end if;
    end loop;
    --Cst of detail node
    for i in (select c.n0_cdt attrno
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant = 4
                 and c.rcd_cdt = p_constant.DETAIL_NODE) loop
      select nvl(max(p.crtserie36_em_addr), -1)
        into nAttrValID
        from pvtcrt p
       where p.pvt35_em_addr = pIn_nDetailNodeID
         and p.numero_crt_pvt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        insert into tmp_aggnoderule
          (attributetype, attributeno, attributevalid)
        values
          (p_Constant.DETAIL_NODE, i.attrno, nAttrValID);
      end if;
    end loop;
    --Step3,process conditions = <> <<
    --conditions of product;
    for i in (select c.n0_cdt     attrno,
                     c.operant    ope,
                     c.adr_cdt    addr,
                     c.n0_val_cdt
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant in (1, 2, 3)
                 and c.rcd_cdt = p_constant.v_ProductAttr
               order by 1, 2, 3) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.fam7_em_addr = nPid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        case i.ope
          when 1 then
            if nAttrValID <> i.addr then
              return;
            end if;
          when 2 then
            if nAttrValID = i.addr then
              return;
            end if;
          when 3 then
            select v.val
              into vNodeVal
              from vct v
             where v.vct_em_addr = nAttrValID;
            select v.val
              into VRuleVal
              from vct v
             where v.vct_em_addr = i.addr;
            case i.n0_val_cdt
              when 0 then
                --expect >=
                if vNodeVal < VRuleVal then
                  return;
                end if;
              when 1 then
                --expect <=
                if vNodeVal > VRuleVal then
                  return;
                end if;
            end case;
        end case;
      end if;
    end loop;

    --conditions of sale territory
    for i in (select c.n0_cdt     attrno,
                     c.operant    ope,
                     c.adr_cdt    addr,
                     c.n0_val_cdt
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant in (1, 2, 3)
                 and c.rcd_cdt = p_constant.v_STAttr
               order by 1, 2, 3) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.geo8_em_addr = nSTid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        case i.ope
          when 1 then
            if nAttrValID <> i.addr then
              return;
            end if;
          when 2 then
            if nAttrValID = i.addr then
              return;
            end if;
          when 3 then
            select v.val
              into vNodeVal
              from vct v
             where v.vct_em_addr = nAttrValID;
            select v.val
              into VRuleVal
              from vct v
             where v.vct_em_addr = i.addr;
            case i.n0_val_cdt
              when 0 then
                --expect >=
                if vNodeVal < VRuleVal then
                  return;
                end if;
              when 1 then
                --expect <=
                if vNodeVal > VRuleVal then
                  return;
                end if;
            end case;
        end case;
      end if;
    end loop;

    --conditions of trade channel;
    for i in (select c.n0_cdt     attrno,
                     c.operant    ope,
                     c.adr_cdt    addr,
                     c.n0_val_cdt
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant in (1, 2, 3)
                 and c.rcd_cdt = p_constant.v_TCAttr
               order by 1, 2, 3) loop
      select nvl(max(r.vct10_em_addr), -1)
        into nAttrValID
        from rfc r
       where r.dis9_em_addr = nTCid
         and r.numero_crt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        case i.ope
          when 1 then
            if nAttrValID <> i.addr then
              return;
            end if;
          when 2 then
            if nAttrValID = i.addr then
              return;
            end if;
          when 3 then
            select v.val
              into vNodeVal
              from vct v
             where v.vct_em_addr = nAttrValID;
            select v.val
              into VRuleVal
              from vct v
             where v.vct_em_addr = i.addr;
            case i.n0_val_cdt
              when 0 then
                --expect >=
                if vNodeVal < VRuleVal then
                  return;
                end if;
              when 1 then
                --expect <=
                if vNodeVal > VRuleVal then
                  return;
                end if;
            end case;
        end case;
      end if;
    end loop;
    --conditions of detail node
    for i in (select c.n0_cdt     attrno,
                     c.operant    ope,
                     c.adr_cdt    addr,
                     c.n0_val_cdt
                from cdt c
               where c.prv12_em_addr = pIn_nAggRuleID
                 and c.operant in (1, 2, 3)
                 and c.rcd_cdt = p_constant.DETAIL_NODE
               order by 1, 2, 3) loop
      select nvl(max(r.crtserie36_em_addr), -1)
        into nAttrValID
        from pvtcrt r
       where r.pvt35_em_addr = pIn_nDetailNodeID
         and r.numero_crt_pvt = i.attrno + 49;
      if nAttrValID = -1 then
        return;
      else
        case i.ope
          when 1 then
            if nAttrValID <> i.addr then
              return;
            end if;
          when 2 then
            if nAttrValID = i.addr then
              return;
            end if;
          when 3 then
            select v.val_crt_serie
              into vNodeVal
              from crtserie v
             where v.crtserie_em_addr = nAttrValID;
            select v.val_crt_serie
              into VRuleVal
              from crtserie v
             where v.crtserie_em_addr = i.addr;
            case i.n0_val_cdt
              when 0 then
                --expect >=
                if vNodeVal < VRuleVal then
                  return;
                end if;
              when 1 then
                --expect <=
                if vNodeVal > VRuleVal then
                  return;
                end if;
            end case;
        end case;
      end if;
    end loop;

    pOut_bIsBelong := true;
    pOut_nSqlCode  := 0;
  exception
    when others then
      pOut_bIsBelong := false;
      pOut_nSqlCode  := sqlcode;
  end;

  procedure FMSP_CreateAggNode(pIn_nDetailNode in number,
                               pOut_nSqlCode   out number)
  --*****************************************************************
    -- Description: create teh aggregate node as the specified detail node
    --
    -- Parameters:
    --       pIn_nDetailNode:
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0       25-JAN-2013     JY.Liu      Created.
    -- **************************************************************
   is
    bIsBelong boolean;
    nExisted  number;
  begin
    for i in (select p.prv_em_addr id from prv p) loop
      FMSP_IsBelongToAggRule(pIn_nDetailNodeID => pIn_nDetailNode,
                             pIn_nAggRuleID    => i.id,
                             pOut_bIsBelong    => bIsBelong,
                             pOut_nSqlCode     => pOut_nSqlCode);
      begin
        if bIsBelong then
          select count(*)
            into nExisted
            from prvselpvt l
           where l.prvid = i.id
             and l.pvtid = pIn_nDetailNode;
          if nExisted > 0 then
            raise_application_error(-20004, 'continue');
          end if;
          fmsp_execsql('truncate table aggregation_detailnode');
          insert into aggregation_detailnode
            (aggregationid, detailnodeid)
          values
            (i.id, pIn_nDetailNode);
          
          p_aggregation.spprv_ProduceAggNodeAndLinks(P_AggregateRuleID => i.id,
                                                     pIn_nType         => 1,
                                                     p_sqlcode         => pOut_nSqlCode);
        end if;
      exception
        when others then
          null;
      end;
    end loop;
  exception
    when others then
      FMP_LOG.LOGERROR;
      raise;
  end;

end FMP_CreateAggNode;
/
