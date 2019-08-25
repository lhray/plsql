create or replace package FMP_Batch is

  g_vChr1 varchar2(6) := 'chr(1)';
  g_nZero number := 0;
  type g_FMRT_Switches is record(
    --key format
    b2keys          boolean := false,
    dis             boolean := false,
    dis_n0crt       number := 0,
    fam_n0crt       number := 0,
    geo             boolean := false,
    geo_n0crt       number := 0,
    key_dis         varchar2(100) := null,
    key_dis_default varchar2(100) := null,
    key_geo         varchar2(100) := null,
    key_geo_default varchar2(100) := null,
    nodis           boolean := false,
    nogeo           boolean := false,
    p2r             boolean := false,
    pro             boolean := false,
    pro_n0crt       number := 0,
    r2p             boolean := false,
    --date format
    a_m   boolean := false,
    a_m_j boolean := false,
    aa_mm boolean := false,
    am_w  boolean := false,
    --field seperator
    esp  boolean := false,
    sbs  boolean := false,
    sdlt boolean := false,
    sep  varchar2(100),
    spv  boolean := false,
    stab boolean := false,
    --value format
    unit    number := null,
    version number := null,
    
    --no category
    maj_sel boolean := false);

  procedure FMSP_Parse(pIn_vSwitches  in varchar2,
                       pOut_tSwitches out g_FMRT_Switches,
                       pOut_nSqlCode  out number);

  procedure FMSP_ImpNode(pIn_nNodeType     in number,
                         pInOut_vTablename in out varchar2,
                         pIn_tSwitches     in g_FMRT_Switches,
                         pIn_vFMUSER       in varchar2,
                         pIn_vDesc         in varchar2,
                         pOut_nSqlCode     out number);

  procedure FMSP_ExpNode(pIn_nNodeType   in number,
                         pIn_vTablename  in varchar2,
                         pIn_tSwitches   in g_FMRT_Switches,
                         pIn_vSQLField   in varchar2,
                         pIn_vFMUSER     in varchar2,
                         pOut_vTablename out varchar2,
                         pOut_nSqlCode   out number);
end FMP_Batch;
/
create or replace package body FMP_Batch is
  --*****************************************************************
  -- Description:
  --
  --
  -- Author:      JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        11-JAN-2013     JY.Liu     Created.
  -- **************************************************************
  procedure FMSP_Parse(pIn_vSwitches  in varchar2,
                       pOut_tSwitches out g_FMRT_Switches,
                       pOut_nSqlCode  out number)
  --*****************************************************************
    -- Description: parse the input switches string
    --
    -- Parameters:
    --       pIn_vSwitches
    --       pOut_tSwitches
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
  
    vSwitches     varchar2(5000) := '';
    nSwitchLen    integer := 0;
    vSingleSwitch varchar2(100) := '';
    vSwitchKey    varchar2(100) := '';
    vSwitchParam  varchar2(100) := '';
    nOffset       number := 0;
  begin
    pOut_nSqlCode := 0;
    vSwitches     := trim(pIn_vSwitches);
    nSwitchLen    := length(vSwitches);
  
    while nSwitchLen > 0 loop
      --## the seperator index
      nOffset := instr(vSwitches, '##', 1, 1);
    
      vSwitchKey   := null;
      vSwitchParam := null;
    
      if nOffset = 0 then
        vSingleSwitch := vSwitches;
        nSwitchLen    := 0;
      else
        vSingleSwitch := trim(substr(vSwitches, 0, nOffset - 1));
        vSwitches     := trim(substr(vSwitches, nOffset + 2));
        nSwitchLen    := length(vSwitches);
      END IF;
    
      nOffset := instr(vSingleSwitch, ':', 1, 1);
    
      if nOffset = 0 then
        vSwitchKey := vSingleSwitch;
      else
        vSwitchKey   := trim(substr(vSingleSwitch, 1, nOffset - 1));
        vSwitchParam := trim(substr(vSingleSwitch, nOffset + 1));
      end if;
    
      vSwitchKey   := rtrim(lower(vSwitchKey));
      vSwitchParam := trim(both '"' from vSwitchParam);
    
      case vSwitchKey
      --key format begin
        when '2keys' then
          pOut_tSwitches.b2keys := true;
        when 'dis' then
          pOut_tSwitches.dis := true;
        when 'dis_n0cft' then
          pOut_tSwitches.dis_n0crt := to_number(vswitchparam);
        when 'fam_n0crt' then
          pOut_tSwitches.fam_n0crt := to_number(vswitchparam);
        when 'geo' then
          pOut_tSwitches.geo := true;
        when 'geo_n0crt' then
          pOut_tSwitches.geo_n0crt := to_number(vswitchparam);
        when 'key_dis' then
          pOut_tSwitches.key_dis := vSwitchParam;
        when 'key_dis_default' then
          pOut_tSwitches.key_dis_default := vSwitchParam;
        when 'key_geo' then
          pOut_tSwitches.key_geo := vSwitchParam;
        when 'key_geo_default' then
          pOut_tSwitches.key_geo_default := vSwitchParam;
        when 'nodis' then
          pOut_tSwitches.nodis := true;
        when 'nogeo' then
          pOut_tSwitches.nogeo := true;
        when 'p2r' then
          pOut_tSwitches.p2r := true;
        when 'pro' then
          pOut_tSwitches.pro := true;
        when 'pro_n0crt' then
          pOut_tSwitches.pro_n0crt := to_number(vswitchparam);
        when 'r2p' then
          pOut_tSwitches.r2p := true;
        
      --date format
        when 'a_m' then
          pOut_tSwitches.a_m := true;
        when 'a_m_j' then
          pOut_tSwitches.a_m_j := true;
        when 'aa_mm' then
          pOut_tSwitches.aa_mm := true;
        when 'am_w' then
          pOut_tSwitches.am_w := true;
        
      --value format
        when 'unit' then
          if vSwitchParam is not null then
            pOut_tSwitches.unit := to_number(vSwitchParam);
          end if;
        when 'version' then
          pOut_tSwitches.version := to_number(vSwitchParam);
        when 'maj_sel' then
          pOut_tSwitches.maj_sel := true;
        else
          null;
      end case;
    end loop;
  exception
    when others then
      Fmp_Log.LOGERROR(pIn_vTextErrm => 'parse switch error');
  end;

  procedure FMSP_ImpProduct(pIn_vTablename in varchar2,
                            pIn_tSwitches  in g_FMRT_Switches,
                            pIn_vFMUSER    in varchar2,
                            pIn_vDesc      in varchar2,
                            pOut_cSelect   out clob,
                            pOut_cFrom     out clob)
  --*****************************************************************
    -- Description: process switch of product
    --
    -- Parameters:
    --       pIn_vTablename
    --       pIn_tSwitches
    --       pIn_vFMUSER
    --       pIn_vDesc
    --       pOut_cSelect
    --       pOut_cFrom
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    nAttrNO      number;
    nCurrentTime number;
    nRootID      number;
    vSql         varchar2(4000);
  begin
  
    if pIn_tSwitches.fam_n0crt <> 0 then
      nAttrNo      := pIn_tSwitches.fam_n0crt;
      pOut_cselect := 'f.f_cle pname,f.pid,f.pdesc';
      pOut_cFrom   := '  left join (select max(f.f_cle) f_cle, v.val,f.fam_em_addr pid,f.f_desc pdesc
                        from rfc r, vct v, fam f
                       where f.fam_em_addr = r.fam7_em_addr
                         and f.id_fam = 70
                         and r.ident_crt = 70
                         and r.numero_crt = ' ||
                      nAttrNo || '
                         and r.vct10_em_addr = v.vct_em_addr
                       group by v.val) f on t.product=f.val ';
      return;
    end if;
  
    if pIn_tSwitches.pro_n0crt = 0 AND pIn_tSwitches.pro = false then
      nCurrentTime := F_ConvertDateToOleDateTime(sysdate);
      nRootID      := FMP_Dimesion.FMF_GetRoot(pIn_nDimesion => 1);
    
      vSql := 'insert /*+ append */ into fam(fam_em_addr,id_fam,f_cle,F_Desc,user_create_fam,date_create_fam,fam0_em_addr)
        select seq_fam.nextval, 80, t.product, ''' ||
              pIn_vDesc || ''',''' || pIn_vFMUSER || ''',' || nCurrentTime || ',' ||
              nRootID || '
          from ' || pIn_vTablename || ' t
         where rowid =(select max(rowid) from ' ||
              pIn_vTablename || ' t2 where t.product=t2.product ) and not exists (select 1
                  from fam f
                 where t.product = f.f_cle)';
      FMSP_ExecSql(pIn_cSql => vSql);
    
      pOut_cSelect := ' t.product pname,f.fam_em_addr pid,f.f_desc pdesc';
      pOut_cFrom   := ' left join fam f on t.product=f.f_cle ';
    else
      nAttrNo      := pIn_tSwitches.pro_n0crt;
      pOut_cselect := ' f.f_cle pname,f.pid,f.pdesc';
      pOut_cFrom   := '  left join (select f.f_cle  ,v.val,f.fam_em_addr pid ,f.f_desc pdesc
                      from rfc r ,vct v, fam f
                      where f.fam_em_addr=r.fam7_em_addr and f.id_fam=80
                       and r.ident_crt=70 and r.numero_crt=' ||
                      nAttrNo ||
                      ' and r.vct10_em_addr=v.vct_em_addr
                      ) f on t.product=f.val ';
    end if;
  exception
    when others then
      fmp_log.LOGERROR;
  end;

  procedure FMSP_ImpST(pIn_vTablename in varchar2,
                       pIn_tSwitches  in g_FMRT_Switches,
                       pIn_vFMUSER    in varchar2,
                       pIn_vDesc      in varchar2,
                       pOut_cSelect   out clob,
                       pOut_cFrom     out clob)
  --*****************************************************************
    -- Description: process switch of sale territory
    --
    -- Parameters:
    --       pIn_vTablename
    --       pIn_tSwitches
    --       pIn_vFMUSER
    --       pIn_vDesc
    --       pOut_cSelect
    --       pOut_cFrom
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    nAttrNO      number;
    vSql         varchar2(4000);
    nCurrentTime number;
    nRootID      number;
    vGeoName     geo.g_cle%type;
    vStrSql      varchar2(20);
  begin
    if pIn_tSwitches.nogeo then
      pOut_cselect := ',' || g_vChr1 || ' stname,' || g_nZero || ' stid,' ||
                      g_vChr1 || ' stdesc ';
      return;
    end if;
    if pIn_tSwitches.key_geo is not null then
      vGeoName := pIn_tSwitches.key_geo;
      vStrSql  := vGeoName;
    
    elsif pIn_tSwitches.key_dis_default is not null then
      vGeoName := pIn_tSwitches.key_dis_default;
      vStrSql  := 'nvl(t.sales, ' || vGeoName || ')';
    else
      vStrSql := ' t.sales ';
    end if;
  
    if pIn_tSwitches.geo then
      -- only import the data matched
      null;
    else
      -- all data must be imp
      --current time number
      if pIn_tSwitches.geo_n0crt <> 0 then
        nCurrentTime := F_ConvertDateToOleDateTime(sysdate);
        --get root id;
        nRootID := FMP_Dimesion.FMF_GetRoot(pIn_nDimesion => 2);
      
        vSql := 'insert /*+ append */ into geo(geo_em_addr,g_cle,g_desc,user_create_geo,date_create_geo, geo1_em_addr)
          select seq_geo.nextval,' || vStrSql || ',''' ||
                pIn_vDesc || ''',''' || pIn_vFMUSER || ''',' ||
                nCurrentTime || ',' || nRootID || '
            from ' || pIn_vTablename || ' t
           where rowid =(select max(rowid) from ' ||
                pIn_vTablename ||
                ' t2 where t.sales=t2.sales ) and not exists (select 1 from geo g where ' ||
                vStrSql || ' = g.g_cle)';
        FMSP_execsql(pIn_cSql => vSql);
      end if;
    end if;
  
    if pIn_tSwitches.geo_n0crt = 0 then
      pOut_cselect := ', ' || vStrSql ||
                      '  stname,g.geo_em_addr stid,g.g_desc stdesc';
      pOut_cFrom   := ' left join geo g on g.g_cle=' || vStrSql;
    else
      nAttrNo      := pIn_tSwitches.geo_n0crt;
      pOut_cselect := ',g.g_cle stname,g.stid,g.stdesc ';
      pOut_cFrom   := '  left join (select g.g_cle stname ,v.val,g.geo_em_addr stid,g.g_desc stdesc
                      from rfc r ,vct v, geo g
                      where g.geo_em_addr=r.geo8_em_addr and r.ident_crt=71 and r.numero_crt=' ||
                      nAttrNo ||
                      ' and r.vct10_em_addr=v.vct_em_addr
                      ) g on t.sales=g.val ';
    end if;
  exception
    when others then
      fmp_log.LOGERROR;
  end;

  procedure FMSP_ImpTC(pIn_vTablename in varchar2,
                       pIn_tSwitches  in g_FMRT_Switches,
                       pIn_vFMUSER    in varchar2,
                       pIn_vDesc      in varchar2,
                       pOut_cSelect   out clob,
                       pOut_cFrom     out clob)
  --*****************************************************************
    -- Description: process switch of trade channel
    --
    -- Parameters:
    --       pIn_vTablename
    --       pIn_tSwitches
    --       pIn_vFMUSER
    --       pIn_vDesc
    --       pOut_cSelect
    --       pOut_cFrom
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    nAttrNO      number;
    vSql         varchar2(4000);
    nCurrentTime number;
    nRootID      number;
    vDisName     geo.g_cle%type;
    vStrSql      varchar2(20);
  begin
  
    if pIn_tSwitches.b2keys or pIn_tSwitches.nodis then
      pOut_cselect := ',' || g_vChr1 || ' tcname,' || g_nZero || ' tcid, ' ||
                      g_vChr1 || ' tcdesc';
      return;
    end if;
  
    if pIn_tSwitches.key_dis is not null then
      vDisName := pIn_tSwitches.key_dis;
      vStrSql  := vDisName;
    
    elsif pIn_tSwitches.key_dis_default is not null then
      vDisName := pIn_tSwitches.key_dis_default;
      vStrSql  := 'nvl(t.trade, ' || vDisName || ')';
    else
      vStrSql := ' t.trade ';
    end if;
  
    if pIn_tSwitches.dis then
      -- only import the data matched
      null;
    else
      -- all data must be imp
      --current time number
      if pIn_tSwitches.dis_n0crt <> 0 then
        nCurrentTime := F_ConvertDateToOleDateTime(sysdate);
        --get root id;
        nRootID := FMP_Dimesion.FMF_GetRoot(pIn_nDimesion => 3);
      
        vSql := 'insert /*+ append */ into dis(dis_em_addr,d_cle,d_desc,user_create_geo,date_create_dis, dis2_em_addr)
          select seq_dis.nextval,' || vStrSql || ',''' ||
                pIn_vDesc || ''',''' || pIn_vFMUSER || ''',' ||
                nCurrentTime || ',' || nRootID || '
            from ' || pIn_vTablename || ' t
           where rowid =(select max(rowid) from ' ||
                pIn_vTablename ||
                ' t2 where t.trade=t2.trade ) and  not exists (select 1 from dis d where ' ||
                vStrSql || ' = d.d_cle)';
        FMSP_execsql(pIn_cSql => vSql);
      end if;
    end if;
  
    if pIn_tSwitches.dis_n0crt = 0 then
      pOut_cselect := ', ' || vStrSql ||
                      ' as tcname,d.dis_em_addr tcid,d.d_desc tcdesc ';
      pOut_cFrom   := ' left join dis d on d.d_cle=' || vStrSql;
    else
      nAttrNo      := pIn_tSwitches.dis_n0crt;
      pOut_cselect := ',d.d_cle stname,d.tcid,d.tcdesc';
      pOut_cFrom   := '  left join (select d.d_cle stname ,v.val,d.dis_em_addr tcid,d.d_desc tcdesc
                      from rfc r ,vct v, dis d
                      where d.dis_em_addr=r.dis9_em_addr and r.ident_crt=68 and r.numero_crt=' ||
                      nAttrNo ||
                      ' and r.vct10_em_addr=v.vct_em_addr
                      ) d on t.sales=d.val ';
    end if;
  exception
    when others then
      fmp_log.LOGERROR;
  end;

  function FMF_GetNodeInfo(pIn_vKey in varchar2, pIn_nNodeType in number)
    return clob
  --*****************************************************************
    -- Description: get sql context contains node name or desc
    --
    -- Parameters:
    --       pIn_vKey
    --       pIn_nNodeType
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    cSqlCase clob;
  begin
    cSqlCase := cSqlCase || ' case  when nvl(t.p' || pIn_vKey || ',' ||
                g_vChr1 || ')<>' || g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' ||
                g_vChr1 || ')<>' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                g_vChr1 || ') <>' || g_vChr1 || ' then';
    --P1-S1-T1
    cSqlCase := cSqlCase || ' t.p' || pIn_vKey || '  ||''' || '-' ||
                ''' || t.st' || pIn_vKey || '  ||''' || '-' || '''|| t.tc' ||
                pIn_vKey || ' ';
    cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 ||
                ') <>' || g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' ||
                g_vChr1 || ') <>' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                g_vChr1 || ')=' || g_vChr1 || ' then ';
    --P1-S1
    cSqlCase := cSqlCase || ' t.p' || pIn_vKey || '  ||''' || '-' ||
                '''|| t.st' || pIn_vKey || ' ';
    cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 ||
                ') <>' || g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' ||
                g_vChr1 || ')=' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                g_vChr1 || ') <>' || g_vChr1 || ' then ';
    --P1-T1
    cSqlCase := cSqlCase || ' t.p' || pIn_vKey || '  ||''' || '-' ||
                '''|| t.tc' || pIn_vKey || ' ';
    cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 ||
                ') <>' || g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' ||
                g_vChr1 || ')=' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                g_vChr1 || ')=' || g_vChr1 || ' then ';
    ---P1
    cSqlCase := cSqlCase || ' t.p' || pIn_vKey || '    ';
    if pIn_nNodeType = 2 then
      --aggregate node
      cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 || ')=' ||
                  g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' || g_vChr1 ||
                  ') <>' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                  g_vChr1 || ')=' || g_vChr1 || ' then ';
      --S1
      cSqlCase := cSqlCase || ' t.st' || pIn_vKey || '  ';
      cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 || ')=' ||
                  g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' || g_vChr1 || ')=' ||
                  g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' || g_vChr1 ||
                  ') <>' || g_vChr1 || ' then ';
      --T1
      cSqlCase := cSqlCase || ' t.tc' || pIn_vKey || '  ';
      cSqlCase := cSqlCase || ' when nvl(t.p' || pIn_vKey || ',' || g_vChr1 || ')=' ||
                  g_vChr1 || ' and nvl(t.st' || pIn_vKey || ',' || g_vChr1 ||
                  ') <>' || g_vChr1 || ' and nvl(t.tc' || pIn_vKey || ',' ||
                  g_vChr1 || ') <>' || g_vChr1 || ' then ';
      --S1-T1
      cSqlCase := cSqlCase || 't.st' || pIn_vKey || ' ||''' || '-' ||
                  '''||t.tc' || pIn_vKey || '  ';
    end if;
  
    cSqlCase := cSqlCase || '   end  ';
    return cSqlCase;
  end;

  procedure FMSP_ImpNode(pIn_nNodeType     in number,
                         pInOut_vTablename in out varchar2,
                         pIn_tSwitches     in g_FMRT_Switches,
                         pIn_vFMUSER       in varchar2,
                         pIn_vDesc         in varchar2,
                         pOut_nSqlCode     out number)
  --*****************************************************************
    -- Description: imp detail node data only
    --
    -- Parameters:
    --       pIn_nNodeType:1 detail node ;2 aggregate node
    --       pIn_vSwitches
    --       pIn_tSwitches
    --       pIn_vFMUSER
    --       pIn_vDesc
    --       pOut_nSqlCode
  
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        11-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
  
    vTable     varchar2(30); --create a new table only involves ID after processed 3D and its datas
    cCreate    clob;
    cSqlSelect clob;
    cSqlFrom   clob;
    cSql       clob;
  
    cPSelect  clob;
    cPFrom    clob;
    cSTSelect clob;
    cSTFrom   clob;
    cTCSelect clob;
    cTCFrom   clob;
  
    cName      clob;
    cDesc      clob;
    cInsertPvt clob;
    cInsertBdg clob;
  
    nCurrentTime number;
  begin
  
    FMSP_ImpProduct(pIn_vTablename => pInOut_vTablename,
                    pIn_tSwitches  => pIn_tSwitches,
                    pIn_vFMUSER    => pIn_vFMUSER,
                    pIn_vDesc      => pIn_vDesc,
                    pOut_cSelect   => cPSelect,
                    pOut_cFrom     => cPFrom);
  
    FMSP_ImpST(pIn_vTablename => pInOut_vTablename,
               pIn_tSwitches  => pIn_tSwitches,
               pIn_vFMUSER    => pIn_vFMUSER,
               pIn_vDesc      => pIn_vDesc,
               pOut_cSelect   => cSTSelect,
               pOut_cFrom     => cSTFrom);
  
    FMSP_ImpTC(pIn_vTablename => pInOut_vTablename,
               pIn_tSwitches  => pIn_tSwitches,
               pIn_vFMUSER    => pIn_vFMUSER,
               pIn_vDesc      => pIn_vDesc,
               pOut_cSelect   => cTCSelect,
               pOut_cFrom     => cTCFrom);
  
    cSqlSelect := ' select ' || cPSelect || cSTSelect || cTCSelect ||
                  ',t.* ';
    cSqlFrom   := ' from ' || pInOut_vTablename || ' t ' || cPFrom ||
                  cSTFrom || cTCFrom;
  
    cSql := '(' || cSqlSelect || cSqlFrom || ') t';
    --get the sql to produce detail node or aggregate node name
    cName := FMF_GetNodeInfo('name', pIn_nNodeType);
    --get the sql to produce detail node or aggregate node description
    cDesc  := FMF_GetNodeInfo('desc', pIn_nNodeType);
    vTable := FMF_GetTmpTableName;
    if pIn_nNodeType = 1 or pIn_tSwitches.p2r then
      --detail node
      nCurrentTime := F_ConvertDateToOleDateTime(sysdate);
      cInsertPvt   := 'insert /*+ append */ into pvt(pvt_em_addr,pvt_cle,pvt_desc,
                                                fam4_em_addr,geo5_em_addr,dis6_em_addr,adr_pro,adr_geo,adr_dis
                                                ,user_create_pvt,date_create_pvt)
                    select seq_pvt.nextval id,name,description,pid,stid,tcid,pid1,stid1,tcid1,cuser,time
                    from (select ';
      cInsertPvt   := cInsertPvt || '' || cName || 'name ,' || cDesc ||
                      ' description
                      ,pid,stid,tcid,pid pid1,stid stid1 ,tcid tcid1,''' ||
                      pIn_vFMUSER || ''' cuser,' || nCurrentTime ||
                      ' time ';
      cInsertPvt   := cInsertPvt || ' from ' || cSql || ' ';
    
      cInsertPvt := cInsertPvt ||
                    ' where not exists (select 1 from pvt p where p.pvt_cle=' ||
                    cName || ')) where name is not null '; -- remove comma at the 1st index
      FMSP_execsql(pIn_cSql => cInsertPvt);
    
      cInsertBdg := 'insert /*+ append */ into bdg(bdg_em_addr,ID_bdg,b_cle,bdg_desc)
                   select seq_bdg.nextval id,id_bdg,name,description from (select 80 ID_bdg , ' ||
                    cName || 'name ,' || cDesc || ' description ';
    
      cInsertBdg := cInsertBdg || ' from ' || cSql ||
                    ' where not exists (select 1 from pvt p where p.pvt_cle=' ||
                    cName || ')) where name is not null ';
      FMSP_execsql(pIn_cSql => cInsertBdg);
    
      cCreate := ' create table ' || vTable ||
                 ' as select p.pvt_em_addr nodeid,t.* as from ' || cSql ||
                 ' left join pvt p on p.pvt_cle=' || cName ||
                 ' where p.pvt_em_addr is not null ';
    elsif pIn_nNodeType = 2 or pIn_tSwitches.r2p then
      --aggregate node
      cCreate := ' create table ' || vTable ||
                 ' as select s.sel_em_addr nodeid,t.* as from ' || cSql ||
                 ' left join sel s on s.sel_bud=71 and s.sel_cle=' || cName ||
                 ' where s.sel_em_addr is not null ';
    end if;
    FMSP_ExecSql(pIn_cSql => cCreate);
    fmsp_execsql(pIn_cSql => 'drop table ' || pInOut_vTablename ||
                             ' purge ');
    pInOut_vTablename := vTable;
    pOut_nSqlCode     := 0;
  exception
    when others then
      fmp_log.LOGERROR;
  end;

  procedure FMSP_ExpNode(pIn_nNodeType   in number,
                         pIn_vTablename  in varchar2,
                         pIn_tSwitches   in g_FMRT_Switches,
                         pIn_vSQLField   in varchar2,
                         pIn_vFMUSER     in varchar2,
                         pOut_vTablename out varchar2,
                         pOut_nSqlCode   out number)
  --*****************************************************************
    -- Description: export detail node  and  aggregate node data
    --
    -- Parameters:
    --       pIn_nNodeType:1 detail node ;2 aggregate node
    --       pIn_vSwitches
    --       pIn_tSwitches
    --       pIn_vFMUSER
    --       pIn_vDesc
    --       pOut_nSqlCode
  
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        16-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   is
  
    vTable       varchar2(30); --create a new table only involves ID after processed 3D and its datas
    cCreate      clob;
    cSqlSelect   clob;
    cSqlKeyField clob;
  
    vSqlFamField          varchar2(100) := '';
    vSqlGeoField          varchar2(100) := '';
    vSqlDisField          varchar2(100) := '';
    v_nvalue              Number;
    v_strsql              varchar2(30000);
    v_strSelectField      varchar2(3000);
    pOut_vResultTablename varchar2(100);
  begin
    vSqlFamField := ' t.product,';
  
    --geo switch
    if pIn_tSwitches.nogeo then
      vSqlGeoField := '';
    elsif not pIn_tSwitches.nogeo then
      vSqlGeoField := 't.sales';
      if pIn_tSwitches.key_geo is not null then
        vSqlGeoField := '''' || pIn_tSwitches.key_geo || '''' ||
                        ' as sales';
        /*  --Temporary shielding
         elsif pIn_tSwitches.key_geo is null and
             pIn_tSwitches.key_geo_default is not null then
         vSqlGeoField := '''' || pIn_tSwitches.key_geo_default || '''' ||
                         ' as sales';
        */
      end if;
    end if;
  
    ----dis switch
    if pIn_tSwitches.nodis then
      vSqlDisField := '';
    elsif not pIn_tSwitches.nodis then
      vSqlDisField := 't.trade ';
      if pIn_tSwitches.key_dis is not null then
        vSqlDisField := '''' || pIn_tSwitches.key_dis || '''' ||
                        ' as trade';
        /* --Temporary shielding
        elsif pIn_tSwitches.key_dis is null and
              pIn_tSwitches.key_dis_default is not null then
          vSqlDisField := '''' || pIn_tSwitches.key_dis_default || '''' ||
                          ' as trade';
        */
      end if;
    
    end if;
  
    --DetailNode
    if pIn_nNodeType = 1 then
      cSqlKeyField := vSqlFamField;
    end if;
  
    --Aggnode
    if pIn_nNodeType = 2 then
      if pIn_tSwitches.r2p then
        cSqlKeyField := vSqlFamField;
      elsif not pIn_tSwitches.r2p then
        cSqlKeyField := 't.AggNode,' || vSqlFamField;
      end if;
    end if;
  
    if not pIn_tSwitches.nogeo then
      cSqlKeyField := cSqlKeyField || vSqlGeoField || ',';
    end if;
  
    if not pIn_tSwitches.nodis then
      cSqlKeyField := cSqlKeyField || vSqlDisField || ',';
    end if;
  
    pOut_vTablename := fmf_gettmptablename();
  
    cSqlSelect := 'Create Table ' || pOut_vTablename || ' AS ' ||
                  ' SELECT ' || cSqlKeyField || pIn_vSQLField || ' FROM ' ||
                  pIn_vTablename || ' t';
  
    /*    execute immediate 'truncate table t_test';
    insert into t_test values (cSqlSelect);
    commit;*/
    FMSP_ExecSql(cSqlSelect);
  
    /* --Temporary shielding
        if not pIn_tSwitches.nogeo and pIn_tSwitches.key_geo is null and
           pIn_tSwitches.geo_n0crt <> 0 then
          if instr(upper(cSqlKeyField), 'SALES') > 0 then
            --sales territory attribute
            v_nvalue := 48 + pIn_tSwitches.geo_n0crt;
            v_strsql := 'update ' || pOut_vTablename ||
                        ' set sales = (select max(g_cle) from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                        v_nvalue ||
                        ' = v.vct_em_addr  and v.val = sales )
                 where exists
                 (select 1 from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                        v_nvalue || ' = v.vct_em_addr  and v.val = sales)';
            FMSP_ExecSql(v_strsql);
            commit;
    
          end if;
        end if;
    */
  
    /*    --v_strSelectField
    
    pOut_vResultTablename := fmf_gettmptablename();
    cSqlSelect            := 'Create Table ' || pOut_vResultTablename ||
                             ' AS ' || ' SELECT ' || v_strSelectField ||
                             ' FROM ' || pOut_vTablename || ' t';
    
    execute immediate 'truncate table t_test';
    insert into t_test values (cSqlSelect);
    commit;
    FMSP_ExecSql(cSqlSelect);*/
    pOut_vTablename := pOut_vTablename;
    pOut_nSqlCode   := 0;
  exception
    when others then
      fmp_log.LOGERROR;
  end;

end FMP_Batch;
/
