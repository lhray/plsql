create or replace package FMP_ExpDimensionAttrs is

  procedure FMSP_ExpDimensionAttrs(pIn_nCommandNumber in number,
                                   pIn_nChronology    in number,
                                   pIn_vFMUSER        in varchar2,
                                   pIn_vOptions       in varchar2,
                                   pIn_vSeperator     in varchar2 default ',',
                                   pIn_chSdlt         in char,
                                   pOut_vTmpTableName out varchar2,
                                   pOut_nSqlCode      out number);
end FMP_ExpDimensionAttrs;
/
create or replace package body FMP_ExpDimensionAttrs is
  --*****************************************************************
  -- Description: export dimension(product ,product group ,sale territory and trade channel)
  --
  -- Author:  JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        12-DEC-2012     JY.Liu      Created.
  -- **************************************************************

  procedure FMSP_ExpProduct(pIn_bIsgroup       in boolean,
                            pIn_tOptions       in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                            pIn_vSeperator     in varchar2,
                            pIn_chSdlt         in char,
                            pOut_vTmpTableName out varchar2,
                            pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: export product or product group
    --
    -- Parameters:
    --       pIn_vOptions:options
    --       pIn_vSeperator:seperator between two values. chr(ascii)
    --       pIn_chSdlt:export option sdlt.if sdlt specified no "". chr(34) or chr(null)
    --       pOut_vTmpTableName:all the export datas stored in this table
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-DEC-2012     JY.Liu      Created.
    -- **************************************************************
  
   as
    nProductType  fam.id_fam%type := 80; --80 product 70 product group
    cSQL          clob;
    cSQLSelect    clob;
    cSQLLeft      clob;
    vSeperateors  varchar2(100); -- seperators
    nUMcnt        number;
    nPriceCnt     number := 3;
    nMaxAttrNo    number; -- attribute number (49..67)
    cSelection    clob;
    nSelectionID  number;
    nConditionCnt number;
  begin
  
    pOut_nSqlcode := 0;
    if pIn_bIsgroup then
      --product group
      nProductType := 70;
    end if;
  
    if pIn_tOptions.bSel then
      select nvl(max(s.sel_em_addr), -1)
        into nSelectionID
        from sel s
       where s.sel_cle = pIn_tOptions.strSel;
      if nSelectionID <> -1 then
        fmp_getdetailnodesql.FMSP_GetProductSQLBySelection(pIn_nSelectionID   => nSelectionID,
                                                           pIn_bIsGroup       => pIn_bIsgroup,
                                                           pOut_cSQL          => cSelection,
                                                           pOut_nConditionCnt => nConditionCnt,
                                                           pOut_nSqlCode      => pOut_nSqlCode);
        cSelection := ' and f1.fam_em_addr in (' || cSelection || ') ';
      end if;
    end if;
    vSeperateors := '||' || pIn_chSdlt || '||' || pIn_vSeperator ||
                    pIn_chSdlt || '||'; --'||chr(34)|| chr(44) || chr(34)||'
  
    cSQLLeft   := '  from (select f1.*, f2.f_cle keyowner
          from fam f1, fam f2
         where f1.id_fam = ' || nProductType || '
           and f2.id_fam in ( 70,1)
           and f1.fam0_em_addr = f2.fam_em_addr ' ||
                  cSelection || ' ) t';
    cSQLSelect := pIn_chSdlt || '||t.f_cle';
  
    case pIn_tOptions.nDescription
      when 0 then
        -- no lable
        null;
      when 1 then
        --short descripton
        cSQLSelect := cSQLSelect || vSeperateors || 't.f_desc_court ';
      when 2 then
        -- both short and long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.f_desc' ||
                      vSeperateors || 't.f_desc_court ';
      when -1 then
        -- default long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.f_desc';
    end case;
  
    cSQLSelect := cSQLSelect || vSeperateors || 't.keyowner ||' ||
                  pIn_chSdlt || '||' || pIn_vSeperator;
  
    if pIn_tOptions.bSuite then
      -- suite option specified
      cSQLSelect := cSQLSelect || pIn_chSdlt || '||c.f_cle||' || pIn_chSdlt || '||' ||
                    pIn_vSeperator || 'c.qute||' || pIn_vSeperator ||
                    'chr(null)||';
      cSQLLeft   := cSQLLeft || '   left join (select n.pere_pro_nmc, n.fils_pro_nmc, f.f_cle, n.qute
               from nmc n, fam f
              where n.nmc_field = 83
                and n.fils_pro_nmc = f.fam_em_addr) c
    on t.fam_em_addr = c.pere_pro_nmc ';
    end if;
  
    cSQLSelect := cSQLSelect || pIn_chSdlt || '||cy.val ||' || pIn_chSdlt;
    cSQLLeft   := cSQLLeft || ' left join (select r.fam7_em_addr, v.val
               from rfc r, vct v
              where r.numero_crt = 68
                and r.vct10_em_addr = v.vct_em_addr) cy
    on cy.fam7_em_addr = t.fam_em_addr ';
  
    --option price (tab prix)
    nPriceCnt := case
                   when pIn_tOptions.bPrix then
                    5
                   else
                    3
                 end;
    -- 1 to n n<=5
    for iprix in 1 .. nPriceCnt loop
      cSQLSelect := cSQLSelect || '||' || pIn_vSeperator || ' p.prix_' ||
                    iprix;
      if pIn_tOptions.bTab then
        cSQLSelect := cSQLSelect || '||' || pIn_vSeperator || pIn_chSdlt ||
                      ' ||p.c' || iprix || '||' || pIn_chSdlt;
      end if;
    end loop;
    cSQLLeft := cSQLLeft ||
                ' left join v_productprices p on t.fam_em_addr=p.id ';
  
    -- option (um)
    if pIn_tOptions.bUM then
      if pIn_tOptions.nUM > 0 then
        nUMcnt := pIn_tOptions.nUM;
      else
        nUMcnt := 6;
      end if;
    else
      nUMcnt := 4;
    end if;
    --from 1 to n n<=10
    for ium in 1 .. nUMcnt loop
      if ium = 1 then
        cSQLSelect := cSQLSelect || '||' || pIn_vSeperator || pIn_chSdlt ||
                      ' ||u.c69||' || pIn_chSdlt;
      else
        --process 70 to 78
        cSQLSelect := cSQLSelect || '||' || pIn_vSeperator || ' u.um' || ium || '||' ||
                      pIn_vSeperator || pIn_chSdlt || '||u.c' ||
                      to_char(68 + ium) || '||' || pIn_chSdlt;
      end if;
    end loop;
    cSQLLeft := cSQLLeft ||
                ' left join v_productunites u on t.fam_em_addr=u.id ';
  
    --attributes
    select max(v.num_crt)
      into nMaxAttrNo
      from vct v
     where v.num_crt between 49 and 67
       and v.id_crt = P_CONSTANT.v_ProductData;
    if nMaxAttrNo is not null then
      for i in 49 .. nMaxAttrNo loop
        cSQLSelect := cSQLSelect || '||' || pIn_vSeperator || pIn_chSdlt ||
                      '||a.c' || i || '||' || pIn_chSdlt;
      end loop;
    end if;
  
    cSQLLeft   := cSQLLeft ||
                  ' left join v_productvalues a on t.fam_em_addr=a.id ';
    cSQLSelect := cSQLSelect || ' t_data';
  
    pOut_vTmpTableName := fmf_gettmptablename(); --'TB' || to_char(seq_tb_pimport.nextval);
  
    cSQL := 'create table  ' || pOut_vTmpTableName || ' as select ' ||
            cSQLSelect || ' ' || cSQLLeft || 'order by t.f_cle';
  
    fmsp_execsql(pIn_cSql => cSQL);
  exception
    when others then
      pOut_nSqlcode := sqlcode;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
    
  end;

  procedure FMSP_ExpST(pIn_tOptions       in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                       pIn_vSeperator     in varchar2,
                       pIn_chSdlt         in char,
                       pOut_vTmpTableName out varchar2,
                       pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: export product or product group
    --
    -- Parameters:
    --       pIn_vOptions:options
    --       pIn_vSeperator:seperator between two values
    --       pIn_cSdlt:export option sdlt
    --       pOut_vTmpTableName:all the export datas stored in this table
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-DEC-2012     JY.Liu      Created.
    --  V7.0        05-JAN-2013     JY.Liu      The root node name is chr(1)
    -- **************************************************************
   as
    cSQL           clob;
    cSQLSelect     clob;
    cSQLLeft       clob;
    vSeperateors   varchar2(100); -- seperators
    nMaxAttrNo     number; -- attribute number (49..67)
    nExchangeRates number := 1; -- max 6
  
  begin
    vSeperateors := '||' || pIn_chSdlt || '||' || pIn_vSeperator ||
                    pIn_chSdlt || '||'; --'||chr(34)|| chr(44) || chr(34)||'
  
    cSQLLeft   := ' from (select g1.geo_em_addr, g1.g_cle, g1.g_desc, g1.g_desc_court,
                    decode(g2.g_cle,chr(1),null,g2.g_cle) keyowner
                    from geo g1, geo g2
                   where g1.geo1_em_addr = g2.geo_em_addr) t';
    cSQLSelect := pIn_chSdlt || '||t.g_cle';
  
    case pIn_tOptions.nDescription
      when 0 then
        -- no lable
        null;
      when 1 then
        --short descripton
        cSQLSelect := cSQLSelect || vSeperateors || 't.g_desc_court ';
      when 2 then
        -- both short and long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.g_desc' ||
                      vSeperateors || 't.g_desc_court ';
      when -1 then
        -- default long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.g_desc';
    end case;
    cSQLSelect := cSQLSelect || vSeperateors || 't.keyowner';
  
    if pIn_tOptions.bTab then
      if pIn_tOptions.bPrix then
        nExchangeRates := 6;
      end if;
      for i in 1 .. nExchangeRates loop
        cSQLSelect := cSQLSelect || vSeperateors || 'e.c' || i;
      end loop;
      cSQLLeft := cSQLLeft ||
                  ' left join v_saleterritorycoeff e on e.id =t.geo_em_addr ';
    end if;
  
    --attributes
    select max(v.num_crt)
      into nMaxAttrNo
      from vct v
     where v.num_crt between 49 and 67
       and v.id_crt = P_CONSTANT.v_STData;
    if nMaxAttrNo is not null then
      for i in 49 .. nMaxAttrNo loop
        cSQLSelect := cSQLSelect || vSeperateors || 'a.c' || i;
      end loop;
    end if;
  
    cSQLLeft   := cSQLLeft ||
                  ' left join v_saleterritoryvalues a on t.geo_em_addr=a.id ';
    cSQLSelect := cSQLSelect || '||' || pIn_chSdlt || ' t_data';
  
    pOut_vTmpTableName := fmf_gettmptablename(); --'TB' || to_char(seq_tb_pimport.nextval);
  
    cSQL := 'create table  ' || pOut_vTmpTableName || ' as select ' ||
            cSQLSelect || ' ' || cSQLLeft || 'order by t.g_cle';
    sp_execsql(p_Sql => cSQL);
  exception
    when others then
      pOut_nSqlcode := sqlcode;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_ExpTC(pIn_tOptions       in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                       pIn_vSeperator     in varchar2,
                       pIn_chSdlt         in char,
                       pOut_vTmpTableName out varchar2,
                       pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: export product or product group
    --
    -- Parameters:
    --       pIn_vOptions:options
    --       pIn_vSeperator:seperator between two values
    --       pIn_chSdlt:export option sdlt
    --       pOut_vTmpTableName:all the export datas stored in this table
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-DEC-2012     JY.Liu      Created.
    --  V7.0        05-JAN-2013     JY.Liu      The root node name is chr(1)
    -- **************************************************************
   as
    cSQL         clob;
    cSQLSelect   clob;
    cSQLLeft     clob;
    vSeperateors varchar2(100); -- seperators
    nMaxAttrNo   number; -- attribute number (49..67)
  
  begin
    vSeperateors := '||' || pIn_chSdlt || '||' || pIn_vSeperator ||
                    pIn_chSdlt || '||'; --'||chr(34)|| chr(44) || chr(34)||'
  
    cSQLLeft   := ' from (select d1.dis_em_addr,d1.d_cle,d1.d_desc,d1.d_desc_court,d1.rms40_em_addr,
                    decode(d2.d_cle,chr(1),null,d2.d_cle) keyowner
                    from dis d1 ,dis d2
                    where d1.dis2_em_addr=d2.dis_em_addr) t';
    cSQLSelect := pIn_chSdlt || '||t.d_cle';
  
    case pIn_tOptions.nDescription
      when 0 then
        -- no lable
        null;
      when 1 then
        --short descripton
        cSQLSelect := cSQLSelect || vSeperateors || 't.d_desc_court ';
      when 2 then
        -- both short and long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.d_desc' ||
                      vSeperateors || 't.d_desc_court ';
      when -1 then
        -- default long description
        cSQLSelect := cSQLSelect || vSeperateors || 't.d_desc';
    end case;
    cSQLSelect := cSQLSelect || vSeperateors || 't.keyowner';
  
    if pIn_tOptions.bTab then
      cSQLSelect := cSQLSelect || vSeperateors || 'r.rms_cle';
      cSQLLeft   := cSQLLeft || ' left join rms r
       on t.rms40_em_addr = r.rms_em_addr ';
    end if;
  
    --attributes
    select max(v.num_crt)
      into nMaxAttrNo
      from vct v
     where v.num_crt between 49 and 67
       and v.id_crt = P_CONSTANT.v_TCData;
    if nMaxAttrNo is not null then
      for i in 49 .. nMaxAttrNo loop
        cSQLSelect := cSQLSelect || vSeperateors || 'a.c' || i;
      end loop;
    end if;
    cSQLLeft   := cSQLLeft ||
                  ' left join v_tradechannelvalues a on t.dis_em_addr=a.id ';
    cSQLSelect := cSQLSelect || '||' || pIn_chSdlt || ' t_data';
  
    pOut_vTmpTableName := fmf_gettmptablename(); --'TB' || to_char(seq_tb_pimport.nextval);
  
    cSQL := 'create table  ' || pOut_vTmpTableName || ' as select ' ||
            cSQLSelect || ' ' || cSQLLeft || 'order by t.d_cle';
    sp_execsql(p_Sql => cSQL);
  exception
    when others then
      pOut_nSqlcode := sqlcode;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure FMSP_ExpAttributes(pIn_chDataType     in char,
                               pIn_tOptions       in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                               pIn_vSeperator     in varchar2,
                               pIn_chSdlt         in char,
                               pOut_vTmpTableName out varchar2,
                               pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: export attributes(product,sale territory ,trade channel)
    --
    -- Parameters:
    --       pIn_chDataType:product or sale territory or trade channel
    --       pIn_vSeperator:seperator between two values
    --       pIn_chSdlt:export option sdlt
    --       pOut_vTmpTableName:all the export datas stored in this table
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-DEC-2012     JY.Liu      Created.
    -- **************************************************************
   as
    cSQL clob;
  begin
    pOut_nSqlCode      := 0;
    pOut_vTmpTableName := fmf_gettmptablename(); -- 'TB' || to_char(seq_tb_pimport.nextval);
  
    if pIn_chDataType = 'S' then
      --export time series
      cSQL := 'create table ' || pOut_vTmpTableName ||
              ' as select n.num_ct - 48 ||' || pIn_vSeperator || pIn_chSdlt ||
              '||v.val_crt_serie||' || pIn_chSdlt;
    
      if pIn_tOptions.bLib then
        --lib sepecified
        cSQL := cSQL || '||' || pIn_vSeperator || pIn_chSdlt ||
                '||v.lib_crt_serie||' || pIn_chSdlt;
      end if;
      cSQL := cSQL || '||' || pIn_vSeperator || pIn_chSdlt || '||n.nom||' ||
              pIn_chSdlt || ' t_data
            from nct n, crtserie v
           where v.id_crt_serie = ' ||
              to_char(ascii(pIn_chDataType)) || '
             and n.id_ct = v.id_crt_serie
             and v.num_crt_serie between 49 and 67
             and n.num_ct = v.num_crt_serie
           order by num_ct, v.val_crt_serie nulls first';
    else
      --export 3 dimensions
      cSQL := 'create table ' || pOut_vTmpTableName ||
              ' as select n.num_ct - 48 ||' || pIn_vSeperator || pIn_chSdlt ||
              '||v.val||' || pIn_chSdlt;
    
      if pIn_tOptions.bLib then
        --lib sepecified
        cSQL := cSQL || '||' || pIn_vSeperator || pIn_chSdlt ||
                '||v.lib_crt||' || pIn_chSdlt;
      end if;
      cSQL := cSQL || '||' || pIn_vSeperator || pIn_chSdlt || '||n.nom||' ||
              pIn_chSdlt || ' t_data
            from nct n, vct v
           where v.id_crt = ' ||
              to_char(ascii(pIn_chDataType)) || '
             and n.id_ct = v.id_crt
             and v.num_crt between 49 and 67
             and n.num_ct = v.num_crt
           order by num_ct, v.val nulls first';
    end if;
    sp_execsql(p_Sql => cSQL);
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;
  procedure FMSP_ExpDimensionAttrs(pIn_nCommandNumber in number,
                                   pIn_nChronology    in number,
                                   pIn_vFMUSER        in varchar2,
                                   pIn_vOptions       in varchar2,
                                   pIn_vSeperator     in varchar2 default ',',
                                   pIn_chSdlt         in char,
                                   pOut_vTmpTableName out varchar2,
                                   pOut_nSqlCode      out number)
  --*****************************************************************
    -- Description: export product or product group
    --
    -- Parameters:
    --       pIn_nCommandNumber: 1-product attribute;2- sale territory attribute;3- trade channel attribute;501- time series attributes
    --       4- product ;5-product group ;6-sale territory;7-trade channel
    --       pIn_nChronology:retention parameter
    --       pIn_vFMUSER:retention parameter
    --       pIn_vOptions:a string contains options which seperated by '##'
    --       pIn_vSeperator:seperator character between two value
    --       pIn_chSdlt:export option sdlt
    --       pOut_vTmpTableName:all the export datas stored in this table
    --       pOut_nSqlCode:0 successful otherwise failed
    -- Error Conditions Raised:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-DEC-2012     JY.Liu      Created.
    -- **************************************************************
  
   as
    tOptions   P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    vSeperator varchar2(30) := 'chr(44)||'; --default ,
    vSdlt      varchar2(10);
    chDataType char(1);
    nASCII     number;
  begin
    pOut_nSqlCode := 0;
    Fmp_Log.FMP_SetValue(pIn_nCommandNumber);
    Fmp_Log.FMP_SetValue(pIn_nChronology);
    Fmp_Log.FMP_SetValue(pIn_vFMUSER);
    Fmp_Log.FMP_SetValue(pIn_vOptions);
    Fmp_Log.FMP_SetValue(pIn_vSeperator);
    Fmp_Log.FMP_SetValue(pIn_chSdlt);
    Fmp_Log.LOGBEGIN;
    if upper(pIn_chSdlt) = '"' then
      --if sdlt specified ,the text delimiter is '"'
      vSdlt := 'chr(34)';
    else
      -- otherwise  ''
      vSdlt := 'chr(null)';
    end if;
  
    case upper(pIn_vSeperator)
      when 'ESP' then
        --space
        vSeperator := 'chr(32)||';
      when 'SBS' then
        --the same as ESP
        vSeperator := 'chr(32)||';
      when 'STAB' then
        --tab
        vSeperator := 'chr(9)||';
      else
        --ascii to decimal ,then to ascii
        nASCII     := ascii(nvl(pIn_vSeperator, ','));
        vSeperator := 'chr(' || nASCII || ')||';
    end case;
  
    p_batchcommand_common.sp_ParseOptions(p_strOptions => pIn_vOptions,
                                          p_oOptions   => tOptions,
                                          p_nSqlCode   => pOut_nSqlCode);
    case
      when pIn_nCommandNumber in (1, 2, 3, 501) then
        chDataType := case pIn_nCommandNumber
                        when 1 then --export product attributes
                         'P'
                        when 2 then --export sale territory  attributes
                         'G'
                        when 3 then --export trade channel attributes
                         'D'
                        when 501 then --export time series attributes
                         'S'
                      end;
        FMSP_ExpAttributes(pIn_chDataType     => chDataType,
                           pIn_tOptions       => tOptions,
                           pIn_vSeperator     => vSeperator,
                           pIn_chSdlt         => vSdlt,
                           pOut_vTmpTableName => pOut_vTmpTableName,
                           pOut_nSqlCode      => pOut_nSqlCode);
      when pIn_nCommandNumber = 4 then
        FMSP_ExpProduct(pIn_bIsgroup       => true,
                        pIn_tOptions       => tOptions,
                        pIn_vSeperator     => vSeperator,
                        pIn_chSdlt         => vSdlt,
                        pOut_vTmpTableName => pOut_vTmpTableName,
                        pOut_nSqlCode      => pOut_nSqlCode);
      when pIn_nCommandNumber = 5 then
        FMSP_ExpProduct(pIn_bIsgroup       => false,
                        pIn_tOptions       => tOptions,
                        pIn_vSeperator     => vSeperator,
                        pIn_chSdlt         => vSdlt,
                        pOut_vTmpTableName => pOut_vTmpTableName,
                        pOut_nSqlCode      => pOut_nSqlCode);
      when pIn_nCommandNumber = 6 then
        FMSP_ExpST(pIn_tOptions       => tOptions,
                   pIn_vSeperator     => vSeperator,
                   pIn_chSdlt         => vSdlt,
                   pOut_vTmpTableName => pOut_vTmpTableName,
                   pOut_nSqlCode      => pOut_nSqlCode);
      when pIn_nCommandNumber = 7 then
        FMSP_ExpTC(pIn_tOptions       => tOptions,
                   pIn_vSeperator     => vSeperator,
                   pIn_chSdlt         => vSdlt,
                   pOut_vTmpTableName => pOut_vTmpTableName,
                   pOut_nSqlCode      => pOut_nSqlCode);
    end case;
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      Fmp_log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

end FMP_ExpDimensionAttrs;
/
