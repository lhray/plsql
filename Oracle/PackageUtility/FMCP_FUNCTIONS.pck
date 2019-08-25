create or replace package FMCP_FUNCTIONS is

  -- Author  : LZHANG
  -- Created : 2/19/2013 4:47:27 PM
  -- Purpose :

  -- Public type declarations
  function FMCF_GETXMLKEYVALUE(pIn_bBLOB in blob, pIn_vKeyName in varchar2)
    return varchar2;

  function FMCF_GETXMLKEYVALUE(pIn_cCLOB in clob, pIn_vKeyName in varchar2)
    return varchar2;

  function str2nlist(pin_str        in clob,
                     pIn_vSeperator in varchar2 default ',')
    return fmt_tnlists
    pipelined
    parallel_enable;
  function FMF_String2List(pin_str in clob, pIn_vSeperator in char)
    return FMT_type
    pipelined
    parallel_enable;
  function FMF_GetConditionBySel(pIn_SelName in varchar2) return varchar2;
  function FMCF_NumberFormat(pIn_nData in number, pIn_nDecimal in number)
    return number;
  function FMCF_NumberFormatSQL(pIn_vDataColumn in varchar2,
                                pIn_nDecimal    in number) return varchar2;

end FMCP_FUNCTIONS;
/
create or replace package body FMCP_FUNCTIONS is
  --*****************************************************************
  -- Description: analyze XML GET  KEYNAME'S VALUE
  --
  -- Parameters:
  --
  -- Author:  Lei Zhang
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        17-Feb-2013     Lei Zhang      Created.
  -- **************************************************************
  function FMCF_GETXMLKEYVALUE(pIn_cCLOB in clob, pIn_vKeyName in varchar2)
    return varchar2 is
    --*****************************************************************
    -- Description: analyze XML GET  KEYNAME'S VALUE  FROM CLOB
    --
    -- Parameters:
    --
    -- Author:  Lei Zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        17-Feb-2013     Lei Zhang      Created.
    -- **************************************************************
  begin
    declare
      -- Local variables here
    
      v_parser   dbms_xmlparser.Parser;
      v_doc      dbms_xmldom.DOMDocument;
      v_nodelist dbms_xmldom.DOMNodeList;
      v_node     dbms_xmldom.DOMNode;
      v_val      varchar2(1000) := null;
    begin
      if pIn_cCLOB is not null then
        v_parser := dbms_xmlparser.newParser;
        dbms_xmlparser.parseClob(v_parser, pIn_cCLOB);
        v_doc      := dbms_xmlparser.getdocument(v_parser);
        v_nodelist := dbms_xmldom.getelementsbytagname(v_doc, pIn_vKeyName);
        v_node     := dbms_xmldom.item(v_nodelist, 0);
        v_val      := dbms_xmldom.getNodeName(v_node);
        v_val      := dbms_xmldom.getNodeValue(dbms_xmldom.getFirstChild(v_node));
        dbms_xmlparser.freeParser(v_parser);
      end if;
      return v_val;
    exception
      when others then
        raise_application_error(-20004, sqlcode);
    end;
  end FMCF_GETXMLKEYVALUE;

  function FMCF_GETXMLKEYVALUE(pIn_bBLOB in blob, pIn_vKeyName in varchar2)
    return varchar2 is
    --*****************************************************************
    -- Description: analyze XML GET  KEYNAME'S VALUE  FROM BLOB
    --
    -- Parameters:
    --
    -- Author:  Lei Zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        17-Feb-2013     Lei Zhang      Created.
    -- **************************************************************
  begin
    declare
      -- Local variables here
      v_clob         clob;
      v_amount       number := DBMS_LOB.LOBMAXSIZE;
      v_dest_offset  number := 1;
      v_src_offset   number := 1;
      v_lang_context number := DBMS_LOB.DEFAULT_LANG_CTX;
      v_warning      number;
    
      v_val varchar2(1000) := null;
    begin
      dbms_lob.createtemporary(v_clob, true, dbms_lob.session);
      dbms_lob.convertToClob(dest_lob     => v_clob,
                             src_blob     => pIn_bBLOB,
                             amount       => v_amount,
                             dest_offset  => v_dest_offset,
                             src_offset   => v_src_offset,
                             blob_csid    => nls_charset_id('UTF8'),
                             lang_context => v_lang_context,
                             warning      => v_warning);
      if v_clob is not null then
        v_val := FMCF_GETXMLKEYVALUE(pIn_cCLOB    => v_clob,
                                     pIn_vKeyName => pIn_vKeyName);
      end if;
      dbms_lob.freetemporary(v_clob);
      return v_val;
    exception
      when others then
        raise_application_error(-20004, sqlcode);
    end;
  end FMCF_GETXMLKEYVALUE;

  --*****************************************************************
  -- Description: string to number list
  --
  -- Parameters: pin_str clob
  --
  -- Author:  Lei Zhang
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0       20-Mar-2013     Yi Zhu       Created.
  -- **************************************************************
  function str2nlist(pin_str        in clob,
                     pIn_vSeperator in varchar2 default ',')
    return fmt_tnlists
    pipelined
    parallel_enable as
    v_clob     clob;
    v_post     pls_integer := 0;
    v_nextpost pls_integer := 0;
    v_cnt      pls_integer;
  begin
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
    dbms_lob.append(dest_lob => v_clob, src_lob => pin_str);
    if substr(pin_str, -1) <> pIn_vSeperator then
      dbms_lob.append(dest_lob => v_clob, src_lob => pIn_vSeperator);
    end if;
  
    v_cnt := length(v_clob) - length(replace(v_clob, pIn_vSeperator));
    for i in 1 .. v_cnt loop
      v_nextpost := dbms_lob.instr(v_clob, pIn_vSeperator, v_post + 1, 1);
      pipe row(to_number(dbms_lob.substr(v_clob,
                                         v_nextpost - v_post - 1,
                                         v_post + 1)));
      v_post := v_nextpost;
    end loop;
    return;
  end;

  function FMF_GetConditionBySel(pIn_SelName in varchar2) return varchar2 is
    vResult varchar2(4000) := '';
  begin
    declare
      sCursor sys_refcursor;
      cSql    clob;
      type selConditionType is record(
        n0_cdt     cdt.rcd_cdt%type,
        rcd_cdt    cdt.n0_cdt%type,
        operant    cdt.operant%type,
        n0_val_cdt cdt.n0_val_cdt%type,
        adr_cdt    cdt.adr_cdt%type);
      selCondition selConditionType;
    begin
      cSql := 'select cdt.n0_cdt,cdt.rcd_cdt, cdt.operant, cdt.n0_val_cdt, cdt.adr_cdt
    from cdt, sel
   where cdt.sel11_em_addr = sel.sel_em_addr
     and sel.sel_cle=''' || pIn_SelName || '''';
      open sCursor for cSql;
      vResult := ' ';
      loop
        fetch sCursor
          into selCondition;
        exit when sCursor%notfound;
        vResult := vResult || to_char(selCondition.n0_cdt) || ',' ||
                   to_char(selCondition.rcd_cdt) || ',' ||
                   to_char(selCondition.operant) || ',' ||
                   to_char(selCondition.n0_val_cdt) || ',' ||
                   to_char(selCondition.adr_cdt) || ';';
      end loop;
      return(vResult);
    end;
  end FMF_GetConditionBySel;

  function FMCF_NumberFormat(pIn_nData in number, pIn_nDecimal in number)
    return number
  --*****************************************************************
    -- Description: formant number return the result
    --
    -- Parameters:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        03-APR-2013     JY.Liu     Created.
    -- **************************************************************
   is
    vFormant varchar2(120) := 'fm9999999999999999999990';
    result   number;
  begin
    if pIn_nDecimal = 0 then
      result := round(pIn_nData);
    else
      for i in 1 .. pIn_nDecimal loop
        if i = 1 then
          vFormant := vFormant || '.0';
        else
          vFormant := vFormant || '0';
        end if;
      end loop;
      result := to_char(pIn_nData, vFormant);
    end if;
    return result;
  end;

  function FMCF_NumberFormatSQL(pIn_vDataColumn in varchar2,
                                pIn_nDecimal    in number) return varchar2
  --*****************************************************************
    -- Description: formant number but return a sql
    --
    -- Parameters:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        03-APR-2013     JY.Liu     Created.
    -- **************************************************************
   is
    vFormant varchar2(120) := '''fm9999999999999999999990';
    vResult  varchar2(1024);
  begin
    if pIn_nDecimal = 0 then
      vResult := ' round(' || pIn_vDataColumn || ') ';
    else
      for i in 1 .. pIn_nDecimal loop
        if i = 1 then
          vFormant := vFormant || '.0';
        else
          vFormant := vFormant || '0';
        end if;
      end loop;
      vResult := ' to_char(' || pIn_vDataColumn || ' , ' || vFormant ||
                 ''')';
    end if;
    return vResult;
  end;

  function FMF_String2List(pin_str in clob, pIn_vSeperator in char)
    return FMT_type
    pipelined
    parallel_enable
  --*****************************************************************
    -- Description: parse the string to a list
    --
    -- Parameters:
    --
    -- Author:  JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        27-APR-2013     JY.Liu     Created.
    -- **************************************************************
   as
    v_clob     clob;
    v_post     pls_integer := 0;
    v_nextpost pls_integer := 0;
    v_cnt      pls_integer;
  begin
    dbms_lob.createtemporary(lob_loc => v_clob, cache => true);
    dbms_lob.append(dest_lob => v_clob, src_lob => pin_str);
    if substr(pin_str, -1) <> pIn_vSeperator then
      dbms_lob.append(dest_lob => v_clob, src_lob => pIn_vSeperator);
    end if;
  
    v_cnt := length(v_clob) - length(replace(v_clob, pIn_vSeperator));
    for i in 1 .. v_cnt loop
      v_nextpost := dbms_lob.instr(v_clob, pIn_vSeperator, v_post + 1, 1);
      pipe row(dbms_lob.substr(v_clob, v_nextpost - v_post - 1, v_post + 1));
      v_post := v_nextpost;
    end loop;
    return;
  end;

end FMCP_FUNCTIONS;
/
