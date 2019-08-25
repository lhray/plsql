create or replace procedure FMSP_SaveNodeAttributeValues(pIn_nNodeType      in number,
                                                        pIn_nAttriNumber   in number,
                                                        pIn_strNodesAttris in clob,
                                                        pOut_nSqlCode      out number,
                                                        pOut_strSqlMsg     out varchar2)
--*****************************************************************
  -- Description: Save an attribute value of multiple nodes
  --              
  --              
  -- Parameters:
  --       pIn_nNodeType            : 0 ¨C Detail Node, 1 ¨C Aggregate Node 
  --       pIn_nAttriNumber         : 49~67 Number of attribute      
  --       pIn_strNodesAttris       : Array of nodes¡¯ attribute values: NodeID,AttributeValueID; NodeID,AttributeValueID; NodeID,AttributeValueID;¡­¡­
  --       pOut_nSqlCode            : error code
  --       pOut_nSqlCode            : error message
  -- Error Conditions Raised:
  --
  -- Author:     Yeyi.Sun
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        28-DEC-2012     Yeyi.Sun     Created.
  -- **************************************************************
 as

  --variant
  vSepComma   varchar2(1) := ',';
  vSepSicolon varchar2(1) := ';';
  nValueCnt   number := 0;
  nCurIdx     number := 0;
  nNextIdx    number := 0;
  nValue      number := 0;

  --variant of custom type
  NodeAttriValue    FMT_NodeAttriValue;
  arrNodeAttriValue FMT_NodeAttriValue_Array;

begin
  pOut_nSqlCode  := 0;
  pOut_strSqlMsg := '';

  if pIn_strNodesAttris is null then
    return;
  end if;

  NodeAttriValue    := FMT_NodeAttriValue(0, 0);
  nValueCnt         := length(pIn_strNodesAttris) -
                       length(replace(pIn_strNodesAttris, vSepSicolon));
  arrNodeAttriValue := FMT_NodeAttriValue_Array();
  arrNodeAttriValue.Extend(nValueCnt);

  for i in 1 .. nValueCnt loop
    --get node id from string
    nNextIdx := instr(pIn_strNodesAttris, vSepComma, nCurIdx + 1, 1);
  
    nValue                := to_number(substr(pIn_strNodesAttris,
                                              nCurIdx + 1,
                                              nNextIdx - nCurIdx - 1));
    NodeAttriValue.NodeID := nValue;
  
    nCurIdx := nNextIdx;
  
    --get attribute value id from string
    nNextIdx := instr(pIn_strNodesAttris, vSepSicolon, nCurIdx + 1, 1);
  
    nValue                      := to_number(substr(pIn_strNodesAttris,
                                                    nCurIdx + 1,
                                                    nNextIdx - nCurIdx - 1));
    NodeAttriValue.AttriValueID := nValue;
  
    arrNodeAttriValue(i) := NodeAttriValue;
  
    nCurIdx := nNextIdx;
  
  end loop;

  if pIn_nNodeType = 0 then
    --update detail nodes' attribute value
    merge into pvtcrt p
    using (select NodeID, AttriValueID from table(arrNodeAttriValue)) a
    on (p.pvt35_em_addr = a.NodeID and p.numero_crt_pvt = pIn_nAttriNumber)
    when not matched then
      insert
        (PVTCRT_EM_ADDR,
         ID_CRT_PVT,
         NUMERO_CRT_PVT,
         PVT35_EM_ADDR,
         CRTSERIE36_EM_ADDR)
      values
        (seq_pvtcrt.nextval, 0, pIn_nAttriNumber, a.NodeID, a.AttriValueID)
    when matched then
      update set p.crtserie36_em_addr = a.AttriValueID;
  end if;
  if pIn_nNodeType = 1 then
    --update aggregate nodes' attribute value  
    merge into selcrt s
    using (select NodeID, AttriValueID from table(arrNodeAttriValue)) a
    on (s.sel53_em_addr = a.NodeID and s.numero_crt_sel = pIn_nAttriNumber)
    when not matched then
      insert
        (SELCRT_EM_ADDR,
         ID_CRT_SEL,
         NUMERO_CRT_SEL,
         SEL53_EM_ADDR,
         CRTSERIE54_EM_ADDR)
      values
        (seq_pvtcrt.nextval, 0, pIn_nAttriNumber, a.NodeID, a.AttriValueID)
    when matched then
      update set s.crtserie54_em_addr = a.AttriValueID;
  end if;

exception
  when others then
    pOut_nSqlCode  := sqlcode;
    pOut_strSqlMsg := sqlerrm;
    raise_application_error(-20004, sqlcode);
  
end;
/
