create or replace procedure FMSP_ClobToNestedTable(pIn_cClob     in clob,
                                                  pOut_tNestTab out fmt_nest_tab_nodeid,
                                                  pOut_nSqlCode out number)
--*****************************************************************
  -- Description: convert the long string to an nest table contains node id
  --              use regexp_substr function
  --              note:when the length ranther than 2000 ,the procedure become slower
  -- Parameters:
  --       pIn_cClob: the string contains node id,seperated by comma
  --       pOut_tNestTab:type of t_nest_tab_nodeid
  --       pOut_nSqlCode:error code
  -- Error Conditions Raised:
  --
  -- Author:     JY>Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        20-DEC-2012     JY.Liu     Created.
  -- **************************************************************
 as
  vSeperator  varchar2(1) := ',';
  nValueCnt   number;
  cSubStr     clob;
  nCurrentIdx number := 0;
  nNextIdx    number := 0;
  nValue      number;
begin
  pOut_nSqlCode := 0;

  if pIn_cClob is null then
    return;
  end if;

  if substr(pIn_cClob, -1) <> ',' then
    --add comma at the last of the string
    cSubStr := pIn_cClob || vSeperator;
  else
    cSubStr := pIn_cClob;
  end if;

  nValueCnt     := length(cSubStr) - length(replace(cSubStr, vSeperator));
  pOut_tNestTab := fmt_nest_tab_nodeid();
  pOut_tNestTab.Extend(nValueCnt);

  for i in 1 .. nValueCnt loop
    nNextIdx := instr(cSubStr, vSeperator, nCurrentIdx + 1, 1); --get the next comma index
    nValue := to_number(substr(cSubStr,
                               nCurrentIdx + 1,
                               nNextIdx - nCurrentIdx - 1)); --get the value before teh next comma
    pOut_tNestTab(i) := fmt_obj_nodeid(nValue); --add the value to the array
    nCurrentIdx := nNextIdx;
  end loop;

exception
  when others then
    pOut_nSqlCode := sqlcode;
    raise_application_error(-20004, sqlcode||sqlerrm);
end;
/
