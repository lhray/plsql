create or replace procedure FMSP_ClobToTable(pIn_cClob     in clob,
                                             pOut_nSqlCode out number) is
  --*****************************************************************
  -- Description: Get Clob Content to Table tb_node
  -- Parameters:
  --       pIn_cClob: the string contains node id,seperated by comma
  --
  -- Error Conditions Raised:
  --
  -- Author:     JY>Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        21-MAR-2013     lzhang     Created
  --  V7.0        22-MAR-2013     Jy.liu     parse the nodelist by pipeline
  -- **************************************************************
  nCnt number;
begin
  fmp_log.LOGDEBUG(pIn_vText => 'pipeline begin');
  declare
    vSeperator  varchar2(1) := ',';
    nValueCnt   number;
    cSubStr     clob;
    nCurrentIdx number := 0;
    nNextIdx    number := 0;
    nValue      number;
    tNestTab    fmt_nest_tab_nodeid;
    vStrSql     varchar2(200);
    nParseType  number := 3; -- 1 normal  2 xmltable  3 pipeline
  begin
    pOut_nSqlCode := 0;
  
    if pIn_cClob is null then
      return;
    end if;
    --truncate table
    vStrSql := 'truncate table tb_node';
    execute immediate vStrSql;
    if substr(pIn_cClob, -1) <> ',' then
      --add comma at the last of the string
      cSubStr := pIn_cClob || vSeperator;
    else
      cSubStr := pIn_cClob;
    end if;
    -- Original
    nValueCnt := length(cSubStr) - length(replace(cSubStr, vSeperator));
    tNestTab  := fmt_nest_tab_nodeid();
    tNestTab.Extend(nValueCnt);
    case nParseType
      when 1 then
        -- nomal       
        for i in 1 .. nValueCnt loop
          nNextIdx := instr(cSubStr, vSeperator, nCurrentIdx + 1, 1); --get the next comma index
          nValue := to_number(substr(cSubStr,
                                     nCurrentIdx + 1,
                                     nNextIdx - nCurrentIdx - 1)); --get the value before teh next comma
          tNestTab(i) := fmt_obj_nodeid(nValue); --add the value to the array
          nCurrentIdx := nNextIdx;
        end loop;
        insert into tb_node
          select * from table(tNestTab);
      when 2 then
        -- xmltable      
        insert into tb_node
          select to_number(t.column_value) from xmltable(pIn_cClob) T;
      when 3 then
        -- plpeline      
        execute immediate ' insert into tb_node
          select column_value id
            from table(FMCP_FUNCTIONS.str2nlist(:1))'
          using pIn_cClob;
    end case;
  end;
  nCnt := SQL%ROWCOUNT;
  fmp_log.LOGDEBUG(pIn_vText => 'pipeline end ' || nCnt);
exception
  when others then
    pOut_nSqlCode := sqlcode;
    raise_application_error(-20004, sqlcode || sqlerrm);
end FMSP_ClobToTable;
/
