create or replace procedure FMISP_ClobToNestedTable(pIn_cClob     in clob,
                                                    pOut_vab      out varchar2,
                                                    pOut_nSqlCode out number)
--*****************************************************************
  -- Description: convert the long string to an nest table contains node id
  -- Parameters:
  --       pIn_cClob: the string contains node id,seperated by comma
  --       pOut_vab:the table stored node IDS
  --       pOut_nSqlCode:error code
  -- Error Conditions Raised:
  --
  -- Author:     JY>Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        11-MAR-2013     JY.Liu     Created
  -- **************************************************************
 as
  nCnt number;
begin
  fmp_log.LOGDEBUG(pIn_vText => 'pipeline begin');
  pOut_nSqlCode := 0;
  pOut_vab      := fmf_gettmptablename;
  fmsp_execsql(pIn_cSql => ' create table  ' || pOut_vab || '(id number)');
  execute immediate ' insert into ' || pOut_vab || '
                           select column_value id
            from table(FMCP_FUNCTIONS.str2nlist(:1)) '
    using pIn_cClob;
  nCnt := SQL%ROWCOUNT;
  commit;
  fmp_log.LOGDEBUG(pIn_vText => 'pipeline end ' || nCnt);
exception
  when others then
    pOut_nSqlCode := sqlcode;
    raise;
end;
/
