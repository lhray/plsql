create or replace function FMF_GetTmpTableName return varchar2
--*****************************************************************
  -- Description: get table name
  --
  -- Parameters:
  --
  -- Author:  JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        11-JAN-2013     JY.Liu      Created.
  -- **************************************************************
 is
  Result varchar2(30) := 'TBMID';
begin
  Result := substr(Result || SEQ_TBMID.nextval, 1, 30);
  return(Result);
exception
  when others then
    raise_application_error(-20004, sqlcode);
end FMF_GetTmpTablename;
/
