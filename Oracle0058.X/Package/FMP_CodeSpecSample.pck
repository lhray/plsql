CREATE OR REPLACE PACKAGE FMP_CodeSpecSample IS

  --public
  gc_nConstantName constant number := 100; --comments
  g_nVarName number; --comments
  g_eException exception; --comments

  PROCEDURE FMSP_ProcedureName(pIn_nPars     IN number,
                               pInOut_vPars  IN OUT varchar2,
                               pOut_nPars    OUT number,
                               pOut_nSqlCode OUT number);

  FUNCTION FMF_FunctionName(pIn_nPars    IN number,
                            pOut_nPars   OUT number,
                            pInOut_vPars IN OUT varchar2) RETURN number;

END FMP_CodeSpecSample;
/
CREATE OR REPLACE PACKAGE BODY FMP_CodeSpecSample IS

  --*****************************************************************
  -- Description: Describe the purpose of the object. If necessary,
  -- describe the design of the object at a very high level.
  --
  -- Author:      <your name>
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        03-JAN-1997     J.Schmoe     Created.
  --  V7.0        06-DEC-2012     San,Zhang    Change .........
  -- **************************************************************

  PROCEDURE FMSP_ProcedureName(pIn_nPars     IN number,
                               pInOut_vPars  IN OUT varchar2,
                               pOut_nPars    OUT number,
                               pOut_nSqlCode OUT number)
  --*****************************************************************
    -- Description: Describe the purpose of the object. If necessary,
    -- describe the design of the object at a very high level.
    --
    -- Parameters:
    --       pIn_nPars
    --       pInOut_vPars
    --       pOut_nPars
    --       pOut_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      <your name>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        03-JAN-1997     J.Schmoe     Created.
    --  V7.0        06-DEC-2012     San,Zhang    Change .......
    -- **************************************************************
   as
    TYPE FMT_TypeName IS TABLE OF USER_TABLES%rowtype;
  
    tVarTypeName  FMT_TypeName; -- comments
    vVariableName varchar2(200); --comments
    nVariableName USER_TABLES.NUM_ROWS%type; --comments
    lVarName      long;
    iVarName      integer;
    dVarName      date;
    cVarname      char;
  
    CURSOR cur_CursorName IS
      SELECT T.TABLE_NAME, T.TABLESPACE_NAME FROM USER_TABLES T;
  BEGIN
  
    IF nVariableName < 100 THEN
      -- comments
      null;
    ELSIF nVariableName = 100 THEN
      -- comments
      null;
    ELSE
      null;
    END IF;
  
    --comments
    FOR iMonth IN 1 .. 12 LOOP
      null;
    END LOOP;
  
  exception
    WHEN g_eException THEN
      null;
    WHEN NO_DATA_FOUND THEN
      null;
    WHEN others THEN
      null;
  END;

  FUNCTION FMF_FunctionName(pIn_nPars    IN number,
                            pOut_nPars   OUT number,
                            pInOut_vPars IN OUT varchar2) RETURN number
  --*****************************************************************
    -- Description: Describe the purpose of the object. If necessary,
    -- describe the design of the object at a very high level.
    --
    -- Parameters:
    --       pIn_nPars
    --       pOut_nPars
    --       pInOut_vPars
    -- Error Conditions Raised:
    --
    -- Author:      <your name>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        03-JAN-1997     J.Schmoe     Created.
    --  V7.0        06-DEC-2012     San,Zhang    Change .......
    -- **************************************************************
   AS
    nResult number;
  BEGIN
    nResult := 0;
    RETURN nResult;
  exception
    WHEN g_eException THEN
      null;
    WHEN NO_DATA_FOUND THEN
      null;
    WHEN others THEN
      null;
  END;

BEGIN
  SELECT COUNT(*) INTO g_nVarName FROM user_tables;
END FMP_CodeSpecSample;
/
