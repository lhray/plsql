CREATE OR REPLACE PROCEDURE FMSP_LOG_DEMO(pIn_vValue_1 IN VARCHAR2,
                                          pIn_nValue_2 IN NUMBER,
                                          pIn_vValue_3 IN VARCHAR2,
                                          pIn_dValue_4 IN DATE)

  /****************************************************************/
  -- Author  : LSANG
  -- Created : 2013/01/07  10:28:50  AM
  -- Purpose :

  -- Public type declarations
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V1.0        07-JAN-2013     LiSang         Created.
  -- **************************************************************
 IS
  cCLOB CLOB;
BEGIN

  cCLOB := 'this is clob';
  --log begin
  FMP_LOG.FMP_SETVALUE(cCLOB);
  FMP_LOG.FMP_SETVALUE(pIn_vValue_1);
  FMP_LOG.FMP_SETVALUE(pIn_nValue_2);
  FMP_LOG.FMP_SETVALUE(pIn_vValue_3);
  FMP_LOG.FMP_SETVALUE(pIn_dValue_4);
  FMP_log.logBegin;

  IF pIn_vValue_1 IS NOT NULL THEN
    -- debug
    FMP_LOG.LogDebug(pIn_vModules => 'module id or name',
                     pIn_vText    => 'pIn_vValue_1= ' || pIn_vValue_1);
  
  END IF;
  --log end
  FMP_LOG.LOGEND;

EXCEPTION
  WHEN OTHERS THEN
    --error
    FMP_LOG.LOGERROR;
END FMSP_LOG_DEMO;
/
