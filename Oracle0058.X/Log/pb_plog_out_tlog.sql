create or replace PACKAGE BODY PLOG_OUT_TLOG AS

--*******************************************************************************
--   NAME:   PLOG_OUT_TLOG (body)
--
--   writes the log information in the table TLOG trough the public procedure
--   log()
--
--   Ver    Date        Autor             Comment
--   -----  ----------  ----------------  ---------------------------------------
--   1.0    14.04.2008  Bertrand Caradec  First version.
--*******************************************************************************
  
PROCEDURE addRow
(
  pID         IN TLOG.id%TYPE,
  pLDate      IN TLOG.ldate%TYPE,
  pLHSECS     IN TLOG.lhsecs%TYPE,
  pLLEVEL     IN TLOG.llevel%TYPE,
  pLSECTION   IN TLOG.lsection%TYPE,
  pLUSER      IN TLOG.luser%TYPE,
  pLTEXT      IN TLOG.LTEXT%TYPE,
  pLPARAMETERS IN      TLOG.LPARAMETERS%TYPE,
  pLSQLTEXT    IN      TLOG.LSQLTEXT%TYPE
)
--*******************************************************************************
--   NAME:   addRow
--
--   PARAMETERS:
--
--      pID                ID of the log message, generated by the sequence
--      pLDate             Date of the log message (SYSDATE)
--      pLHSECS            Number of seconds since the beginning of the epoch
--      pLSection          formated call stack
--      pLUSER             database user (SYSUSER)
--      pLTEXT             log text
--
--   Private. Insert a row in the table TLOG
--
--   Ver    Date        Autor             Comment
--   -----  ----------  ---------------   ----------------------------------------
--   1.0    04.14.2008  Bertrand Caradec  Initial version
--*******************************************************************************

IS
BEGIN
  INSERT INTO TLOG(
             ID         ,
             LDate      ,
             LHSECS     ,
             LLEVEL     ,
             LSECTION   ,
             LUSER      ,
             LTEXT      ,
             LPARAMETERS,
             LSQLTEXT
             ) VALUES (
             pID,
             pLDate,
             pLHSECS,
             pLLEVEL,
             pLSECTION,
             pLUSER,
             pLTEXT,
             pLPARAMETERS,
             pLSQLTEXT);
END addRow;



PROCEDURE addRowAutonomous
(
  pID         IN TLOG.id%TYPE,
  pLDate      IN TLOG.ldate%TYPE,
  pLHSECS     IN TLOG.lhsecs%TYPE,
  pLLEVEL     IN TLOG.llevel%TYPE,
  pLSECTION   IN TLOG.lsection%TYPE,
  pLUSER      IN TLOG.luser%TYPE,
  pLTEXT      IN TLOG.LTEXT%TYPE,
  pLPARAMETERS IN      TLOG.LPARAMETERS%TYPE,
  pLSQLTEXT    IN      TLOG.LSQLTEXT%TYPE
)
--*******************************************************************************
--   NAME:   addRowAutonomous
--
--   PARAMETERS:
--
--      pID                ID of the log message, generated by the sequence
--      pLDate             Date of the log message (SYSDATE)
--      pLHSECS            Number of seconds since the beginning of the epoch
--      pLSection          formated call stack
--      pLUSER             database user (SYSUSER)
--      pLTEXT             log text
--
--   Private. Insert a row in the table TLOG in an autonomous transaction.
--   The insert statement takes place in another transaction as the calling function.
--
--   Ver    Date        Autor             Comment
--   -----  ----------  ---------------   ----------------------------------------
--   1.0    04.14.2008  Bertrand Caradec  Initial version
--*******************************************************************************
IS
PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
 
 addRow
  (
   pID         => pID,
   pLDate      => pLDate,
   pLHSECS     => pLHSECS,
   pLLEVEL     => pLLEVEL,
   pLSECTION   => pLSECTION,
   pLUSER      => pLUSER,
   pLTEXT      => pLTEXT,
   pLPARAMETERS => pLPARAMETERS,
   pLSQLTEXT    => pLSQLTEXT
  );
  
  COMMIT;
  
  EXCEPTION WHEN OTHERS THEN
      ROLLBACK;
      RAISE;
END addRowAutonomous;

PROCEDURE log
(
    pCTX        IN OUT NOCOPY PLOGPARAM.LOG_CTX                ,  
    pID         IN       TLOG.id%TYPE                      ,
    pLDate      IN       TLOG.ldate%TYPE                   ,
    pLHSECS     IN       TLOG.lhsecs%TYPE                  ,
    pLLEVEL     IN       TLOG.llevel%TYPE                  ,
    pLSECTION   IN       TLOG.lsection%TYPE                ,
    pLUSER      IN       TLOG.luser%TYPE                   ,
    pLTEXT      IN       TLOG.LTEXT%TYPE                   ,
    pLPARAMETERS IN      TLOG.LPARAMETERS%TYPE             ,
    pLSQLTEXT    IN      TLOG.LSQLTEXT%TYPE
) 
--*******************************************************************************
--   NAME:   log
--
--   PARAMETERS:
--
--      pCTX               log context
--      pID                ID of the log message, generated by the sequence
--      pLDate             Date of the log message (SYSDATE)
--      pLHSECS            Number of seconds since the beginning of the epoch
--      pLSection          formated call stack
--      pLUSER             database user (SYSUSER)
--      pLTEXT             log text
--
--   Public. Insert a row in the table TLOG.
--   According to the context configuration, the insert statement may take place
--   in an autonomous transaction (default configuration)
--
--   Ver    Date        Autor             Comment
--   -----  ----------  ---------------   ----------------------------------------
--   1.0    04.14.2008  Bertrand Caradec  Initial version
--*******************************************************************************
IS
BEGIN
  IF pCTX.USE_LOGTABLE = TRUE THEN
    IF pCTX.USE_OUT_TRANS = FALSE THEN
      -- insert the row using the same transaction as the calling function
      addRow(pID         => pID,
             pLDate      => pLDate,
             pLHSECS     => pLHSECS,
             pLLEVEL     => pLLEVEL,
             pLSECTION   => pLSECTION,
             pLUSER      => pLUSER,
             pLTEXT      => pLTEXT,
             pLPARAMETERS => pLPARAMETERS,
             pLSQLTEXT    => pLSQLTEXT);
    ELSE
      -- insert the row using an autonomous transaction
      addRowAutonomous(pID         => pID,
                       pLDate      => pLDate,
                       pLHSECS     => pLHSECS,
                       pLLEVEL     => pLLEVEL,
                       pLSECTION   => pLSECTION,
                       pLUSER      => pLUSER,
                       pLTEXT      => pLTEXT,
                       pLPARAMETERS => pLPARAMETERS,
                       pLSQLTEXT    => pLSQLTEXT);
    END IF;
  END IF;
END log;

PROCEDURE purge(
  pDateMax      IN DATE DEFAULT NULL                                   
) 
--******************************************************************************
--   NAME:   purge
--
--  PARAMETERS:
--
--        pDateMax         Limit date before which old records are deleted
--    
--   Public. Deletes all records of the table TLOG with a date smaller than
--   the parameter pDateMax. If no date is specificated, the table is entirely deleted.
--
--   Ver    Date        Autor             Comment
--   -----  ----------  ---------------   --------------------------------------
--   1.0    17.04.2008  Bertrand Caradec  Initial version
--******************************************************************************

IS
BEGIN

  IF pDateMax IS NOT NULL THEN
    DELETE FROM TLOG 
    WHERE ldate < pDateMax;
  ELSE
    DELETE FROM TLOG;
  END IF;
  
  EXCEPTION 
     WHEN OTHERS THEN
       RAISE;
END purge;

-- end of the package
END;
/
