CREATE OR REPLACE PACKAGE FMP_LOG IS

  TYPE g_ARRAY IS ARRAY(5000) OF CLOB;

  PROCEDURE LOGDEBUG(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                     pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                     pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL);

  PROCEDURE LOGINFO(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                    pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                    pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL);

  PROCEDURE LOGBEGIN(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL);

  PROCEDURE LOGEND(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL);

  PROCEDURE LOGERROR(pIn_cSqlText  IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                     pIn_vTextCode IN VARCHAR2 DEFAULT SQLCODE,
                     pIn_vTextErrm IN VARCHAR2 DEFAULT SQLERRM,
                     pIn_vModules  IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL);

  PROCEDURE LogCrucInfo(pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                        pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                        pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL);

  PROCEDURE FMP_DEL(pIn_dDate DATE DEFAULT NULL);

  PROCEDURE FMP_SetValue(pIn_vVar2 VARCHAR2);

  PROCEDURE FMP_SetValue(pIn_nNum NUMBER);

  PROCEDURE FMP_SetValue(pIn_dDate DATE);

  PROCEDURE FMP_SetValue(pIn_cClob CLOB);

  PROCEDURE FMP_SetValue(pIn_bval boolean);

END;
/
CREATE OR REPLACE PACKAGE BODY FMP_LOG IS

  T_ARRAY FMP_LOG.G_ARRAY := FMP_LOG.G_ARRAY();

  gc_nDefault_Level       CONSTANT TLOG.LLEVEL%TYPE := 80;
  gc_vDefault_Begin       CONSTANT VARCHAR2(10) := 'BEGIN';
  gc_vDefault_End         CONSTANT VARCHAR2(10) := 'END';
  gc_vDefault_Table_Nname CONSTANT VARCHAR2(100) := 'LOG_OPERATION';

  FUNCTION FMF_GetProcedureName(pIn_vLine     in varchar2,
                                pIn_vUserCode in varchar2) RETURN VARCHAR2 IS
    --*****************************************************************
    -- Description: this function is get procedure's name
    --
    -- Parameters:
    -- pIn_vLine  the content of callstack
    -- pIn_vUserCode the name of the  package which the procedure belonged
    -- Error Conditions Raised:
    -- Return:
    -- the procedure's name or the name of the  package which the procedure belonged
    -- Author:   Lei Zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        28-JAN-2013     Lei Zhang         Created.
    -- **************************************************************
    nSqlcode number;
    vSQL     varchar2(2000);
  BEGIN
    declare
      c_cEndOfField CONSTANT CHAR(1) := chr(32);
      vLineNum          VARCHAR2(4000);
      vPackageName      VARCHAR2(4000);
      nIndexBegin       number := 0;
      nIndexEnd         number := 0;
      sGetProcedureName sys_refcursor;
      vProcedureName    VARCHAR2(4000) := ' ';
      vContent          varchar2(2000);
      nNthBlank         number;
    begin
      if instr(lower(pIn_vLine), 'package body') > 0 then
        --locate the blank to 5 if the PLSQL block is package body
        nNthBlank := 5;
      else
        --locate the blank to 4 if the PLSQL block is procedure function ...
        nNthBlank := 4;
      end if;
      nIndexBegin := instr(pIn_vLine, c_cEndOfField, -1, nNthBlank);
      nIndexEnd   := instr(pIn_vLine, c_cEndOfField, -1, nNthBlank - 1);
      if nIndexEnd = 0 then
        return pIn_vUserCode;
      end if;
      vLineNum := substr(pIn_vLine,
                         nIndexBegin + 1,
                         nIndexEnd - nIndexBegin - 1);
      --vLineNum     := nvl(vLineNum, 0);
      nIndexBegin  := 0;
      nIndexBegin  := instr(pIn_vUserCode, '.');
      vPackageName := substr(pIn_vUserCode, nIndexBegin + 1);

      vSQL := 'select procedureName from (select trim(substr(upper(trim(text)),instr(upper(trim(text)),''PROCEDURE'')+length(''PROCEDURE''),instr(upper(trim(replace(text,''('','' ''))),'' '',1,2)-instr(upper(trim(text)),''PROCEDURE'')-length(''PROCEDURE'') )) procedureName ,row_number() over(order by line desc ) r from user_source where  name=upper(''' ||
              vPackageName || ''') and line<=' || vLineNum ||
              'and TYPE=''PACKAGE BODY'' and lower(Text) like ''%procedure%'') where r=1';
      open sGetProcedureName for vSQL;
      if sGetProcedureName%notfound then
        vContent := pIn_vUserCode;
      else
        fetch sGetProcedureName
          into vProcedureName;
        if vProcedureName is null then
          open sGetProcedureName for 'select procedureName from (select trim(substr(upper(trim(text)),instr(upper(trim(text)),''PROCEDURE'')+length(''PROCEDURE''),instr(upper(trim(replace(text,''('','' ''))),'' '',1,2)-instr(upper(trim(text)),''PROCEDURE'')-length(''PROCEDURE'') )) procedureName ,row_number() over(order by line desc ) r from user_source where  name=upper(''' || vPackageName || ''') and line<=' || vLineNum || 'and TYPE=''PACKAGE BODY'' and lower(Text) like ''%procedure%'') where r=2';
          if sGetProcedureName%notfound then
            vContent := pIn_vUserCode;
          else
            fetch sGetProcedureName
              into vProcedureName;
          end if;
        end if;

        vContent := pIn_vUserCode || '.' || vProcedureName;
      end if;
      close sGetProcedureName;
      return vContent;
    exception
      when others then
        close sGetProcedureName;
        nSqlcode := sqlcode;
        return pIn_vUserCode;
    end;

  END FMF_GetProcedureName;

  FUNCTION FMF_GetCallStack RETURN VARCHAR2 IS
    --*****************************************************************
    -- Description: this function is get CallStack.
    --
    -- Parameters:
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    --  V7.0        28-JAN-2013     LeiZhang       get procedure's name
    -- **************************************************************
    c_cEndOfLine  CONSTANT CHAR(1) := chr(10);
    c_cEndOfField CONSTANT CHAR(1) := chr(32);
    nptFinLigne NUMBER;
    nptDebLigne NUMBER;
    nptDebCode  NUMBER;
    ncpt        NUMBER;
    vAllLines   VARCHAR2(4000);
    vResult     VARCHAR2(4000);
    vLine       VARCHAR2(4000);
    vUserCode   VARCHAR2(4000);
    vMyName     VARCHAR2(2000) := '.FMP_LOG';
  BEGIN
    vAllLines := DBMS_UTILITY.FORMAT_CALL_STACK;

    ncpt        := 2;
    nptFinLigne := LENGTH(vAllLines);
    nptDebLigne := nptFinLigne;

    WHILE nptFinLigne > 0 AND nptDebLigne > 83 LOOP
      nptDebLigne := INSTR(vAllLines, c_cEndOfLine, -1, ncpt) + 1;
      ncpt        := ncpt + 1;
      -- process the line
      vLine      := SUBSTR(vAllLines,
                           nptDebLigne,
                           nptFinLigne - nptDebLigne);
      nPtdebCode := INSTR(vLine, c_cEndOffIeld, -1, 1);
      vUserCode  := SUBSTR(vLine, nPtdebCode + 1);

      IF INSTR(vUserCode, vMyName) = 0 THEN
        IF ncpt > 3 THEN
          vUserCode := FMF_GetProcedureName(vLine, vUserCode);
          vResult   := vResult || '-->';
        END IF;
        vResult := vResult || vUserCode;
      END IF;
      nptFinLigne := nptDebLigne - 1;
    END LOOP;

    RETURN vResult;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN 'block';

  END FMF_GetCallStack;

  FUNCTION FMF_GetParameter(pIn_aPar_Array IN G_ARRAY) RETURN CLOB
  --*****************************************************************
    -- Description: this function is split the array,return clob.
    --
    -- Parameters:
    --            pIn_aPar_Array
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   AS
    cPar CLOB;

  BEGIN
    IF pIn_aPar_Array IS NULL THEN
      RETURN NULL;
    END IF;

    FOR X IN 1 .. pIn_aPar_Array.COUNT LOOP
      IF cPar IS NULL THEN
        cPar := '[' || pIn_aPar_Array(X) || ']';
      ELSE
        cPar := cPar || ',[' || pIn_aPar_Array(X) || ']';
      END IF;
    END LOOP;

    RETURN cPar;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN NULL;

  END FMF_GetParameter;

  FUNCTION FMF_GetTask(pIn_vTask IN LOG_OPERATION.LTASK%TYPE) RETURN VARCHAR2
  --*****************************************************************
    -- Description: this function is get task name,return task name.
    --
    -- Parameters:
    --            pIn_vTask
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

    nTask1 NUMBER;
    nTask2 NUMBER;

  BEGIN
    IF pIn_vTask = 'block' OR pIn_vTask IS NULL THEN
      RETURN pIn_vTask;
    END IF;

    nTask1 := INSTR(pIn_vTask, '.');
    nTask2 := INSTR(pIn_vTask, '-->', 1, 2);
    IF nTask2 = 0 THEN
      RETURN SUBSTR(pIn_vTask, nTask1 + 1, LENGTH(pIn_vTask) - nTask1);
    ELSE
      RETURN SUBSTR(pIn_vTask, nTask1 + 1, nTask2 - nTask1 - 1);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN 'block';
  END FMF_GetTask;

  FUNCTION FMF_GetLevel(pIn_vCode IN LOG_OPERATION_LEVEL.LCODE%TYPE)
    RETURN LOG_OPERATION.LLEVEL%TYPE
  --*****************************************************************
    -- Description: this function is return log level .
    --
    -- Parameters:
    --            pIn_vCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS
    nRet     LOG_OPERATION.LLEVEL%TYPE;
    nRet_MAX LOG_OPERATION.LLEVEL%TYPE;

  BEGIN
    BEGIN
      SELECT LLEVEL
        INTO nRet_max
        FROM LOG_OPERATION_LEVEL
       WHERE LTYPE = '1'
         AND ROWNUM = 1;
    EXCEPTION
      WHEN OTHERS THEN
        nRet_max := gc_nDefault_Level;
    END;

    BEGIN
      SELECT LLEVEL
        INTO nRet
        FROM LOG_OPERATION_LEVEL
       WHERE LCODE = pIn_vCode;
    EXCEPTION
      WHEN OTHERS THEN
        nRet := gc_nDefault_Level;
    END;

    IF nRet > nRet_max THEN
      RETURN NULL;
    ELSE
      RETURN nRet;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      RETURN gc_nDefault_Level;
  END FMF_GetLevel;

  PROCEDURE FMP_Clear_Array
  --*****************************************************************
    -- Description: this function is DELETE array .
    --

    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN
    T_ARRAY.DELETE;

  END FMP_Clear_Array;

  PROCEDURE FMP_AddRow(pIn_nId         IN LOG_OPERATION.ID%TYPE,
                       pIn_dDate       IN LOG_OPERATION.LDATE%TYPE,
                       pIn_nLevel      IN LOG_OPERATION.LLEVEL%TYPE,
                       PIN_vSection    IN LOG_OPERATION.LSECTION%TYPE,
                       pIn_vText       IN LOG_OPERATION.LTEXT%TYPE,
                       pIn_cParameters IN LOG_OPERATION.LPARAMETERS%TYPE,
                       pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE,
                       pIn_vTask       IN LOG_OPERATION.LTASK%TYPE,
                       pIn_vModules    IN LOG_OPERATION.LPARAMETERS%TYPE,
                       PIN_vSessionId  IN LOG_OPERATION.LSESSIONID%TYPE)

    --******************************************************************************
    --   Description: this function is  insert  db,
    --
    --  PARAMETERS:
    --
    --        nId                 Sequence  ID
    --        pIn_dDate           log date
    --        pIn_nLevel          the log level info
    --        PIN_vSection        CallStack
    --        pIn_vText           debug info
    --        pIn_cParameters     procedure paramtetr
    --        pIn_cSqlText        sql text
    --        pIn_vTask           Task  name
    --        pIn_vModules        MODULES name
    --        PIN_vSessionId      Task session ID.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0   07-JAN-2013   LiSang                Create
    --******************************************************************************
   as
    pragma autonomous_transaction;
  BEGIN
    INSERT INTO LOG_OPERATION
      (ID,
       LDATE,
       LLEVEL,
       LSECTION,
       LTEXT,
       LTASK,
       LPARAMETERS,
       LMODULES,
       LSQLTEXT,
       LSESSIONID)
    VALUES
      (pIn_nId,
       pIn_dDate,
       pIn_nLevel,
       PIN_vSection,
       pIn_vText,
       pIn_vTask,
       pIn_cParameters,
       pIn_vModules,
       pIn_cSqlText,
       pIn_vSessionId);
    COMMIT;

  exception
    when others then
      rollback;

  END FMP_AddRow;

  PROCEDURE FMP_SetLog(pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                       pIn_aParameters IN LOG_OPERATION.LPARAMETERS%TYPE DEFAULT NULL,
                       pIn_nLevel      IN LOG_OPERATION_LEVEL.LCODE%TYPE,
                       pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                       pIn_vText       IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is  write log,
    --
    -- Parameters:
    --            pIn_vText
    --            pIn_aParameters
    --            pIn_cSqlText
    --            pIn_vModules
    --            pIn_nLevel
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS
    vSection   LOG_OPERATION.LSECTION%TYPE;
    vSessionId LOG_OPERATION.LSESSIONID%TYPE;
    vTask      LOG_OPERATION.LTASK%TYPE;
    nId        LOG_OPERATION.ID%TYPE;
  BEGIN

    IF pIn_nLevel IS NULL THEN
      RETURN;
    END IF;

    vSection   := FMF_GETCALLSTACK;
    vSessionId := USERENV('sessionID');
    vTask      := FMF_GETTASK(vSection);
    nId        := SEQ_FMLOG_ID.NEXTVAL;

    FMP_ADDROW(nId,
               SYSDATE,
               pIn_nLevel,
               vSection,
               pIn_vText,
               pIn_aParameters,
               pIn_cSqlText,
               vTask,
               pIn_vModules,
               vSessionId);

  END FMP_SetLog;

  PROCEDURE FMP_Error(pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                      pIn_vText       IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                      pIn_aParameters IN G_ARRAY DEFAULT NULL,
                      pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log error  .
    --
    -- Parameters:
    --            pIn_cSqlText
    --            pIn_vText
    --            pIn_aParameters
    --            pIn_vModules
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN

    FMP_SetLog(pIn_vModules    => pIn_vModules,
               pIn_aParameters => FMF_GETPARAMETER(pIn_aParameters),
               pIn_vText       => pIn_vText,
               pIn_cSqlText    => pIn_cSqlText,
               pIn_nLevel      => FMF_GETLEVEL('ERROR'));

  END FMP_Error;
  PROCEDURE FMP_CrucInfo_Private(pIn_vText       IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                                 pIn_aParameters IN G_ARRAY DEFAULT NULL,
                                 pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                                 pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log CRUCINFO  .
    --
    -- Parameters:
    --            pIn_vText
    --            pIn_aParameters
    --            pIn_cSqlText
    --            pIn_vTextCode
    --            pIn_vTextErrm
    --            pIn_vModules
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN

    FMP_SetLog(pIn_vModules    => pIn_vModules,
               pIn_aParameters => FMF_GETPARAMETER(pIn_aParameters),
               pIn_nLevel      => FMF_GETLEVEL('CrucInfo'),
               pIn_cSqlText    => pIn_cSqlText,
               pIn_vText       => pIn_vText);

  END FMP_CrucInfo_Private;

  PROCEDURE FMP_LogBegin_Private(pIn_aParameters IN G_ARRAY DEFAULT NULL,
                                 pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)

    --******************************************************************************
    --  Description:   PRIVATEARRAY  LOGBEGIN
    --
    --  PARAMETERS:
    --
    --        pIn_aParameters     Pocedure paramtetr
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013  LiSang                Create
    --******************************************************************************
   IS

  BEGIN

    FMP_CrucInfo_private(pIn_aParameters => pIn_aParameters,
                         pIn_vModules    => pIn_vModules,
                         pIn_vText       => gc_vDefault_Begin);

  END FMP_LogBegin_Private;

  PROCEDURE FMP_LogEnd_Private(pIn_aParameters IN G_ARRAY DEFAULT NULL,
                               pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --******************************************************************************
    --  Description:   the function is LOGEND private.
    --
    --  PARAMETERS:
    --
    --        pIn_aParameters     Pocedure paramtetr
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN

    FMP_CrucInfo_private(pIn_aParameters => pIn_aParameters,
                         pIn_vModules    => pIn_vModules,
                         pIn_vText       => gc_vDEFAULT_END);
    FMP_CLEAR_ARRAY;

  END FMP_LogEnd_Private;
  PROCEDURE FMP_Debug_Private(pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                              pIn_aParameters IN G_ARRAY DEFAULT NULL,
                              pIn_vText       IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                              pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log debug PRIVATE  .
    --
    -- Parameters:
    --            pIn_vModules
    --            pIn_aParameters
    --            pIn_vText
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN

    FMP_SetLog(pIn_vModules    => pIn_vModules,
               pIn_aParameters => FMF_GETPARAMETER(pIn_aParameters),
               pIn_nLevel      => FMF_GETLEVEL('DEBUG'),
               pIn_cSqlText    => pIn_cSqlText,
               pIn_vText       => pIn_vText);

  END FMP_Debug_Private;

  PROCEDURE FMP_LogError_Private(pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                                 pIn_vTextCode   IN VARCHAR2 DEFAULT SQLCODE,
                                 pIn_vTextErrm   IN VARCHAR2 DEFAULT SQLERRM,
                                 pIn_aParameters IN G_ARRAY DEFAULT NULL,
                                 pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --******************************************************************************
    --   Description:   the funciton is log error private . if  you definition  exception ,so
    --                  you  need input parameter pIn_vTextCode,pIn_vTextErrm.
    --
    --  PARAMETERS:
    --
    --        pIn_cSqlText        error sql text
    --        pIn_vTextCode       error code
    --        pIn_vTextErrm       error info
    --        pIn_aParameters     Pocedure paramtetr
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013  LiSang                Create
    --******************************************************************************
   IS

  BEGIN

    FMP_Error(pIn_cSqlText    => pIn_cSqlText,
              pIn_vText       => pIn_vTextErrm || ' ;p_logcode is ' ||
                                 pIn_vTextCode,
              pIn_aParameters => pIn_aParameters,
              pIn_vModules    => pIn_vModules);
    FMP_CLEAR_ARRAY;

  END FMP_LogError_Private;

  PROCEDURE FMP_Info_Private(pIn_vModules    IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                             pIn_aParameters IN G_ARRAY DEFAULT NULL,
                             pIn_vText       IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                             pIn_cSqlText    IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log info private .
    --
    -- Parameters:
    --            pIn_vModules
    --            pIn_aParameters
    --            pIn_vText
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN

    FMP_SetLog(pIn_vModules    => pIn_vModules,
               pIn_aParameters => FMF_GETPARAMETER(pIn_aParameters),
               pIn_nLevel      => FMF_GETLEVEL('INFO'),
               pIn_cSqlText    => pIn_cSqlText,
               pIn_vText       => pIn_vText);

  END FMP_Info_Private;

  PROCEDURE FMP_Del(pIn_dDate DATE DEFAULT NULL)
  --******************************************************************************
    --   Description:   the funciton si delete log data.
    --                  if parameter  "pIn_dDate" is null then truncate table .
    --
    --
    --  PARAMETERS:
    --
    --        pIn_dDate        user the date delete log info.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN

    IF pIn_dDate IS NULL THEN
      execute immediate 'truncate table ' || gc_vDefault_Table_Nname;
    ELSE
      DELETE FROM LOG_OPERATION T WHERE T.LDATE < pIn_dDate;
    END IF;

  END FMP_Del;

  PROCEDURE FMP_SetValue(PIN_VVAR2 VARCHAR2)
  --******************************************************************************
    --   Description:   type is varchar2
    --
    --  PARAMETERS:
    --
    --        PIN_VVAR2       input parameter type is varchar2.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN
    T_ARRAY.EXTEND(1);
    T_ARRAY(T_ARRAY.COUNT) := '''' || PIN_VVAR2 || '''';

  END FMP_SetValue;

  PROCEDURE FMP_SetValue(PIN_NNUM NUMBER)
  --******************************************************************************
    --   Descirption:   type is  number.
    --
    --  PARAMETERS:
    --
    --        PIN_NNUM       input parameter type is number.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN
    T_ARRAY.EXTEND(1);
    T_ARRAY(T_ARRAY.COUNT) := TO_CHAR(PIN_NNUM);
  END FMP_SetValue;

  PROCEDURE FMP_SetValue(pIn_dDate DATE) IS

  BEGIN
    T_ARRAY.EXTEND(1);
    T_ARRAY(T_ARRAY.COUNT) := TO_CHAR(pIn_dDate, 'yyyy-mm-dd HH24:mi:ss');

  END FMP_SetValue;

  PROCEDURE FMP_SetValue(PIN_CCLOB CLOB)
  --******************************************************************************
    --   Descirption:   type is CLOB
    --
    --  PARAMETERS:
    --
    --        PIN_CCLOB       input parameter type is CLOB.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN

    T_ARRAY.EXTEND(1);
    T_ARRAY(T_ARRAY.COUNT) := '''' || PIN_CCLOB || '''';
  END FMP_SetValue;

  PROCEDURE FMP_SetValue(pIn_bval boolean)
  --******************************************************************************
    --   Descirption:   type is boolean
    --
    --  PARAMETERS:
    --
    --        PIN_CCLOB       input parameter type is boolean.
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    29-JAN-2013   LiSang                Create
    --******************************************************************************
   IS
  BEGIN
    T_ARRAY.EXTEND(1);
    if pIn_bval then
      T_ARRAY(T_ARRAY.COUNT) := TO_CHAR('TRUE');
    else
      T_ARRAY(T_ARRAY.COUNT) := TO_CHAR('FALSE');
    end if;

  END FMP_SetValue;

  PROCEDURE LogBegin(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --******************************************************************************
    --  Description: this function is  logbegin , level is CRUCINFO.
    --
    --  PARAMETERS:
    --
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0   07-JAN-2013   LiSang                Create
    --******************************************************************************
   IS

  BEGIN
    FMP_LogBegin_Private(pIn_aParameters => T_ARRAY,
                         pIn_vModules    => pIn_vModules);

  END LogBegin;

  PROCEDURE LogEnd(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --******************************************************************************
    --  Description:   the function is LOGEND.
    --
    --  PARAMETERS:
    --
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************

   IS

  BEGIN
    FMP_LogEnd_Private(pIn_aParameters => T_ARRAY,
                       pIn_vModules    => pIn_vModules);

  END LogEnd;

  PROCEDURE LogInfo(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                    pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                    pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log info  .
    --
    -- Parameters:
    --            pIn_vModules
    --            pIn_vText
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN
    FMP_INFO_PRIVATE(pIn_aParameters => T_ARRAY,
                     pIn_vModules    => pIn_vModules,
                     pIn_vText       => pIn_vText,
                     pIn_cSqlText    => pIn_cSqlText);

  END LogInfo;

  PROCEDURE LogDebug(pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL,
                     pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                     pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log debug  .
    --
    -- Parameters:
    --            pIn_vModules
    --            pIn_vText
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN
    FMP_DEBUG_PRIVATE(pIn_aParameters => T_ARRAY,
                      pIn_vModules    => pIn_vModules,
                      pIn_vText       => pIn_vText,
                      pIn_cSqlText    => pIn_cSqlText);

  END LogDebug;
  PROCEDURE logCrucInfo(pIn_vText    IN LOG_OPERATION.LTEXT%TYPE DEFAULT NULL,
                        pIn_cSqlText IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                        pIn_vModules IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --*****************************************************************
    -- Description: this function is log CRUCINFO  .
    --
    -- Parameters:
    --            pIn_vText
    --            pIn_cSqlText
    --            pIn_vTextCode
    --            pIn_vTextErrm
    --            pIn_vModules
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        07-JAN-2013     LiSang         Created.
    -- **************************************************************
   IS

  BEGIN

    FMP_CrucInfo_Private(pIn_vText       => pIn_vText,
                         pIn_aParameters => T_ARRAY,
                         pIn_cSqlText    => pIn_cSqlText,
                         pIn_vModules    => pIn_vModules);

  END logCrucInfo;

  procedure FMSP_ROLLBACK as
    --*****************************************************************
    -- Description: drop temp table
    --
    -- Parameters:
    --
    -- Author:  L.Sang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        24-April-2013     L.Sang      Created.
    -- **************************************************************

    nCount number;
    cSQL   clob;
    nTname varchar2(1000);

  begin

    select count(*)
      into nCount
      from TMP_TMPTable tg, tab t
     where tg.tname = t.tname;

    if nCount = 0 then
      return;
    end if;

    for i in 1 .. nCount loop

      select t.tname
        into nTname
        from (select tg.tname, rownum rn
                from TMP_TMPTable tg, tab t
               where tg.tname = t.tname) t
       where t.rn = 1;
      cSQL := 'drop table ' || nTname || ' purge';
      execute immediate cSQL;
    end loop;
  end;

  PROCEDURE LogError(pIn_cSqlText  IN LOG_OPERATION.LSQLTEXT%TYPE DEFAULT NULL,
                     pIn_vTextCode IN VARCHAR2 DEFAULT SQLCODE,
                     pIn_vTextErrm IN VARCHAR2 DEFAULT SQLERRM,
                     pIn_vModules  IN LOG_OPERATION.LMODULES%TYPE DEFAULT NULL)
  --******************************************************************************
    --   Description:   the funciton is log error. if  you definition  exception ,so
    --                  you  need input parameter pIn_vTextCode,pIn_vTextErrm.
    --
    --  PARAMETERS:
    --
    --        pIn_cSqlText        error sql text
    --        pIn_vTextCode       error code
    --        pIn_vTextErrm       error info
    --        pIn_vModules        MODULES name
    --
    --
    --   Ver    Date        Autor             Reason for Change
    --   -----  ----------  ---------------   --------------------------------------
    --   1.0    07-JAN-2013   LiSang                Create
    --******************************************************************************

   IS

  BEGIN
    FMP_LogError_Private(pIn_cSqlText    => pIn_cSqlText,
                         pIn_aParameters => T_ARRAY,
                         pIn_vTextCode   => pIn_vTextCode,
                         pIn_vTextErrm   => pIn_vTextErrm,
                         pIn_vModules    => pIn_vModules);
    FMSP_ROLLBACK;
  END LogError;

END FMP_LOG;
/
