CREATE OR REPLACE PACKAGE FMP_UT_DBA IS

  TYPE cur_tsql IS REF CURSOR;
  --generate export file location
  g_vDictLocFileName CONSTANT VARCHAR2(40) := '/logmnr/dict.ora';
  --logminer data dictionary location
  g_vDictLocation CONSTANT VARCHAR2(40) := '/logmnr';
  --Specifies that the LogMiner dictionary is written to a flat file 
  g_vDictFileName CONSTANT VARCHAR2(20) := 'dict.ora';

  g_vUserName  VARCHAR(20) := 'FMUSER_REPOS';
  g_vTableName VARCHAR2(20) := '.FMT_LogContent';
  g_vScnTable  VARCHAR2(20) := '';

  PROCEDURE FMSP_CreateDict(pIn_vFileName VARCHAR2 DEFAULT g_vDictFileName,
                            
                            pIn_vLocation VARCHAR2 DEFAULT g_vDictLocation);

  PROCEDURE FMSP_RunLogMiner( /*pIn_iAnalysisType IN INTEGER,*/
                             --pIn_vStartTime    IN VARCHAR2 DEFAULT '',
                             /* pIn_vStartScn IN VARCHAR2 DEFAULT '',*/
                             --pIn_vEndScn       IN VARCHAR2 DEFAULT '',
                             pIn_vUserName IN VARCHAR2,
                             pIn_vFileName IN VARCHAR2,
                             pOut_nSqlCode OUT NUMBER);

END FMP_UT_DBA;
/
CREATE OR REPLACE PACKAGE BODY FMP_UT_DBA IS

  PROCEDURE FMSP_CreateDict(pIn_vFileName VARCHAR2 DEFAULT g_vDictFileName,
                            pIn_vLocation VARCHAR2 DEFAULT g_vDictLocation) IS
    --*****************************************************************
    -- Description: Let LogMiner reference involves the internal parts 
    -- of the data dictionary for their actual name,if the database table has 
    -- chanaged or analysis another log file,must recreate the dictionary
    --
    -- Parameters:
    --       pIn_vFileName      generate the filename
    --       pIn_vLocation      location
    -- Error Conditions Raised:
    --
    -- Author:      zhangxf
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        12-28-2012      zhangxf           Created.
    -- **************************************************************
  BEGIN
    dbms_logmnr_d.build(dictionary_filename => pIn_vFileName,
                        dictionary_location => pIn_vLocation,
                        options             => dbms_logmnr_d.store_in_flat_file);
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END FMSP_CreateDict;

  PROCEDURE FMSP_RunLogMiner( --pIn_iAnalysisType IN INTEGER,
                             --pIn_vStartTime    IN VARCHAR2 DEFAULT '',
                             --pIn_vStartScn IN VARCHAR2 DEFAULT '',
                             -- pIn_vEndScn       IN VARCHAR2 DEFAULT '',
                             pIn_vUserName IN VARCHAR2,
                             pIn_vFileName IN VARCHAR2,
                             pOut_nSqlCode OUT NUMBER)
  --*****************************************************************
    -- Description: Analysis online Redo log files to get sql statements,
    -- send the file to database server
    --
    -- Parameters:
    --       pIn_iAnalyzType   0:all log file;1:scn ; 2: timestamp     
    --       pIn_vStartTime
    --       pIn_vEndTime
    --       pIn_vStartScn
    --       pIn_vEndScn
    -- Error Conditions Raised:
    --
    -- Author:      zhangxf
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V1.0        12-28-2012      zhangxf           Created.
    -- **************************************************************
   IS
    vLogFileName VARCHAR2(100);
    vStartScn    VARCHAR2(40);
    vEndScn      VARCHAR2(40);
    vStartDate   VARCHAR2(40);
    vEndTime     VARCHAR2(40);
    --c_redosql    cur_tsql;
    vSql VARCHAR2(800);
    nCnt number;
  
  BEGIN
    --create dictionary
    FMSP_CreateDict();
    --get current logfile
    SELECT LF.MEMBER
      INTO vLogFileName
      FROM V$LOG L, V$LOGFILE LF
     WHERE L.GROUP# = LF.GROUP#
       AND L.STATUS = 'CURRENT';
    --Get StartSCN
    BEGIN
      select f.scnnum
        into vStartScn
        from fmuser_repos.fmt_scn f
       where f.typ = 'startscn';
    END;
    --Get current SCN
    SELECT dbms_flashback.get_system_change_number INTO vEndScn FROM dual;
  
    --Create the analysis of log
    dbms_logmnr.add_logfile(LogFileName => vLogFileName,
                            Options     => dbms_logmnr.new);
    --Start analysis  
    dbms_logmnr.start_logmnr(DictFileName => g_vDictLocFileName,
                             StartScn     => vStartScn,
                             EndScn       => vEndScn);
  
    vSql := 'SELECT COUNT(*)  from  ' || g_vUserName ||
            g_vTableName || ' where username=''' || pIn_vUserName ||
            ''' and filename=''' || pIn_vFileName || '''';
    execute immediate vSql into nCnt;
    if nCnt = 0 then
    --not contain temporary table
    vSql := 'INSERT INTO ' || g_vUserName || g_vTableName || '
      SELECT rownum,''' || pIn_vUserName || ''',''' ||
            pIn_vFileName || ''',sql_redo
        FROM v$logmnr_contents
       WHERE seg_owner = ''FMUSER_UT''
         AND status <> 2
         AND table_name not in
             (''LOG_OPERATION'',
              ''FMT_SCN'',
              ''FMT_LOGCONTENT'')
       ORDER BY scn,timestamp ';
    execute immediate vSql;
    --Extract matched data into file
    else 
      raise_application_error(-20123,'username and filename is exists,please change it.');
   end if;
    --Terminate the logminer 
    dbms_logmnr.end_logmnr;
  
    COMMIT;
    pOut_nSqlCode := 0;
  EXCEPTION
    WHEN OTHERS THEN
      dbms_logmnr.end_logmnr;
      pOut_nSqlCode := sqlcode;
      RAISE;
  END FMSP_RunLogMiner;

END FMP_UT_DBA;
/
