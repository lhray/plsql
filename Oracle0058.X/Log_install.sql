-------------------------------------------------------------------
--
--  File : install.sql (SQLPlus script)
--
--  Description : installation of the LOG4PLSQL framework
-------------------------------------------------------------------
--
-- history : who                 created     comment
--     v1    Bertrand Caradec   15-MAY-08    creation
--                                     
--
-------------------------------------------------------------------
/*
 * Copyright (C) LOG4PLSQL project team. All rights reserved.
 *
 * This software is published under the terms of the The LOG4PLSQL 
 * Software License, a copy of which has been included with this
 * distribution in the LICENSE.txt file.  
 * see: <http://log4plsql.sourceforge.net>  */


spool install.txt

PROMPT LOG4PLSQL Installation
PROMPT **********************
PROMPT 

SET VERIFY OFF

declare
begin
for CUR_SEQ in (select t.sequence_name from user_sequences t where t.sequence_name like 'FM$$/_%' ESCAPE '/') loop
  execute immediate 'drop sequence ' || CUR_SEQ.SEQUENCE_NAME;
end loop;
end;
/


DECLARE
  TYPE T_CURSOR IS REF CURSOR;

  CUR_TAB T_CURSOR;
  S_NEW_TAB_NAME      VARCHAR2(30);

  S_RENAME_TAB_SQL VARCHAR2(5000) := '';
BEGIN
 
    FOR CUR_TAB IN (SELECT TABLE_NAME
                      FROM USER_TABLES T
                     WHERE T.TABLE_NAME LIKE 'FM$$/_%' ESCAPE '/') LOOP
      S_RENAME_TAB_SQL := ' DROP TABLE ' || CUR_TAB.TABLE_NAME||' CASCADE CONSTRAINTS PURGE';
      EXECUTE IMMEDIATE S_RENAME_TAB_SQL;
    END LOOP;
EXCEPTION
  WHEN OTHERS THEN 
    DBMS_OUTPUT.put_line('ERROR:'||SQLCODE);
END;
/

PROMPT Create table TLOGLEVEL ...

@@.\log\create_table_tloglevel



PROMPT Create table TLOG ...

@@.\log\create_table_tlog

--new log table
@@.\log\create_table_log_operation_level.sql
@@.\log\create_table_log_operation.sql
--new log table

create or replace procedure SP_Old_Tab_Is_Exists(p_new_tab_name in varchar2,
                                                 E_flag         out boolean)
 is
  v_num         number;
  v_new_tabname varchar2(50);
begin
  v_new_tabname := p_new_tab_name;
  E_flag        := False;
  v_num         := 0;

  select count(1)
    into v_num
    from user_tables t
   where t.TABLE_NAME =
         upper(substr(trim(v_new_tabname), 6, length(trim(v_new_tabname)) - 5));

  if v_num = 1 then
    E_flag := TRUE;
  else
    E_flag := FALSE;
  end if;

Exception
  when others then
    null;
end SP_Old_Tab_Is_Exists;
/

--
DECLARE
  TYPE T_CURSOR IS REF CURSOR;

  CUR_OBJ_NAME   T_CURSOR;
  CUR_TAB1       T_CURSOR;
  S_NEW_TAB_NAME VARCHAR2(30);
  S_OLD_TAB_NAME VARCHAR2(30);

  V_TAB_EXISTS_FLAG   BOOLEAN := FALSE;
  V_COL_EXISTS_FLAG   NUMBER := 0;
  V_I_TAB_EXISTS_FLAG NUMBER := 0;

  S_NEW_IND_NAME VARCHAR2(30);
  S_OLD_IND_NAME VARCHAR2(30);

  S_RENAME_TAB_SQL VARCHAR2(5000) := '';
BEGIN
  S_RENAME_TAB_SQL := '';
  --diff table
  FOR CUR_TAB IN (SELECT TABLE_NAME
                    FROM USER_TABLES T
                   WHERE T.TABLE_NAME LIKE 'FM$$/_%' ESCAPE '/') LOOP
    S_NEW_TAB_NAME := TRIM(CUR_TAB.TABLE_NAME);
    S_OLD_TAB_NAME := SUBSTR(TRIM(CUR_TAB.TABLE_NAME),
                             6,
                             LENGTH(TRIM(CUR_TAB.TABLE_NAME)) - 5);
  
    IF S_NEW_TAB_NAME IS NOT NULL THEN
      SP_OLD_TAB_IS_EXISTS(CUR_TAB.TABLE_NAME, V_TAB_EXISTS_FLAG);
      IF V_TAB_EXISTS_FLAG THEN
      
        FOR CUR_OBJ_NAME IN (SELECT TABLE_NAME,
                                    COLUMN_NAME,
                                    DATA_TYPE,
                                    DATA_LENGTH,
                                    NULLABLE,
                                    DATA_DEFAULT
                               FROM USER_TAB_COLS T
                              WHERE T.TABLE_NAME = S_NEW_TAB_NAME) LOOP
          SELECT COUNT(1)
            INTO V_COL_EXISTS_FLAG
            FROM USER_TAB_COLS M
           WHERE M.TABLE_NAME = S_OLD_TAB_NAME
             AND M.COLUMN_NAME = CUR_OBJ_NAME.COLUMN_NAME;
        
          IF V_COL_EXISTS_FLAG <> 1 THEN
            if CUR_OBJ_NAME.NULLABLE = 'N' then
              S_RENAME_TAB_SQL := 'ALTER  TABLE ' || S_OLD_TAB_NAME ||
                                  ' ADD ' || CUR_OBJ_NAME.COLUMN_NAME || ' ' ||
                                  CUR_OBJ_NAME.DATA_TYPE || '(' ||
                                  CUR_OBJ_NAME.DATA_LENGTH || ') default ' ||
                                  CUR_OBJ_NAME.DATA_DEFAULT || ' not null';
            
            else
              if CUR_OBJ_NAME.DATA_TYPE in ('BLOB', 'CLOB') then
                S_RENAME_TAB_SQL := 'ALTER  TABLE ' || S_OLD_TAB_NAME ||
                                    ' ADD(' || CUR_OBJ_NAME.COLUMN_NAME || ' ' ||
                                    CUR_OBJ_NAME.DATA_TYPE || ' )';
              else
                S_RENAME_TAB_SQL := 'ALTER  TABLE ' || S_OLD_TAB_NAME ||
                                    ' ADD(' || CUR_OBJ_NAME.COLUMN_NAME || ' ' ||
                                    CUR_OBJ_NAME.DATA_TYPE || '(' ||
                                    CUR_OBJ_NAME.DATA_LENGTH || '))';
              end if;
            end if;
            EXECUTE IMMEDIATE S_RENAME_TAB_SQL;
          END IF;
        END LOOP;
      
        /*S_RENAME_TAB_SQL := ' DROP TABLE ' || S_NEW_TAB_NAME|| 'CASCADE CONSTRAINTS PURGE';
        EXECUTE IMMEDIATE S_RENAME_TAB_SQL;*/
      ELSE
        --if old tablename not exits, rename new tablename to old tablename
        S_RENAME_TAB_SQL := '';
        S_RENAME_TAB_SQL := S_RENAME_TAB_SQL || ' RENAME ';
        S_RENAME_TAB_SQL := S_RENAME_TAB_SQL || S_NEW_TAB_NAME;
        S_RENAME_TAB_SQL := S_RENAME_TAB_SQL || ' TO ';
        S_RENAME_TAB_SQL := S_RENAME_TAB_SQL || S_OLD_TAB_NAME;
        EXECUTE IMMEDIATE S_RENAME_TAB_SQL;
      END IF;
    END IF;
  END LOOP;

  FOR CUR_TAB1 IN (SELECT TABLE_NAME
                     FROM USER_TABLES T
                    WHERE T.TABLE_NAME LIKE 'FM$$/_%' ESCAPE '/') LOOP
    S_RENAME_TAB_SQL := ' DROP TABLE ' || CUR_TAB1.TABLE_NAME ||
                        ' CASCADE constraints PURGE';
    EXECUTE IMMEDIATE S_RENAME_TAB_SQL;
  END LOOP;

  /*
    --diff index
    FOR CUR_TAB IN (SELECT INDEX_NAME
                      FROM USER_INDEXES T
                     WHERE T.INDEX_NAME LIKE 'FM$$_%') LOOP
      S_NEW_IND_NAME := TRIM(CUR_TAB.INDEX_NAME);
      S_OLD_IND_NAME := SUBSTR(TRIM(CUR_TAB.INDEX_NAME),
                               6,
                               LENGTH(TRIM(CUR_TAB.INDEX_NAME)) - 5);
  
      V_I_TAB_EXISTS_FLAG := 0;
      SELECT COUNT(1)
        INTO V_I_TAB_EXISTS_FLAG
        FROM USER_INDEXES
       WHERE INDEX_NAME = S_OLD_IND_NAME;
      IF V_I_TAB_EXISTS_FLAG = 1 THEN
        S_RENAME_TAB_SQL := 'DROP INDEX ' || S_NEW_IND_NAME;
      ELSE
        S_RENAME_TAB_SQL := 'ALTER INDEX  RENAME ' || S_NEW_IND_NAME ||
                            ' TO ' || S_OLD_IND_NAME;
      END IF;
      EXECUTE IMMEDIATE S_RENAME_TAB_SQL;
    END LOOP;
  */

EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('UPGRADE TABLE STRUCT ERROR:' || SQLCODE);
END;
/


PROMPT Create sequence SQ_STG ...

@@.\log\create_sequence_sq_stg

--new log sequence
@@.\log\create_sequence_seq_fmlog_id.sql
--new log sequence


declare
  TYPE T_CURSOR IS REF CURSOR;
  CUR_SEQ       T_CURSOR;
  OLD_SEQ_NAME  VARCHAR2(40);
  NEW_SEQ_NAME  VARCHAR2(40);
  OLD_SEQ_FLAG  NUMBER := 0;
  i_Key         NUMBER := 0;
  str_txt       VARCHAR2(2000) := '';
  str_SQL       VARCHAR2(2000);
  str_SQL_START VARCHAR2(2000) := '';
  str_SQL_END   VARCHAR2(2000) := '';
begin
  str_SQL := '';
  for CUR_SEQ in (select t.sequence_name, t.last_number from user_sequences t where t.sequence_name like 'FM$$/_%' ESCAPE '/') loop
    NEW_SEQ_NAME := TRIM(CUR_SEQ.SEQUENCE_NAME);
    OLD_SEQ_NAME := SUBSTR(TRIM(CUR_SEQ.SEQUENCE_NAME),
                           6,
                           LENGTH(TRIM(CUR_SEQ.SEQUENCE_NAME)) - 5);
    SELECT COUNT(1)
      INTO OLD_SEQ_FLAG
      FROM USER_SEQUENCES N
     WHERE N.sequence_name = OLD_SEQ_NAME;
    IF OLD_SEQ_FLAG = 0 THEN
      str_SQL := DBMS_METADATA.GET_DDL('SEQUENCE', NEW_SEQ_NAME);
    
      str_SQL := substr(str_SQL, 1, instr(str_SQL, 'FM$$_') - 1) ||
                 substr(str_SQL, instr(str_SQL, 'FM$$_') + 5);
      str_SQL := replace(str_SQL, '"', '');
      str_txt := 'drop SEQUENCE ' || NEW_SEQ_NAME;
      execute immediate str_txt;
    

      execute immediate str_SQL;
    ELSE
      SELECT t.last_number
        INTO i_Key
        FROM USER_SEQUENCES t
       WHERE t.sequence_name = OLD_SEQ_NAME;
    
      str_SQL       := DBMS_METADATA.GET_DDL('SEQUENCE', NEW_SEQ_NAME);
      str_SQL_START := substr(str_SQL, 1, instr(str_SQL, 'START WITH') - 1);
      str_SQL_START := substr(str_SQL_START,
                              1,
                              instr(str_SQL_START, 'FM$$_') - 1) ||
                       substr(str_SQL_START,
                              instr(str_SQL_START, 'FM$$_') + 5);
    
      str_SQL_END := substr(str_SQL, instr(str_SQL, 'CACHE'));
      str_SQL     := str_SQL_START || ' START WITH ' || to_char(i_Key + 1000) || ' ' ||
                     str_SQL_END;
    
      str_txt := 'drop SEQUENCE ' || OLD_SEQ_NAME;

      execute immediate str_txt;
    
      str_txt := 'drop SEQUENCE ' || NEW_SEQ_NAME;
      execute immediate str_txt;
      str_SQL := replace(str_SQL, '"', '');
      execute immediate str_SQL;

    end if;
  end loop;
end;
/




PROMPT Insert rows into TLOGLEVEL ...

@@.\log\insert_into_tloglevel

PROMPT Insert rows into log_operation_level...
@@.\log\insert_into_log_operation_level.sql

PROMPT Create package PLOGPARAM ...

@@.\log\ps_plogparam
@@.\log\pb_plogparam

PROMPT Create package PLOG_OUT_TLOG ...

@@.\log\ps_plog_out_tlog
@@.\log\pb_plog_out_tlog


PROMPT Create dynamically the package PLOG_INTERFACE ...

@@.\log\ps_plog_interface
@@.\log\pb_plog_interface

PROMPT Create the main package PLOG ...

@@.\log\ps_plog
@@.\log\pb_plog

PROMPT Create the view VLOG

@@.\log\create_view_vlog

PROMPT  sp_Log_v1 log
@@.\log\sp_Log_v1.sql


--new log code(pkg procedure)
@@.\Package\FMP_LOG.pck
@@.\Procedure\FMSP_LOG_DEMO.prc
--new log code(pkg procedure)


spool off

