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

@.\BasicTable\FM_CreateTables.sql

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