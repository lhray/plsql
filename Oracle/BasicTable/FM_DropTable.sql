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