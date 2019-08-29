CREATE OR REPLACE PACKAGE FMP_UT AUTHID CURRENT_USER  IS
  --1139693
  TYPE cur_tsql IS REF CURSOR;
  --generate export file location
  g_vFileDirectory CONSTANT VARCHAR2(20) := 'EXP_SQL_DIR';
  --generate export file name
  g_vFileName CONSTANT VARCHAR2(16) := 'test1.sql';

  TYPE tbl_TBName IS VARRAY(5) OF VARCHAR2(30);

  PROCEDURE FMSP_FlashbackTB(pOut_nSqlCode OUT NUMBER);

  PROCEDURE FMSP_ENABLEROWMOVE;

  PROCEDURE FMSP_PrepareData(pIn_vUserName IN VARCHAR2,
                             pIn_vFileName IN VARCHAR2,
                             pOut_nSqlCode OUT number);
  PROCEDURE FMSP_GetSCN;

END FMP_UT;
/
CREATE OR REPLACE PACKAGE BODY FMP_UT IS

  PROCEDURE FMSP_FlashbackTB(pOut_nSqlCode OUT NUMBER) IS
    cursor cur_data is
      select u.TABLE_NAME
        from all_tables u
       where u.OWNER = 'FMUSER_UT'
         AND u.TEMPORARY = 'N'
         and table_name not like 'TBMID%'
         and table_name not like 'UT_%'
         and u.TABLE_NAME not in ('FMT_LOGCONTENT', 'FMT_SCN');
    vTableName varchar2(4000);
    vSql       varchar2(4000);
    vScn       varchar2(200);
  begin
    for cur in cur_data loop
      vTableName := vTableName || 'FMUSER_UT.'||cur.table_name || ',';
    end loop;
    vTableName := substr(vTableName, 0, length(vTableName) - 1);
    if vTableName is null then
      return;
    end if;
    select t.scnnum
      into vScn
      from fmuser_repos.fmt_scn t
     where t.typ = 'startscn';
  
    vSql := ' flashback table ' || vTableName || ' to scn ' || vScn;
  
    execute immediate vSql;
  
    FMSP_GetSCN();
  
    pOut_nSqlCode := 0;
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      pOut_nSqlCode := -1;
      RAISE;
  END FMSP_FlashbackTB;

  PROCEDURE FMSP_ENABLEROWMOVE IS
    cursor cur_data is
      select u.TABLE_NAME
        from all_tables u
       where U.OWNER = 'FMUSER_UT'
         AND u.TEMPORARY = 'N'
         and table_name not like 'TBMID%'
         and table_name not like 'UT_%'
         and u.TABLE_NAME not in ('FMT_LOGCONTENT', 'FMT_SCN');
  
    vSql varchar2(4000);
  begin
    for cur in cur_data loop
      vSql := 'alter table FMUSER_UT.' || cur.table_name || ' enable row movement';
      execute immediate vSql;
    end loop;
  
  END FMSP_ENABLEROWMOVE;

  PROCEDURE FMSP_PrepareData(pIn_vUserName IN VARCHAR2,
                             pIn_vFileName IN VARCHAR2,
                             pOut_nSqlCode OUT number) IS
    vsql clob;
    cursor cur is
      select f.sql_redo
        from fmuser_repos.fmt_logcontent f
       where f.filename = pIn_vFileName
         and f.username = pIn_vUserName
       order by id;
  begin
    for c in cur loop
      vsql := c.sql_redo;
      BEGIN
      execute immediate substr(vsql, 0, length(vsql) - 1);
      EXCEPTION
        WHEN OTHERS THEN
          NULL;
      END;
    end loop;
    commit;
    pOut_nSqlCode := 0;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
      raise;
  END FMSP_PrepareData;

  PROCEDURE FMSP_GetSCN
  
   IS
    vSql varchar2(200);
  BEGIN
    update fmuser_repos.fmt_scn f
       set f.scnnum = dbms_flashback.get_system_change_number
     where f.typ = 'startscn';
    commit;
  END FMSP_GetSCN;

END FMP_UT;
/
