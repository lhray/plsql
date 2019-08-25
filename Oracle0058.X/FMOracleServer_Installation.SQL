set serverout on


whenever sqlerror exit;

select to_number('Warning:Do not exec this script in the current user') 
from (select user name from dual) where upper(name)  in 
(
'SCOTT',
'ORACLE_OCM',
'XS$NULL',
'MDDATA',
'DIP',
'APEX_PUBLIC_USER',
'SPATIAL_CSW_ADMIN_USR',
'SPATIAL_WFS_ADMIN_USR',
'DBSNMP',
'SYSMAN',
'FLOWS_FILES',
'MDSYS',
'ORDSYS',
'EXFSYS',
'WMSYS',
'APPQOSSYS',
'APEX_030200',
'OWBSYS_AUDIT',
'ORDDATA',
'CTXSYS',
'ANONYMOUS',
'XDB',
'ORDPLUGINS',
'OWBSYS',
'SI_INFORMTN_SCHEMA',
'OLAPSYS',
'MGMT_VIEW',
'SYS',
'SYSTEM',
'OUTLN');


@@.\SP_GetCodeVersion.prc
@@.\SP_GetFMDBUpgradeVersion.prc

declare
  v_Version    varchar2(100) := '';
  p_version_id varchar2(100) := '';
  p_ncount     number := 0;
  n            number;
  v_values     varchar2(100) := '';
begin
  v_values := '';
  SP_GetFMDBUpgradeVersion(v_Version);
  dbms_output.put_line('==========================================================================');
  dbms_output.put_line('    FM database Server version number is:'||v_Version);
  dbms_output.put_line('==========================================================================');
  select count(1)
    into p_ncount
    from user_tables t
   where t.TABLE_NAME = 'FM_VERSION';

  if p_ncount = 0 then
    execute immediate ' create table FM_VERSION(version_id varchar2(100))';
    v_values := '0';
    execute immediate ' insert into FM_VERSION values (''' || v_values ||
                      ''')';
    commit;
  end if;

  execute immediate 'select version_id  from FM_VERSION t'
    into p_version_id;


  if p_version_id <> '0' and p_version_id = v_Version then
    --The local database and server version is consistent, Without upgrade! 
    dbms_output.put_line('==========================================================================');
    dbms_output.put_line('    The local database version number is:'||v_Version);
    dbms_output.put_line('==========================================================================');

    dbms_output.put_line('==========================================================================');
    dbms_output.put_line('    The local database and server version is consistent, Without upgrade!  ');
    dbms_output.put_line('==========================================================================');
    
    select t.* into n from dual t where t.dummy <> 'X';

    
  
  elsif p_version_id <> '0' and p_version_id <> v_Version then
     null;
  else
    null;
  end if;
end;
/



@@.\Log_install.sql


declare
begin
  for cur_type_name in  (select type_name from User_Coll_Types t where t.elem_type_owner is not null ) loop
      execute immediate 'drop type '||cur_type_name.type_name|| ' force';   
  end loop;
end;
/
@@.\BasicTable\CreateType.sql

@@.\FMDDL_Schema.sql

declare
type t_cur is ref cursor;
   str_sql varchar2(400):='';
t_cur_key  t_cur;  
begin
   for t_cur_key in (select T.table_name,T.constraint_name,T.constraint_type 
                          from user_constraints t 
                          where t.table_name not like 'BIN$%' 
                          /*and t.constraint_type in ('P','U') */) LOOP
      /*IF T_CUR_KEY.constraint_type='P' then
          str_sql:= 'alter table '||T_CUR_KEY.table_name ||' drop primary key';
      else
      */
         str_sql:= 'alter table '||T_CUR_KEY.table_name ||' drop constraints '||t_cur_key.constraint_name;     
      /*end if;*/
       
      execute immediate str_sql;                  
    END LOOP;
END;
/   

/*
declare
type t_cur is ref cursor;
   str_sql varchar2(400):='';
t_cur_key  t_cur;  
begin
   for t_cur_key in (select t.index_name,t.table_name from user_indexes t where t.index_name not like 'BIN$%' and t.index_type='NORMAL' ) LOOP
         str_sql:= 'drop index '||t_cur_key.index_name;       
      execute immediate str_sql;                  
    END LOOP;
END;
/  
*/
@@.\BasicTable\CreatePrimaryKey.sql

/*
@@.\BasicTable\CreateIndex.sql
*/

prompt drop materialized view 
declare
  i integer := 0;
begin
  --mview
  for i_mv in (select mview_name name from user_mviews) loop
    execute immediate 'drop materialized view ' || i_mv.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' mview droped.');

  --mview log
  i := 0;
  for i_mvlog in (select master name from user_mview_logs) loop
    execute immediate 'drop materialized view log on  ' || i_mvlog.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' mview log  droped.');
exception
  when others then
    dbms_output.put_line('drop mview/log error:ora' || sqlcode);
end;
/
@ .\Function\functions.sql
@@.\Mview\MViews.sql




prompt login is sucessfully!

@@.\BasicTable\FM_CreateSeq.sql

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

@@.\FMDDL_Code.sql

declare
   v_Version varchar2(100);
   v_strsql   varchar2(2000):='';
begin
   SP_GetFMDBUpgradeVersion(v_Version);
   
   execute immediate 'delete FM_VERSION';
   commit;
   v_strsql:='insert into fm_version values(''' ||v_Version||''')';
   execute immediate v_strsql;
   commit;
end;
/

exit;
