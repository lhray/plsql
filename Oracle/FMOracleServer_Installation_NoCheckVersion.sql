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
--PackageUtility
@ .\PackageUtility\PackageUtility.sql

@ .\Function\functions.sql
@@.\Mview\MViews.sql




prompt login is sucessfully!

@@.\BasicTable\FM_CreateSeq.sql

-- modify seq
@@.\BasicTable\FM_ModifySeq.sql

@@.\FMDDL_Code.sql
-- add job.sql by zhangl
@@.\job.sql
-- add end
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

@@.\gather_statistic.sql

exit;
