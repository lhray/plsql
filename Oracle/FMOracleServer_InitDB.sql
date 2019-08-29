

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

prompt login is sucessfully!


@@.\FMOracleServer_UnInstallation.SQL
@@.\Log_install.sql

@@.\BasicTable\CreateType.sql
@@.\Install.SQL
-- add job.sql by zhangl
@@.\job.sql
-- add end
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

  select count(1)
    into p_ncount
    from user_tables t
   where t.TABLE_NAME = 'FM_VERSION';

  if p_ncount = 0 then
    execute immediate ' create table FM_VERSION(version_id varchar2(100))';
    execute immediate ' insert into FM_VERSION values (''' || v_Version ||
                      ''')';
    commit;
  else
   execute immediate ' delete FM_VERSION';
   commit;
    execute immediate ' insert into FM_VERSION values (''' || v_Version ||
                      ''')';
    commit;

  end if;
end;
/
exit;
