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
  begin
	execute immediate 'select version_id  from FM_VERSION t'
		into p_version_id;
	exception when others then
		null;
  end;

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

@@.\FMOracleServer_Installation_NoCheckVersion.sql


