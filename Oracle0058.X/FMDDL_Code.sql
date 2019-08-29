
spool CodeUpgrade.log
set termout on; 
--close the result of the command
set serveroutput on;
-- print the dbms_output value

-------------------temporary script begin--------------------------------

/*drop triggers :check if the triggers exists ,then drop */
var execscript varchar2(128);
declare 
   v_cnt number;
begin 
   select count(*) into v_cnt from user_triggers;
   if v_cnt >0 then 
    :execscript :='.\BasicTable\DropTrigger';
   else 
    :execscript :='output';
   end if;
end ;
/ 
column sql_col_trigger new_value nsql_col_trigger noprint;
select :execscript sql_col_trigger from dual;
@@&nsql_col_trigger

/*change column type RAW(16)->INTEGER:check if the raw16 exists then alter */

var execscript varchar2(128);
declare 
   v_cnt number;
begin 
   select count(*) into v_cnt from user_tab_cols where table_name='CDT' and data_type='RAW' and data_length=16;
   if v_cnt >0 then 
    :execscript :='ColumnsRawConvertToNumber';
   else 
    :execscript :='output';
   end if;
end ;
/ 
column sql_col_trigger new_value nsql_col_trigger noprint;
select :execscript sql_col_trigger from dual;
@@&nsql_col_trigger


/*add cache in sequence*/

var execscript varchar2(128);
declare 
   v_cnt number;
begin 
   select count(*) into v_cnt from user_sequences u where u.CACHE_SIZE=0 and sequence_name='SEQ_ADR_EMAIL';
   if v_cnt >0 then 
    :execscript :='SequenceCache';
   else 
    :execscript :='output';
   end if;
end ;
/ 
column sql_col_trigger new_value nsql_col_trigger noprint;
select :execscript sql_col_trigger from dual;
@@&nsql_col_trigger

/*drop the deprecated mviews*/
declare
  i integer;
begin

  for i_mv_log in (select t.master
                     from user_mview_logs t
                    where master in ('DIS', 'GEO', 'PRVSEL', 'RSP')) loop
    execute immediate 'drop materialized view log on ' || i_mv_log.master;
  end loop;

  for i_mviews in (select mview_name
                     from user_mviews
                    where Mview_name in
                          ('MV_PRVSELPVT', 'MV_DIS', 'MV_GEO', 'MV_FAM')) loop
    execute immediate 'drop materialized view ' || i_mviews.mview_name;
  end loop;
end;
/


prompt drop summarize procedure 

declare
  i integer := 0;
begin
  for i_procedure in (select u.object_name name
                        from user_procedures u
                       where u.object_type = 'PROCEDURE'
                         and u.object_name in
                             ('SPPRV_AGGNODETSNULLREPLACE',
                              'SPPRV_SUMTSTOAGGNODE',
                              'SPPRV_SUMAGGNODETIMESERIES',
                              'SPPRV_SUMAGGNODETIMESERIESUOM',
                              'SPPRV_SUMMARIZEAGGREGATENODE',
                              'SPPRV_SUMAGGREGATENODETS',
                              'SP_PROCESSGROUPINUOM',
                              'SPPRV_SUMMARIZEAGGREGATENODES')) loop
    execute immediate 'drop procedure ' || i_procedure.name;
    i := i + 1;
  end loop;
  dbms_output.put_line(i || ' procedures droped!');
exception
  when others then
    dbms_output.put_line('drop summarize procedure error:ora' || sqlcode);
end;
/

-------------------temporary script   end -------------------------------
--functions
--@ .\Function\functions.sql
--views 
@ .\View\views.sql
--mviews
--@ .\Mview\MViews.sql
--procedures
@ .\Procedure\procedures.sql
--packages
@ .\Package\packages.sql

@@SP_GetCodeVersion.prc

--set termout on; 

spool off