SET TERMOUT OFF
SET HEADING ON
SET LONG 20000
SET LINESIZE 8000

spool Datas.txt
select * from TBMID1;
spool off;
exit;