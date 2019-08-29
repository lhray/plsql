@@BasicTable\FM_CreateTables.sql
@@BasicTable\Fm610Datas.sql
set serveroutput on 
declare
  TYPE T_CURSOR is ref CURSOR;
  CUR_TABLE T_CURSOR;
  v_strsql varchar2(500):='';
begin
   for CUR_TABLE in (select t.TABLE_NAME from user_tables t where t.TABLE_NAME like '%FM$$/_%' ESCAPE '/') loop
       v_strsql:='alter table '||CUR_TABLE.table_name || ' rename to ' ||substr(CUR_TABLE.table_name,6,length(CUR_TABLE.table_name)-5);  
       execute immediate v_strsql;
   end loop;
EXCEPTION
  WHEN others THEN
     dbms_output.put_line('ALTER RENAME TABLE NAME ERROR:'||SQLCODE||v_strsql); 
end;
/

set serveroutput off

@@BasicTable\CreatePrimaryKey.sql

@@BasicTable\CreateIndex.sql

@@BasicTable\FM_CreateSeq.sql


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
    
      --dbms_output.put_line(str_SQL);
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
      str_SQL     := str_SQL_START || ' START WITH ' || to_char(i_Key + 10) || ' ' ||
                     str_SQL_END;
    
      str_txt := 'drop SEQUENCE ' || OLD_SEQ_NAME;
      --dbms_output.put_line(str_txt);
      execute immediate str_txt;
    
      str_txt := 'drop SEQUENCE ' || NEW_SEQ_NAME;
      execute immediate str_txt;
      str_SQL := replace(str_SQL, '"', '');
      execute immediate str_SQL;
      --dbms_output.put_line(str_SQL);
    end if;
  end loop;
end;
/



@@BasicTable\TB_version.sql
