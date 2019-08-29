CREATE TABLE FMT_LOGCONTENT
   (  ID NUMBER, 
  USERNAME VARCHAR2(40), 
  FILENAME VARCHAR2(40), 
  SQL_REDO CLOB
   );
   
   CREATE TABLE FMT_SCN
   (  TYP VARCHAR2(40), 
      SCNNUM NUMBER
   );
   
   insert into FMT_SCN values('startscn',dbms_flashback.get_system_change_number);

   commit;
