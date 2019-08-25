--FM_CreateTable.sql modify table SUPPLIER new ddl
drop table SUPPLIER cascade constraints purge;
create table SUPPLIER                      
(                                          
  SUPPLIER_EM_ADDR  NUMBER(19) not null,   
  ID_SUPPLIER       INTEGER,               
  PERE_BDG          INTEGER,               
  FILS_BDG          INTEGER,               
  PARAM_SUP         RAW(470),              
  STARTYEAR         INTEGER, --new column  
  STARTPERIOD       INTEGER, --new column  
  ENDYEAR           INTEGER, --new column  
  ENDPERIOD         INTEGER, --new column  
  COEFF             NUMBER, --new column   
  COEF_PERTE        NUMBER, --new column   
  DELAI             INTEGER, --new column  
  PRIORITE          INTEGER, --new column  
  ACTIVATION        INTEGER, --new column  
  TYPE_CHOIX        INTEGER, --new column  
  TYPE_LIEN         INTEGER, --new column  
  BDG51_EM_ADDR     INTEGER,               
  BDG51_EM_ADDR_ORD INTEGER                
);          
alter table SUPPLIER   add constraint PK_SUPPLIER primary key (SUPPLIER_EM_ADDR);                              