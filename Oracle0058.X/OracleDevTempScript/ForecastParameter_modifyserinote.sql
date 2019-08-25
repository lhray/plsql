-- Create table
drop table SERINOTE purge;

create table SERINOTE
(
  SERINOTE_EM_ADDR NUMBER(19) not null,
  NOPAGE           INTEGER,
  DATE_MODIF_TEXTE INTEGER,
  TEXTE            CLOB,
  BDG3_EM_ADDR     INTEGER,
  BDG3_EM_ADDR_ORD INTEGER,
  NUM_MOD          INTEGER
);
-- Create/Recreate primary, unique and foreign key constraints 
alter table SERINOTE add constraint PK_SERINOTE primary key (SERINOTE_EM_ADDR);