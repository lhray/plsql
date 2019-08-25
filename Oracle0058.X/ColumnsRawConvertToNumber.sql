/********
modify the RAW(16) column to integer.
Note :The script only run once.
********/
prompt process CDT

declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='CDT_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table CDT_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/
create table cdt_bak_raw2num as 
select * from cdt;


alter table cdt add adr_cdt2 integer;
update cdt set adr_cdt2 = f_convert_hex2dec(adr_cdt);
commit;
--check
update cdt set adr_cdt = null;
commit;
--check
alter table cdt modify adr_cdt integer;
update cdt c set c.adr_cdt=c.adr_cdt2;
commit;
--check
alter table cdt drop column adr_cdt2;

prompt CDT processed!



prompt process PVT

declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='PVT_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table PVT_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/

create table PVT_BAK_RAW2NUM as 
select * from pvt;

alter table pvt add adr_pro2 integer;
alter table pvt add adr_geo2 integer;
alter table pvt add adr_dis2 integer;
alter table pvt add PVT_PARENT_BDG2 integer;

update pvt
   set adr_pro2        = f_convert_hex2dec(adr_pro),
       adr_geo2        = f_convert_hex2dec(adr_geo),
       adr_dis2        = f_convert_hex2dec(adr_dis),
       pvt_parent_bdg2 = f_convert_hex2dec(pvt_parent_bdg);
commit;

update pvt
   set adr_pro        = null,
       adr_geo        =null,
       adr_dis        = null,
       pvt_parent_bdg = null;
commit;


alter table pvt modify adr_pro integer;
alter table pvt modify adr_geo integer;
alter table pvt modify adr_dis integer;
alter table pvt modify pvt_parent_bdg integer;

update pvt
   set adr_pro        = adr_pro2,
       adr_geo        =adr_geo2,
       adr_dis        = adr_dis2,
       pvt_parent_bdg = pvt_parent_bdg2;
commit;


alter table pvt drop column adr_pro2;
alter table pvt drop column adr_geo2;
alter table pvt drop column adr_dis2;
alter table pvt drop column pvt_parent_bdg2;

prompt PVT processed !


prompt process SEL

declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='SEL_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table SEL_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/

create table SEL_BAK_RAW2NUM as select * from sel;
alter table sel  add SEL_PARENT_BDG2 integer;
update sel set SEL_PARENT_BDG2=f_convert_hex2dec(SEL_PARENT_BDG);
commit;
update sel set SEL_PARENT_BDG = null;
commit;
alter table sel modify SEL_PARENT_BDG integer;
update sel set SEL_PARENT_BDG = SEL_PARENT_BDG2;
commit;
alter table sel drop column SEL_PARENT_BDG2;

prompt SEL processed!



prompt process BDG

declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='BDG_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table BDG_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/

create table BDG_BAK_RAW2NUM as select * from sel;
alter table bdg  add BDG_PARENT_NODE2 integer;
update bdg set BDG_PARENT_NODE2=f_convert_hex2dec(BDG_PARENT_NODE);
commit;
update bdg set BDG_PARENT_NODE = null;
commit;
alter table bdg modify BDG_PARENT_NODE integer;
update bdg set BDG_PARENT_NODE = BDG_PARENT_NODE2;
commit;
alter table bdg drop column BDG_PARENT_NODE2;

prompt BDG processed!


prompt process NMC 


declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='NMC_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table NMC_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/

create table NMC_BAK_RAW2NUM as
select * from nmc;

alter table nmc add (PERE_PRO_NMC2 integer,FILS_PRO_NMC2 integer);
update nmc set PERE_PRO_NMC2=f_convert_hex2dec(PERE_PRO_NMC),FILS_PRO_NMC2=f_convert_hex2dec(FILS_PRO_NMC);
commit;
update nmc set PERE_PRO_NMC=null,FILS_PRO_NMC=null;
commit; 
alter table nmc modify PERE_PRO_NMC integer;
alter table nmc modify FILS_PRO_NMC integer;

update nmc set PERE_PRO_NMC=PERE_PRO_NMC2,FILS_PRO_NMC=FILS_PRO_NMC2;
commit;
alter table nmc drop column PERE_PRO_NMC2;
alter table nmc drop column FILS_PRO_NMC2;
prompt NMC processed!


prompt process SUPPLIER 

declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='SUPPLIER_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table SUPPLIER_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/
create table SUPPLIER_BAK_RAW2NUM as
select * from SUPPLIER;
alter table SUPPLIER add (PERE_BDG2 integer,FILS_BDG2 integer);
update SUPPLIER
   set PERE_BDG2 = f_convert_hex2dec(PERE_BDG),
       FILS_BDG2 = f_convert_hex2dec(FILS_BDG);
commit;
update SUPPLIER
   set PERE_BDG = null,
       FILS_BDG = null;
commit;


alter table SUPPLIER modify PERE_BDG integer;
alter table SUPPLIER modify FILS_BDG integer;

update SUPPLIER set PERE_BDG=PERE_BDG2,FILS_BDG=FILS_BDG2;
commit;
alter table SUPPLIER drop column PERE_BDG2;
alter table SUPPLIER drop column FILS_BDG2;


prompt SUPPLIER processed!


prompt process SELECT_SEL


declare 
v_cnt number;
begin 
 select count(*) into v_cnt from user_tables where table_name ='SELECT_SEL_BAK_RAW2NUM'; 
 if V_cnt >0 then 
   execute immediate 'drop table SELECT_SEL_BAK_RAW2NUM';
 end if;
 exception 
   when others then 
     dbms_output.put_line(sqlcode||sqlerrm);   
end;
/
create table SELECT_SEL_BAK_RAW2NUM as
select * from SELECT_SEL;
alter table SELECT_SEL add (ADR_PRV2 integer,PERE_SEL2 integer,FILS_SEL2 integer);


update SELECT_SEL
   set ADR_PRV2  = f_convert_hex2dec(ADR_PRV),
       PERE_SEL2 = f_convert_hex2dec(PERE_SEL),
       FILS_SEL2 = f_convert_hex2dec(FILS_SEL);
commit;
update SELECT_SEL set ADR_PRV = null, PERE_SEL = null, FILS_SEL = null;
commit;

alter table SELECT_SEL modify ADR_PRV integer;
alter table SELECT_SEL modify PERE_SEL integer;
alter table SELECT_SEL modify FILS_SEL integer;


update SELECT_SEL set ADR_PRV=ADR_PRV2,PERE_SEL=PERE_SEL2,FILS_SEL=FILS_SEL2;
commit;
alter table SELECT_SEL drop column ADR_PRV2;
alter table SELECT_SEL drop column PERE_SEL2;
alter table SELECT_SEL drop column FILS_SEL2;


prompt SELECT_SEL processed!

drop function f_Convert_Hex2Dec;
drop function F_DectoHex;

prompt  NOTE:the script run successfully ended!  Don't do it agagin!!!!




