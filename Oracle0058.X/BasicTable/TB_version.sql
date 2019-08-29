insert into tb_version(id,version,versiondate,remark,state)
values(1,'6.1.0.1',to_date('2012-03-13 00:00:00','yyyy-mm-dd hh24:mi:ss'),'Ramia to SQL',0);
insert into tb_version(id,version,versiondate,remark,state)
values(2,'6.1.0.2',to_date('2012-03-15 00:00:00','yyyy-mm-dd hh24:mi:ss'),'delete fields inst_id',0);
insert into tb_version(id,version,versiondate,remark,state)
values(3,'6.1.0.3',to_date('2012-03-18 00:00:00','yyyy-mm-dd hh24:mi:ss'),'edit  fields from char(1) and nchar(1) to smallint',0);
insert into tb_version(id,version,versiondate,remark,state)
values(4,'6.1.0.4',to_date('2012-03-20 00:00:00','yyyy-mm-dd hh24:mi:ss'),'edit  fields from char/nchar to Nvarchar',0);
insert into tb_version(id,version,versiondate,remark,state)
values(5,'6.1.0.1',to_date('2012-03-21 00:00:00','yyyy-mm-dd hh24:mi:ss'),'add Uniquekey',0);
insert into tb_version(id,version,versiondate,remark,state)
values(6,'6.1.0.6',to_date('2012-04-03 00:00:00','yyyy-mm-dd hh24:mi:ss'),'create procedure CreateFK',0);
insert into tb_version(id,version,versiondate,remark,state)
values(7,'6.1.0.1',to_date('2012-04-06 13:41:36','yyyy-mm-dd hh24:mi:ss'),'edit  fields from bigint to binary(16)',0);
insert into tb_version(id,version,versiondate,remark,state)
values(8,'6.1.0.8',to_date('2012-04-11 14:41:36','yyyy-mm-dd hh24:mi:ss'),' add Version table',0);
insert into tb_version(id,version,versiondate,remark,state)
values(9,'6.1.0.9',to_date('2012-04-12 10:34:54','yyyy-mm-dd hh24:mi:ss'),'Modify the stored procedure ClearTable, do not empty the table TB_JC_Version',0);
insert into tb_version(id,version,versiondate,remark,state)
values(10,'6.1.0.10',to_date('2012-04-16 10:35:44','yyyy-mm-dd hh24:mi:ss'),'edit  fields from Smallint to Tinyint',0);
insert into tb_version(id,version,versiondate,remark,state)
values(11,'6.1.0.11',to_date('2012-04-18 11:44:26','yyyy-mm-dd hh24:mi:ss'),'Set the collation is case sensitive, and an updated version of set',0);
insert into tb_version(id,Version,versiondate,Remark,State)
values(12,'6.1.0.12',to_date('2012-05-03 18:44:26','yyyy-mm-dd hh24:mi:ss'),'Each foreign key to have added a sort field',0);
insert into tb_version(id,Version,versiondate,Remark,State)
values(13,'6.1.0.13',sysdate,'add configuration table fm_config',0);
commit;