declare

  vTablename varchar2(30) := 'TBMID1';
  nCnt       number;
  cSql       clob;
begin
  select count(*) into ncnt from user_tables where table_name = vTablename;
  if ncnt = 1 then
    execute immediate 'drop table ' || vTablename || ' purge';
  end if;

  cSql := 'CREATE TABLE ' || vTablename || '(
      NodeType int ,
      PVTID int ,
      TSType int ,
      version int ,
      BeginYY int,
      Beginperiod int,
      EndYY int,
      Endperiod int';
  for i in 1 .. 53 loop
    cSql := cSql || ',T_' || i || ' NUMBER';
  end loop;
  cSql := cSql || ') nologging';
  execute immediate cSql;
end;
/
exit;