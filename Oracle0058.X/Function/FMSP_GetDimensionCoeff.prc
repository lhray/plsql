create or replace procedure FMSP_GetDimensionCoeff(pIn_nNodeType     in number, --10003: node in pvt, 10004: node in sel, 10005:node in bdg
                                                   pIn_nChronology   in number, --1: monthly, 2: weekly, 3: daily
                                                   pIn_arrNodeAddr   in varchar2, --db addr separated by ","
                                                   pOut_strTableName out varchar2, --temporary table name used to save coefficient/UOM data
                                                   pOut_nPeriodCount out number, --period count of coefficient
                                                   pOut_nSQLCode     out number) as
  --*****************************************************************
  -- Description: Handling Revise Operations.
  --
  -- Parameters:
  --   pIn_nNodeType     , --10003: node in pvt, 10004: node in sel, 10005:node in bdg
  --   pIn_nChronology    --1: monthly, 2: weekly, 3: daily
  --   pIn_arrNodeAddr   --db addr separated by ","

  -- Error Conditions Raised:
  --
  -- Author:      <wfq>
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        24-12-2012     wfq           Created.
  -- **************************************************************
  vStrSql    varchar(8000) := '';
  vStrSelect varchar(4000) := '';
  vStrfrom   varchar(4000) := '';
  vStrWhere  varchar(4000) := '';
  iCycle     int;
  i          int := 0;
  vTableName varchar2(80);
  iPeriod    int := 0;
  vsql       varchar2(8000);
  tc_DataYM  sys_refcursor;
  iBeginYY   int:=0;
  iEndYY     int:=0;
  iNodeID    int;
  vtssql     varchar2(8000);
  vtsfromsql varchar2(8000);
  iYY        int;
  iType      int;
BEGIN
  pOut_nSqlCode := 0;

  vTableName := 'trf';
  IF pIn_nChronology = 1 THEN
    --1: monthly
    iCycle     := 12;
    vTableName := vTableName || '_M';
  ELSIF pIn_nChronology = 2 THEN
    --2: weekly
    iCycle     := 52;
    vTableName := vTableName || '_W';
  ELSE
    -- 3: daily
    iCycle := 14;
  END IF;

  -- get max Period
  vStrSql := ' 
    select nvl(Min(YY),0) minYY,nvl(Max(YY),0) maxyy  from (
    select YY from trf_m
    union
    select YY from dvs_m
    union
    select YY from rms_m
    )';
  execute immediate vStrSql
    into iBeginYY, iEndYY;

  iPeriod           := (iEndYY - iBeginYY + 1) * iCycle;
  pOut_nPeriodCount := iPeriod;

  select seq_tb_pimport.Nextval into pOut_strTableName from dual;
  pOut_strTableName := 'TB' || pOut_strTableName;

  vStrSql := 'CREATE TABLE ' || pOut_strTableName || '(
      type int ,
      coefficientID int ,
      dimensionID int ,
      No int ,
      Key varchar(50),
      Description varchar(500),
      Target number,
      UOM varchar(500),
      BeginYY int,
      Beginperiod int,
      EndYY int,
      Endperiod int
        ';
  for i in 1 .. iPeriod loop
    vStrSql := vStrSql || ',T_' || i || ' NUMBER';
  end loop;
  vStrSql := vStrSql || ')';
  execute immediate vStrSql;

  --add log
  sp_Log(p_Type      => 1,
         p_operation => 'FMSP_GetDimensionCoeff_CTB',
         p_status    => 0,
         p_logmsg    => pIn_nNodeType || '}' || pIn_nChronology || '}' ||
                        pIn_arrNodeAddr,
         p_sqltext   => vStrSql,
         p_logcode   => pOut_nSQLCode);
  if pOut_nSQLCode <> 0 then
    return;
  end if;

  vStrSql := 'select ';
  IF pIn_nNodeType = 10003 THEN
    --10003: node in pvt
  
    vStrfrom  := ' from pvt T';
    vStrWhere := ' where pvt_em_addr in (' || pIn_arrNodeAddr || ') ';
  
  ELSIF pIn_nNodeType = 1004 THEN
    --10004: node in sel 
  
    vStrfrom  := ' from v_sel_detailnode T';
    vStrWhere := ' where sel_em_addr in (' || pIn_arrNodeAddr || ') ';
  
  END IF;

  --10009:product*********************************************************
  iType   := 10009;
  vStrSql := ' select ' || iType ||
             ', coefficientID, dimensionID,No,Key ,Description
  ,(case No when 1 then prix_1 when 2 then prix_2 when 3 then prix_3 when 4 then prix_4 when 5 then prix_5 when 6 then prix_6 when 7 then prix_7 when 8 then prix_8 when 9 then prix_9 end) Target
  ,0 UOM 
  ,minYY BeginYY,1 Beginperiod, MaxYY EndYY,' || iCycle ||
             ' Endperiod';

  vStrfrom := vStrfrom ||
              '  join FMV_famtrf f on T.fam4_em_addr=f.dimensionID ';
  --Time series SQL
  vtssql     := ' select a.Nodeid,minYY,maxyy';
  vtsfromsql := ' from (select NodeID, Min(YY) minYY,Max(YY) maxyy from trf_m group by NodeID) a ';

  FOR iYY in iBeginYY .. iEndYY LOOP
    FOR iPeriod in 1 .. iCycle LOOP
      i       := i + 1;
      vtssql  := vtssql || ',t' || iYY || '.T' || iPeriod || ' T' || i;
      vStrSql := vStrSql || ',T' || i;
    END LOOP;
  
    vtsfromsql := vtsfromsql || ' left join trf_m t' || iYY ||
                  ' on a.Nodeid=t' || iYY || '.NodeID and t' || iYY ||
                  '.YY=' || iYY;
  
  END LOOP;
  vtssql := substr(vtssql, 2);

  vStrfrom := vStrfrom || ' left join (' || vtssql || vtsfromsql ||
              ') ts  on f.coefficientID=ts.NodeID ';

  vStrSql := 'insert into ' || pOut_strTableName || vStrSql || vStrfrom ||
             vStrWhere;
  execute immediate vStrSql;
  commit;
  --add log
  sp_Log(p_Type      => 1,
         p_operation => 'FMSP_GetDimensionCoeff',
         p_status    => 0,
         p_logmsg    => pIn_nNodeType || '}' || pIn_nChronology || '}' ||
                        pIn_arrNodeAddr,
         p_sqltext   => vStrSql,
         p_logcode   => pOut_nSQLCode);

  if pOut_nSQLCode <> 0 then
    return;
  end if;

exception
  when others then
    rollback;
    pOut_nSqlCode := SQLCODE;
    --raise_application_error(SQLCODE,SQLERRM);
    sp_log(p_type      => 1,
           p_operation => ' FMSP_GetDimensionCoeff ',
           p_status    => 1,
           p_logmsg    => ' FMSP_GetDimensionCoeff with error ' ||
                          substr(sqlerrm, 1, 200),
           p_sqltext   => vStrSql,
           p_logcode   => pOut_nSqlCode);
END;
/
