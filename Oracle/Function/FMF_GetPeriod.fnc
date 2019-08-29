create or replace function FMF_GetPeriod(pIn_nType     in number, --1 Monthly ,2 Weekly,3 Dayly
                                         pIn_nNodeType in number, --1  Detail Node  2  Aggregate Node
                                         pIn_nNodeID   in number,
                                         pIn_nTSID     in number,
                                         pIn_nBeginYY  in number,
                                         pIn_nEndYY    in number)
  RETURN number IS
  --*****************************************************************
  -- Description: get Period from time series
  --
  -- Parameters:
  --
  -- Author:  wfq
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        30-JAN-2013     wfq      Created.
  -- **************************************************************

  vStrSql      varchar(8000);
  vStrInto     varchar(8000);
  vStrField    varchar(8000);
  vType        varchar2(50);
  vTBName      varchar2(50);
  vCycle       int;
  VID          varchar2(50);
  iBeginYY     int := 0;
  iEndYY       int := 0;
  iBeginPeriod int := 0;
  iEndPeriod   int := 0;
  iPeriod      int := 0;

begin
  IF pIn_nType = 1 THEN
    vType  := 'M';
    vCycle := 12;
    --2 Week
  ELSIF pIn_nType = 2 THEN
    vType  := 'W';
    vCycle := 52;
  END IF;

  IF PIn_nNodeType = 1 THEN
    --1  Detail Node
    vTBName := 'DON_' || vType;
    VID     := 'PVTID';
  ELSIF PIn_nNodeType = 2 THEN
    --2  Aggregate Node
    vTBName := 'prb_' || vType;
    VID     := 'SELID';
  END IF;

  vStrSql := 'select min(YY),max(YY) from ' || vTBName || ' where tsID=' ||
             pIn_nTSID || ' and ' || VID || '=' || pIn_nNodeID ||
             ' and YY between ' || pIn_nBeginYY || ' and ' || pIn_nEndYY;
  execute immediate vStrSql
    into iBeginYY, iEndYY;
  if iBeginYY is null then
    return(iPeriod);
  end if;

  FOR iPeriod in 1 .. vCycle LOOP
    --vStrField := vStrField || '||nvl2(T' || iPeriod || ',''1'','','')';
    vStrField := vStrField || '||decode(T' || iPeriod ||
                 ','''','','',0,0,1)';
  END LOOP;

  --Start 0 ist nicht(cass:'00 123 10' return:6)
  while iBeginPeriod < 1 and iBeginYY <= iEndYY loop
    vStrSql := ' select nvl(max(' || substr(vStrField, 3) ||
               '),'',,,,'') from ' || vTBName || ' where tsID = ' ||
               pIn_nTSID || ' and YY = ' || iBeginYY || ' and ' || VID ||
               ' = ' || pIn_nNodeID;
    execute immediate vStrSql
      into vStrInto;
    --get first digital position
    iBeginPeriod := regexp_instr(vStrInto, '[1-9]');
    iBeginYY     := iBeginYY + 1;
  end loop;
  iBeginYY := iBeginYY - 1;

  IF iBeginYY = iEndYY THEN
    select vCycle - regexp_instr(reverse(vStrInto), '[0-9]') + 1
      into iEndPeriod
      from dual;
  
    iPeriod := iEndPeriod - iBeginPeriod + 1;
  ELSE
    vStrSql := ' select nvl(max(' || substr(vStrField, 3) ||
               '),'''') from ' || vTBName || ' where tsID = ' || pIn_nTSID ||
               ' and YY = ' || iEndYY || ' and ' || VID || ' = ' ||
               pIn_nNodeID;
    execute immediate vStrSql
      into vStrInto;
  
    select vCycle - regexp_instr(reverse(vStrInto), '[0-9]') + 1
      into iEndPeriod
      from dual;
    iPeriod := (iEndYY - iBeginYY - 1) * vCycle + iEndPeriod +
               (vCycle - iBeginPeriod) + 1;
  END IF;

  -- RETURN := iPeriod;

  return(iPeriod);
exception
  when others then
    Fmp_Log.loginfo(pIn_nNodeID);
    raise_application_error(-20114, sqlcode);
end FMF_GetPeriod;
/
