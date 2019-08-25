CREATE OR REPLACE PACKAGE FMIP_PEXPORT_BOS IS

  g_eException exception; --comments

  procedure FMSP_CreateTmpTable(pOut_vTableName OUT varchar2,
                                pOut_nSQLCode   out number);

  procedure FMISP_BOStoTable(pIn_vOptions    in varchar2,
                             pIn_sSeparator  in varchar2,
                             pIn_SDelimiter  in varchar2,
                             pOut_vTableName out varchar2,
                             pOut_nSQLCode   out number);

END FMIP_PEXPORT_BOS;
/
CREATE OR REPLACE PACKAGE BODY FMIP_PEXPORT_BOS IS

  --*****************************************************************
  -- Description: BOS.
  --
  -- Author:      <wfq>
  -- Revise
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        22-APR-2013     wfq           Created.

  -- **************************************************************

  procedure FMSP_CreateTmpTable(pOut_vTableName OUT varchar2,
                                pOut_nSQLCode   out number) as
    --*****************************************************************
    -- Description: create temp table
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        22-APR-2013     wfq           Created.
    -- **************************************************************
  
    vStrSql varchar(8000);
  
  BEGIN
    pOut_nSQLCode := 0;
  
    pOut_vTableName := fmf_gettmptablename();
    vStrSql         := 'create table ' || pOut_vTableName || ' (
      PVTNode varchar2(60) ,
      Bos clob )';
    execute immediate vStrSql;
  
  exception
    when others then
      pOut_nSqlCode := SQLCODE;
  END;

  procedure FMISP_BOStoTable(pIn_vOptions    in varchar2,
                             pIn_sSeparator  in varchar2,
                             pIn_SDelimiter  in varchar2,
                             pOut_vTableName out varchar2,
                             pOut_nSQLCode   out number) as
  
     cStrSql       clob;
    optionsRecord P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
  
  begin
  
    Fmp_Log.FMP_SetValue(pIn_vOptions);
    Fmp_Log.FMP_SetValue(pIn_sSeparator);
    Fmp_Log.FMP_SetValue(pIn_SDelimiter);
    Fmp_Log.LOGBEGIN;
  
    pOut_nSQLCode   := 0;
    pOut_vTableName := '';
    --Parse Options
    P_BATCHCOMMAND_COMMON.sp_ParseOptions(p_strOptions => pIn_vOptions,
                                          p_oOptions   => optionsRecord,
                                          p_nSqlCode   => pOut_nSqlCode);
  
    --create temp
    FMSP_CreateTmpTable(pOut_vTableName => pOut_vTableName,
                        pOut_nSQLCode   => pOut_nSQLCode);
    if pOut_nSQLCode <> 0 then
      return;
    end if;
  
    --insert temp table
    cStrSql := 'insert into ' || pOut_vTableName;
  
    cStrSql := cStrSql ||
               ' select t.pvt ,replace((max(sys_connect_by_path(t.val, ''' ||
               pIn_sSeparator || '''))),''/'',''' || pIn_sSeparator ||
               ''') as value
            from (select pvt,
               val,
               cur||pvt cur,
               case
                 when prev > 1 or prev = 1 then
                  prev||pvt
               end prev
          from (select ''' || pIn_SDelimiter ||
               '''||b1.b_cle||''' || pIn_SDelimiter || ''' pvt,''' ||
               pIn_SDelimiter || '''||b2.b_cle||''' || pIn_SDelimiter ||
               '/''||nvl(coeff,1)';
  
    IF optionsRecord.bBomtotal and optionsRecord.bDay THEN
      --Bomtotal and day
      --start day
      cStrSql := cStrSql ||
                 '||''/''||DECODE(startperiod,0,0,to_char(to_number(substr(startperiod,0,1))-1))';
      --startyear and startperiod
      cStrSql := cStrSql ||
                 '||''/''||startyear||nvl2(startyear,lpad(nvl(substr(startperiod,-2),startperiod),2,0),null)';
      --end day
      cStrSql := cStrSql ||
                 '||''/''||DECODE(endperiod,0,0,to_char(to_number(substr(endperiod,0,1))-1))';
      --endyear and endperiod
      cStrSql := cStrSql ||
                 '||''/''||endyear||nvl2(endyear,lpad(nvl(substr(endperiod,-2),endperiod),2,0),null)';
      --coef_perte
      cStrSql := cStrSql || '||''/''||coef_perte';
      --priority
      cStrSql := cStrSql || '||''/''||priorite';
      --Choice
      cStrSql := cStrSql || '||''/''||delai';
    
    ELSIF optionsRecord.bBomtotal THEN
      --Bomtotal
    
      --startyear and startperiod
      cStrSql := cStrSql ||
                 '||''/''||startyear||nvl2(startyear,lpad(nvl(substr(startperiod,-2),startperiod),2,0),null)';
      --endyear and endperiod
      cStrSql := cStrSql ||
                 '||''/''||endyear||nvl2(endyear,lpad(nvl(substr(endperiod,-2),endperiod),2,0),null)';
      --coef_perte
      cStrSql := cStrSql || '||''/''||coef_perte';
      --priority
      cStrSql := cStrSql || '||''/''||priorite';
      --Choice
      cStrSql := cStrSql || '||''/''||delai';
    
    ELSIF optionsRecord.bMtotal and optionsRecord.bDay THEN
      --Mtotal and day
    
      --start day
      cStrSql := cStrSql ||
                 '||''/''||DECODE(startperiod,0,0,to_char(to_number(substr(startperiod,0,1))-1))';
      --startyear and startperiod
      cStrSql := cStrSql ||
                 '||''/''||startyear||nvl2(startyear,lpad(nvl(substr(startperiod,-2),startperiod),2,0),null)';
      --end day
      cStrSql := cStrSql ||
                 '||''/''||DECODE(endperiod,0,0,to_char(to_number(substr(endperiod,0,1))-1))';
      --endyear and endperiod
      cStrSql := cStrSql ||
                 '||''/''||endyear||nvl2(endyear,lpad(nvl(substr(endperiod,-2),endperiod),2,0),null)';
      --coef_perte
      cStrSql := cStrSql || '||''/''||coef_perte';
    
    ELSIF optionsRecord.bMtotal THEN
      --Mtotal
    
      --startyear and startperiod
      cStrSql := cStrSql ||
                 '||''/''||startyear||nvl2(startyear,lpad(nvl(substr(startperiod,-2),startperiod),2,0),null)';
      --endyear and endperiod
      cStrSql := cStrSql ||
                 '||''/''||endyear||nvl2(endyear,lpad(nvl(substr(endperiod,-2),endperiod),2,0),null)';
      --coef_perte
      cStrSql := cStrSql || '||''/''||coef_perte';
    
    END IF;
  
    cStrSql := cStrSql || ' as val,
               row_number() over(partition by b1.b_cle order by b2.b_cle) cur,
               row_number() over(partition by b1.b_cle order by b2.b_cle) - 1 prev

      from supplier s, bdg b1, bdg b2
      where s.id_supplier=78
      and s.pere_bdg=b1.bdg_em_addr
      and s.fils_bdg=b2.bdg_em_addr )) t
                group by t.pvt
       start with t.prev is null
      connect by prior t.cur = t.prev';
    --add log
    Fmp_Log.logInfo(pIn_cSqlText => cStrSql);
    fmsp_execsql(pIn_cSql => cStrSql);
    Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSQLCode := sqlcode;
  end;

END FMIP_PEXPORT_BOS;
/
