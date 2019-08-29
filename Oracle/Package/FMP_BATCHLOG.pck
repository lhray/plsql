create or replace package FMP_BATCHLOG is

  -- Author  : LSANG
  -- Created : 4/9/2013 6:43:17 PM
  -- Purpose :

  -- Public type declarations
  TaskID varchar2(1000);

  procedure FMSP_BatchLogInit;

  function FMF_GetTaskID return varchar2;

  procedure FMSP_ADDBatchLOG(pIn_vLineNumber in varchar2 default null,
                             pIn_vLineText   in varchar2 default null,
                             pIn_vLogCode    in varchar2 default null,
                             pIn_vLogParams  in varchar2 default null,
                             pOut_nSqlCode   out number);

  procedure FMSP_DelBatchLog(pIn_nTaskID   in number,
                             pOut_nSqlCode out number);

  procedure FMSP_InsertLog(pIn_strConditionSql in varchar2,
                           pIn_strDeleteSql    in varchar2,
                           pIn_nLogCode        in number,
                           pIn_strLogParams    in varchar2,
                           pIn_strTable        in varchar2,
                           pIn_nDelete         in number,
                           pInOut_nTaskId      in out number);

  procedure FMSP_InsertLog(pIn_nLogCode     in number,
                           pIn_strLogParams in varchar2,
                           pIn_strTable     in varchar2,
                           pInOut_nTaskId   in out number);

  procedure FMSP_LogBOS(pIn_strTableName     in varchar2,
                        pIn_nMaxColumnLength in number,
                        pIn_oOptionsRecord   in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        pInOut_nTaskId       in out number);

  procedure FMSP_LogDimension(pIn_strTableName   in varchar2,
                              pIn_nCommandNumber in integer,
                              pInOut_nTaskId     in out number);
  procedure FMSP_LogDimensionAttribute(pIn_strTableName   in varchar2,
                                       pIn_nCommandNumber in integer,
                                       pInOut_nTaskId     in out number);

  procedure FMSP_LogDetailLevel(pIn_strTableName  in varchar2,
                                pIn_strOption     in varchar2,
                                pIn_strProColName in varchar2 := 'product',
                                pIn_strSTColName  in varchar2 := 'sales',
                                pIn_strTCColName  in varchar2 := 'trade',
                                pInOut_nTaskId    in out number);

  procedure FMSP_LogAggLevel(pIn_strTableName      in varchar2,
                             pIn_strOption         in varchar2,
                             pIn_nDeleteAggNode    in number, -- 1 delete agg node, other number not delete
                             pIn_strAggNodeColName in varchar2 := 'aggnode',
                             pIn_strProColName     in varchar2 := 'product',
                             pIn_strSTColName      in varchar2 := 'sales',
                             pIn_strTCColName      in varchar2 := 'trade',
                             pInOut_nTaskId        in out number);

end FMP_BATCHLOG;
/
create or replace package body FMP_BATCHLOG is

  -- Private type declarations
  procedure FMSP_BatchLogInit as
  
  begin
    TaskID := SEQ_BatchTaskID.Nextval;
  end;

  function FMF_GetTaskID return varchar2 as
  begin
    return TaskID;
  end;

  procedure FMSP_ADDBatchLOG(pIn_vLineNumber in varchar2 default null,
                             pIn_vLineText   in varchar2 default null,
                             pIn_vLogCode    in varchar2 default null,
                             pIn_vLogParams  in varchar2 default null,
                             pOut_nSqlCode   out number) as
    --*****************************************************************
    -- Description: this procedure  is add batch log .
    --
    -- Parameters:
    --            pIn_vLineNumber
    --            pIn_vLineText
    --            pIn_vLogCode
    --            pIn_vLogParams
    --            pOut_nSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        10-Arpil-2013     LiSang         Created.
    -- **************************************************************
    PRAGMA autonomous_transaction;
  
  begin
  
    insert into FMBatchLOG
      (Taskid, Logtime, Linenumber, Linetext, Logcode, Logparams)
    values
      (TaskID,
       sysdate,
       pIn_vLineNumber,
       pIn_vLineText,
       pIn_vLogCode,
       pIn_vLogParams);
    commit;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_DelBatchLog(pIn_nTaskID   in number,
                             pOut_nSqlCode out number)
  
   as
  
  begin
  
    if pIn_nTaskID is null then
      pOut_nSqlCode := 0;
      return;
    end if;
  
    delete from FMBatchLOG t where t.taskid = pIn_nTaskID;
    commit;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_InsertLog(pIn_nLogCode     in number,
                           pIn_strLogParams in varchar2,
                           pIn_strTable     in varchar2,
                           pInOut_nTaskId   in out number) as
    v_strsql varchar2(32767);
  begin
    if pInOut_nTaskId = 0 then
      -- Set Task ID
      FMP_BATCHLOG.FMSP_BatchLogInit;
      pInOut_nTaskId := FMP_BATCHLOG.FMF_GetTaskID;
    end if;
  
    v_strsql := 'insert into FMBatchLOG (TaskID,LogTime, LineNumber, LogCode, LOGparams)
               select ' || pInOut_nTaskId ||
                ',sysdate,LineNumber,' || pIn_nLogCode || ',(' ||
                pIn_strLogParams || ') LOGparams from ' || pIn_strTable;
  
    fmsp_execsql(v_strsql);
  end FMSP_InsertLog;

  procedure FMSP_InsertLog(pIn_strConditionSql in varchar2,
                           pIn_strDeleteSql    in varchar2,
                           pIn_nLogCode        in number,
                           pIn_strLogParams    in varchar2,
                           pIn_strTable        in varchar2,
                           pIn_nDelete         in number,
                           pInOut_nTaskId      in out number) as
    v_nBatchLogCount number;
  begin
    execute immediate pIn_strConditionSql
      into v_nBatchLogCount;
  
    if v_nBatchLogCount > 0 then
      FMSP_InsertLog(pIn_nLogCode,
                     pIn_strLogParams,
                     pIn_strTable,
                     pInOut_nTaskId);
    
      if pIn_nDelete = 1 then
        fmsp_execsql(pIn_strDeleteSql);
      end if;
    end if;
  
  end FMSP_InsertLog;

  procedure FMSP_LogBOS(pIn_strTableName     in varchar2,
                        pIn_nMaxColumnLength in number,
                        pIn_oOptionsRecord   in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        pInOut_nTaskId       in out number) as
    v_nMaxColumnCount number;
    v_nCount          number;
    v_strSql          varchar(5000); --clob;
    v_nLogCode        number;
    v_strLogParams    varchar(1000);
    v_strConditionSql varchar(5000); --clob;
    v_strSourceSql    varchar(5000); --clob;
  begin
    Fmp_Log.LOGBEGIN('FMSP_LogBOS');
    v_nMaxColumnCount := pIn_nMaxColumnLength - 1;
    v_nCount          := 1;
  
    v_strSql := 'select LineNumber, PERE_BDG_Key b_cle
                   from ' || pIn_strTableName || ' ';
  
    while v_nMaxColumnCount > 0 loop
      v_strSql := v_strSql || '
                    union all (select LineNumber, FILS_BDG_Key_' ||
                  to_char(v_nCount) ||
                  ' b_cle
                                from ' || pIn_strTableName || '
                                where FILS_BDG_Key_' ||
                  to_char(v_nCount) || ' is not null)';
    
      v_nMaxColumnCount := v_nMaxColumnCount - 2;
    
      -- day##bomtotal or day##mototal
      if pIn_oOptionsRecord.bDay and
         (pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal) then
        v_nMaxColumnCount := v_nMaxColumnCount - 1;
      end if;
    
      -- mtotal or bomtotal
      if pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
        v_nMaxColumnCount := v_nMaxColumnCount - 1;
      end if;
    
      -- day##mtotal or day##bomtotal
      if pIn_oOptionsRecord.bDay and
         (pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal) then
        v_nMaxColumnCount := v_nMaxColumnCount - 1;
      end if;
    
      -- mtotal or bomtotal
      if pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
        v_nMaxColumnCount := v_nMaxColumnCount - 2;
      end if;
    
      -- bomtotal
      if pIn_oOptionsRecord.bBomtotal then
        v_nMaxColumnCount := v_nMaxColumnCount - 2;
      end if;
    
      v_nCount := v_nCount + 1;
    end loop;
  
    v_strConditionSql := 'select count(*) 
                            from (' || v_strSql || ') t
                           where not exists (select 1 
                                               from bdg b
                                              where b.b_cle = t.b_cle)';
    v_strLogParams    := 'b_cle||chr(9)||LineNumber';
    v_nLogCode        := p_Constant.KeyNotExists_LOGCODE;
    v_strSourceSql    := '(select LineNumber, b_cle 
                             from (' || v_strSql || ') t
                            where not exists (select 1 
                                                from bdg b 
                                               where b.b_cle = t.b_cle))';
  
    FMP_BATCHLOG.FMSP_InsertLog(v_strConditionSql,
                                '',
                                v_nLogCode,
                                v_strLogParams,
                                v_strSourceSql,
                                0,
                                pInOut_nTaskId);
                                
    Fmp_Log.LOGEND('FMSP_LogBOS');
  end;

  procedure FMSP_LogDimension(pIn_strTableName   in varchar2,
                              pIn_nCommandNumber in integer,
                              pInOut_nTaskId     in out number) as
    v_strConditionSql varchar2(5000);
    v_strDeleteSql    varchar2(5000);
    v_nLogCode        number;
    v_strLogParams    varchar2(100);
    v_strTable        varchar2(5000);
  begin
    fmp_log.LOGBEGIN('FMSP_LogDimension');
  
    v_strConditionSql := 'select count(*) 
                            from ' ||
                         pIn_strTableName || ' a
                           where LineNumber <(select max(LineNumber) 
                                                from ' ||
                         pIn_strTableName || ' b
                                                where a.key=b.key)';
  
    v_strDeleteSql := 'delete from ' || pIn_strTableName || ' a
                              where LineNumber <
                                    (select max(LineNumber) 
                                    from ' ||
                      pIn_strTableName || ' b
                                    where a.key=b.key)';
  
    v_nLogCode     := f_GetLOGCODE(pIn_nCommandNumber);
    v_strLogParams := 'tDelete.key||chr(9)||tDelete.LineNumber||chr(9)||tDelete.LeftLineNumber';
    v_strTable     := '(select t1.LineNumber, t1.Key, t2.LineNumber as LeftLineNumber
                         from (select distinct LineNumber, a.Key 
                                 from ' ||
                      pIn_strTableName || ' a
                                 where LineNumber < 
                                 (select max(LineNumber) 
                                    from ' ||
                      pIn_strTableName || ' b 
                                    where a.key = b.key)) t1
                          left join (select Max(a.LineNumber) LineNumber, a.Key 
                                       from ' ||
                      pIn_strTableName || ' a 
                                       group by a.key) t2
                          on t1.key = t2.key) tDelete';
  
    FMSP_InsertLog(v_strConditionSql,
                   v_strDeleteSql,
                   v_nLogCode,
                   v_strLogParams,
                   v_strTable,
                   1,
                   pInOut_nTaskId);
  
    fmp_log.LOGEND('FMSP_LogDimension');
  end;

  procedure FMSP_LogDimensionAttribute(pIn_strTableName   in varchar2,
                                       pIn_nCommandNumber in integer,
                                       pInOut_nTaskId     in out number) as
    v_strConditionSql varchar2(5000);
    v_nLogCode        number;
    v_strDeleteSql    varchar(5000);
    v_strLogParams    varchar2(500);
    v_strTable        clob;
  begin
    fmp_log.LOGBEGIN('FMSP_LogDimensionAttribute');
  
    v_strConditionSql := 'select count(*) from ' || pIn_strTableName ||
                         ' where linenumber not in (select min(linenumber) from ' ||
                         pIn_strTableName || ' group by ' ||
                         pIn_strTableName || '.Attr_No, ' ||
                         pIn_strTableName || '.Attr_Value_Key)';
  
    --delete duplicate data
    v_strDeleteSql := 'delete from ' || pIn_strTableName ||
                      ' where linenumber not in (select min(linenumber) from ' ||
                      pIn_strTableName || ' group by ' || pIn_strTableName ||
                      '.Attr_No, ' || pIn_strTableName ||
                      '.Attr_Value_Key)';
  
    -- Set Log Code
    v_nLogCode     := f_GetLOGCODE(pIn_nCommandNumber);
    v_strLogParams := 'tDelete.Attr_No||chr(9)||nvl(tDelete.Attr_Value_Key, ''null'')||chr(9)||tDelete.LineNumber||chr(9)||tDelete.LeftLineNumber';
  
    v_strTable := '(select t1.LineNumber,
                                   t1.Attr_No,
                                   t1.Attr_Value_Key,
                                   t2.LineNumber LeftLineNumber
                               from (select LineNumber, a.Attr_No, a.Attr_Value_Key
                                       from ' ||
                  pIn_strTableName || ' a
                              where linenumber not in
                                    (select min(linenumber)
                                       from ' ||
                  pIn_strTableName || ' b
                                      group by b.Attr_No, b.Attr_Value_Key)) t1
                               left join (select Min(LineNumber) LineNumber, Attr_No, Attr_Value_Key
                                            from ' ||
                  pIn_strTableName || '
                                          group by Attr_No, Attr_Value_Key) t2
                                on (t1.Attr_No = t2.Attr_No and nvl(t1.Attr_Value_Key,0) = nvl(t2.Attr_Value_Key,0))) tDelete';
  
    FMSP_InsertLog(v_strConditionSql,
                   v_strDeleteSql,
                   v_nLogCode,
                   v_strLogParams,
                   v_strTable,
                   1,
                   pInOut_nTaskId);
  
    fmp_log.LOGEND('FMSP_LogDimensionAttribute');
  end;

  procedure FMSP_GetNodeSql(pIn_strOption     in varchar2,
                            pIn_strProColName in varchar2 := 'product',
                            pIn_strSTColName  in varchar2 := 'sales',
                            pIn_strTCColName  in varchar2 := 'trade',
                            pOut_cSql         out clob) IS
    v_strNodeSql varchar(5000);
  begin
  
    --Have no option -NODIS
    if instr(upper(pIn_strOption), 'NODIS') <= 0 or
       instr(upper(pIn_strOption), 'NODIS') is null then
    
      -- product sales and trade as DetailNode
      --Have no option -NOGEO
      if instr(upper(pIn_strOption), 'NOGEO') <= 0 or
         instr(upper(pIn_strOption), 'NOGEO') is null then
      
        v_strNodeSql := 'replace(case';
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strSTColName ||
                        ' is not null and';
        v_strNodeSql := v_strNodeSql || '       t.' || pIn_strTCColName ||
                        ' is not null) then';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName ||
                        ' ||''' || '-' || ''' || t.' || pIn_strSTColName ||
                        ' ||''' || '-' || '''|| t.' || pIn_strTCColName || '';
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strSTColName ||
                        ' is not null and t.' || pIn_strTCColName ||
                        ' is null) then ';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName ||
                        ' ||''' || '-' || '''|| t.' || pIn_strSTColName || '';
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strSTColName ||
                        ' is null and t.' || pIn_strTCColName ||
                        ' is not null) then ';
        v_strNodeSql := v_strNodeSql || '      t.' || pIn_strProColName ||
                        ' ||''' || '-' || '''|| t.' || pIn_strTCColName || '';
        v_strNodeSql := v_strNodeSql || '     when (t.' || pIn_strSTColName ||
                        ' is null and t.' || pIn_strTCColName ||
                        ' is null) then ';
        v_strNodeSql := v_strNodeSql || '      t.' || pIn_strProColName || ' ';
        v_strNodeSql := v_strNodeSql || '   end,';
        v_strNodeSql := v_strNodeSql || '''' || '"' || ''',';
        v_strNodeSql := v_strNodeSql || '''' || ''')';
      else
        -- Have option -NOGEO
        -- product and trade as DetailNode
        --Don't consider sales
        v_strNodeSql := 'replace(case when (t.' || pIn_strProColName ||
                        ' is not null and t.' || pIn_strTCColName ||
                        ' is not null';
        v_strNodeSql := v_strNodeSql || '     ) then';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName ||
                        ' ||''' || '-' || ''' || t.' || pIn_strTCColName || ' ';
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strProColName ||
                        ' is not null and t.' || pIn_strTCColName ||
                        ' is  null ) then ';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName || ' ';
        --t.product is null
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strProColName ||
                        ' is  null and t.' || pIn_strTCColName ||
                        ' is not null ) then ';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strTCColName || ' ';
        v_strNodeSql := v_strNodeSql || '   end,';
        v_strNodeSql := v_strNodeSql || '''' || '"' || ''',';
        v_strNodeSql := v_strNodeSql || '''' || ''')';
      
      end if;
    
    else
    
      if instr(upper(pIn_strOption), 'NOGEO') <= 0 or
         instr(upper(pIn_strOption), 'NOGEO') is null then
        -- Have option -NODIS
        -- product and Sales as DetailNode 
        --t.product is not null
        v_strNodeSql := 'replace(case when (t.' || pIn_strProColName ||
                        ' is not null and t.' || pIn_strSTColName ||
                        ' is not null';
        v_strNodeSql := v_strNodeSql || '     ) then';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName ||
                        ' ||''' || '-' || ''' || t.' || pIn_strSTColName || ' ';
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strProColName ||
                        ' is not null and t.' || pIn_strSTColName ||
                        ' is  null ) then ';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strProColName || ' ';
        --t.product is null
        v_strNodeSql := v_strNodeSql || ' when (t.' || pIn_strProColName ||
                        ' is  null and t.' || pIn_strSTColName ||
                        ' is not null ) then ';
        v_strNodeSql := v_strNodeSql || ' t.' || pIn_strSTColName || ' ';
        v_strNodeSql := v_strNodeSql || '   end,';
        v_strNodeSql := v_strNodeSql || '''' || '"' || ''',';
        v_strNodeSql := v_strNodeSql || '''' || ''')';
      else
        -- product as DetailNode
        v_strNodeSql := 't.' || pIn_strProColName;
      end if;
    
    end if;
  
    pOut_cSql := v_strNodeSql;
  
  end;

  procedure FMSP_LogDetailLevel(pIn_strTableName  in varchar2,
                                pIn_strOption     in varchar2,
                                pIn_strProColName in varchar2 := 'product',
                                pIn_strSTColName  in varchar2 := 'sales',
                                pIn_strTCColName  in varchar2 := 'trade',
                                pInOut_nTaskId    in out number) as
    v_strConditionSql  varchar(5000);
    v_strDetailNodeSql varchar(5000);
  
    v_nBatchLogCount number;
    v_strTable       varchar(100);
    v_strTempTable   varchar(100);
    v_strLogSql      clob;
    v_strLogNodeSql  clob;
    v_strProNodeSql  varchar(5000);
    v_strGEONodeSql  varchar(5000);
    v_strDisNodeSql  varchar(5000);
    v_nProNodeSql    number;
    v_nGEONodeSql    number;
    v_nDisNodeSql    number;
    v_strSql         clob;
  begin
    v_strLogNodeSql := '';
    v_strProNodeSql := '';
    v_strGEONodeSql := '';
    v_strDisNodeSql := '';
  
    v_nProNodeSql := 0;
    v_nGEONodeSql := 0;
    v_nDisNodeSql := 0;
  
    Fmp_Log.LOGBEGIN('FMSP_LogDetailLevel');
  
    FMSP_GetNodeSql(pIn_strOption,
                    pIn_strProColName,
                    pIn_strSTColName,
                    pIn_strTCColName,
                    v_strDetailNodeSql);
  
    -- Get not Existed product and sales
    v_strSql := 'select LineNumber, ' || pIn_strProColName || ', ' ||
                pIn_strSTColName || ', ' || pIn_strTCColName || '
                     from ' || pIn_strTableName || ' t
                   where not exists (select F_CLE
                                        from fam f
                                       where t.' ||
                pIn_strProColName || ' = f.F_CLE and ID_FAM = 80)
                         and t.' || pIn_strProColName ||
                ' is not null
                     ';
  
    if instr(upper(pIn_strOption), 'NOGEO') <= 0 or
       instr(upper(pIn_strOption), 'NOGEO') is null then
      v_strSql := v_strSql || 'union all (select LineNumber, ' ||
                  pIn_strProColName || ', ' || pIn_strSTColName || ',' ||
                  pIn_strTCColName || '
                                  from ' ||
                  pIn_strTableName || ' t
                                 where not exists(select G_CLE 
                                                    from GEO f 
                                                   where t.' ||
                  pIn_strSTColName ||
                  ' = f.G_CLE)
                                        and t.' ||
                  pIn_strSTColName || ' is not null)';
    
    end if;
  
    if instr(upper(pIn_strOption), 'NODIS') <= 0 or
       instr(upper(pIn_strOption), 'NODIS') is null then
    
      -- Get not existed trade
      v_strSql := v_strSql || 'union all (select LineNumber, ' ||
                  pIn_strProColName || ', ' || pIn_strSTColName || ', ' ||
                  pIn_strTCColName || '
                                  from ' ||
                  pIn_strTableName || ' t
                                 where not exists(select D_CLE 
                                                    from dis d 
                                                   where d.D_CLE = t.' ||
                  pIn_strTCColName || ')
                                        and t.' ||
                  pIn_strTCColName || ' is not null)';
    
    end if;
  
    v_strTable := fmf_gettmptablename();
    v_strSql   := 'create table ' || v_strTable || ' as ' || v_strSql;
    fmsp_execsql(v_strSql);
  
    v_strConditionSql := 'select count(*) from ' || v_strTable || '';
  
    execute immediate v_strConditionSql
      into v_nBatchLogCount;
  
    -- Has not existed product sales or trade
    if v_nBatchLogCount > 0 then
    
      if pInOut_nTaskId = 0 then
        -- Set Task ID
        FMP_BATCHLOG.FMSP_BatchLogInit;
        pInOut_nTaskId := FMP_BATCHLOG.FMF_GetTaskID;
      end if;
    
      v_strLogSql := 'select ' || pInOut_nTaskId ||
                     ' as TaskID,
                           sysdate as LogTime,
                           LineNumber,
                           ' ||
                     p_Constant.ProductKeyNotExists_LOGCODE ||
                     ' as LogCode,
                           (' || pIn_strProColName ||
                     ' || chr(9) || LineNumber) LOGparams
                    from (select distinct t.Linenumber, t.' ||
                     pIn_strProColName || '
                           from ' || v_strTable || ' t
                          where not exists (select F_CLE 
                                             from fam f
                                             where t.' ||
                     pIn_strProColName ||
                     ' = f.F_CLE
                                             and ID_FAM = 80))';
    
      if instr(upper(pIn_strOption), 'NOGEO') <= 0 or
         instr(upper(pIn_strOption), 'NOGEO') is null then
        v_strLogSql := v_strLogSql || 'union all (select ' ||
                       pInOut_nTaskId ||
                       '  as TaskID,
                                      sysdate as LogTime,
                                      LineNumber,
                                      ' ||
                       p_Constant.STKeyNotExists_LOGCODE ||
                       ' as LogCode,
                                      (' ||
                       pIn_strSTColName ||
                       ' || chr(9) || LineNumber) LOGparams
                               from (select distinct t.Linenumber, t.' ||
                       pIn_strSTColName || '
                                      from  ' ||
                       v_strTable || ' t
                                     where not exists(select G_CLE 
                                                        from GEO f 
                                                       where t.' ||
                       pIn_strSTColName || ' = f.G_CLE)) t2)';
      end if;
    
      if instr(upper(pIn_strOption), 'NODIS') <= 0 or
         instr(upper(pIn_strOption), 'NODIS') is null then
      
        v_strLogSql := v_strLogSql || ' union all (select ' ||
                       pInOut_nTaskId ||
                       ' as TaskID,
                                                        sysdate as LogTime,
                                        LineNumber,
                                        ' ||
                       p_Constant.TCKeyNotExists_LOGCODE ||
                       ' as LogCode,
                                        (' ||
                       pIn_strTCColName ||
                       ' || chr(9) || LineNumber) LOGparams
                                  from (select distinct t.Linenumber, t.' ||
                       pIn_strTCColName || '
                                               from ' ||
                       v_strTable || ' t
                                where not exists(select D_CLE 
                                                   from dis d
                                                  where d.D_CLE = t.' ||
                       pIn_strTCColName || ')) t3)';
      
      end if;
    
      if instr(upper(pIn_strOption), 'PRO') <= 0 or
         instr(upper(pIn_strOption), 'PRO') is null then
        v_nProNodeSql   := 1;
        v_strProNodeSql := 'select distinct LineNumber,(' ||
                           v_strDetailNodeSql ||
                           ') detailnode
                              from ' ||
                           v_strTable || ' t
                            where not exists (select F_CLE 
                                               from fam f
                                              where t.' ||
                           pIn_strProColName ||
                           ' = f.F_CLE
                                              and ID_FAM = 80)';
      
      end if;
    
      if instr(upper(pIn_strOption), 'GEO') <= 0 or
         instr(upper(pIn_strOption), 'GEO') is null then
        if instr(upper(pIn_strOption), 'NOGEO') > 0 then
          --Have option -NOGEO, Don't consider sales
          null;
        else
          v_nGEONodeSql   := 1;
          v_strGEONodeSql := 'select distinct LineNumber,(' ||
                             v_strDetailNodeSql ||
                             ') detailnode
                           from ' || v_strTable || ' t
                           where not exists (select G_CLE 
                                               from GEO f 
                                              where t.' ||
                             pIn_strSTColName || ' = f.G_CLE)';
        end if;
      
      end if;
    
      if instr(upper(pIn_strOption), 'DIS') <= 0 or
         instr(upper(pIn_strOption), 'DIS') is null then
      
        if instr(upper(pIn_strOption), 'NODIS') > 0 then
          --Have option -NODIS, Don't consider dis
          null;
        else
          v_nDisNodeSql   := 1;
          v_strDisNodeSql := 'select distinct LineNumber,(' ||
                             v_strDetailNodeSql ||
                             ') detailnode
                             from ' ||
                             v_strTable ||
                             ' t where not exists (select D_CLE from dis d where d.D_CLE = t.' ||
                             pIn_strTCColName || ')';
        end if;
      
      end if;
    
      -- option has -pro or -geo or -dis
      if v_nProNodeSql = 1 or v_nGEONodeSql = 1 or v_nDisNodeSql = 1 then
      
        if v_nProNodeSql = 1 then
          v_strLogNodeSql := v_strProNodeSql;
        end if;
      
        if v_nGEONodeSql = 1 then
          if v_nProNodeSql = 0 then
            v_strLogNodeSql := v_strGEONodeSql;
          else
            v_strLogNodeSql := v_strLogNodeSql || ' union all (' ||
                               v_strGEONodeSql || ')';
          end if;
        end if;
      
        if v_nDisNodeSql = 1 then
          if v_nProNodeSql = 0 and v_nGEONodeSql = 0 then
            v_strLogNodeSql := v_strDisNodeSql;
          else
            v_strLogNodeSql := v_strLogNodeSql || ' union all (' ||
                               v_strDisNodeSql || ')';
          end if;
        end if;
      
        v_strLogNodeSql := 'select ' || pInOut_nTaskId ||
                           ' as TaskID,
                                         sysdate as LogTime,
                                         LineNumber,
                                         ' ||
                           p_Constant.DetaiNodeKeyNotExists_LOGCODE ||
                           ' as LogCode,
                                         (detailnode|| chr(9) ||LineNumber) LOGparams 
                              from (select distinct LineNumber, detailnode
                                      from (' ||
                           v_strLogNodeSql || '))';
      
        v_strLogSql := v_strLogSql || 'union all (' || v_strLogNodeSql || ')';
      end if;
    
      v_strLogSql := 'insert into FMBatchLOG (TaskID,LogTime, LineNumber, LogCode, LOGparams)' ||
                     v_strLogSql;
    
      fmsp_execsql(v_strLogSql);
    
    end if;
  
    --3D are existed but detailnode not existed
    --Not existed detail node
    v_strSql := 'select linenumber,detailnode from (select linenumber,
        ' || v_strDetailNodeSql || 'as detailnode
          from ' || pIn_strTableName ||
                ' t where linenumber not in(select linenumber from ' ||
                v_strTable ||
                ') ) t
                   where 
                   not exists(select 1 from bdg b where b.b_cle = t.detailnode)';
  
    v_strTempTable := fmf_gettmptablename();
    v_strSql       := 'create table ' || v_strTempTable || ' as ' ||
                      v_strSql;
    fmsp_execsql(v_strSql);
  
    v_strConditionSql := 'select count(*) from ' || v_strTempTable || '';
  
    execute immediate v_strConditionSql
      into v_nBatchLogCount;
  
    -- Has not existed detail node
    if v_nBatchLogCount > 0 then
    
      if pInOut_nTaskId = 0 then
        -- Set Task ID
        FMP_BATCHLOG.FMSP_BatchLogInit;
        pInOut_nTaskId := FMP_BATCHLOG.FMF_GetTaskID;
      end if;
    
      v_strLogNodeSql := 'select ' || pInOut_nTaskId ||
                         ' as TaskID,
                                         sysdate as LogTime,
                                         LineNumber,
                                         ' ||
                         p_Constant.DetaiNodeKeyNotExists_LOGCODE ||
                         ' as LogCode,
                                         (detailnode|| chr(9) ||LineNumber) LOGparams 
                              from (select distinct LineNumber, detailnode
                                      from ' ||
                         v_strTempTable || ')';
    
      v_strLogSql := 'insert into FMBatchLOG (TaskID,LogTime, LineNumber, LogCode, LOGparams)' ||
                     v_strLogNodeSql;
    
      fmsp_execsql(v_strLogSql);
    
    end if;
  
    fmsp_execsql('drop table ' || v_strTable);
    fmsp_execsql('drop table ' || v_strTempTable);
  
    Fmp_Log.LOGEND('FMSP_LogDetailLevel');
  end;

  procedure FMSP_LogAggLevel(pIn_strTableName      in varchar2,
                             pIn_strOption         in varchar2,
                             pIn_nDeleteAggNode    in number, -- 1 delete agg node, other number not delete
                             pIn_strAggNodeColName in varchar2 := 'aggnode',
                             pIn_strProColName     in varchar2 := 'product',
                             pIn_strSTColName      in varchar2 := 'sales',
                             pIn_strTCColName      in varchar2 := 'trade',
                             pInOut_nTaskId        in out number) as
    v_strConditionSql Clob;
    v_strTempSql      varchar(5000);
    v_strAggrNodeSql  varchar(5000);
    v_strTable        varchar(100);
    v_strSql          Clob;
    v_nBatchLogCount  number;
  begin
    Fmp_Log.LOGBEGIN('FMSP_LogAggLevel');
    v_strTable := fmf_gettmptablename();
  
    if instr(upper(pIn_strOption), 'P2R') > 0 then
    
      FMSP_GetNodeSql(pIn_strOption,
                      pIn_strProColName,
                      pIn_strSTColName,
                      pIn_strTCColName,
                      v_strAggrNodeSql);
    
      v_strSql := 'select LineNumber, ' || pIn_strAggNodeColName || '
                                   from ' ||
                  pIn_strTableName || '  n
                                   where not exists (select n.' ||
                  pIn_strAggNodeColName || '
                                                       from sel t
                                                       where t.sel_bud = 71
                                                       and t.sel_cle = n.' ||
                  pIn_strAggNodeColName || ')  and n.' ||
                  pIn_strAggNodeColName ||
                  ' is not null
                            union all
                                  (select LineNumber, ' ||
                  pIn_strAggNodeColName || '
                                     from (select LineNumber,' ||
                  v_strAggrNodeSql || '
                                                  as ' ||
                  pIn_strAggNodeColName || '
                                             from ' ||
                  pIn_strTableName || '  t
                                             where t.' ||
                  pIn_strAggNodeColName ||
                  ' is null) n
                                    where not exists (select n.' ||
                  pIn_strAggNodeColName || '
                                    from sel t
                                    where t.sel_bud = 71
                                    and t.sel_cle = n.' ||
                  pIn_strAggNodeColName || ')
                                    and n.' ||
                  pIn_strAggNodeColName || ' is not null)';
    
    else
    
      v_strSql := 'select LineNumber, ' || pIn_strAggNodeColName || '
                              from ' || pIn_strTableName || ' n
                              where not exists (select n.' ||
                  pIn_strAggNodeColName || '
                              from sel t
                              where t.sel_bud = 71
                              and t.sel_cle = n.' ||
                  pIn_strAggNodeColName || ')';
    end if;
  
    v_strSql := 'create table ' || v_strTable || ' as ' || v_strSql;
    fmsp_execsql(v_strSql);
  
    v_strConditionSql := 'select count(*) from ' || v_strTable;
  
    execute immediate v_strConditionSql
      into v_nBatchLogCount;
  
    -- Has not existed AggNode
    if v_nBatchLogCount > 0 then
    
      if pInOut_nTaskId = 0 then
        -- Set Task ID
        FMP_BATCHLOG.FMSP_BatchLogInit;
        pInOut_nTaskId := FMP_BATCHLOG.FMF_GetTaskID;
      end if;
    
      v_strTempSql := 'select ' || pInOut_nTaskId ||
                      ' as TaskID,
                           sysdate as LogTime,
                           LineNumber,
                           ' ||
                      p_Constant.AggrNodeNotExists_LOGCODE ||
                      ' as LogCode,
                           (' || pIn_strAggNodeColName ||
                      '|| chr(9) || LineNumber) LOGparams
                    from ' || v_strTable;
    
      v_strTempSql := 'insert into FMBatchLOG (TaskID,LogTime, LineNumber, LogCode, LOGparams)' ||
                      v_strTempSql;
    
      fmsp_execsql(v_strTempSql);
    
      if pIn_nDeleteAggNode = 1 then
      
        v_strTempSql := 'delete from pIn_strTableName where lineNumber in (select linenumber from ' ||
                        v_strTable || ')';
      
        fmsp_execsql(v_strTempSql);
      
      end if;
    
    end if;
  
    fmsp_execsql('drop table ' || v_strTable);
    Fmp_Log.LOGEND('FMSP_LogAggLevel');
  end;

end FMP_BATCHLOG;
/
