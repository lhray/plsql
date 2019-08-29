create or replace package FMIP_PIMPORTBOS is

  -- Author  : LZHANG
  -- Created : 4/12/2013 10:02:57 AM
  -- Purpose :
  procedure FMISP_CreateTmpTable(pIn_vOptions         in varchar2,
                                 pIn_nMaxColumnLength in number,
                                 pOut_vTableName      OUT varchar2,
                                 pOut_nSqlCode        OUT number);
  procedure FMISP_ImportBOS(pIn_vOptions            in varchar2,
                            pIn_nMaxColumnLength    in number,
                            pIn_vTableName          in varchar2,
                            p_nTaskId               out integer,
                            p_nImportedSuccessCount out integer, --Reurn imported record count
                            pOut_nSqlCode           OUT number);

end FMIP_PIMPORTBOS;
/
create or replace package body FMIP_PIMPORTBOS is
  GC_CREATETEMPTABLE constant number := 1;
  GC_MERGETABLE      constant number := 2;

  procedure FMSP_GetSql(pIn_oOptionsRecord   in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        pIn_vOptions         in varchar2,
                        pIn_nOperation       in number,
                        pIn_nMaxColumnLength in number,
                        pIn_vTempTable       in varchar2,
                        pOut_cSql            out clob) IS
    --*****************************************************************
    -- Description:   Get SQL
    -- Parameters:
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSqlMerge             clob;
      cSqlMergeSelect       clob;
      cSqlMergeOn           clob;
      cSqlMergeUpdate       clob;
      cSqlMergeInsert       clob;
      cSqlMergeInsertValues clob;
      cSqlCreate            clob;
      cSql                  clob;
      cSqlSelect            clob;
      nLength               number;
      nCount                number;
      cSqlDelete            clob;
    begin
      nLength               := pIn_nMaxColumnLength;
      cSqlMerge             := 'MERGE INTO supplier V USING ';
      cSqlCreate            := 'CREATE TABLE ' || pIn_vTempTable ||
                               '(LineNumber number not null,PERE_BDG_KEY VARCHAR2(2000)';
      cSqlMergeOn           := ' ON(V.PERE_BDG =T.PERE_BDG  AND V.FILS_BDG=T.FILS_BDG AND ID_SUPPLIER=78)';
      cSqlMergeUpdate       := ' WHEN MATCHED THEN Update Set
                               COEFF=NVL(T.COEFF,1)';
      cSqlMergeSelect       := 'SELECT  Pere_Bdg,
                                        Fils_Bdg,
                                        Coeff ';
      cSqlMergeInsert       := ' WHEN NOT MATCHED THEN
                                 Insert(SUPPLIER_EM_ADDR,
                                        PERE_BDG,
                                        FILS_BDG,
                                        COEFF';
      cSqlMergeInsertValues := 'VALUES(seq_supplier.nextval,
                                       T.PERE_BDG,
                                       T.FILS_BDG,T.COEFF';
      nCount                := 1;
      nLength               := nLength - 1;
      if pIn_vOptions is null or
         (pIn_oOptionsRecord.bDay and pIn_oOptionsRecord.bMtotal = false and
         pIn_oOptionsRecord.bBomtotal = false) then
        cSqlMergeUpdate := cSqlMergeUpdate ||
                           ',startPeriod=0,PRIORITE=0,delai=0,endPeriod=0';
        -- coef_perte=0,startYear=0,endYear=0';
        cSqlMergeInsert       := cSqlMergeInsert ||
                                 ',startPeriod,endPeriod,PRIORITE,delai'; --,startYear,endYear,coef_perte';
        cSqlMergeInsertValues := cSqlMergeInsertValues || ',0,0,0,0'; --,0,0,0
      end if;
      while nlength > 0 loop
        cSqlSelect := 'SELECT ba.bdg_em_addr Pere_Bdg,
                              bb.bdg_em_addr Fils_Bdg,
                              TBMS.Coeff_' ||
                      to_char(nCount) || ' Coeff';
        cSqlCreate := cSqlCreate || ',FILS_BDG_KEY_' || to_char(nCount) ||
                      ' VARCHAR2(2000),
                               COEFF_' ||
                      to_char(nCount) || ' INTEGER';
        nLength    := nLength - 2;
        -- day##bomtotal or day##mototal
        if pIn_oOptionsRecord.bDay and
           (pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal) then
          nLength := nLength - 1;
          -- temp table with column name is startPeriod
          cSqlCreate := cSqlCreate || '
                                 ,Startperiod_' ||
                        to_char(nCount) || ' number';
          -- case when startPeriod is not null then startPeriod*1000 + MM
          -- when startPeriod is null then 0
          cSqlSelect := cSqlSelect || ',
           case when TBMS.Startperiod_' ||
                        to_char(ncount) || ' is not null  then ' ||
                        '(TBMS.Startperiod_' || to_char(ncount) ||
                        '+1) * 1000 +
            to_number(substr(to_char(TBMS.startYear_' ||
                        to_char(nCount) ||
                        '), 5, 2))
            when TBMS.Startperiod_' ||
                        to_char(ncount) || ' is  null  then 0
             end startperiod';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,Startperiod=T.Startperiod';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 , startperiod';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,Startperiod';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.Startperiod';
          end if;
          -- mtotal or bomtotal
        elsif pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
          -- case when startYear is not null then startPeriod:=(0+1)*1000 + MM
          -- when startYear is null then 0
          cSqlSelect := cSqlSelect || ',
          case when TBMS.startYear_' ||
                        to_char(nCount) || ' is not null then ' ||
                        (0 + 1) * 1000 || '
                                 +to_number(substr(to_char(TBMS.startYear_' ||
                        to_char(nCount) || '), 5, 2)) ' ||
                        ' when TBMS.startYear_' || to_char(nCount) ||
                        ' is null then
                         0
          end startperiod';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,Startperiod=T.Startperiod';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 ,startperiod';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,Startperiod';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.Startperiod';
          end if;
        end if;
        -- mtotal or bomtotal
        if pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
          nLength := nLength - 1;
          -- temptable with column name is startYear
          cSqlCreate := cSqlCreate || '
                                 ,StartYear_' ||
                        to_char(nCount) || ' number';
          -- case when startyear is not null then supplier.StartYear:=YEAR
          -- case when startYear is null then supplier.StartYear:=0
          cSqlSelect := cSqlSelect || '
                                 , case when TBMS.startYear_' ||
                        to_char(nCount) ||
                        ' is not null then
                         to_number(substr(to_char(TBMS.startYear_' ||
                        to_char(nCount) ||
                        '), 1, 4))
                       
                        end startYear';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate ||
                                     ',StartYear=T.StartYear';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 ,startYear';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,StartYear';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.StartYear';
          end if;
        end if;
        -- day##mtotal or day##bomtotal
        if pIn_oOptionsRecord.bDay and
           (pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal) then
          -- EndPeriod
          nLength := nLength - 1;
          -- temp table with column name is endPeriod
          cSqlCreate := cSqlCreate || '
                                 ,EndPeriod_' ||
                        to_char(nCount) || ' number';
          -- case when endPeriod is not null then MM
          -- when endPeriod is null then 0
          cSqlSelect := cSqlSelect || ',
           case when TBMS.Endperiod_' ||
                        to_char(ncount) || ' is not null  then ' ||
                        '(TBMS.Endperiod_' || to_char(ncount) ||
                        '+1) * 1000 +
            to_number(substr(to_char(TBMS.EndYear_' ||
                        to_char(nCount) || '), 5, 2))
            when TBMS.Endperiod_' || to_char(ncount) ||
                        ' is  null  then 0
             end endperiod';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,EndPeriod=T.EndPeriod';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 ,EndPeriod';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,EndPeriod';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.EndPeriod';
          end if;
          -- mtotal or bomtotal
        elsif pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
          -- case when endYear is not null then endPeriod := (0+1)*1000+MM
          -- when endYear is  null then endPerid:=0
          cSqlSelect := cSqlSelect || ',
          case when TBMS.endYear_' ||
                        to_char(nCount) || ' is not null then ' ||
                        (0 + 1) * 1000 || '
                                 +to_number(substr(to_char(TBMS.endYear_' ||
                        to_char(nCount) || '), 5, 2)) ' ||
                        ' when TBMS.endYear_' || to_char(nCount) ||
                        ' is null then
                         0
          end endperiod';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,EndPeriod=T.EndPeriod';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 ,EndPeriod';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,EndPeriod';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.EndPeriod';
          end if;
        end if;
        -- mtotal or bomtotal
        if pIn_oOptionsRecord.bMtotal OR pIn_oOptionsRecord.bBomtotal then
          -- EndYear coef_perte
          nlength := nlength - 2;
          -- temp table with endYear and Coef_Perte
          cSqlCreate := cSqlCreate || '
                                 ,EndYear_' ||
                        to_char(nCount) || ' number,Coef_perte_' ||
                        to_char(nCount) || ' number';
          -- case when endYear is not null then endYear:=Year
          -- when endYear is null then 0
          -- case when Coef_Perte is not null then Coef_Perte
          -- when Coef_Perte is null then 0
          cSqlSelect := cSqlSelect || '
                                 , case when TBMS.endYear_' ||
                        to_char(nCount) ||
                        ' is not null then
                         to_number(substr(to_char(TBMS.endYear_' ||
                        to_char(nCount) ||
                        '), 1, 4))
                       
                        end endYear,
                        case when Coef_perte_' ||
                        to_char(nCount) ||
                        ' is not null then
                        Coef_perte_' ||
                        to_char(nCount) || '
                         end  Coef_perte';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,EndYear=T.EndYear,Coef_perte=T.Coef_perte';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                  ,EndYear,
                                  Coef_perte';
            cSqlMergeInsert       := cSqlMergeInsert || '
                                 ,EndYear,Coef_perte';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.EndYear,T.Coef_perte';
          end if;
        end if;
        -- bomtotal
        if pIn_oOptionsRecord.bBomtotal then
          -- priorite delai
          nLength := nLength - 2;
          --temp table with priorite and delai
          cSqlCreate := cSqlCreate || '
                                 ,PRIORITE_' ||
                        to_char(nCount) || ' number,DELAI_' ||
                        to_char(nCount) || ' number';
          -- case when PRIORITE is not null then PRIORITE
          -- when PRIORITE is null then 0
          -- case when DELAI is not null then DELAI
          -- when DELAI is null then 0
          cSqlSelect := cSqlSelect || '
                                 ,
                        case when TBMS.PRIORITE_' ||
                        to_char(nCount) ||
                        ' is not null then
                        TBMS.PRIORITE_' ||
                        to_char(nCount) || '
                        when TBMS.PRIORITE_' ||
                        to_char(nCount) ||
                        ' is  null then 0
                        end PRIORITE,
                        case when TBMS.DELAI_' ||
                        to_char(nCount) ||
                        ' is not null then
                        TBMS.DELAI_' ||
                        to_char(nCount) || '
                         when TBMS.DELAI_' ||
                        to_char(nCount) || ' is  null then 0 end DELAI ';
          if nCount = 1 then
            cSqlMergeUpdate       := cSqlMergeUpdate || '
                                 ,PRIORITE=T.PRIORITE,DELAI=T.DELAI ';
            cSqlMergeSelect       := cSqlMergeSelect || '
                                 ,PRIORITE,DELAI ';
            cSqlMergeInsert       := cSqlMergeInsert || ',PRIORITE,DELAI';
            cSqlMergeInsertValues := cSqlMergeInsertValues || '
                                 ,T.PRIORITE,T.DELAI';
          end if;
        
        end if;
        cSqlSelect := cSqlSelect || ' FROM bdg ba, bdg bb,' ||
                      pIn_vTempTable ||
                      ' TBMS WHERE
                         TBMS.PERE_BDG_KEY
                          = ba.b_cle
                         AND TBMS.PERE_BDG_KEY in 
                         (select b_cle
                                 from bdg
                                 where TBMS.PERE_BDG_KEY = b_cle
                                 group by b_cle
                                 having count(b_cle) < 2 and count(b_cle) > 0) 
                         AND 
                         TBMS.FILS_BDG_KEY_' ||
                      to_char(nCount) || ' = bb.b_cle';
        if cSql is null then
          cSql := cSqlSelect;
        else
          cSql := cSql || ' UNION ALL ' || cSqlSelect;
        end if;
        nCount := nCount + 1;
      end loop;
    
      cSqlMergeInsert       := cSqlMergeInsert || '
                               ,BDG51_EM_ADDR,ID_SUPPLIER)';
      cSqlMergeInsertValues := cSqlMergeInsertValues || '
                               ,T.PERE_BDG,78)';
      cSqlCreate            := cSqlCreate || ')';
      cSqlMergeSelect       := cSqlMergeSelect || ' FROM (' || cSql ||
                               ') WHERE FILS_BDG IS NOT NULL';
      cSqlDelete            := 'delete from supplier A where A.PERE_BDG in (select distinct PERE_BDG FROM (' ||
                               cSqlMergeSelect ||
                               ') B ) AND ID_SUPPLIER=78';
      cSqlMerge             := cSqlMerge || '(' || cSqlMergeSelect ||
                               ') T ' || cSqlMergeOn || cSqlMergeUpdate ||
                               cSqlMergeInsert || cSqlMergeInsertValues;
      if pIn_nOperation = GC_CREATETEMPTABLE then
        pOut_cSql := cSqlCreate;
      elsif pIn_nOperation = GC_MERGETABLE then
        pOut_cSql := cSqlMerge;
        --delete all data in supplier but not in import
        fmsp_execsql(pIn_cSql => cSqlDelete);
      end if;
    end;
  End FMSP_GetSql;

  procedure FMISP_CreateTmpTable(pIn_vOptions         in varchar2,
                                 pIn_nMaxColumnLength in number,
                                 pOut_vTableName      OUT varchar2,
                                 pOut_nSqlCode        OUT number) IS
    --*****************************************************************
    -- Description:   create temp table
    --
    -- Parameters:
    -- pIn_vOptions: if all options exist example :pIn_vOptions= -mtotal##-day##-bomtotal
    --               if one or more option not exists  example: -mtotal not exists pIn_vOptions:= -day##-bomtotal
    -- pOut_vTableName temp table Name
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSql          clob;
      optionsRecord P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    begin
      Fmp_Log.FMP_SetValue(pIn_vOptions);
      fmp_log.FMP_SetValue(pIn_nMaxColumnLength);
      Fmp_Log.LOGBEGIN;
      pOut_nSqlCode := 0;
      P_BATCHCOMMAND_COMMON.sp_ParseOptions(p_strOptions => pIn_vOptions,
                                            p_oOptions   => optionsRecord,
                                            p_nSqlCode   => pOut_nSqlCode);
      pOut_vTableName := fmf_gettmptablename();
      FMSP_GetSql(pIn_oOptionsRecord   => optionsRecord,
                  pIn_vOptions         => pIn_vOptions,
                  pIn_nOperation       => GC_CREATETEMPTABLE,
                  pIn_nMaxColumnLength => pIn_nMaxColumnLength,
                  pIn_vTempTable       => pOut_vTableName,
                  pOut_cSql            => cSql);
      fmsp_execsql(pIn_cSql => cSql);
      Fmp_Log.LOGEND;
    exception
      when others then
        pOut_nSqlCode := sqlcode;
        Fmp_Log.LOGERROR;
        raise_application_error(-20004, sqlcode || ';' || sqlerrm);
    end;
  End FMISP_CreateTmpTable;

  procedure FMISP_ImportBOS(pIn_vOptions            in varchar2,
                            pIn_nMaxColumnLength    in number,
                            pIn_vTableName          in varchar2,
                            p_nTaskId               out integer,
                            p_nImportedSuccessCount out integer, --Reurn imported record count
                            pOut_nSqlCode           OUT number) IS
    --*****************************************************************
    -- Description:   import BOS Data
    --
    -- Parameters:
    -- pIn_nCommandNumber command Number
    -- pIn_nChronology  not used now
    -- pIn_vFMUSER  FMUSER
    -- pIn_vOptions: if all options exist example :pIn_vOptions= -mtotal##-day##-bomtotal
    --               if one or more option not exists  example: -mtotal not exists pIn_vOptions:= -day##-bomtotal
    -- pIn_vTableName temp table Name
    -- Error Conditions Raised:
    --
    -- Author:      lei zhang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APR-2013     lei zhang     Created.
    -- **************************************************************
  Begin
    declare
      cSql          clob;
      optionsRecord P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    begin
    
      Fmp_Log.FMP_SetValue(pIn_vOptions);
      fmp_log.FMP_SetValue(pIn_nMaxColumnLength);
      Fmp_Log.FMP_SetValue(pIn_vTableName);
      Fmp_Log.LOGBEGIN;
      pOut_nSqlCode           := 0;
      p_nTaskId               := 0;
      p_nImportedSuccessCount := 0;
    
      P_BATCHCOMMAND_COMMON.sp_ParseOptions(p_strOptions => pIn_vOptions,
                                            p_oOptions   => optionsRecord,
                                            p_nSqlCode   => pOut_nSqlCode);
      FMSP_GetSql(pIn_oOptionsRecord   => optionsRecord,
                  pIn_nOperation       => GC_MERGETABLE,
                  pIn_vOptions         => pIn_vOptions,
                  pIn_vTempTable       => pIn_vTableName,
                  pIn_nMaxColumnLength => pIn_nMaxColumnLength,
                  pOut_cSql            => cSql);
      fmsp_execsql(pIn_cSql => cSql);
    
      Fmp_Log.LOGEND;
      --fmsp_execsql(pIn_cSql => 'DROP TABLE ' || pIn_vTableName);
    exception
      when others then
        pOut_nSqlCode := sqlCode;
        fmp_log.LOGERROR;
        raise_application_error(-20004, sqlcode || ';' || sqlerrm);
        --fmsp_execsql(pIn_cSql => 'DROP TABLE ' || pIn_vTableName);
    end;
  End FMISP_ImportBOS;
end FMIP_PIMPORTBOS;
/
