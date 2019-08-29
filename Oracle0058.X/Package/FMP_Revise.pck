CREATE OR REPLACE PACKAGE FMP_Revise IS

  --public
  gc_nConstantName constant number := 100; --comments
  g_nVarName number; --comments
  g_eException exception; --comments

  PROCEDURE FMSP_Revise(pIn_vOperation   IN varchar2,
                        pIn_vkey         IN varchar2,
                        pIn_nSeriesID    IN number,
                        PIn_nChronology  IN number, --1:monthly 2:Weekly 3:daily
                        Pin_nVersion     IN number,
                        PIn_vNodetype    IN number,
                        pIn_nStartYear   IN number,
                        pIn_nStartPeriod IN number,
                        pIn_nEndYear     IN number,
                        pIn_nEndPeriod   IN number,
                        pOut_nSqlCode    OUT number);

END FMP_Revise;
/
CREATE OR REPLACE PACKAGE BODY FMP_Revise IS

  --*****************************************************************
  -- Description: Handling Revise Operations.
  --
  -- Author:      <wfq>
  -- Revise
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        12-12-2012     wfq           Created.

  -- **************************************************************

  PROCEDURE FMSP_Revise(pIn_vOperation   IN varchar2,
                        pIn_vkey         IN varchar2,
                        pIn_nSeriesID    IN number,
                        PIn_nChronology  IN number, --1:monthly 2:Weekly 3:daily
                        Pin_nVersion     IN number,
                        PIn_vNodetype    IN number, --1:Detail Node or 2:Aggregate Node
                        pIn_nStartYear   IN number,
                        pIn_nStartPeriod IN number,
                        pIn_nEndYear     IN number,
                        pIn_nEndPeriod   IN number,
                        pOut_nSqlCode    OUT number)
  --*****************************************************************
    -- Description: Handling Revise Operations.
    --
    -- Parameters:
    --       pIn_vOperation  --
    --       pIn_vkey
    --       pIn_nSeriesID   -- time series ID
    --       PIn_nChronology --1:monthly 2:Weekly 3:daily
    --       Pin_nVersion
    --       PIn_vNodetype   --1:Detail Node or 2:Aggregate Node
    --       pIn_nStartYear
    --       pIn_nStartPeriod
    --       pIn_nEndYear
    --       pIn_nEndPeriod
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-12-2012     wfq           Created.
    -- **************************************************************
   as
  
    vStrSql    varchar2(8000);
    vStrSet    varchar2(4000);
    vWhereStr  varchar2(4000);
    vTableName varchar2(80);
    iCycle     int;
    i          int;
  BEGIN
    pOut_nSqlCode := 0;
  
    --1:Detail Node or 2:Aggregate Node
    IF PIn_vNodetype = 1 THEN
      vTableName := 'don';
    ELSIF PIn_vNodetype = 2 THEN
      vTableName := 'prb';
    ELSE
      vTableName := '';
    END IF;
  
    --monthly or weekly
    IF PIn_nChronology = 1 THEN
      vTableName := vTableName || '_M';
      iCycle     := 12;
    ELSIF PIn_nChronology = 2 THEN
      vTableName := vTableName || '_W';
      iCycle     := 52;
    ELSE
      vTableName := vTableName || '';
      iCycle     := 12;
    END IF;
  
    --StrWhere SQL
    vWhereStr := ' where version=' || Pin_nVersion;
    IF pIn_nSeriesID <> 0 THEN
      vWhereStr := vWhereStr || ' and tsID=' || pIn_nSeriesID;
    END IF;
    if pIn_nStartYear <> 0 then
      vWhereStr := vWhereStr || ' and yy>=' || pIn_nStartYear;
    end if;
    if pIn_nEndYear <> 0 then
      vWhereStr := vWhereStr || ' and yy<=' || pIn_nEndYear;
    end if;
  
    CASE upper(trim(pIn_vOperation))
      WHEN 'DESTRUCTION' THEN
        --Deletes considered elements
      
        CASE upper(trim(pIn_vkey))
          WHEN 'FAMILLE' THEN
            --delete of the product groups at the lowest level if they do not contain products
            vStrSql := 'delete fam f
                    where id_fam=70 and
                    not exists (select  1 from fam f1
                    where f1.fam0_em_addr=f.fam_em_addr ) ';
            execute immediate vStrSql;
            commit;
          
          WHEN 'PRODUIT' THEN
            --delete of products that are not attached to a node
            vStrSql := 'delete fam f where f.id_fam=80 and
                    not exists (select  1 from pvt p
                    where p.fam4_em_addr=f.fam_em_addr ) ';
            execute immediate vStrSql;
            commit;
          
          WHEN 'DISTRIBUTION' THEN
            --delete of trade channel that are not attached to a node
            vStrSql := 'delete dis d where ascii(d_cle)<>1 and
                    dis_em_addr not in (select dis2_em_addr from dis) and
                    not exists (select  1 from pvt p where
                    p.dis6_em_addr=d.dis_em_addr ) ';
            execute immediate vStrSql;
            commit;
          
          WHEN 'GEOGRAPHIE' THEN
            --delete of sales territory that are not attached to a node
            vStrSql := 'delete geo d where ascii(g_cle)<>1 and
                    geo_em_addr not in (select geo1_em_addr from geo) and
                    not exists (select  1 from pvt p where
                    p.geo5_em_addr=d.geo_em_addr ) ';
            execute immediate vStrSql;
            commit;
          
          ELSE
            NULL;
        END CASE;
      
      WHEN 'ABLANC' THEN
        --Sets the values of the series to blanks
      
        --delete StartYear and pIn_nEndYear
        vStrSql := 'delete ' || vTableName || ' where tsID=' ||
                   pIn_nSeriesID || ' and version=' || Pin_nVersion;
        IF pIn_nStartYear > 0 THEN
          vStrSql := vStrSql || ' and yy>' || pIn_nStartYear;
        END IF;
        IF pIn_nEndYear > 0 THEN
          vStrSql := vStrSql || ' and yy<' || pIn_nEndYear;
        END IF;
      
        execute immediate vStrSql;
        commit;
      
        --update StartYear = pIn_nEndYear
        IF pIn_nStartYear = pIn_nEndYear THEN
        
          vStrSql := 'update ' || vTableName || ' set ';
        
          vStrSet := '';
          FOR i IN pIn_nStartPeriod .. pIn_nEndPeriod LOOP
            vStrSet := vStrSet || ',T' || i || '= null';
          END LOOP;
          vStrSet := substr(vStrSet, 2);
        
          vStrSql := vStrSql || vStrSet || ' where tsID=' || pIn_nSeriesID ||
                     ' and version=' || Pin_nVersion || ' and yy=' ||
                     pIn_nStartYear;
          execute immediate vStrSql;
          commit;
        
        ELSE
          --update YY=StartYear
          IF pIn_nStartYear > 0 THEN
            vStrSql := 'update ' || vTableName || ' set ';
          
            vStrSet := '';
            FOR i IN pIn_nStartPeriod .. iCycle LOOP
              vStrSet := vStrSet || ',T' || i || '= null';
            END LOOP;
            vStrSet := substr(vStrSet, 2);
          
            vStrSql := vStrSql || vStrSet || ' where tsID=' ||
                       pIn_nSeriesID || ' and version=' || Pin_nVersion ||
                       ' and yy=' || pIn_nStartYear;
            execute immediate vStrSql;
            commit;
          END IF;
        
          --update YY=pIn_nEndYear
          IF pIn_nEndYear > 0 THEN
            vStrSql := 'update ' || vTableName || ' set ';
          
            vStrSet := '';
            FOR i IN 1 .. pIn_nEndPeriod LOOP
              vStrSet := vStrSet || ',T' || i || '= null';
            END LOOP;
            vStrSet := substr(vStrSet, 2);
          
            vStrSql := vStrSql || vStrSet || ' where tsID=' ||
                       pIn_nSeriesID || ' and version=' || Pin_nVersion ||
                       ' and yy=' || pIn_nEndYear;
            execute immediate vStrSql;
            commit;
          END IF;
        
        END IF;
      
      WHEN 'AZERO' THEN
        --Sets the values of the series to zeros
        NULL;
      WHEN 'BLANCDEBZERO' THEN
        --Replaces blanks by zeros in a time series, from the beginning in chronological order up to the first value different from blank
        NULL;
      WHEN 'BLANCSETVALUE' THEN
        --Replaces blanks by a specified value in a time series(e.g. BLANCSETVALUE:50)
        NULL;
      WHEN 'BLANCZERO' THEN
        --Replaces blanks by zeros in a time series (does not work if all values are blanks)
        NULL;
      WHEN 'DEL_ALL_OLD_DATA' THEN
        --Deletes old data in all series
        NULL;
      WHEN 'DEL_OLD_DATA' THEN
        --Deletes old data in specified series
        NULL;
      WHEN 'SETVALUE' THEN
        --Replaces all values of a time series by the specified value (e.g. SETVALUE:30)
        NULL;
      WHEN 'ZEROBLANC' THEN
        --Replaces zeros by blanks in a time series (to gain space in the database)
        NULL;
      WHEN 'ZERODEB' THEN
        --Replaces the zeros at the beginning of the series by blanks
        NULL;
      WHEN 'SEL' THEN
        --Define the aggregation (Aggregate level) or the selection (Detail level)
        NULL;
      WHEN 'SEL_CONDIT' THEN
        --Define the Selection (Aggregate level)
        NULL;
      
      ELSE
        NULL;
    END CASE;
  
  exception
    when others then
      rollback;
      pOut_nSqlCode := SQLCODE;
      FMP_LOG.LOGERROR;
      --raise_application_error(-20004,SQLERRM);      
  END;

END FMP_Revise;
/
