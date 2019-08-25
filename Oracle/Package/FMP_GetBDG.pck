CREATE OR REPLACE PACKAGE FMP_GetBDG IS

  procedure FMSP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                              pIn_arrNodeAddr   in clob default null,
                              pOut_strTableName out varchar2, --temporary table name
                              pOut_nSQLCode     out number);

  procedure FMISP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                               pIn_vTabName      in varchar2 default null,
                               pOut_strTableName out varchar2, --temporary table name
                               pOut_nSQLCode     out number);

end FMP_GetBDG;
/
CREATE OR REPLACE PACKAGE BODY FMP_GetBDG IS
  --*****************************************************************
  -- Description: from Detail NodeID and Aggregate NodeID Convert to BDGID

  -- Author:     wfq
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        15-3-2012     wfq     Created.
  -- **************************************************************

  procedure FMSP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                              pIn_arrNodeAddr   in clob default null,
                              pIn_vTabName      in varchar2 default null,
                              pOut_strTableName out varchar2, --temporary table name
                              pOut_nSQLCode     out number) as
    --*****************************************************************
    -- Description: from Detail NodeID and Aggregate NodeID Convert to BDGID
    --
    -- Parameters:
    --   pIn_nNodeType   in int, --1  Detail Node  2  Aggregate Node
    --   pIn_arrNodeAddr   --db addr separated by ","
  
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        10-1-2013     wfq           Created.
    -- **************************************************************
    vOut_tNestTab fmt_nest_tab_nodeid;
    vStrSql       varchar(5000);
    vTabName      varchar(50);
  
  BEGIN
    pOut_nSqlCode := 0;
  
    vTabName := pIn_vTabName;
    IF vTabName IS NULL THEN
      --truncate table
      vStrSql := 'truncate table tb_node';
      execute immediate vStrSql;
    
      FMSP_ClobToNestedTable(pIn_arrNodeAddr, vOut_tNestTab, pOut_nSQLCode);
      insert into tb_node
        select distinct * from table(vOut_tNestTab);
    
      vTabName := 'tb_node';
    END IF;
    --select seq_TB_bdg.Nextval into pOut_strTableName from dual;
    pOut_strTableName := fmf_gettmptablename();
  
    vStrSql := 'CREATE TABLE ' || pOut_strTableName || '(ID int,bdgID int)';
    execute immediate vStrSql;
  
    IF pIn_nNodeType = 1 THEN
      --from Aggregate NodeID Convert to BDGID
      vStrSql := 'insert into ' || pOut_strTableName || '
      select t.id,b.bdg_em_addr
        from pvt p, ' || vTabName ||
                 ' t, bdg b
       where p.pvt_em_addr = t.ID
         and b.b_cle = p.pvt_cle
         and b.ID_BDG = 80';
    
    ELSIF pIn_nNodeType = 2 THEN
      --from Aggregate NodeID Convert to BDGID
      vStrSql := 'insert into ' || pOut_strTableName || '
      select t.ID,b.bdg_em_addr
        from sel s, ' || vTabName ||
                 ' t, bdg b
       where s.sel_em_addr = t.ID
         and b.b_cle = s.sel_cle
         and b.ID_BDG = 71';
    
    elsIF pIn_nNodeType = 3 THEN
      vStrSql := 'insert into ' || pOut_strTableName ||
                 ' select ID,ID from tb_node ';
    END IF;
  
    execute immediate vStrSql;
    commit;
  
  exception
    when others then
    
      pOut_nSqlCode := SQLCODE;
      Fmp_Log.LOGERROR;
  END;

  procedure FMSP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                              pIn_arrNodeAddr   in clob default null,
                              pOut_strTableName out varchar2, --temporary table name
                              pOut_nSQLCode     out number) as
  BEGIN
  
    --add log
    Fmp_log.FMP_SetValue(pIn_nNodeType);
    Fmp_log.FMP_SetValue(pIn_arrNodeAddr);
    Fmp_log.LOGBEGIN;
    FMSP_ConverttoBDG(pIn_nNodeType     => pIn_nNodeType,
                      pIn_arrNodeAddr   => pIn_arrNodeAddr,
                      pIn_vTabName      => null,
                      pOut_strTableName => pOut_strTableName,
                      pOut_nSQLCode     => pOut_nSQLCode);
    fmp_log.LOGEND;
  
  END;

  procedure FMISP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                               pIn_vTabName      in varchar2 default null,
                               pOut_strTableName out varchar2, --temporary table name
                               pOut_nSQLCode     out number) as
  BEGIN
  
    --add log
    Fmp_log.FMP_SetValue(pIn_nNodeType);
    Fmp_log.FMP_SetValue(pIn_vTabName);
    Fmp_log.LOGBEGIN;
    FMSP_ConverttoBDG(pIn_nNodeType     => pIn_nNodeType,
                      pIn_vTabName      => pIn_vTabName,
                      pOut_strTableName => pOut_strTableName,
                      pOut_nSQLCode     => pOut_nSQLCode);
    fmp_log.LOGEND;
  
  END;

END FMP_GetBDG;
/
