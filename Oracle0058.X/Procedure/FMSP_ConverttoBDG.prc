create or replace procedure FMSP_ConverttoBDG(pIn_nNodeType     in int, --1  Detail Node  2  Aggregate Node 3 BDG
                                              pIn_arrNodeAddr   in clob,
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
BEGIN
  pOut_nSqlCode := 0;
  --add log
  Fmp_log.FMP_SetValue(pIn_nNodeType);
  Fmp_log.FMP_SetValue(pIn_arrNodeAddr);
  Fmp_log.LOGBEGIN;
  --select seq_TB_bdg.Nextval into pOut_strTableName from dual;
  pOut_strTableName := fmf_gettmptablename();

  vStrSql := 'CREATE TABLE ' || pOut_strTableName || '(ID int,bdgID int)';
  execute immediate vStrSql;

  FMSP_ClobToTable(pIn_cClob     => pIn_arrNodeAddr,
                   pOut_nSqlCode => pOut_nSqlCode);

  IF pIn_nNodeType = 1 THEN
    --from Aggregate NodeID Convert to BDGID
    vStrSql := 'insert into ' || pOut_strTableName || '
      select t.id,b.bdg_em_addr
        from pvt p, tb_node t, bdg b
       where p.pvt_em_addr = t.ID
         and b.b_cle = p.pvt_cle
         and b.ID_BDG = 80';
  
  ELSIF pIn_nNodeType = 2 THEN
    --from Aggregate NodeID Convert to BDGID
    vStrSql := 'insert into ' || pOut_strTableName || '
      select t.ID,b.bdg_em_addr
        from sel s, tb_node t, bdg b
       where s.sel_em_addr = t.ID
         and b.b_cle = s.sel_cle
         and b.ID_BDG = 71';
  elsIF pIn_nNodeType = 3 THEN
    vStrSql := 'insert into ' || pOut_strTableName ||
               ' select ID,ID from tb_node ';
  END IF;

  execute immediate vStrSql;
  commit;
  Fmp_Log.LOGEND;
exception
  when others then
    rollback;
    pOut_nSqlCode := SQLCODE;
    Fmp_Log.LOGERROR;
    --raise_application_error(SQLCODE,SQLERRM);
END;
/
