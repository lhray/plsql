create or replace procedure FMISP_GetExternalEvents(pIn_vTabName    in varchar2,
                                                    pIn_nChronology in number,
                                                    pOut_vTableName out varchar2,
                                                    pOut_nSQLCode   out number)

  --*****************************************************************
  -- Description: locate a external event of a node
  -- Parameters:
  --       pIn_vTabName: table stored nodes id
  --       pIn_nChronology:--1: monthly, 2: weekly, 4: daily
  --       pOut_vTableName
  --       pOut_nSqlCode:error code
  -- Error Conditions Raised:
  --
  -- Author:     wfq
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        17-2-2013     wfq     Created.
  --  V7.0        21-FEB-2013   JY.Liu  copy code from FMSP_GetExternalEvents
  -- **************************************************************
 as
  vStrSql       varchar(8000);
BEGIN
  pOut_nSqlCode := 0;

  pOut_vTableName := fmf_gettmptablename(); --'TB' || pOut_strTableName;

  vStrSql := 'CREATE TABLE ' || pOut_vTableName || '(
      NodeID NUMBER ,
      exoID NUMBER ,
      exo_cle NVARCHAR2(60) ,
      exo_desc NVARCHAR2(120) ,
      exo_dure INTEGER ,
      exo_type RAW(52),
      BGCtype int,
      bgcID number
        ';
  for i in 1 .. 14 loop
    vStrSql := vStrSql || ',T_' || i || ' NUMBER';
  end loop;
  vStrSql := vStrSql || ')';
  execute immediate vStrSql;
  fmsp_execsql(pIn_cSql => 'truncate table tb_node');
  fmsp_execsql(pIn_cSql => 'insert into tb_node select id from ' ||
                           pIn_vTabName);

  vStrSql := 'select t.ID,
         e.exo_em_addr,
         e.exo_cle,
         e.exo_desc,
         e.exo_dure,
         e.exo_type,
         1 BGCtype
         ,b.bgc_em_addr
         ,d.m_bdg_1 T1
         ,d.m_bdg_2 T2
         ,d.m_bdg_3 T3
         ,d.m_bdg_4 T4
         ,d.m_bdg_5 T5
         ,d.m_bdg_6 T6
         ,d.m_bdg_7 T7
         ,d.m_bdg_8 T8
         ,d.m_bdg_9 T9
         ,d.m_bdg_10 T10
         ,d.m_bdg_11 T11
         ,d.m_bdg_12 T12
         ,d.m_bdg_13 T13
         ,d.m_bdg_14 T14
    from tb_node t, bgc b, exo e, bud d
   where t.ID = b.bdg31_em_addr
     and b.exo43_em_addr = e.exo_em_addr
     and b.bgc_em_addr = d.bgc32_em_addr
union
select t.ID,
         e.exo_em_addr,
         e.exo_cle,
         e.exo_desc,
         e.exo_dure,
         e.exo_type,
         2 BGCtype
         ,s.serie_budget_em_addr
         ,d.m_don_budget_1 t1
         ,d.m_don_budget_2 t2
         ,d.m_don_budget_3 t3
         ,d.m_don_budget_4 t4
         ,d.m_don_budget_5 t5
         ,d.m_don_budget_6 t6
         ,d.m_don_budget_7 t7
         ,d.m_don_budget_8 t8
         ,d.m_don_budget_9 t9
         ,d.m_don_budget_10 t10
         ,d.m_don_budget_11 t11
         ,d.m_don_budget_12 t12
         ,d.m_don_budget_13 t13
         ,d.m_don_budget_14 t14
    from tb_node t, serie_budget s, exo e, don_budget d
   where t.ID = s.bdg70_em_addr
     and s.exo72_em_addr = e.exo_em_addr
     and s.serie_budget_em_addr = d.serie_budget71_em_addr';

  vStrSql := 'insert  into ' || pOut_vTableName || vStrSql;

exception
  when others then
    pOut_nSqlCode := sqlcode;
    raise_application_error(-20005, sqlcode || sqlerrm);
end;
/
