create or replace procedure FMSP_GetExistNodesByKey(pIn_strNodesKeyList in clob,
                                                    pIn_vSeperator      in varchar2,
                                                    pOut_sDataSet       out sys_refcursor,
                                                    pOut_nSqlCode       out number,
                                                    pOut_vSQLMsg        out varchar2)
--*****************************************************************
  -- Description: 
  -- Parameters:
  --       pIn_strNodesKeyList: node key list 
  --       pIn_vSeperator
  --       pOut_sDataSet:
  --       pOut_nSqlCode:error code
  --       pOut_vSQLMsg:
  -- Error Conditions Raised:
  --
  -- Author:     JY>Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        27-APR-2013     JY.Liu     Created.
  -- **************************************************************
 is
begin
  pOut_nSqlCode := 0;
  pOut_vSQLMsg  := '';
  fmp_log.FMP_SetValue(pIn_strNodesKeyList);
  fmp_log.FMP_SetValue(pIn_vSeperator);
  fmp_log.LOGBEGIN;  
  open pOut_sDataSet for
    select decode(b.id_bdg, 80, 0, 1) type,
           case b.id_bdg
             when 80 then
              p.pvt_em_addr
             when 71 then
              s.sel_em_addr
           end id
      from (select column_value as key
              from table(FMCP_FUNCTIONS.FMF_String2List(pIn_strNodesKeyList,
                                                        pIn_vSeperator))) t,
           bdg b
      left join pvt p
        on b.b_cle = p.pvt_cle
      left join sel s
        on b.b_cle = s.sel_cle
     where t.key = b.b_cle;
  fmp_log.LOGEND;     
exception
  when others then
    pOut_nSqlCode := sqlcode;
    pOut_vSQLMsg  := sqlerrm;
    fmp_log.LOGERROR;
    raise;
end;
/
