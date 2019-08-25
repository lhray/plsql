create or replace package P_Hierarchy is

  -- Author  : YYSUN
  -- Created : 2012-11-01 1:52:36 PM
  -- Purpose : for load product hierarchy, sales territory etc...

  /*
  * Load product hierarchy in the deep-first order and all childer is in the descending order by its key.
  * The level of the root node is -1.
  */
  procedure SP_LoadProductHierarchy(p_nRootProductID      in number,
                                    p_csrProductHierarchy out sys_refcursor,
                                    p_nSqlCode            out number,
                                    p_strSqlMsg           out string);
  /*
  * Load sales territory hierarchy in the deep-first order and all childer is in the descending order by its key.
  * The level of the root node is -1.
  */
  procedure SP_LoadSalesTerritoryHierarchy(p_nRootSalesTerritoryID in number,
                                           p_csrSalesTerritory     out sys_refcursor,
                                           p_nSqlCode              out number,
                                           p_strSqlMsg             out string);
  /*
  * Load trade channel hierarchy in the deep-first order and all childer is in the descending order by its key.
  * The level of the root node is -1.
  */
  procedure SP_LoadTradeChannelHierarchy(p_nRootTradeChannelID in number,
                                         p_csrTradeChannel     out sys_refcursor,
                                         p_nSqlCode            out number,
                                         p_strSqlMsg           out string);

end P_Hierarchy;
/
create or replace package body P_Hierarchy is

  /*
  * Function: refer to the declare of this package
  */
  procedure SP_LoadProductHierarchy(p_nRootProductID      in number,
                                    p_csrProductHierarchy out sys_refcursor,
                                    p_nSqlCode            out number,
                                    p_strSqlMsg           out string) as
  begin
    --log start
    Fmp_Log.FMP_SetValue(p_nRootProductID);
    Fmp_Log.LOGBEGIN;
    --open cursor for return record set
    open p_csrProductHierarchy for
      select fam_em_addr,
             level - 2 as ProductLevel,
             f_cle,
             f_desc,
             fam0_em_addr
        from fam
       start with fam_em_addr = p_nRootProductID
      connect by fam0_em_addr = prior fam_em_addr
       order siblings by f_cle desc;
    --log end
    Fmp_Log.LOGEND;
  
  exception
    when others then
      p_nSqlCode  := SqlCode;
      p_strSqlMsg := SqlErrm;
      Fmp_Log.LOGERROR;
      --log exception
  
  end;

  /*
  * Function: refer to the declare of this package
  */
  procedure SP_LoadSalesTerritoryHierarchy(p_nRootSalesTerritoryID in number,
                                           p_csrSalesTerritory     out sys_refcursor,
                                           p_nSqlCode              out number,
                                           p_strSqlMsg             out string) as
  begin
    --log start
    Fmp_Log.FMP_SetValue(p_nRootSalesTerritoryID);
    Fmp_Log.LOGBEGIN;
    --open cursor for return record set
    open p_csrSalesTerritory for
      select geo_em_addr,
             level - 2 as SalesTerritoryLevel,
             g_cle,
             g_desc,
             geo1_em_addr
        from geo
       start with geo_em_addr = p_nRootSalesTerritoryID
      connect by geo1_em_addr = prior geo_em_addr
       order siblings by g_cle desc;
  
    --log end
    Fmp_Log.LOGEND;
  exception
    when others then
      p_nSqlCode  := SqlCode;
      p_strSqlMsg := SqlErrm;
    
      --log exception
      Fmp_Log.LOGERROR;
  end;

  /*
  * Function: refer to the declare of this package
  */
  procedure SP_LoadTradeChannelHierarchy(p_nRootTradeChannelID in number,
                                         p_csrTradeChannel     out sys_refcursor,
                                         p_nSqlCode            out number,
                                         p_strSqlMsg           out string) as
  begin
    --log start
    Fmp_Log.FMP_SetValue(p_nRootTradeChannelID);
    Fmp_Log.LOGBEGIN;
    --open cursor for return record set
    open p_csrTradeChannel for
      select dis_em_addr,
             level - 2 as TradeChannelLevel,
             d_cle,
             d_desc,
             dis2_em_addr
        from dis
       start with dis_em_addr = p_nRootTradeChannelID
      connect by dis2_em_addr = prior dis_em_addr
       order siblings by d_cle desc;
  
    --log end
    Fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode  := SqlCode;
      p_strSqlMsg := SqlErrm;
    
      --log exception
      Fmp_log.LOGERROR;
  end;

end P_Hierarchy;
/
