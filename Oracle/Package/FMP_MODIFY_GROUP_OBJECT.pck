create or replace package FMP_MODIFY_GROUP_OBJECT is

  procedure FMSP_MODIFY_PRODUCT_GROUP(pIn_vObjID            in varchar2, -- this is object id
                                      pIn_nValueUnits       in number, -- 0:is not change 1: is change
                                      pIn_vValueUnits       in varchar2, --  ValueUnits id
                                      pIn_vCoefficientTable in varchar2, -- farmat 1:ID,2:ID ...
                                      pIn_vTargetValue      in varchar2, -- farmat 1:ID,2:ID ...
                                      pIn_vRatio            in varchar2, -- farmat 1:ratioID;2:ratioID ....
                                      pIn_vAttribute        in varchar2, -- farmat 49:attid;50:attid ...
                                      pOut_nSqlCode         out number -- sql code
                                      );

  procedure FMSP_MODIFY_SALES(pIn_vObjID        in varchar2, -- this is object id
                              pIn_vExchangeRate in varchar2, -- farmat 1:ID,2:ID
                              pIn_vAttribute    in varchar2, -- farmat 49:attid;50:attid ...
                              pOut_nSqlCode     out number -- sql code
                              );

  procedure FMSP_MODIFY_TRADE(pIn_vObjID      in varchar2, -- this is object id
                              pIn_vCoeffcient in varchar2, -- farmat 1:ID,2:ID
                              pIn_vAttribute  in varchar2, -- farmat 49:attid;50:attid ...
                              pOut_nSqlCode   out number -- sql code
                              );

end FMP_MODIFY_GROUP_OBJECT;
/
create or replace package body FMP_MODIFY_GROUP_OBJECT is

  procedure FMSP_MODIFY_CoefficientTable(pIn_vCoefficientT in varchar2,
                                         pIn_nObjID        in varchar2,
                                         pOut_nSqlCode     out number);
  procedure FMSP_MODIFY_ATTRIBUTE(pIn_vAttribute in varchar2,
                                  pIn_vObjID     in varchar2,
                                  pIn_nObjType   in varchar2,
                                  pOut_nSqlCode  out number);

  procedure FMSP_MODIFY_TargetValue(pIn_vTargetValue in varchar2,
                                    pIn_nObjID       in varchar2,
                                    pOut_nSqlCode    out number);
  procedure FMSP_MODIFY_Ratio(pIn_vRatio    in varchar2,
                              pIn_nObjID    in varchar2,
                              pOut_nSqlCode out number);
  procedure FMSP_MODIFY_ExchangeRate(pIn_vExchangeRate in varchar2,
                                     pIn_nObjID        in varchar2,
                                     pOut_nSqlCode     out number);

  procedure FMSP_MODIFY_Coeffcient(pIn_vCoeffcient in varchar2,
                                   pIn_nObjID      in varchar2,
                                   pOut_nSqlCode   out number) ;

  procedure FMSP_MODIFY_PRODUCT_GROUP(pIn_vObjID            in varchar2,
                                      pIn_nValueUnits       in number,
                                      pIn_vValueUnits       in varchar2, -- true : ValueUnits ,false : nothing
                                      pIn_vCoefficientTable in varchar2,
                                      pIn_vTargetValue      in varchar2, -- 1:value1;2:value2
                                      pIn_vRatio            in varchar2, -- 1:value1;2:value2
                                      pIn_vAttribute        in varchar2, -- farmat 49:value49;50:value50
                                      pOut_nSqlCode         out number -- sql code
                                      )
    --*****************************************************************
    -- Description: this procedure  is modify product group   .
    --
    -- Parameters:
    --            pIn_vObjID
    --            pIn_nValueUnits
    --            pIn_vValueUnits
    --            pIn_vCoefficientTable
    --            pIn_vTargetValue
    --            pIn_vRatio
    --            pIn_vAttribute
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APril-2013     LiSang         Created.
    -- **************************************************************
   as

  begin
    Fmp_Log.FMP_SetValue(pIn_vObjID);
    Fmp_Log.FMP_SetValue(pIn_nValueUnits);
    Fmp_Log.FMP_SetValue(pIn_vValueUnits);
    Fmp_Log.FMP_SetValue(pIn_vCoefficientTable);
    Fmp_Log.FMP_SetValue(pIn_vTargetValue);
    Fmp_Log.FMP_SetValue(pIn_vRatio);
    Fmp_Log.FMP_SetValue(pIn_vAttribute);
    Fmp_Log.LOGBEGIN;

    -- this is value units
    if pIn_vValueUnits is not null and pIn_nValueUnits = 1 then
      merge into rfc r
      using (SELECT f.fam_em_addr
               FROM fam f
              where f.fam_em_addr != pIn_vObjID
              START WITH f.fam_em_addr = pIn_vObjID
             CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) t
      on (r.fam7_em_addr = t.fam_em_addr and r.numero_crt = p_constant.NumberForVU)
      when matched then
        update set r.vct10_em_addr = pIn_vValueUnits
      when not matched then
        insert
          (r.rfc_em_addr,
           r.ident_crt,
           r.numero_crt,
           r.fam7_em_addr,
           r.geo8_em_addr,
           r.dis9_em_addr,
           r.vct10_em_addr)
        values
          (seq_rfc.nextval,
           70,
           p_Constant.NumberForVU,
           t.fam_em_addr,
           0,
           0,
           pIn_vValueUnits);
      commit;

    elsif pIn_vValueUnits is null and pIn_nValueUnits = 1 then
      delete from rfc r
       where r.fam7_em_addr in
             (SELECT f.fam_em_addr
                FROM fam f
               where f.fam_em_addr != pIn_vObjID
               START WITH f.fam_em_addr = pIn_vObjID
              CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr)
         and r.numero_crt = p_constant.NumberForVU;
    end if;

    -- this is coefficient table
    FMSP_MODIFY_CoefficientTable(pIn_vCoefficientT => pIn_vCoefficientTable,
                                 pIn_nObjID        => pIn_vObjID,
                                 pOut_nSqlCode     => pOut_nSqlCode);

    -- this is attribute
    FMSP_MODIFY_ATTRIBUTE(pIn_vAttribute => pIn_vAttribute,
                          pIn_vObjID     => pIn_vObjID,
                          pIn_nObjType   => 1,
                          pOut_nSqlCode  => pOut_nSqlCode);

    --this is modify TargetValue
    FMSP_MODIFY_TargetValue(pIn_vTargetValue => pIn_vTargetValue,
                            pIn_nObjID       => pIn_vObjID,
                            pOut_nSqlCode    => pOut_nSqlCode);
    -- this is modify ratio
    FMSP_MODIFY_Ratio(pIn_vRatio    => pIn_vRatio,
                      pIn_nObjID    => pIn_vObjID,
                      pOut_nSqlCode => pOut_nSqlCode);

    Fmp_Log.LOGEND;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_SALES(pIn_vObjID        in varchar2,
                              pIn_vExchangeRate in varchar2,
                              pIn_vAttribute    in varchar2,
                              pOut_nSqlCode     out number -- sql code
                              )

    --*****************************************************************
    -- Description: this procedure  is modify sales group   .
    --
    -- Parameters:
    --            pIn_vObjID
    --            pIn_vExchangeRate
    --            pIn_vAttribute
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APril-2013     LiSang         Created.
    -- **************************************************************
   as

  begin
    Fmp_Log.FMP_SetValue(pIn_vObjID);
    Fmp_Log.FMP_SetValue(pIn_vExchangeRate);
    Fmp_Log.FMP_SetValue(pIn_vAttribute);
    Fmp_Log.LOGBEGIN;

    FMSP_MODIFY_ExchangeRate(pIn_vExchangeRate => pIn_vExchangeRate,
                             pIn_nObjID        => pIn_vObjID,
                             pOut_nSqlCode     => pOut_nSqlCode);

    FMSP_MODIFY_ATTRIBUTE(pIn_vAttribute => pIn_vAttribute,
                          pIn_vObjID     => pIn_vObjID,
                          pIn_nObjType   => 2,
                          pOut_nSqlCode  => pOut_nSqlCode);

    Fmp_Log.LOGEND;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_TRADE(pIn_vObjID      in varchar2,
                              pIn_vCoeffcient in varchar2,
                              pIn_vAttribute  in varchar2,
                              pOut_nSqlCode   out number -- sql code
                              )
    --*****************************************************************
    -- Description: this procedure  is modify trade group   .
    --
    -- Parameters:
    --            pIn_vObjID
    --            pIn_vCoeffcient
    --            pIn_vAttribute
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        12-APril-2013     LiSang         Created.
    -- **************************************************************

   as

  begin
    Fmp_Log.FMP_SetValue(pIn_vObjID);
    Fmp_Log.FMP_SetValue(pIn_vCoeffcient);
    Fmp_Log.FMP_SetValue(pIn_vAttribute);
    Fmp_Log.LOGBEGIN;

    FMSP_MODIFY_ATTRIBUTE(pIn_vAttribute => pIn_vAttribute,
                          pIn_vObjID     => pIn_vObjID,
                          pIn_nObjType   => 3,
                          pOut_nSqlCode  => pOut_nSqlCode);

    FMSP_MODIFY_Coeffcient(pIn_vCoeffcient  =>pIn_vCoeffcient ,
                           pIn_nObjID       => pIn_vObjID,
                           pOut_nSqlCode    => pOut_nSqlCode) ;

  Fmp_Log.LOGEND;
  exception
    when others then
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_TargetValue(pIn_vTargetValue in varchar2,
                                    pIn_nObjID       in varchar2,
                                    pOut_nSqlCode    out number)

   as
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    nValCount    number;
    nObjType     number;

  begin

    if pIn_vTargetValue is null or pIn_nObjID is null then
      return;
    end if;

    vNewVal := pIn_vTargetValue;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);

      if nNodeAttNum is not null then
        /*if nNodeAttID is null then 
          delete from rfc r where r.fam7_em_addr in 
          (SELECT f.fam_em_addr
                 FROM fam f
                where f.fam_em_addr != pIn_nObjID
                START WITH f.fam_em_addr = pIn_nObjID
               CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr)
               and r.numero_crt = 68+nNodeAttNum 
               and r.
        end if;*/
        merge into rfc r
        using (SELECT f.fam_em_addr
                 FROM fam f
                where f.fam_em_addr != pIn_nObjID
                START WITH f.fam_em_addr = pIn_nObjID
               CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) t
        on (r.fam7_em_addr = t.fam_em_addr and r.numero_crt = 68 + nNodeAttNum and r.ident_crt = p_constant.v_RFC_P)
        when matched then 
          update set r.vct10_em_addr = nNodeAttID
          delete  where  nNodeAttID is null
        when not matched then
          insert
            (r.rfc_em_addr,
             r.ident_crt,
             r.numero_crt,
             r.fam7_em_addr,
             r.geo8_em_addr,
             r.dis9_em_addr,
             r.vct10_em_addr)
          values
            (seq_rfc.nextval,
             p_constant.v_RFC_P,
             68 + nNodeAttNum,
             t.fam_em_addr,
             0,
             0,
             nNodeAttID);
      end if;
    end loop;

  end;

  procedure FMSP_MODIFY_ATTRIBUTE(pIn_vAttribute in varchar2,
                                  pIn_vObjID     in varchar2,
                                  pIn_nObjType   in varchar2,
                                  pOut_nSqlCode  out number)

   as
    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    nValCount    number;
    nObjType     number;
  begin

    if pIn_vAttribute is null or pIn_vObjID is null then
      return;
    end if;

    vNewVal := pIn_vAttribute;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);
      if nNodeAttNum is not null then
        -- this is update product
        if pIn_nObjType = 1 then
          merge into rfc r
          using (SELECT f.fam_em_addr
                   FROM fam f
                  where f.fam_em_addr != pIn_vObjID
                  START WITH f.fam_em_addr = pIn_vObjID
                 CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) t
          on (r.fam7_em_addr = t.fam_em_addr and r.numero_crt = nNodeAttNum and r.ident_crt = p_Constant.v_RFC_P)
          when matched then
            update set r.vct10_em_addr = nNodeAttID
          when not matched then
            insert
              (r.rfc_em_addr,
               r.ident_crt,
               r.numero_crt,
               r.fam7_em_addr,
               r.geo8_em_addr,
               r.dis9_em_addr,
               r.vct10_em_addr)
            values
              (seq_rfc.nextval,
               p_Constant.v_RFC_P,
               nNodeAttNum,
               t.fam_em_addr,
               0,
               0,
               nNodeAttID);
          commit;
          -- this is update sale
        elsif pIn_nObjType = 2 then

          merge into rfc r
          using (SELECT g.geo_em_addr
                   FROM geo g
                  where g.geo_em_addr != pIn_vObjID
                  START WITH g.geo_em_addr = pIn_vObjID
                 CONNECT BY g.geo1_em_addr = PRIOR g.geo_em_addr) t
          on (r.geo8_em_addr = t.geo_em_addr and r.numero_crt = nNodeAttNum and r.ident_crt = p_Constant.v_RFC_ST)
          when matched then
            update set r.vct10_em_addr = nNodeAttID
          when not matched then
            insert
              (r.rfc_em_addr,
               r.ident_crt,
               r.numero_crt,
               r.fam7_em_addr,
               r.geo8_em_addr,
               r.dis9_em_addr,
               r.vct10_em_addr)
            values
              (seq_rfc.nextval,
               p_Constant.v_RFC_ST,
               nNodeAttNum,
               0,
               t.geo_em_addr,
               0,
               nNodeAttID);
          commit;

          -- this is update trade
        elsif pIn_nObjType = 3 then

          merge into rfc r
          using (SELECT d.dis_em_addr
                   FROM dis d
                  where d.dis_em_addr != pIn_vObjID
                  START WITH d.dis_em_addr = pIn_vObjID
                 CONNECT BY d.dis2_em_addr = PRIOR d.dis_em_addr) t
          on (r.dis9_em_addr = t.dis_em_addr and r.numero_crt = nNodeAttNum and r.ident_crt = p_Constant.v_RFC_TC)
          when matched then
            update set r.vct10_em_addr = nNodeAttID
          when not matched then
            insert
              (r.rfc_em_addr,
               r.ident_crt,
               r.numero_crt,
               r.fam7_em_addr,
               r.geo8_em_addr,
               r.dis9_em_addr,
               r.vct10_em_addr)
            values
              (seq_rfc.nextval,
               p_Constant.v_RFC_TC,
               nNodeAttNum,
               0,
               0,
               t.dis_em_addr,
               nNodeAttID);

        end if;
      end if;
    end loop;
  exception
    when others then
      Fmp_Log.LOGERROR;
  end;

  procedure FMSP_MODIFY_CoefficientTable(pIn_vCoefficientT in varchar2,
                                         pIn_nObjID        in varchar2,
                                         pOut_nSqlCode     out number) as

    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;
  begin

    if pIn_vCoefficientT is null or pIn_nObjID is null then
      return;
    end if;

    vNewVal := pIn_vCoefficientT;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);

      if nNodeAttNum is not null then
        merge into famtrf ft
        using (SELECT f.fam_em_addr
                 FROM fam f
                where f.fam_em_addr != pIn_nObjID
                START WITH f.fam_em_addr = pIn_nObjID
               CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) t
        on (ft.fam33_em_addr = t.fam_em_addr and ft.num_trf = nNodeAttNum - 1)
        when matched then
          update set ft.trf34_em_addr = nNodeAttID
        when not matched then
          insert
            (ft.famtrf_em_addr,
             ft.num_trf,
             ft.fam33_em_addr,
             ft.trf34_em_addr)
          values
            (seq_famtrf.nextval,
             nNodeAttNum - 1,
             t.fam_em_addr,
             nNodeAttID);

      end if;
    end loop;

  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_Ratio(pIn_vRatio    in varchar2,
                              pIn_nObjID    in varchar2,
                              pOut_nSqlCode out number)

   as

    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   varchar2(4000);
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;
    cSQL         clob;
  begin

    if pIn_vRatio is null or pIn_nObjID is null then
      return;
    end if;

    vNewVal := pIn_vRatio;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);
    Fmp_Log.LOGINFO(nNodeAttID);
    if nNodeAttID =0 then nNodeAttID := 'null'; end if;
      if nNodeAttNum is not null then
        cSQL := 'update fam f set f.unite_' || (nNodeAttNum + 1) || ' = ' ||
                nNodeAttID
                ||' where f.fam_em_addr in ' ||
                ' (SELECT f.fam_em_addr FROM fam f ' ||
                ' where f.fam_em_addr != ' || pIn_nObjID ||
                ' START WITH f.fam_em_addr = ' || pIn_nObjID ||
                ' CONNECT BY f.fam0_em_addr = PRIOR f.fam_em_addr) ';
        fmsp_execsql(cSQL);
      end if;

    end loop;
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_ExchangeRate(pIn_vExchangeRate in varchar2,
                                     pIn_nObjID        in varchar2,
                                     pOut_nSqlCode     out number)

   as

    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;
  begin

    if pIn_vExchangeRate is null or pIn_nObjID is null then
      return;
    end if;

    vNewVal := pIn_vExchangeRate;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);

      if nNodeAttNum is not null then
        merge into geodvs gd
        using (SELECT g.geo_em_addr
                 FROM geo g
                where g.geo_em_addr != pIn_nObjID
                START WITH g.geo_em_addr = pIn_nObjID
               CONNECT BY g.geo1_em_addr = PRIOR g.geo_em_addr) t
        on (gd.geo39_em_addr = t.geo_em_addr and gd.n0_dvs = (nNodeAttNum - 1))
        when matched then
          update set gd.dvs38_em_addr = nNodeAttID
        when not matched then
          insert
            (gd.geodvs_em_addr,
             gd.n0_dvs,
             gd.dvs38_em_addr,
             gd.geo39_em_addr)
          values
            (seq_geodvs.nextval,
             nNodeAttNum - 1,
             nNodeAttID,
             t.geo_em_addr);
      end if;
    end loop;

  exception
    when others then
      pOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_MODIFY_Coeffcient(pIn_vCoeffcient in varchar2,
                                   pIn_nObjID      in varchar2,
                                   pOut_nSqlCode   out number)
   as

    nPvtCount    number;
    nNodeAttNum  number;
    nNodeAttID   number;
    vNewVal      varchar2(4000);
    vTmpNewVal   varchar2(4000);
    nSel_Em_Addr number;
    cCursor      sys_refcursor;
    nValCount    number;
    nObjType     number;

  begin

  if pIn_vCoeffcient is null or  pIn_nObjID is null then
    return;
  end if;

    vNewVal := pIn_vCoeffcient;

    nValCount := (length(vNewVal) - length(replace(vNewVal, ';'))) + 1;
    if nValCount is null then
      nValCount := 1;
    end if;

    for i in 1 .. nValCount loop

      vTmpNewVal := substr(vNewVal, 0, instr(vNewVal, ';', 1, 1) - 1);

      if nValCount = 1 or vTmpNewVal is null then
        vTmpNewVal := vNewVal;
      else
        vNewVal := substr(vNewVal, length(vTmpNewVal) + 2);
      end if;

      nNodeAttNum := substr(vTmpNewVal, 0, instr(vTmpNewVal, ':', 1) - 1);
      nNodeAttID  := substr(vTmpNewVal, length(nNodeAttNum) + 2);

      if nNodeAttID is not null then
      update dis d set d.rms40_em_addr = nNodeAttID
      where d.dis_em_addr in
      (SELECT d.dis_em_addr
          FROM dis d
          where d.dis_em_addr != pIn_nObjID
          START WITH d.dis_em_addr = pIn_nObjID
          CONNECT BY d.dis2_em_addr = PRIOR d.dis_em_addr );
  end if;
end loop;
  end;

end FMP_MODIFY_GROUP_OBJECT;
/
