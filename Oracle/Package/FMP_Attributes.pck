create or replace package FMP_Attributes is

  procedure FMSP_ExportAttributesValue(pIn_nChronology         IN NUMBER,
                                       pIn_nTimeSeriesNodeType IN NUMBER,
                                       pIn_strFMUSER           IN VARCHAR2,
                                       pIn_vSeperator          in VARCHAR2,
                                       pIn_vOption             in VARCHAR2,
                                       pOut_vTmpTableName      out VARCHAR2,
                                       pOut_nSqlCode           out number);

  procedure FMSP_ImportAttributesValue(pIn_nChronology         IN NUMBER,
                                       pIn_nTimeSeriesNodeType IN NUMBER,
                                       pIn_strFMUSER           IN VARCHAR2,
                                       pIn_nNumberOfAttribute  in varchar2,
                                       pIn_vSeperator          in varchar2,
                                       pIn_vOption             in varchar2,
                                       pIn_vTmpTableName       in varchar2,
                                       pIn_vDescription        in varchar2,
                                       p_nTaskId               out integer,
                                       p_nImportedSuccessCount out integer,
                                       pOut_nSqlCode           out number);

  procedure FMSP_CreateSaveTSTable(pIn_nChronology           IN NUMBER,
                                   pIn_nTimeSeriesNodeType   IN NUMBER,
                                   pIn_nNumberOfAttribute    in number,
                                   pIn_vOption               in varchar2,
                                   pIn_strTemporaryTableName out varchar2,
                                   pOut_nSqlCode             out number);


procedure FMSP_GetImportSQL(pIn_nTimeSeriesNodeType in number,
                              pIn_nNumberOfAttribute  in varchar2,
                              pIn_vTmpTableName       in varchar2,
                              pOut_nSqlCode           out number);
end FMP_Attributes;
/
create or replace package body FMP_Attributes is

  procedure FMSP_GetExportSQL(pIn_nTableName          in varchar2,
                              pIn_nTimeSeriesNodeType in number,
                              pIn_vSeperator          in varchar2,
                              pIn_vSdlt               in varchar2,
                              pIn_oOptions            in P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                              pOut_cSql               out clob,
                              pOut_nSqlCode           out number);

  /*procedure FMSP_GetImportSQL(pIn_nTimeSeriesNodeType in number,
                              pIn_nNumberOfAttribute  in varchar2,
                              pIn_vTmpTableName       in varchar2,
                              pOut_nSqlCode           out number);*/

  procedure FMSP_ExportAttributesValue(pIn_nChronology         IN NUMBER,
                                       pIn_nTimeSeriesNodeType IN NUMBER,
                                       pIn_strFMUSER           IN VARCHAR2,
                                       pIn_vSeperator          in VARCHAR2,
                                       pIn_vOption             in VARCHAR2,
                                       pOut_vTmpTableName      out VARCHAR2,
                                       pOut_nSqlCode           out number)

   is
    --*****************************************************************
    -- Description: this procedure  is get Export attribute.
    --
    -- Parameters:
    --            pIn_nChronology
    --            pIn_nTimeSeriesNodeType  1  Detail Node  2  Aggregate Node
    --            pIn_strFMUSER
    --            pIn_vSeperator
    --            pIn_vOption
    --            pOut_vTmpTableName
    --            pOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     LiSang         Created.
    -- **************************************************************
    cSQL       clob;
    vSeperator varchar2(30) := 'chr(44)||'; --default ,
    vSdlt      varchar2(100);
    nSqlCode   number;
    tOptions   P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType;
    v_vOption_tmp        varchar2(4000);

  begin
    Fmp_Log.FMP_SetValue(pIn_nTimeSeriesNodeType);
    Fmp_Log.FMP_SetValue(pIn_vSeperator);
    Fmp_Log.FMP_SetValue(pIn_vOption);
    FMP_log.LOGBEGIN;

    pOut_vTmpTableName := FMF_GetTmpTableName;
    v_vOption_tmp   := pIn_vOption;

    if v_vOption_tmp = 'showwindow:0' then
    v_vOption_tmp :=null;
    end if;

    FMP_CommonUtil.FMSP_ParseOptions(pIn_vStrOptions   => v_vOption_tmp,
                                     pIn_vSeperator    => pIn_vSeperator,
                                     pInOut_vSeparator => vSeperator,
                                     pInOut_vSdlt      => vSdlt,
                                     pOut_oOptions     => tOptions,
                                     pInOut_nSqlCode   => nSqlCode);

    FMSP_GetExportSQL(pIn_nTableName          => pOut_vTmpTableName,
                      pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                      pIn_vSeperator          => vSeperator,
                      pIn_vSdlt               => vSdlt,
                      pIn_oOptions            => tOptions,
                      pOut_cSql               => cSQL,
                      pOut_nSqlCode           => pOut_nSqlCode);

    fmsp_execsql(cSQL);
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;

  end FMSP_ExportAttributesValue;

  procedure FMSP_ImportAttributesValue(pIn_nChronology         IN NUMBER,
                                       pIn_nTimeSeriesNodeType IN NUMBER,
                                       pIn_strFMUSER           IN VARCHAR2,
                                       pIn_nNumberOfAttribute  in varchar2,
                                       pIn_vSeperator          in varchar2,
                                       pIn_vOption             in varchar2,
                                       pIn_vTmpTableName       in varchar2,
                                       pIn_vDescription        in varchar2,
                                       p_nTaskId               out integer,
                                       p_nImportedSuccessCount out integer,
                                       pOut_nSqlCode           out number)

   is
    --*****************************************************************
    -- Description: this procedure  is get Import attribute.
    --
    -- Parameters:
    --            pIn_nChronology
    --            pIn_nTimeSeriesNodeType 1  Detail Node  2  Aggregate Node
    --            pIn_strFMUSER
    --            pIn_nNumberOfAttribute
    --            pIn_vSeperator
    --            pIn_vOption
    --            pOut_vTmpTableName
    --            pIn_vDescription
    --            pOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     LiSang         Created.
    -- **************************************************************
    tSwitches     Fmp_Batch.g_FMRT_Switches;
    vTmpTableName varchar2(4000);
  begin
    Fmp_Log.FMP_SetValue(pIn_nTimeSeriesNodeType);
    Fmp_Log.FMP_SetValue(pIn_nNumberOfAttribute);
    Fmp_Log.FMP_SetValue(pIn_vOption);
    Fmp_Log.FMP_SetValue(pIn_vTmpTableName);
    Fmp_Log.LOGBEGIN;

    p_nTaskId               := 0;
    p_nImportedSuccessCount := 0;
    vTmpTableName           := pIn_vTmpTableName;

    FMP_Batch.FMSP_Parse(pIn_vSwitches  => pIn_vOption,
                         pOut_tSwitches => tSwitches,
                         pOut_nSqlCode  => pOut_nSqlCode);

    FMP_Batch.FMSP_ImpNode(pIn_nNodeType     => pIn_nTimeSeriesNodeType,
                           pInOut_vTablename => vTmpTableName,
                           pIn_tSwitches     => tSwitches,
                           pIn_vFMUSER       => pIn_strFMUSER,
                           pIn_vDesc         => pIn_vDescription,
                           pOut_nSqlCode     => pOut_nSqlCode);

    FMSP_GetImportSQL(pIn_nTimeSeriesNodeType => pIn_nTimeSeriesNodeType,
                      pIn_nNumberOfAttribute  => pIn_nNumberOfAttribute,
                      pIn_vTmpTableName       => vTmpTableName,
                      pOut_nSqlCode           => pOut_nSqlCode);

  exception
    when others then
      FMP_log.LOGERROR;
      pOut_nSqlCode := sqlcode;

  end FMSP_ImportAttributesValue;

  procedure FMSP_CreateSaveTSTable(pIn_nChronology           IN NUMBER,
                                   pIn_nTimeSeriesNodeType   IN NUMBER,
                                   pIn_nNumberOfAttribute    in number,
                                   pIn_vOption               in varchar2,
                                   pIn_strTemporaryTableName out varchar2,
                                   pOut_nSqlCode             out number) is
    --*****************************************************************
    -- Description: this procedure  is get Import attribute  Create temp table.
    --
    -- Parameters:
    --            pIn_nChronology
    --            pIn_nTimeSeriesNodeType 1  Detail Node  2  Aggregate Node
    --            pIn_nNumberOfAttribute
    --            pIn_vOption
    --            pIn_strTemporaryTableName
    --            pOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     LiSang         Created.
    -- **************************************************************
    cSQL          CLOB;
    vAttribute    varchar2(4000);
    oOptions      P_BATCHCOMMAND_DATA_TYPE.OptionsKeyForMatsType;
    vSalesOrTrade varchar2(4000);

  begin
    FMP_log.FMP_SetValue(pIn_nTimeSeriesNodeType);
    FMP_log.FMP_SetValue(pIn_nNumberOfAttribute);
    FMP_log.FMP_SetValue(pIn_vOption);
    Fmp_Log.LOGBEGIN;

    pIn_strTemporaryTableName := FMF_GetTmpTableName;

    vSalesOrTrade := 'PRODUCT  varchar2(4000),SALES varchar2(4000),TRADE varchar2(4000) ';

    IF pIn_nNumberOfAttribute > 0 THEN

      for i in 49 .. 48 + pIn_nNumberOfAttribute loop
        if vAttribute is not null then

          vAttribute := vAttribute || ',C_' || i || ' varchar2(4000)';
        else

          vAttribute := ',C_' || i || ' varchar2(4000)';
        end if;
      end loop;

    END IF;

    Fmp_Commonutil.FMSP_GetOptionFieldFormats(pIn_vStrOptions => pIn_vOption,
                                              pOut_oOptions   => oOptions,
                                              pInOut_nSqlCode => pOut_nSqlCode);

    if oOptions.bnodis then
      vSalesOrTrade := 'PRODUCT  varchar2(4000),SALES  varchar2(4000) ';
    end if;
    IF oOptions.bnogeo THEN
      vSalesOrTrade := 'PRODUCT varchar2(4000),TRADE varchar2(4000) ';
    END IF;

    if pIn_nTimeSeriesNodeType = 2 then
      vSalesOrTrade := 'SEL_CLE varchar2(4000) , ' || vSalesOrTrade;
    end if;

    cSQL := 'CREATE TABLE ' || pIn_strTemporaryTableName || '(LineNumber number not null,' ||
            vSalesOrTrade || vAttribute || ')';

    FMSP_execsql(cSQL);
  EXCEPTION
    WHEN OTHERS THEN
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := SQLCODE;

  end;

  procedure FMSP_GetExportSQL(pIn_nTableName          in varchar2,
                              pIn_nTimeSeriesNodeType in number,
                              pIn_vSeperator          in varchar2,
                              pIn_vSdlt               in varchar2,
                              pIn_oOptions            in P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                              pOut_cSql               out clob,
                              pOut_nSqlCode           out number) as
    --*****************************************************************
    -- Description: this procedure  is get EXport attribute  SQL Text.
    --
    -- Parameters:
    --            pIn_nTableName
    --            pIn_nTimeSeriesNodeType 1  Detail Node  2  Aggregate Node
    --            pIn_vSeperator
    --            pIn_vSdlt
    --            pOut_cSql
    --            pOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     LiSang         Created.
    -- **************************************************************

    vCreateTable varchar2(4000);
    vSelect      varchar2(4000);
    vSelectHead  varchar2(4000) := ' select ';
    vSelectEnd   varchar2(4000) := '';

    vSdlt           varchar2(100);
    vSeperator      varchar2(30) := 'chr(44)||'; --default ,
    nCountAttribute number := 49;
    vAttributeValue varchar2(4000);
    vSqlSeparator   varchar2(1000);
    nPCMax          number;
    nSCMax          number;
    nMax            number;
    vSel            varchar2(4000);
    nsel            number;

  begin
    Fmp_Log.FMP_SetValue(pIn_nTableName);
    Fmp_Log.FMP_SetValue(pIn_nTimeSeriesNodeType);
    Fmp_Log.FMP_SetValue(pIn_vSeperator);
    Fmp_Log.FMP_SetValue(pIn_vSdlt);
    Fmp_Log.LOGBEGIN;

    vSdlt      := pIn_vSdlt;
    vSeperator := pIn_vSeperator;

    vSqlSeparator := '||' || vSdlt || '||' || vSeperator || vSdlt || '||';

    /*select max(pc.numero_crt_pvt) into nPCMax from pvtcrt pc;
    select max(sc.numero_crt_sel) into nSCMax from selcrt sc;*/
    select max(num_ct) into nPCMax from nct t where t.id_ct = 83 ;
    if pIn_nTimeSeriesNodeType = 1 then
      nMax := nPCMax;
    elsif pIn_nTimeSeriesNodeType = 2 then
      nMax := nPCMax;
    else
      return;
    end if;

    if nMax is null then
      nMax := 0;
    end if;

    for i in nCountAttribute ..  nMax  loop
      if vAttributeValue is not null then
        vAttributeValue := vAttributeValue || vSqlSeparator || 'C' || i;
      else
        vAttributeValue := 'C' || i;
      end if;
    end loop;

    vCreateTable := 'create table ' || pIn_nTableName || ' as ';

    vSelectHead := ' select ' || vSdlt || '||';
    vSelectEnd  := '||' || vSdlt;

/*  if vAttributeValue is null then
    vAttributeValue :='chr(null)';
  end if;*/

   if pIn_nTimeSeriesNodeType = 2 then
      vSelect := vSelectHead || ' sel_cle' || vSqlSeparator || ' f_cle ' ||
                 vSqlSeparator || ' g_cle ' || vSqlSeparator || ' d_cle';

                 if vAttributeValue is not null then
                  vSelect :=vSelect ||vSqlSeparator || vAttributeValue || vSelectEnd ||
                 ' PEXPORT from v_pexport_agg ';
                 elsif vAttributeValue is null then
                 vSelect := vSelect || vSelectEnd ||
                 ' PEXPORT from v_pexport_agg ';
                 end if ;
      if pIn_oOptions.bsel then
        select pr.prv_em_addr
          into nsel
          from prv pr
         where pr.prv_cle = pIn_oOptions.strsel;
        if nsel is not null then
          vSelect := vSelect ||
                     ' va  where exists (select 1 from aggregatenode_fullid  af ' ||
                     ' where af.aggregationid = ' || nsel ||
                     ' and af.aggregatenodeid = va.sel_em_addr)';

        end if;
      end if;

      if pIn_oOptions.bsel_condit then
        vSelect := vSelect||' va where va.sel_cle = '''||pIn_oOptions.strsel_condit||'''';
      end if ;
    else
      vSelect := vSelectHead ||
                 ' f_cle ' || vSqlSeparator || ' g_cle ' || vSqlSeparator ||
                 ' d_cle' ;
                 if vAttributeValue is not null then
                   vSelect := vSelect|| vSqlSeparator || vAttributeValue || vSelectEnd ||
                 ' PEXPORT from  v_pexport_detail ';
                 elsif vAttributeValue is null then
                 vSelect := vSelect||  vSelectEnd ||
                 ' PEXPORT from  v_pexport_detail ';
                 end if ;
      if pIn_oOptions.bsel then
        vSel := pIn_oOptions.strsel;
        select s.sel_em_addr
          into nsel
          from sel s
         where s.sel_bud = 0
           and s.sel_cle = pIn_oOptions.strsel;

        if nsel is not null then
          vSelect := vSelect || 'd, rsp r where r.sel13_em_addr =' || nsel ||
                     ' and d.pvt_em_addr = r.pvt14_em_addr';
        end if;

      end if;

    end if;

    pOut_cSql := vCreateTable || vSelect;
 fmp_log.LOGDEBUG(pOut_cSql);
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;

  end;

  procedure FMSP_GetImportSQL(pIn_nTimeSeriesNodeType in number,
                              pIn_nNumberOfAttribute  in varchar2,
                              pIn_vTmpTableName       in varchar2,
                              pOut_nSqlCode           out number) as
    --*****************************************************************
    -- Description: this procedure  is inset Import attribute  .
    --
    -- Parameters:
    --            pIn_nTimeSeriesNodeType 1  Detail Node  2  Aggregate Node
    --            pIn_nNumberOfAttribute
    --            pIn_vTmpTableName
    --            pInOut_nSqlCode
    --
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        15-JAN-2013     LiSang         Created.
    -- **************************************************************

    nColNum           number;
    vNodeid           varchar2(4000);
    cSQL              clob;
    nSeq_crtserie     number;
    nCrtSerie_Em_Addr number;
    nPcMax            number;
    nPcCount          number;
    nScMax            number;
    nScCount          number;
    nScCount_1        number;
    nPcCount_1        number;
    vAttributeVal     varchar2(4000);
    nGetNodeCount     number;

  begin
    FMP_log.FMP_SetValue(pIn_nTimeSeriesNodeType);
    FMP_log.FMP_SetValue(pIn_nNumberOfAttribute);
    FMP_log.FMP_SetValue(pIn_vTmpTableName);
    Fmp_Log.LOGBEGIN;

    cSQL := 'select count(*)  from ' || pIn_vTmpTableName;
    execute immediate cSQL
      into nColNum;
    if nColNum = 0 then
      return;
    end if;

    if nColNum > 0 then
      nSeq_crtserie := seq_crtserie.nextval;
      -- loop rows number
      for i in 1 .. pIn_nNumberOfAttribute loop

        if pIn_nTimeSeriesNodeType = 1 then

          cSQL := 'insert into t_PIMport_TMP x (x.nodeid,crtserie_id,num_crt_serie,seq_value) ' ||
                  ' select t1.nodeid, c.crtserie_em_addr,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie || ' from ' ||
                  pIn_vTmpTableName || ' t1,crtserie c ' || ' where t1.c_' ||
                  to_number(i + 48) || ' = c.val_crt_serie  and c.num_crt_serie='|| to_number(i + 48);
          fmsp_execsql(cSQL);

          cSQL := 'merge into pvtcrt pc using (select * FROM t_PIMport_TMP X  WHERE  X.ROWID =  '||
                  ' (SELECT MAX(Y.ROWID) FROM  t_PIMport_TMP Y  WHERE  X.nodeid = Y.nodeid)) t1 ' ||
                  ' on (pc.PVT35_EM_ADDR = t1.nodeid and ' ||
                  ' t1.seq_value = ' ||nSeq_crtserie ||
                  ' and pc.numero_crt_pvt = ' || to_number(i + 48) ||
                  ') when matched then  ' ||
                  ' update set pc.CRTSERIE36_EM_ADDR = t1.crtserie_id ';
          fmsp_execsql(cSQL);

          fmsp_execsql('truncate table t_PIMport_TMP ');

          cSQL := 'insert into t_PIMport_TMP(nodeid)' ||
                  ' select t1.nodeid from ' || pIn_vTmpTableName ||
                  ' t1,crtserie c' || ' where t1.c_' || to_number(i + 48) ||
                  ' = c.val_crt_serie';
          fmsp_execsql(cSQL);

          cSQL := 'delete from pvtcrt pc where pc.pvt35_em_addr  ' ||
                  ' in (select t1.nodeid from ' || pIn_vTmpTableName ||
                  ' t1 where t1.c_' || to_number(i + 48) || ' is null  )' ||
                  ' and pc.numero_crt_pvt = ' || to_number(i + 48);
          fmsp_execsql(cSQL);
        end if;
        if pIn_nTimeSeriesNodeType = 2 then
          cSQL := 'insert into t_PIMport_TMP x (x.nodeid,crtserie_id,num_crt_serie,seq_value) ' ||
                  ' select t1.nodeid, c.crtserie_em_addr,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie || ' from ' ||
                  pIn_vTmpTableName || ' t1,crtserie c ' || ' where t1.c_' ||
                  to_number(i + 48) || ' = c.val_crt_serie  and c.num_crt_serie='|| to_number(i + 48);
          fmsp_execsql(cSQL);

          cSQL := 'merge into selcrt sc using (select * FROM t_PIMport_TMP X  WHERE  X.ROWID =  '||
                  ' (SELECT MAX(Y.ROWID) FROM  t_PIMport_TMP Y  WHERE  X.nodeid = Y.nodeid)) t1 ' ||
                  ' on (sc.sel53_em_addr = t1.nodeid and ' ||
                  ' t1.seq_value = ' || nSeq_crtserie ||
                  ' and sc.numero_crt_sel = ' || to_number(i + 48) ||
                  ') when matched then  ' ||
                  ' update set sc.crtserie54_em_addr = t1.crtserie_id ';
          fmsp_execsql(cSQL);

          fmsp_execsql('truncate table t_PIMport_TMP ');

          cSQL := 'insert into t_PIMport_TMP(nodeid)' ||
                  ' select t1.nodeid from ' || pIn_vTmpTableName ||
                  ' t1,crtserie c' || ' where t1.c_' || to_number(i + 48) ||
                  ' = c.val_crt_serie and c.num_crt_serie = '||to_number(i + 48);
          fmsp_execsql(cSQL);

          cSQL := 'delete from selcrt sc where sc.sel53_em_addr  ' ||
                  ' in (select t1.nodeid from ' || pIn_vTmpTableName ||
                  ' t1 where t1.c_' || to_number(i + 48) || ' is null  )' ||
                  ' and sc.numero_crt_sel = ' || to_number(i + 48);
          fmsp_execsql(cSQL);

        end if;

        fmsp_execsql('truncate table t_PIMport_TMP ');
        if pIn_nTimeSeriesNodeType = 1 then
          cSQL := 'insert into t_PIMport_TMP select t.nodeid,t.c_' ||
                  to_number(48 + i) || ',seq_crtserie.nextval,' ||
                  to_number(48 + i) || ',' || nSeq_crtserie || '  from ' ||
                  pIn_vTmpTableName || ' t where' ||
                  ' not exists (select c.val_crt_serie from crtserie c where c.val_crt_serie = t.c_' ||
                  to_number(i + 48) || ') and t.c_' || to_number(i + 48) ||
                  ' is not null';
          fmsp_execsql(cSQL);

          delete from t_PIMport_TMP
           where rowid not in (select rid
                                 from (select rowid rid,
                                              row_number() over(partition by c_value order by rownum) rn
                                         from t_PIMport_TMP x)
                                where rn = 1);
                                
          select count(*)
            into nPcMax
            from t_PIMport_TMP tp
           where tp.seq_value = nSeq_crtserie;

          if nPcMax > 0 and pIn_nTimeSeriesNodeType = 1 then
            insert into crtserie
              select tp.crtserie_id,
                     83,
                     tp.num_crt_serie,
                     tp.c_value,
                     tp.c_value,
                     0,
                     0,
                     0,
                     16777215,
                     0,
                     0
                from t_PIMport_TMP tp
               where tp.seq_value = nSeq_crtserie;

            delete from pvtcrt p
             where exists (select 1
                      from t_PIMport_TMP t1
                     where t1.nodeid = p.pvt35_em_addr
                       and t1.num_crt_serie = to_number(i + 48))
               and p.numero_crt_pvt = to_number(i + 48);


            insert into pvtcrt
              select seq_pvtcrt.nextval,
                     0,
                     tp.num_crt_serie,
                     tp.nodeid,
                     tp.crtserie_id,
                     to_number(1 - i),
                     0
                from t_PIMport_TMP tp
               where tp.seq_value = nSeq_crtserie;

          end if;
        end if;
        fmsp_execsql('truncate table t_PIMport_TMP ');

        if pIn_nTimeSeriesNodeType = 1 then
          cSQL := 'insert into t_PIMport_TMP(nodeid,crtserie_ID,num_crt_serie,seq_value) ' ||
                  'select  t2.nodeid,t2.crt,' || to_number(i + 48) || ',' ||
                  nSeq_crtserie || ' from pvtcrt pc,' ||
                  '(  select t1.nodeid,c.crtserie_em_addr crt,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie ||
                  ' from crtserie c ,' || pIn_vTmpTableName ||
                  ' t1 where c.val_crt_serie = t1.c_' || to_number(i + 48) ||
                  ' and c.num_crt_serie =' || to_number(i + 48) || ') t2' ||
                  ' where pc.pvt35_em_addr = t2.nodeid' ||
                  ' and pc.crtserie36_em_addr != t2.crt' ||
                  ' and pc.numero_crt_pvt  = ' || to_number(i + 48) ||
                  ' union all ' || ' select t11.nodeid,t11.crt,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie || ' from ' ||
                  ' (select  c.crtserie_em_addr ' || '   crt,t1.nodeid' ||
                  ' from ' || pIn_vTmpTableName ||
                  ' t1, crtserie c  where t1.c_' || to_number(i + 48) ||
                  ' is not null and c.val_crt_serie = t1.c_' ||
                  to_number(i + 48) || ' and c.num_crt_serie = ' ||
                  to_number(i + 48) || ' ) t11' ||
                  ' where not exists (select pc.pvt35_em_addr from pvtcrt pc where pc.numero_crt_pvt =' ||
                  to_number(i + 48) ||
                  'and pc.crtserie36_em_addr = t11.crt and pc.pvt35_em_addr = t11.nodeid  )';

          fmsp_execsql(cSQL);

          select count(*)
            into nPcMax
            from t_PIMport_TMP tp
           where tp.seq_value = nSeq_crtserie;

          if nPcMax > 0 and pIn_nTimeSeriesNodeType = 1 then
            insert into pvtcrt pv
              select seq_pvtcrt.nextval,
                     0,
                     tp.num_crt_serie,
                     tp.nodeid,
                     tp.crtserie_id,
                     to_number(1 - i),
                     0
                from t_PIMport_TMP tp;

            fmsp_execsql('truncate table t_PIMport_TMP ');
          end if;
        end if;

        fmsp_execsql('truncate table t_PIMport_TMP ');
        if pIn_nTimeSeriesNodeType = 2 then
          cSQL := 'insert into t_PIMport_TMP select t.nodeid,t.c_' ||
                  to_number(48 + i) || ',seq_crtserie.nextval,' ||
                  to_number(48 + i) || ',' || nSeq_crtserie || '  from ' ||
                  pIn_vTmpTableName || ' t where' ||
                  ' not exists (select c.val_crt_serie from crtserie c where c.val_crt_serie = t.c_' ||
                  to_number(i + 48) || ') and t.c_' || to_number(i + 48) ||
                  ' is not null';
          fmsp_execsql(cSQL);

          delete from t_PIMport_TMP
           where rowid not in (select rid
                                 from (select rowid rid,
                                              row_number() over(partition by c_value order by rownum) rn
                                         from t_PIMport_TMP x)
                                where rn = 1);


          select count(*)
            into nPcMax
            from t_PIMport_TMP tp
           where tp.seq_value = nSeq_crtserie;

          if nPcMax > 0 and pIn_nTimeSeriesNodeType = 2 then
            insert into crtserie
              select tp.crtserie_id,
                     83,
                     tp.num_crt_serie,
                     tp.c_value,
                     tp.c_value,
                     0,
                     0,
                     0,
                     16777215,
                     0,
                     0
                from t_PIMport_TMP tp
               where tp.seq_value = nSeq_crtserie;


            delete from selcrt s
             where exists (select 1
                      from t_PIMport_TMP t1
                     where t1.nodeid = s.sel53_em_addr
                       and t1.num_crt_serie = to_number(i + 48))
               and s.numero_crt_sel = to_number(i + 48);


            insert into selcrt
              select seq_selcrt.nextval,
                     0,
                     tp.num_crt_serie,
                     tp.nodeid,
                     tp.crtserie_id,
                     to_number(1 - i),
                     0
                from t_PIMport_TMP tp
               where tp.seq_value = nSeq_crtserie;

          end if;
        end if;
        fmsp_execsql('truncate table t_PIMport_TMP ');

        if pIn_nTimeSeriesNodeType = 2 then
          cSQL := 'insert into t_PIMport_TMP(nodeid,crtserie_ID,num_crt_serie,seq_value) ' ||
                  'select  t2.nodeid,t2.crt,' || to_number(i + 48) || ',' ||
                  nSeq_crtserie || ' from selcrt sc,' ||
                  '(  select t1.nodeid,c.crtserie_em_addr crt,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie ||
                  ' from crtserie c ,' || pIn_vTmpTableName ||
                  ' t1 where c.val_crt_serie = t1.c_' || to_number(i + 48) ||
                  ' and c.num_crt_serie =' || to_number(i + 48) || ') t2' ||
                  ' where sc.sel53_em_addr = t2.nodeid' ||
                  ' and sc.crtserie54_em_addr != t2.crt' ||
                  ' and sc.numero_crt_sel  = ' || to_number(i + 48) ||
                  ' union all ' || ' select t11.nodeid,t11.crt,' ||
                  to_number(i + 48) || ',' || nSeq_crtserie || ' from ' ||
                  ' (select  c.crtserie_em_addr  ' || ' crt,t1.nodeid' ||
                  ' from ' || pIn_vTmpTableName ||
                  ' t1, crtserie c where t1.c_' || to_number(i + 48) ||
                  ' is not null and c.val_crt_serie = t1.c_' ||
                  to_number(i + 48) || '  and c.num_crt_serie = '||to_number(i + 48)||') t11' ||
                  ' where  not exists (select sc.sel53_em_addr from selcrt sc' ||
                  ' where sc.numero_crt_sel =' || to_number(i + 48) ||
                  'and sc.crtserie54_em_addr = t11.crt  and t11.nodeid=sc.sel53_em_addr)';

          fmsp_execsql(cSQL);

          select count(*)
            into nPcMax
            from t_PIMport_TMP tp
           where tp.seq_value = nSeq_crtserie;

          if nPcMax > 0 and pIn_nTimeSeriesNodeType = 2 then
            insert into selcrt pv
              select seq_selcrt.nextval,
                     0,
                     tp.num_crt_serie,
                     tp.nodeid,
                     tp.crtserie_id,
                     to_number(1 - i),
                     0
                from t_PIMport_TMP tp;

            fmsp_execsql('truncate table t_PIMport_TMP ');
          end if;
        end if;
      end loop;

      fmsp_execsql('truncate table t_PIMport_TMP ');
    end if;
    FMP_LOG.LOGEND;
  exception
    when others then
      fmsp_execsql('truncate table t_PIMport_TMP ');
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := sqlcode;
  end;

end FMP_Attributes;
/
