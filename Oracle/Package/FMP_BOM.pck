CREATE OR REPLACE PACKAGE FMP_BOM IS

  g_eException exception; --comments

  procedure FMSP_handleBOM(pIn_nChronology    in number, --1 Monthly ,2 Weekly,3 Dayly
                           pIn_nNodeType      in number, --1  Detail Node  2  Aggregate Node
                           pIn_vOption        in varchar2,
                           pIn_nFromTSID      in number,
                           pIn_nTOTSID        in number,
                           pIn_nUpDown        in number, --1 UP_down 2 Down_up
                           pIn_nBeginYY       in number,
                           pIn_nBeginPeriod   in number,
                           pIn_nEndYY         in number,
                           pIn_nEndPeriod     in number,
                           PIn_nmcfield       in number, --nmc_field field in nmc table
                           pIn_nPeriodPerYear in number,
                           pOut_nSQLCode      out number);

  procedure FMSP_BomTStoSQL(PIn_vID        in varchar2,
                            PIn_vKey       in varchar2,
                            pIn_nFromTSID  in number,
                            pIn_nTOTSID    in number,
                            pIn_vTableName in varchar2,
                            pIn_vUDName    in varchar2,
                            pIn_nBeginYY   in number,
                            pIn_nEndYY     in number,
                            pIn_nNumber    in number,
                            pIn_nCycle     in number,
                            PIn_nmcfield   in number, --nmc_field field in nmc table
                            pOut_nSQLCode  out number);

END FMP_BOM;
/
CREATE OR REPLACE PACKAGE BODY FMP_BOM IS

  --*****************************************************************
  -- Description: BOM.
  --
  -- Author:      <wfq>
  -- Revise
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        20-1-2013     wfq           Created.

  -- **************************************************************

  procedure FMSP_handleBOM(pIn_nChronology    in number,
                           pIn_nNodeType      in number,
                           pIn_vOption        in varchar2,
                           pIn_nFromTSID      in number,
                           pIn_nTOTSID        in number,
                           pIn_nUpDown        in number,
                           pIn_nBeginYY       in number,
                           pIn_nBeginPeriod   in number,
                           pIn_nEndYY         in number,
                           pIn_nEndPeriod     in number,
                           PIn_nmcfield       in number,
                           pIn_nPeriodPerYear in number,
                           pOut_nSQLCode      out number) as
    --*****************************************************************
    -- Description: BOM
    --
    -- Parameters:
    --   pIn_nChronology        in number, --1 Monthly ,2 Weekly,3 Dayly
    /*pIn_nNodeType    in number, --1  Detail Node  2  Aggregate Node
    pIn_vOption      in varchar2, --
    pIn_nFromTSID    in number,
    pIn_nTOTSID      in number,
    pIn_nUpDown      in number, --1 UP_down 2 Down_up
    pIn_nBeginYY     in number,
    pIn_nBeginPeriod in number,
    pIn_nEndYY       in number,
    pIn_nEndPeriod   in number,
    PIn_nmcfield     in number, --nmc_field field in nmc table
    pIn_nPeriodPerYear      in number, -- Period number of per year, Monthly = 12, weekly = 52, daily = 52 * dayperweek
    */
    -- Error Conditions Raised:
    --
    -- Author:      <wfq>
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        20-1-2013     wfq           Created.
    -- **************************************************************
  
    vStrSql      varchar(8000);
    vType        varchar2(50);
    vTBName      varchar2(50);
    vUDName      varchar2(50);
    vCycle       int;
    VID          varchar2(50);
    nNowDate     number;
    iBeginYY     int := 0;
    iEndYY       int := 0;
    iEndYear     int := 0;
    IBeginPeriod int := pIn_nBeginPeriod;
    IEndPeriod   int := pIn_nEndPeriod;
    vKey         varchar2(100);
    igeolevel    int := 1;
    idislevel    int := 1;
    Vpvtcle      varchar2(80);
    VpvtDesc     varchar2(80);
    VFromLevel   varchar2(2000) := '';
    VFrom        varchar2(2000) := '';
    VSTID        varchar2(100);
    VTCID        varchar2(100);
    nPVTID       number;
    VTableName   varchar2(100);
    tc_PVTID     sys_refcursor;
  
    ilength    int := 0;
    inext      int := 0;
    vOption    varchar2(100);
    iposition  int := 0;
    vvalue     varchar2(100);
    VstrOption varchar2(1000);
    vSELvalue  varchar2(100);
    tc_DataYM  sys_refcursor;
  BEGIN
    pOut_nSQLCode := 0;
  
    --add log
    fmp_log.FMP_SetValue(pIn_nChronology);
    fmp_log.FMP_SetValue(pIn_nNodeType);
    fmp_log.FMP_SetValue(pIn_vOption);
    fmp_log.FMP_SetValue(pIn_nFromTSID);
    fmp_log.FMP_SetValue(pIn_nTOTSID);
    fmp_log.FMP_SetValue(pIn_nUpDown);
    fmp_log.FMP_SetValue(pIn_nBeginYY);
    fmp_log.FMP_SetValue(pIn_nBeginPeriod);
    fmp_log.FMP_SetValue(pIn_nEndYY);
    fmp_log.FMP_SetValue(pIn_nEndPeriod);
    fmp_log.FMP_SetValue(PIn_nmcfield);
    fmp_log.FMP_SetValue(pIn_nPeriodPerYear);
    fmp_log.LOGBEGIN;
  
    VstrOption := trim(upper(pIn_vOption));
    ilength    := length(VstrOption);
    while ilength > 0 LOOP
      --'##' Separated values
      inext := instr(VstrOption, '##', 1, 1);
      IF inext = 0 then
        vOption := VstrOption;
        ilength := 0;
      end if;
    
      if inext > 1 then
        vOption    := trim(substr(VstrOption, 0, inext - 1));
        VstrOption := trim(substr(VstrOption, inext + 2));
        ilength    := length(VstrOption);
      END IF;
    
      --':' Separated values
      iposition := INSTR(vOption, ':', 1, 1);
      if iposition = 0 then
        vKey := vOption;
      else
        vKey := trim(substr(vOption, 1, iposition - 1));
      end if;
    
      vKey   := rtrim(vKey);
      vvalue := trim(substr(vOption, iposition + 1));
    
      case vKey
      --=========================================Date formats==========================================
        when 'SEL' then
          vSELvalue := vvalue;
        else
          null;
      end case;
    
    END LOOP;
  
    vCycle := pIn_nPeriodPerYear;
    IF pIn_nChronology = p_constant.Monthly THEN
      --Monthly
      vType := 'M';
    ELSIF pIn_nChronology = p_constant.Weekly THEN
      --weekly
      vType := 'W';
    ELSIF pIn_nChronology = p_constant.Daily THEN
      --daily
      vType := 'D';
    END IF;
  
    IF PIn_nNodeType = 1 THEN
      --1  Detail Node
      vTBName := 'DON_' || vType;
      VID     := 'PVTID';
    ELSIF PIn_nNodeType = 2 THEN
      --2  Aggregate Node
      vTBName := 'prb_' || vType;
      VID     := 'SELID';
    END IF;
  
    IF pIn_nNodeType = 1 THEN
      --Detail Node
      vUDName := 'FMV_BomPvtDownToUp';
      IF pIn_nUpDown = 1 THEN
        --up TO Down
        vUDName := 'FMV_BomPvtUpToDown';
        --add bdg
        vStrSql := 'insert  into bdg(bdg_em_addr,ID_bdg,b_cle,bdg_desc)';
        vStrSql := vStrSql ||
                   ' select seq_bdg.nextval,80,pvt_cle,pvt_desc from
      (select distinct famID,geoID,disID,ToName pvt_cle,Todesc pvt_desc from ' ||
                   vUDName || '
      where  ToID is null and nmc_field=' || PIn_nmcfield;
        IF vSELvalue is not null THEN
          vStrSql := vStrSql || ' and kEY= ''' || vSELvalue || '''';
        END IF;
        vStrSql := vStrSql || ' ) t';
        fmsp_execsql(vStrSql);
      
        --add detail Node
        nNowDate := F_ConvertDateToOleDateTime(sysdate);
        vstrsql  := 'insert  into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr
                 ,adr_pro,adr_geo,adr_dis
             ,pvt_cle,pvt_desc,user_create_pvt,date_create_pvt) ';
        vStrSql  := vStrSql ||
                    ' select seq_pvt.nextval,famID,geoID,disID,famID,geoID,disID,pvt_cle,pvt_desc,''FM'',' ||
                    nNowDate ||
                    ' from
      (select distinct famID,geoID,disID,ToName pvt_cle,Todesc pvt_desc from ' ||
                    vUDName || '
      where ToID is null and nmc_field=' || PIn_nmcfield;
        IF vSELvalue is not null THEN
          vStrSql := vStrSql || ' and kEY= ''' || vSELvalue || '''';
        END IF;
        vStrSql := vStrSql || ' ) t';
        fmsp_execsql(vStrSql);
      
      END IF;
    
    ELSIF pIn_nNodeType = 2 THEN
      --Aggregate Node
      vUDName := 'FMV_BomAggDownToUp';
      IF pIn_nUpDown = 1 THEN
        --up TO Down
        vUDName := 'FMV_BomAggUpToDown';
        --create table
        VTableName := fmf_gettmptablename();
        vstrsql    := 'create table ' || VTableName || '(
        PID int,
        STID int,
        TCID int,
        pvt_cle varchar2(80) ,
        pvt_desc varchar2(120)
        )';
        execute immediate vstrsql;
      
        vstrsql := ' select Regroup_geo,regroup_dis from prv ';
        IF vSELvalue is not null THEN
          vstrsql := vstrsql || ' where PRV_CLE=''' || vSELvalue || '''';
        END IF;
      
        open tc_DataYM for vstrsql;
        loop
          fetch tc_DataYM
            into igeolevel, idislevel;
          exit when tc_DataYM%notfound;
        
          Vpvtcle  := 'T.f_cle';
          VpvtDesc := 'T.f_desc';
          IF igeolevel > 0 THEN
            Vpvtcle  := Vpvtcle || '||''-''||g.g_cle';
            VpvtDesc := VpvtDesc || '||''-''||g.g_desc';
            VFrom    := VFrom ||
                        ' left join geo g on g.geo_em_addr=t.STID ';
          END IF;
          IF idislevel > 0 THEN
            Vpvtcle  := Vpvtcle || '||''-''||D.D_cle';
            Vpvtdesc := Vpvtdesc || '||''-''||D.D_desc';
            VFrom    := VFrom ||
                        ' left join dis d on d.dis_em_addr=t.TCID ';
          END IF;
        
          IF igeolevel > 1 THEN
            VSTID      := 'max(g.L_1_ID)';
            VFromLevel := VFromLevel || ' left join v_geo_level g on g.L_' ||
                          igeolevel || '_ID=b.STID ';
          ELSE
            VSTID := 'B.STID';
          END IF;
          IF idislevel > 1 THEN
            VTCID      := 'max(d.L_1_ID)';
            VFromLevel := VFromLevel || ' left join v_DIS_level d on d.L_' ||
                          idislevel || '_ID=b.TCID ';
          ELSE
            VTCID := 'B.TCID';
          END IF;
        
          vstrsql := 'insert into ' || VTableName ||
                     ' select t.PID,T.STID,T.TCID,' || Vpvtcle ||
                     ' pvt_cle,' || Vpvtdesc ||
                     ' pvt_desc from(select distinct b.PID,' || VSTID ||
                     ' STID,' || VTCID || ' TCID,b.f_cle,b.f_desc
                from ' || vUDName || ' B ';
        
          vstrsql := vstrsql || VFromLevel ||
                     ' where b.ToID is null and nmc_field=' || PIn_nmcfield;
          IF vSELvalue is not null THEN
            vStrSql := vStrSql || ' and kEY= ''' || vSELvalue || '''';
          END IF;
        
          vStrSql := vStrSql ||
                     ' group by b.PID,b.STID,b.TCID,b.f_cle,b.f_desc) T ' ||
                     VFrom;
          --add log
          -- Fmp_Log.logInfo(pIn_cSqlText => vstrsql);
        
          execute immediate vstrsql;
        end loop;
        close tc_DataYM;
      
        --add detail Node
        nNowDate := F_ConvertDateToOleDateTime(sysdate);
        vstrsql  := 'insert  into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr
                 ,adr_pro,adr_geo,adr_dis
             ,pvt_cle,pvt_desc,user_create_pvt,date_create_pvt) ';
        vStrSql  := vStrSql ||
                    ' select seq_pvt.nextval,PID,STID,TCID,PID,STID,TCID,pvt_cle,pvt_desc,''FM'',' ||
                    nNowDate || ' from ' || VTableName || '  t';
        fmsp_execsql(vStrSql);
      
        --add bdg
        vStrSql := 'insert  into bdg(bdg_em_addr,ID_bdg,b_cle,bdg_desc)';
        vStrSql := vStrSql ||
                   ' select seq_bdg.nextval,80,pvt_cle,pvt_desc from ' ||
                   VTableName || ' t';
        fmsp_execsql(vStrSql);
      
        --Generate Aggregate Node
        vStrSql := 'select p.pvt_em_addr from ' || VTableName ||
                   ' t,pvt p where t.pvt_cle=p.pvt_cle';
        open tc_PVTID for vStrSql;
        loop
          fetch tc_PVTID
            into nPVTID;
          exit when tc_PVTID%notfound;
        
          --create teh aggregate node as the specified detail node
          FMP_CreateAggNode.FMSP_CreateAggNode(pIn_nDetailNode => nPVTID,
                                               pOut_nSqlCode   => pOut_nSqlCode);
          if pOut_nSqlCode <> 0 then
            return;
          end if;
        
        end loop;
        close tc_PVTID;
      
        --drop table
        vStrSql := 'drop table ' || VTableName || ' purge ';
        fmsp_execsql(vStrSql);
      
      END IF;
    END IF;
  
    --update time series======================================================
    -- get max year and min year
    vStrSql := '
        select nvl(Min(YY),0) minYY,nvl(Max(YY),0) maxyy  from (
        select distinct YY from ' || vTBName ||
               ' where tsID=' || pIn_nFromTSID ||
               ' and version=0
        union
        select distinct YY from ' || vTBName ||
               ' where tsID=' || pIn_nTOTSID || ' and version=0
        )';
    execute immediate vStrSql
      into iBeginYY, iEndYY;
  
    --get max beginYY
    iBeginYY := greatest(iBeginYY, pIn_nBeginYY);
    IF iBeginYY > pIn_nBeginYY THEN
      iBeginYY     := iBeginYY;
      IBeginPeriod := 1;
    ELSE
      iBeginYY := pIn_nBeginYY;
    END IF;
    --get min endyy
    iEndYY := least(iEndYY, pIn_nEndYY);
    IF iEndYY >= pIn_nEndYY THEN
      iEndYY := pIn_nEndYY;
    ELSE
      IEndPeriod := vCycle;
    END IF;
  
    --beginYY=endyy
    IF iBeginYY = iEndYY THEN
      BEGIN
        FMSP_BomTStoSQL(PIn_vID        => VID,
                        PIn_vKey       => vSELvalue,
                        pIn_nFromTSID  => pIn_nFromTSID,
                        pIn_nTOTSID    => pIn_nTOTSID,
                        pIn_vTableName => vTBName,
                        pIn_vUDName    => vUDName,
                        pIn_nBeginYY   => iBeginYY,
                        pIn_nEndYY     => iBeginYY,
                        pIn_nNumber    => IBeginPeriod,
                        pIn_nCycle     => IEndPeriod,
                        PIn_nmcfield   => PIn_nmcfield,
                        pOut_nSQLCode  => pOut_nSQLCode);
        if pOut_nSQLCode <> 0 then
          return;
        end if;
      END;
    ELSE
      --beginYY<>endyy
      BEGIN
      
        --the first year data
        IF IBeginPeriod > 1 THEN
        
          FMSP_BomTStoSQL(PIn_vID        => VID,
                          PIn_vKey       => vSELvalue,
                          pIn_nFromTSID  => pIn_nFromTSID,
                          pIn_nTOTSID    => pIn_nTOTSID,
                          pIn_vTableName => vTBName,
                          pIn_vUDName    => vUDName,
                          pIn_nBeginYY   => iBeginYY,
                          pIn_nEndYY     => iBeginYY,
                          pIn_nNumber    => IBeginPeriod,
                          pIn_nCycle     => vCycle,
                          PIn_nmcfield   => PIn_nmcfield,
                          pOut_nSQLCode  => pOut_nSQLCode);
          if pOut_nSQLCode <> 0 then
            return;
          end if;
        
          iBeginYY := iBeginYY + 1;
        END IF;
      
        --the last year data
        IF pIn_nEndPeriod < vCycle THEN
          FMSP_BomTStoSQL(PIn_vID        => VID,
                          PIn_vKey       => vSELvalue,
                          pIn_nFromTSID  => pIn_nFromTSID,
                          pIn_nTOTSID    => pIn_nTOTSID,
                          pIn_vTableName => vTBName,
                          pIn_vUDName    => vUDName,
                          pIn_nBeginYY   => iEndYY,
                          pIn_nEndYY     => iEndYY,
                          pIn_nNumber    => 1,
                          pIn_nCycle     => IEndPeriod,
                          PIn_nmcfield   => PIn_nmcfield,
                          pOut_nSQLCode  => pOut_nSQLCode);
          if pOut_nSQLCode <> 0 then
            return;
          end if;
          iEndYear := iEndYY - 1;
        ELSE
          iEndYear := iEndYY;
        END IF;
      
        --Middle years of data
        FMSP_BomTStoSQL(PIn_vID        => VID,
                        PIn_vKey       => vSELvalue,
                        pIn_nFromTSID  => pIn_nFromTSID,
                        pIn_nTOTSID    => pIn_nTOTSID,
                        pIn_vTableName => vTBName,
                        pIn_vUDName    => vUDName,
                        pIn_nBeginYY   => iBeginYY,
                        pIn_nEndYY     => iEndYear,
                        pIn_nNumber    => 1,
                        pIn_nCycle     => vCycle,
                        PIn_nmcfield   => PIn_nmcfield,
                        pOut_nSQLCode  => pOut_nSQLCode);
        if pOut_nSQLCode <> 0 then
          return;
        end if;
      END;
    END IF;
    fmp_log.LOGEND;
  
  exception
    when others then
      Fmp_Log.LOGERROR;
      pOut_nSqlCode := SQLCODE;
      --raise_application_error(SQLCODE,SQLERRM);
      sp_log(p_type      => 1,
             p_operation => ' FMSP_handleBOM ',
             p_status    => 1,
             p_logmsg    => ' FMSP_handleBOM with error ' ||
                            substr(sqlerrm, 1, 200),
             p_sqltext   => vStrSql,
             p_logcode   => pOut_nSqlCode);
  END;

  procedure FMSP_BomTStoSQL(PIn_vID        in varchar2,
                            PIn_vKey       in varchar2,
                            pIn_nFromTSID  in number,
                            pIn_nTOTSID    in number,
                            pIn_vTableName in varchar2,
                            pIn_vUDName    in varchar2,
                            pIn_nBeginYY   in number,
                            pIn_nEndYY     in number,
                            pIn_nNumber    in number,
                            pIn_nCycle     in number,
                            PIn_nmcfield   in number, --nmc_field field in nmc table
                            pOut_nSQLCode  out number) as
  
    vUpdatesql varchar2(8000);
    vstrGroup  varchar2(2000);
    vstrT      varchar2(2000);
    vstrM      varchar2(2000);
    vstrValue  varchar2(2000);
    cStrSql    clob;
  
  begin
  
    pOut_nSQLCode := 0;
  
    for i in pIn_nNumber .. pIn_nCycle loop
    
      vstrT      := vstrT || ',sum(d1.T' || i || ' * f.qute) T' || i;
      vstrGroup  := vstrGroup || ',d.T' || i;
      vstrM      := vstrM || ',T' || i;
      vUpdatesql := vUpdatesql || ',T' || i || '= n.T' || i;
      vstrValue  := vstrValue || ',n.T' || i;
    end loop;
  
    vUpdatesql := substr(vUpdatesql, 2);
  
    cStrSql := '
          MERGE /*+use_hash(d,n)*/  INTO ' || pIn_vTableName || ' d
          USING (select f.ToID ,' || pIn_nTOTSID ||
               ' tsid,0 version,d1.YY' || vstrT ||
               ' from (select distinct ToID,FromID,qute from ' ||
               pIn_vUDName;
  
    cStrSql := cStrSql || ' where nmc_field= ''' || PIn_nmcfield || '''';
    IF PIn_vKey is not null THEN
      cStrSql := cStrSql || ' and Key= ''' || PIn_vKey || '''';
    END IF;
    cStrSql := cStrSql || ') f left join ' || pIn_vTableName ||
               ' d1 on f.FromID=d1.' || PIn_vID || ' and d1.tsID=' ||
               pIn_nFromTSID || ' and d1.version=0
                left join ' || pIn_vTableName ||
               ' d on f.ToID=d.' || PIn_vID ||
               ' and d1.YY=d.YY and d.tsID=' || pIn_nFromTSID ||
               ' and d.version=0
                where d1.YY between ' || pIn_nBeginYY ||
               ' and ' || pIn_nEndYY;
  
    cStrSql := cStrSql || ' group by f.ToID,d.tsid,d.version,d1.YY' ||
               vstrGroup || ') n
             ON (d.YY = n.YY and d.' || PIn_vID ||
               '=n.ToID and d.TSID=n.TSID and d.Version=n.Version)
             WHEN MATCHED THEN
             UPDATE
            SET ';
    cStrSql := cStrSql || vUpdatesql;
    cStrSql := cStrSql || '
           WHEN NOT MATCHED THEN
            INSERT (' || pIn_vTableName || 'ID,' || PIn_vID ||
               ',TSID,Version,YY' || vstrM || ')
            VALUES (seq_' || pIn_vTableName ||
               '.nextval,n.ToID,n.TSID,n.Version,n.YY' || vstrValue || ')';
  
    fmsp_execsql(pIn_cSql => cStrSql);
  
  exception
    when others then
      pOut_nSQLCode := sqlcode;
  end;

END FMP_BOM;
/
