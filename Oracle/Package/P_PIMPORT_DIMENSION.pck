create or replace package P_PIMPORT_DIMENSION is

  -- Author  : YPWANG
  -- Created : 11/22/2012 10:26:46 AM
  -- Purpose : import dimension(product group/product/sales territory/trade channel)

  --import product/product group
  procedure sp_ImportProduct(p_bProductGroup in boolean,
                             p_oOptions      in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                             p_nAttrCount    in integer,
                             p_strTableName  in varchar2,
                             p_strAutoDesc   in varchar2,
                             pIn_vFMUser     in varchar2 default null,
                             p_nSqlCode      out integer);

  --import sales territory
  procedure sp_ImportST(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        p_nAttrCount   in integer,
                        p_strTableName in varchar2,
                        p_strAutoDesc  in varchar2,
                        pIn_vFMUser    in varchar2 default null,
                        p_nSqlCode     out integer);

  --import trade channel
  procedure sp_ImportTC(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        p_nAttrCount   in integer,
                        p_strTableName in varchar2,
                        p_strAutoDesc  in varchar2,
                        pIn_vFMUser    in varchar2 default null,
                        p_nSqlCode     out integer);

  --create temporary table for import product/product group
  procedure sp_CreateTmpTableForProduct(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                        p_nAttrCount   in integer,
                                        p_strTableName out varchar2,
                                        p_nSqlCode     in out integer);

  --create temporary table for import sales territory/trade channel
  procedure sp_CreateTmpTableForSTorTC(p_bIsTradeChannel in boolean,
                                       p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                       p_nAttrCount      in integer,
                                       p_strTableName    out varchar2,
                                       p_nSqlCode        in out integer);

end P_PIMPORT_DIMENSION;
/
create or replace package body P_PIMPORT_DIMENSION is

  --import product/product group
  procedure sp_ImportProduct(p_bProductGroup in boolean,
                             p_oOptions      in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                             p_nAttrCount    in integer,
                             p_strTableName  in varchar2,
                             p_strAutoDesc   in varchar2,
                             pIn_vFMUser     in varchar2 default null,
                             p_nSqlCode      out integer) is
    v_strSQL      varchar2(32767) := '';
    v_strTempSQL  varchar2(32767) := '';
    v_nProductID  integer := '';
    v_nUoMCount   integer := 4;
    v_nPriceCount integer := 3;
    v_nRootAddr   integer := 0;
    v_nPeried     integer;
    VAutoDesc     varchar(60) := '';
    vnowDate      number;
    VFMUSER       varchar(60) := '';
  begin
    Fmp_Log.FMP_SetValue(p_bProductGroup);
    Fmp_Log.FMP_SetValue(p_nAttrCount);
    Fmp_Log.FMP_SetValue(p_strTableName);
    Fmp_Log.FMP_SetValue(p_strAutoDesc);
    Fmp_Log.FMP_SetValue(pIn_vFMUser);
    Fmp_Log.LOGBEGIN;

    p_nSqlCode := 0;
    --==============wfq 2013.2.21=================================
    VFMUSER  := pIn_vFMUser;
    vnowDate := F_ConvertDateToOleDateTime(sysdate);
    --p_strAutoDesc=created automatically
    VAutoDesc := p_strAutoDesc;

    --preprocess according to switches
    if p_bProductGroup then
      v_nProductID := p_constant.product_group_id;
    else
      v_nProductID := p_constant.product_id;
    end if;

    if p_oOptions.bUM then
      if p_oOptions.nUM > 0 then
        v_nUoMCount := p_oOptions.nUM;
      else
        v_nUoMCount := 6;
      end if;
    else
      v_nUoMCount := 4;
    end if;

    if p_oOptions.bPrix then
      v_nPriceCount := 5;
    else
      v_nPriceCount := 3;
    end if;

    --step 1:
    /* --delete duplicate product from temporary table
    v_strSQL := 'delete from ' || p_strTableName ||
                ' a where a.rowid<(select max(b.rowid) from ' ||
                p_strTableName || ' b where a.key=b.key)';
    --execute
    execute immediate v_strSQL;*/
    -- fmsp_execsql('create table abc as select * from '||p_strTableName);
    --Get root node addr, create it if it doesn't exist
    select count(*) into v_nRootAddr from fam where fam.f_cle is null;
    if v_nRootAddr = 0 then
      insert into fam
        (fam.fam_em_addr, fam.id_fam)
      values
        (seq_fam.nextval, 1);
    end if;
    select fam.fam_em_addr
      into v_nRootAddr
      from fam
     where fam.f_cle is null;
    --end of step 1

    --step 2: create all relative things that don't exist in database
    --parent
    --merge into
    v_strSQL := 'merge into fam';
    --using
    v_strSQL := v_strSQL || ' using (select distinct t1.parent from ' ||
                p_strTableName ||
                ' t1 where trim(t1.parent) is not null) t';
    --on
    v_strSQL := v_strSQL || ' on (fam.f_cle = t.parent)';
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL || ' insert (fam.fam_em_addr, fam.id_fam, fam.f_cle, fam.fam0_em_addr, fam.f_desc,user_create_fam,date_create_fam)
                values (seq_fam.nextval,' ||
                p_constant.product_group_id || ', t.parent, ' ||
                v_nRootAddr || ' ,''' || VAutoDesc || ''',''' || VFMUSER ||
                ''',''' || vnowDate || ''')';
    --execute
    --execute immediate v_strSQL;
    fmp_log.LOGDEBUG(pIn_cSqlText =>v_strSQL,pIn_vText => 1 );
    fmsp_execsql(v_strSQL);

    --continuation of
    if p_oOptions.bSuite then
      --merge into
      v_strSQL := 'merge into fam';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.suite from ' ||
                  p_strTableName ||
                  ' t1 where trim(t1.suite) is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (fam.f_cle = t.suite)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (fam.fam_em_addr, fam.id_fam, fam.f_cle, fam.fam0_em_addr, fam.f_desc,user_create_fam,date_create_fam) values (seq_fam.nextval,' ||
                  v_nProductID || ', t.suite, ' || v_nRootAddr || ' ,''' ||
                  VAutoDesc || ''',''' || VFMUSER || ''',''' || vnowDate ||
                  ''')';
      --execute
      --execute immediate v_strSQL;
      fmp_log.LOGDEBUG(pIn_cSqlText =>v_strSQL,pIn_vText => 2 );
      fmsp_execsql(v_strSQL);
    end if;

    --price table
    if p_oOptions.bTab then
      for i in 1 .. v_nPriceCount loop
        v_strSQL := 'merge into trf';
        --using
        v_strSQL := v_strSQL || ' using (select distinct t1.key_prix_' || i ||
                    ' from ' || p_strTableName ||
                    ' t1 where trim(t1.key_prix_' || i ||
                    ') is not null) t';
        --on
        v_strSQL := v_strSQL || ' on (trf.trf_cle=t.key_prix_' || i || ')';
        --when not matched
        v_strSQL := v_strSQL || ' when not matched then';
        --insert
        v_strSQL := v_strSQL ||
                    ' insert (trf.trf_em_addr, trf.trf_cle) values (seq_trf.nextval, t.key_prix_' || i || ')';
        --execute
        --execute immediate v_strSQL;
        fmsp_execsql(v_strSQL);
      end loop;
    end if;

    --value unit
    --merge into
    v_strSQL := 'merge into vct';
    --using
    v_strSQL := v_strSQL || ' using (select distinct t1.Value_Unit from ' ||
                p_strTableName ||
                ' t1 where trim(t1.Value_Unit) is not null) t';
    --on
    v_strSQL := v_strSQL || ' on (vct.id_crt = ' ||
                p_constant.v_ProductData || ' and vct.num_crt = ' ||
                p_constant.NumberForVU || ' and vct.val=t.Value_Unit )';
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val)';
    v_strSQL := v_strSQL || ' values (seq_vct.nextval,' ||
                p_constant.v_ProductData || ',' || p_constant.NumberForVU ||
                ',t.Value_Unit)';
    --execute
    --execute immediate v_strSQL;
    fmsp_execsql(v_strSQL);

    --UOM
    --merge into
    for i in 1 .. v_nUoMCount loop
      v_strSQL := 'merge into vct';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.key_unite_' || i ||
                  ' from ' || p_strTableName ||
                  ' t1 where trim(t1.key_unite_' || i || ') is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (vct.id_crt = ' ||
                  p_constant.v_ProductData || ' and vct.num_crt = ' ||
                  p_constant.NumberForUOM || ' and vct.val=t.key_unite_' || i || ' )';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val)';
      v_strSQL := v_strSQL || ' values (seq_vct.nextval,' ||
                  p_constant.v_ProductData || ',' ||
                  p_constant.NumberForUOM || ',t.key_unite_' || i;
      v_strSQL := v_strSQL || ')';
      --execute
      --execute immediate v_strSQL;
      fmsp_execsql(v_strSQL);
    end loop;

    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into vct';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.attr_' || i ||
                  ' from ' || p_strTableName || ' t1 where trim(t1.attr_' || i ||
                  ') is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (vct.id_crt = ' ||
                  p_constant.v_ProductData || ' and vct.num_crt = ' ||
                  (i + p_constant.BaseNumberOfAttr) ||
                  ' and vct.val=t.attr_' || i || ')';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val)';
      v_strSQL := v_strSQL || ' values (seq_vct.nextval,' ||
                  p_constant.v_ProductData || ',' ||
                  (i + p_constant.BaseNumberOfAttr) || ',t.attr_' || i;
      v_strSQL := v_strSQL || ')';

      --execute
      --execute immediate v_strSQL;
      fmsp_execsql(v_strSQL);
    end loop;
    --end of step 2

    --step 3: create or update products/product groups
    --merge into
    v_strSQL := 'merge into fam';
    --using
    v_strSQL := v_strSQL || ' using (select a.*, b.fam_em_addr as parent_addr
         from ' || p_strTableName || ' a
         left join fam b
           on b.f_cle=a.parent
        where a.parent is not null
       union
        select a.*, nvl(b.fam0_em_addr,' || v_nRootAddr ||
                ') as parent_addr
         from ' || p_strTableName || ' a
         left join fam b
           on b.f_cle=a.Key
        where a.parent is null) t';
    --on
    v_strSQL := v_strSQL || ' on (fam.f_cle = t.key)';
    --when matched
    v_strSQL := v_strSQL || ' when matched then ';
    --update
    v_strSQL := v_strSQL || ' update set fam.fam0_em_addr=t.parent_addr,fam.date_modify_fam='||vnowDate||',fam.user_modify_fam='''||pIn_vFMUser||'''';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL ||
                    ' ,fam.f_desc_court=nvl(t.short_desc,case when fam.f_desc_court=''' ||
                    VAutoDesc || ''' then null else fam.f_desc_court end)';
      when 2 then
        v_strSQL := v_strSQL ||
                    ' ,fam.f_desc=nvl(t.description,case when fam.f_desc=''' ||
                    VAutoDesc || ''' then null else fam.f_desc end)';
        v_strSQL := v_strSQL ||
                    ' ,fam.f_desc_court=nvl(t.short_desc,case when fam.f_desc_court=''' ||
                    VAutoDesc || ''' then null else fam.f_desc_court end)';
      when -1 then
        v_strSQL := v_strSQL ||
                    ' ,fam.f_desc=nvl(t.description,case when fam.f_desc=''' ||
                    VAutoDesc || ''' then null else fam.f_desc end)';
      else
        null;
    end case;
    for i in 1 .. v_nPriceCount loop
      v_strSQL := v_strSQL || ', fam.prix_' || i || '=nvl(t.prix_' || i ||
                  ' ,fam.prix_' || i || ')';
    end loop;
    for i in 2 .. v_nUoMCount loop
      v_strSQL := v_strSQL || ', fam.unite_' || i || '=nvl(t.unite_' || i ||
                  ',fam.unite_' || i || ' )';
    end loop;
    --when not matched
    v_strSQL := v_strSQL || ' where fam.id_fam = ' || v_nProductID ||
                ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (fam.fam_em_addr, fam.id_fam, fam.f_cle,fam0_em_addr,user_create_fam,date_create_fam';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', fam.f_desc_court';
      when 2 then
        v_strSQL := v_strSQL || ', fam.f_desc';
        v_strSQL := v_strSQL || ', fam.f_desc_court';
      when -1 then
        v_strSQL := v_strSQL || ', fam.f_desc';
      else
        null;
    end case;
    for i in 1 .. v_nPriceCount loop
      v_strSQL := v_strSQL || ', fam.prix_' || i;
    end loop;
    for i in 2 .. v_nUoMCount loop
      v_strSQL := v_strSQL || ', fam.unite_' || i;
    end loop;
    v_strSQL := v_strSQL || ') ';
    --value
    v_strSQL := v_strSQL || 'values (seq_fam.nextval,' || v_nProductID ||
                ', t.key, t.parent_addr ' || ',''' || VFMUSER || ''',''' ||
                vnowDate || '''';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', t.short_desc';
      when 2 then
        v_strSQL := v_strSQL || ', t.description';
        v_strSQL := v_strSQL || ', t.short_desc';
      when -1 then
        v_strSQL := v_strSQL || ',  t.description';
      else
        null;
    end case;
    for i in 1 .. v_nPriceCount loop
      v_strSQL := v_strSQL || ', t.prix_' || i;
    end loop;
    for i in 2 .. v_nUoMCount loop
      v_strSQL := v_strSQL || ', t.unite_' || i;
    end loop;
    v_strSQL := v_strSQL || ') ';
    --add log
    Fmp_Log.logInfo(pIn_cSqlText => v_strSQL);
    --execute
    --execute immediate v_strSQL;
    fmp_log.LOGDEBUG(pIn_cSqlText =>v_strSQL,pIn_vText => 3 );
    fmsp_execsql(v_strSQL);

    --end of step 3

    --step 4: create relationship between product/product group and relative things
    /* --delete old relationship
    v_strSQL := 'delete from famtrf t where t.fam33_em_addr in (select distinct b.fam_em_addr from ' ||
                p_strTableName || ' a left join fam b on b.f_cle=a.key)';
    --execute
    execute immediate v_strSQL;
    v_strSQL := 'delete from rfc t where t.fam7_em_addr in (select distinct b.fam_em_addr from ' ||
                p_strTableName || ' a left join fam b on b.f_cle=a.key)';
    --execute
    execute immediate v_strSQL;
    v_strSQL := 'delete from nmc t where t.nmc_field=83 and t.fam1_em_addr in (select distinct b.fam_em_addr from ' ||
                p_strTableName || ' a left join fam b on b.f_cle=a.key)';
    --execute
    execute immediate v_strSQL;*/

    --coefficient of continuation of
    if p_oOptions.bSuite then
      --merge into
      v_strSQL := 'merge into nmc';
      --using
      v_strSQL := v_strSQL ||
                  ' using (select t1.key, t1.suite, t1.suite_coef, a.fam_em_addr as parent_adr, b.fam_em_addr as child_adr from ' ||
                  p_strTableName ||
                  ' t1 left join fam a on a.f_cle=t1.key
                       left join fam b on nvl(b.f_cle,'')FM$('')=nvl(t1.suite,'')FM$('')
                                      and a.id_fam=b.id_fam) t';
      --on
      v_strSQL := v_strSQL || ' on (nmc.nmc_field = ' ||
                  p_constant.NMCID_FAM_CONTINUE ||
                  ' and nmc.fam1_em_addr=t.parent_adr )';
      --when matched
      v_strSQL := v_strSQL || ' when matched then';
      --update
      v_strSQL := v_strSQL ||---nmc.pere_pro_nmc=nvl2(t.child_adr, t.parent_adr, null),
                             -- nmc.fils_pro_nmc=nvl2(t.parent_adr, t.child_adr, null),
                  ' update set nmc.pere_pro_nmc = nvl2(t.child_adr,nmc.pere_pro_nmc, t.parent_adr),
                               nmc.fils_pro_nmc = nvl2(t.parent_adr, nmc.fils_pro_nmc, t.child_adr),   
                               nmc.qute=t.suite_coef';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (nmc.nmc_em_addr, nmc.nmc_field, nmc.fam1_em_addr, nmc.pere_pro_nmc, nmc.fils_pro_nmc, nmc.qute)';
      v_strSQL := v_strSQL || ' values (seq_nmc.nextval, ' ||
                  p_constant.NMCID_FAM_CONTINUE ||
                  ', t.parent_adr, nvl2(t.child_adr, t.parent_adr, null), nvl2(t.parent_adr, t.child_adr, null), t.suite_coef)
                  where t.suite_coef is not null or
    (t.parent_adr is not null and t.child_adr is not null)';
      --execute
      fmsp_execsql(v_strSQL);
      --execute immediate v_strSQL;
      --add log
      
      Fmp_Log.logInfo(pIn_cSqlText => v_strSQL,pIn_vText => 6);
    end if;

    --price table
    if p_oOptions.bTab then
      for i in 0 .. v_nPriceCount - 1 loop

        --merge into
        v_strSQL := 'merge into famtrf';
        --using
        v_strSQL := v_strSQL ||
                    ' using (select t1.key, a.fam_em_addr, b.trf_em_addr from ' ||
                    p_strTableName ||
                    ' t1 left join fam a on a.f_cle=t1.key left join trf b on b.trf_cle=t1.key_prix_' ||
                    (i + 1) || ' where t1.key_prix_' || (i + 1) ||
                    ' is not null) t';
        --on
        v_strSQL := v_strSQL || ' on (famtrf.num_trf = ' || i ||
                    ' and famtrf.fam33_em_addr=t.fam_em_addr and famtrf.trf34_em_addr=t.trf_em_addr)';
        --when not matched
        v_strSQL := v_strSQL || ' when not matched then';
        --insert
        v_strSQL := v_strSQL ||
                    ' insert (famtrf.famtrf_em_addr, famtrf.num_trf, famtrf.fam33_em_addr, famtrf.trf34_em_addr)';
        v_strSQL := v_strSQL || ' values (seq_famtrf.nextval,' || i ||
                    ',t.fam_em_addr, t.trf_em_addr)';
        --execute
        -- execute immediate v_strSQL;
        fmsp_execsql(v_strSQL);
        --add log
        Fmp_Log.logInfo(pIn_cSqlText => v_strSQL);
      end loop;
    end if;

    --value unit

    --merge into
    v_strSQL := 'merge into rfc';
    --using
    v_strSQL := v_strSQL ||
                ' using (select t1.key, a.fam_em_addr, b.vct_em_addr from ' ||
                p_strTableName ||
                ' t1 left join fam a on a.f_cle=t1.key left join vct b on (b.val=t1.Value_Unit and b.id_crt=' ||
                p_constant.v_ProductData || ' and b.num_crt=' ||
                p_constant.NumberForVU ||
                ') where t1.Value_Unit is not null) t';
    --on
    v_strSQL := v_strSQL || ' on (rfc.ident_crt = ' || p_constant.v_RFC_P ||
                ' and rfc.numero_crt=' || p_constant.NumberForVU ||
                ' and rfc.fam7_em_addr=t.fam_em_addr )'; --and rfc.vct10_em_addr=t.vct_em_addr
    --when matchen
    v_strSQL := v_strSQL || ' when matched then ';
    v_strSQL := v_strSQL || ' update set rfc.vct10_em_addr = t.vct_em_addr ';  
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (rfc.rfc_em_addr, rfc.ident_crt, rfc.numero_crt, rfc.fam7_em_addr, rfc.vct10_em_addr)';
    v_strSQL := v_strSQL || ' values (seq_rfc.nextval,' ||
                p_constant.v_RFC_P || ',' || p_constant.NumberForVU ||
                ',t.fam_em_addr, t.vct_em_addr)';
    --execute
    --execute immediate v_strSQL;
    fmsp_execsql(v_strSQL);
    --add log
    Fmp_Log.logInfo(pIn_cSqlText => v_strSQL);

    --parent value unit
    v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
         select seq_rfc.nextval,
                m2.ident_crt,
                m2.numero_crt,
                m1.fam_em_addr,
                m2.vct10_em_addr
         from fam m1,
              rfc m2,TMP_KEYS t1
         where m1.FAM0_EM_ADDR=m2.fam7_em_addr
         and m1.id_fam=80
         and m2.ident_crt=';
    v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
    v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=' ||
                    p_constant.NumberForVU;
    v_strTempSQL := v_strTempSQL || ' and m1.f_cle = t1.key ';
    Fmp_Log.logInfo(pIn_cSqlText => v_strTempSQL);
    fmsp_execsql('truncate table TMP_KEYS');
    v_strSQL := 'insert into TMP_KEYS select t1.key from ' ||
                p_strTableName || ' t1 where  t1.Value_Unit is null';
    fmsp_execsql(v_strSQL);

    /* v_strTempSQL := v_strTempSQL ||
                    '  and  exists (select t1.key from ';
    v_strTempSQL := v_strTempSQL || p_strTableName;
    v_strTempSQL := v_strTempSQL || ' t1 where m1.f_cle = t1.key  and t1.Value_Unit is null)';*/

    --execute
    --execute immediate v_strTempSQL;
    fmsp_execsql(v_strTempSQL);

    --UOM
    for i in 1 .. v_nUoMCount loop
      --merge into
      v_strSQL := 'merge into rfc';
      --using
      v_strSQL := v_strSQL ||
                  ' using (select t1.key, a.fam_em_addr, b.vct_em_addr from ' ||
                  p_strTableName ||
                  ' t1 left join fam a on a.f_cle=t1.key left join vct b on (b.val=t1.key_unite_' || i ||
                  ' and b.id_crt=' || p_constant.v_ProductData ||
                  ' and b.num_crt=' || p_constant.NumberForUOM ||
                  ') where t1.key_unite_' || i || ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (rfc.ident_crt = ' || p_constant.v_RFC_P ||
                  ' and rfc.numero_crt=' ||
                  (p_constant.NumberForUOM + i - 1) ||
                  ' and rfc.fam7_em_addr=t.fam_em_addr)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (rfc.rfc_em_addr, rfc.ident_crt, rfc.numero_crt, rfc.fam7_em_addr, rfc.vct10_em_addr)';
      v_strSQL := v_strSQL || ' values (seq_rfc.nextval,' ||
                  p_constant.v_RFC_P || ',' ||
                  (p_constant.NumberForUOM + i - 1) ||
                  ',t.fam_em_addr, t.vct_em_addr)';
      v_strSQL := v_strSQL ||
                  ' when matched then  update set rfc.vct10_em_addr=t.vct_em_addr';

      fmsp_execsql(v_strSQL);
    end loop;

    --parent UOM
    v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
        select seq_rfc.nextval,
               m2.ident_crt,
               m2.numero_crt,
               m1.fam_em_addr,
               m2.vct10_em_addr
        from fam m1,
             rfc m2
        where m1.FAM0_EM_ADDR=m2.fam7_em_addr
        and m1.id_fam=80
        and m2.ident_crt=';
    v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
    v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=' ||
                    p_constant.NumberForUOM;
    v_strTempSQL := v_strTempSQL || '  and  exists  (select t1.key from ';
    v_strTempSQL := v_strTempSQL || p_strTableName;
    v_strTempSQL := v_strTempSQL ||
                    ' t1 where m1.f_cle = t1.key  and t1.key_unite_1 is null)';

    --execute
    -- execute immediate v_strTempSQL;
    fmsp_execsql(v_strTempSQL);

    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into rfc';
      --using
      v_strSQL := v_strSQL ||
                  ' using (select t1.key, a.fam_em_addr, b.vct_em_addr from ' ||
                  p_strTableName ||
                  ' t1 left join fam a on a.f_cle=t1.key left join vct b on (b.val=t1.attr_' || i ||
                  ' and b.id_crt=' || p_constant.v_ProductData ||
                  ' and b.num_crt=' || (p_constant.BaseNumberOfAttr + i) ||
                  ') where t1.attr_' || i || ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (rfc.ident_crt = ' || p_constant.v_RFC_P ||
                  ' and rfc.numero_crt=' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ' and rfc.fam7_em_addr=t.fam_em_addr)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (rfc.rfc_em_addr, rfc.ident_crt, rfc.numero_crt, rfc.fam7_em_addr, rfc.vct10_em_addr)';
      v_strSQL := v_strSQL || ' values (seq_rfc.nextval,' ||
                  p_constant.v_RFC_P || ',' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ',t.fam_em_addr, t.vct_em_addr)';
      v_strSQL := v_strSQL ||
                  ' when matched then  update set rfc.vct10_em_addr=t.vct_em_addr';

      fmsp_execsql(v_strSQL);
    end loop;
    --end of step 4
    --attribute is null
    for i in 1 .. p_nAttrCount loop
      v_nPeried    := 48 + i;
      v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
      select seq_rfc.nextval,
             m2.ident_crt,
             m2.numero_crt,
             m1.fam_em_addr,
             m2.vct10_em_addr
      from fam m1,
           rfc m2
      where m1.FAM0_EM_ADDR=m2.fam7_em_addr
      and m1.id_fam=80
      and m2.ident_crt=';
      v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
      v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=';
      v_strTempSQL := v_strTempSQL || v_nPeried ;

      fmsp_execsql('truncate table TMP_KEYS');
      v_strSQL :='insert into TMP_KEYS select t1.key from '||p_strTableName||' t1 where t1.attr_' || i ||' is null';
      fmsp_execsql(v_strSQL);
      v_strTempSQL := v_strTempSQL||' and exists(select 1 from TMP_KEYS t1 where m1.f_cle = t1.key )';
      fmsp_execsql(v_strTempSQL);

     /* v_strTempSQL := v_strTempSQL || '  and  exists (select t1.key from ';
      v_strTempSQL := v_strTempSQL || p_strTableName;
      v_strTempSQL := v_strTempSQL ||
                      ' t1 where m1.f_cle = t1.key  and t1.attr_' || i ||
                      ' is null)';*/

      --execute
      --execute immediate v_strTempSQL;
    end loop;

    if p_oOptions.bAttributeInherit then
      if p_bProductGroup then
        for i in 1 .. p_nAttrCount loop
          v_nPeried    := 48 + i;
          v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
              select seq_rfc.nextval,
                     m2.ident_crt,
                     m2.numero_crt,
                     m1.fam_em_addr,
                     m2.vct10_em_addr
              from fam m1,
                   rfc m2
              where m1.FAM0_EM_ADDR=m2.fam7_em_addr
              and m1.id_fam=70
              and m2.ident_crt=';
          v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
          v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=';
          v_strTempSQL := v_strTempSQL || v_nPeried;
          v_strTempSQL := v_strTempSQL ||
                          '  and m1.f_cle in (select t1.key from ';
          v_strTempSQL := v_strTempSQL || p_strTableName;
          v_strTempSQL := v_strTempSQL || ' t1 where t1.attr_' || i ||
                          ' is null)';
          execute immediate v_strTempSQL;
        end loop;

        --product group parent value unit
        v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
        select seq_rfc.nextval,
               m2.ident_crt,
               m2.numero_crt,
               m1.fam_em_addr,
               m2.vct10_em_addr
        from fam m1,
             rfc m2
        where m1.FAM0_EM_ADDR=m2.fam7_em_addr
        and m1.id_fam=70
        and m2.ident_crt=';
        v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
        v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=' ||
                        p_constant.NumberForVU;
        v_strTempSQL := v_strTempSQL ||
                        '  and m1.f_cle in (select t1.key from ';
        v_strTempSQL := v_strTempSQL || p_strTableName;
        v_strTempSQL := v_strTempSQL || ' t1 where t1.Value_Unit is null)';

        --execute
        execute immediate v_strTempSQL;

        --parent UOM
        v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,fam7_em_addr,vct10_em_addr)
        select seq_rfc.nextval,
               m2.ident_crt,
               m2.numero_crt,
               m1.fam_em_addr,
               m2.vct10_em_addr
        from fam m1,
             rfc m2
        where m1.FAM0_EM_ADDR=m2.fam7_em_addr
        and m1.id_fam=70
        and m2.ident_crt=';
        v_strTempSQL := v_strTempSQL || p_constant.v_RFC_P;
        v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=' ||
                        p_constant.NumberForUOM;
        v_strTempSQL := v_strTempSQL ||
                        '  and m1.f_cle in (select t1.key from ';
        v_strTempSQL := v_strTempSQL || p_strTableName;
        v_strTempSQL := v_strTempSQL || ' t1 where t1.key_unite_1 is null)';

        --execute
        execute immediate v_strTempSQL;

      end if;
    end if;
    --this is modify fam data_modify_fam
    update fam f set f.date_modify_fam = vnowDate where f.fam_em_addr =1;
    Fmp_Log.LOGEND;
  exception
    when others then

      p_nSqlCode := sqlcode;
      raise;
  end sp_ImportProduct;

  --import sales territory
  procedure sp_ImportST(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        p_nAttrCount   in integer,
                        p_strTableName in varchar2,
                        p_strAutoDesc  in varchar2,
                        pIn_vFMUser    in varchar2 default null,
                        p_nSqlCode     out integer) is
    v_strSQL      varchar2(32767) := '';
    v_strTempSQL  varchar2(32767) := '';
    v_nPriceCount integer := 1;

    v_nRootAddr integer := 0;
    VAutoDesc   varchar(60) := '';
    vnowDate    number;
    VFMUSER     varchar(60) := '';
    v_nPeried   number;
  begin
    p_nSqlCode := 0;
    --==============wfq 2013.2.21=================================
    VFMUSER  := pIn_vFMUser;
    vnowDate := F_ConvertDateToOleDateTime(sysdate);
    --p_strAutoDesc=created automatically

    VAutoDesc := p_strAutoDesc;

    if p_oOptions.bPrix then
      v_nPriceCount := 6;
    else
      v_nPriceCount := 1;
    end if;

    --step 1:
    --delete duplicate sales territory from temporary table
    v_strSQL := 'delete from ' || p_strTableName ||
                ' a where a.rowid<(select max(b.rowid) from ' ||
                p_strTableName || ' b where a.key=b.key)';
    --execute
    execute immediate v_strSQL;
    --Get root node addr, create it if it doesn't exist
    select count(*) into v_nRootAddr from geo where geo.g_cle = chr(1);
    if v_nRootAddr = 0 then
      insert into geo
        (geo.geo_em_addr, geo.g_cle)
      values
        (seq_geo.nextval, chr(1));
    end if;
    select geo.geo_em_addr
      into v_nRootAddr
      from geo
     where ascii(geo.g_cle) = 1;
    --end of step 1

    --step 2: create all relative things that don't exist in database
    --parent
    v_strSQL := 'update ' || p_strTableName ||
                ' t set t.parent = chr(1) where t.parent is null';
    execute immediate v_strSQL;
    --merge into
    v_strSQL := 'merge into geo';
    --using
    v_strSQL := v_strSQL || ' using (select distinct t1.parent from ' ||
                p_strTableName || ' t1 where t1.parent is not null) t';
    --on
    v_strSQL := v_strSQL || ' on (geo.g_cle = t.parent)';
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (geo.geo_em_addr, geo.g_cle, geo.geo1_em_addr, geo.g_desc,user_create_geo,date_create_geo)
                values (seq_geo.nextval, t.parent,' ||
                v_nRootAddr || ' ,''' || VAutoDesc || ''',''' || VFMUSER ||
                ''',''' || vnowDate || ''')';
    --execute
    fmp_log.LOGDEBUG(pIn_cSqlText => v_strSQL,pIn_vText => 0 );
    execute immediate v_strSQL;

    --price table
    if p_oOptions.bTab then
      for i in 1 .. v_nPriceCount loop
        v_strSQL := 'merge into dvs';
        --using
        v_strSQL := v_strSQL || ' using (select distinct t1.key_prix_' || i ||
                    ' from ' || p_strTableName || ' t1 where t1.key_prix_' || i ||
                    ' is not null) t';
        --on
        v_strSQL := v_strSQL || ' on (dvs.dvs_cle=t.key_prix_' || i || ')';
        --when not matched
        v_strSQL := v_strSQL || ' when not matched then';
        --insert
        v_strSQL := v_strSQL ||
                    ' insert (dvs.dvs_em_addr, dvs.dvs_cle,user_create_dvs,date_create_dvs)
                    values (seq_dvs.nextval, t.key_prix_' || i ||
                    ',''' || VFMUSER || ''',''' || vnowDate || ''')';
        --execute
        execute immediate v_strSQL;
      end loop;
    end if;

    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into vct';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.attr_' || i ||
                  ' from ' || p_strTableName || ' t1 where t1.attr_' || i ||
                  ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (vct.id_crt = ' || p_constant.v_STData ||
                  ' and vct.num_crt = ' ||
                  (i + p_constant.BaseNumberOfAttr) ||
                  ' and vct.val=t.attr_' || i || ')';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val)';
      v_strSQL := v_strSQL || ' values (seq_vct.nextval,' ||
                  p_constant.v_STData || ',' ||
                  (i + p_constant.BaseNumberOfAttr) || ',t.attr_' || i;
      v_strSQL := v_strSQL || ')';
      --execute
      execute immediate v_strSQL;
    end loop;
    --end of step 2

    --step 3: create or update sales territory
    --merge into
    v_strSQL := 'merge into geo';
    --using
    v_strSQL := v_strSQL ||
                ' using (select a.*, b.geo_em_addr as parent_addr from ' ||
                p_strTableName ||
                ' a left join geo b on nvl(b.g_cle,'')FM$('')=nvl(a.parent,'')FM$('') where a.key is not null  ) t';--and  a.parent != chr(1)
    --on
    v_strSQL := v_strSQL || ' on (geo.g_cle = t.key)';
    --when matched
    v_strSQL := v_strSQL || ' when matched then';
    --update
    v_strSQL := v_strSQL || ' update set geo.geo1_em_addr=t.parent_addr ,geo.date_modify_geo='||vnowDate||',geo.user_modify_geo='''||VFMUSER||'''' ;
    fmp_log.LOGDEBUG(pIn_cSqlText => v_strSQL);
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL ||
                    ' ,geo.g_desc_court=nvl(t.short_desc,case when geo.g_desc_court=''' ||
                    VAutoDesc || ''' then null else geo.g_desc_court end)';
      when 2 then
        v_strSQL := v_strSQL ||
                    ' ,geo.g_desc=nvl(t.description,case when geo.g_desc=''' ||
                    VAutoDesc || ''' then null else geo.g_desc end)';
        v_strSQL := v_strSQL ||
                    ' ,geo.g_desc_court=nvl(t.short_desc,case when geo.g_desc_court=''' ||
                    VAutoDesc || ''' then null else geo.g_desc_court end)';
      when -1 then
        v_strSQL := v_strSQL ||
                    ' ,geo.g_desc=nvl(t.description,case when geo.g_desc=''' ||
                    VAutoDesc || ''' then null else geo.g_desc end)';
      else
        null;
    end case;
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (geo.geo_em_addr, geo.g_cle, geo1_em_addr,user_create_geo,date_create_geo';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', geo.g_desc_court';
      when 2 then
        v_strSQL := v_strSQL || ', geo.g_desc';
        v_strSQL := v_strSQL || ', geo.g_desc_court';
      when -1 then
        v_strSQL := v_strSQL || ', geo.g_desc';
      else
        null;
    end case;
    v_strSQL := v_strSQL || ') ';

    v_strSQL := v_strSQL || 'values (seq_geo.nextval, t.key, t.parent_addr' ||
                ',''' || VFMUSER || ''',''' || vnowDate || '''';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', t.short_desc';
      when 2 then
        v_strSQL := v_strSQL || ',  t.description';
        v_strSQL := v_strSQL || ', t.short_desc';
      when -1 then
        v_strSQL := v_strSQL || ', t.description';
      else
        null;
    end case;
    v_strSQL := v_strSQL || ') ';
    --execute
    fmp_log.LOGDEBUG(pIn_cSqlText =>v_strSQL ,pIn_vText => 1);
    execute immediate v_strSQL;
    --end of step 3

    --step 4: create relationship between sales territory and relative things
    --delete old relationship
    /*    v_strSQL := 'delete from geodvs t where t.geo39_em_addr in (select distinct b.geo_em_addr from ' ||
    p_strTableName || ' a left join geo b on b.g_cle=a.key)';*/
    --execute
    --execute immediate v_strSQL;
    /*    v_strSQL := 'delete from rfc t where t.geo8_em_addr in (select distinct b.geo_em_addr from ' ||
                p_strTableName || ' a left join geo b on b.g_cle=a.key)';
    --execute
    execute immediate v_strSQL;*/

    --price table
    if p_oOptions.bTab then
      for i in 1 .. v_nPriceCount loop
        --merge into
        v_strSQL := 'merge into geodvs';
        --using
        v_strSQL := v_strSQL ||
                    ' using (select t1.key, a.geo_em_addr, b.dvs_em_addr from ' ||
                    p_strTableName ||
                    ' t1 left join geo a on a.g_cle=t1.key left join dvs b on b.dvs_cle=t1.key_prix_' || i ||
                    ' where t1.key_prix_' || i || ' is not null) t';
        --on
        v_strSQL := v_strSQL ||
                    ' on (geodvs.n0_dvs = 0 and geodvs.geo39_em_addr=t.geo_em_addr)';
        --when not matched
        v_strSQL := v_strSQL || ' when not matched then';
        --insert
        v_strSQL := v_strSQL ||
                    ' insert (geodvs.geodvs_em_addr, geodvs.n0_dvs, geodvs.geo39_em_addr, geodvs.dvs38_em_addr)';
        v_strSQL := v_strSQL ||
                    ' values (seq_geodvs.nextval, 0, t.geo_em_addr, t.dvs_em_addr)';

        v_strSQL := v_strSQL ||
                    ' when matched then update  set geodvs.dvs38_em_addr=t.dvs_em_addr';
        --execute
        execute immediate v_strSQL;
      end loop;
    end if;
    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into rfc';
      --using
      v_strSQL := v_strSQL ||
                  ' using (select t1.key, a.geo_em_addr, b.vct_em_addr from ' ||
                  p_strTableName ||
                  ' t1 left join geo a on a.g_cle=t1.key left join vct b on (b.val=t1.attr_' || i ||
                  ' and b.id_crt=' || p_constant.v_STData ||
                  ' and b.num_crt=' || (p_constant.BaseNumberOfAttr + i) ||
                  ') where t1.attr_' || i || ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (rfc.ident_crt = ' ||
                  p_constant.v_RFC_ST || ' and rfc.numero_crt=' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ' and rfc.geo8_em_addr=t.geo_em_addr)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (rfc.rfc_em_addr, rfc.ident_crt, rfc.numero_crt, rfc.geo8_em_addr, rfc.vct10_em_addr)';
      v_strSQL := v_strSQL || ' values (seq_rfc.nextval,' ||
                  p_constant.v_RFC_ST || ',' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ',t.geo_em_addr, t.vct_em_addr)';
      v_strSQL := v_strSQL ||
                  ' when matched then update set rfc.vct10_em_addr=t.vct_em_addr';
      --execute
      execute immediate v_strSQL;

    end loop;

    --end of step 4

    if p_oOptions.bAttributeInherit then
      --no attribute
      for i in 1 .. p_nAttrCount loop
        v_nPeried    := 48 + i;
        v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,Geo8_em_addr,vct10_em_addr)
      select seq_rfc.nextval,
             m2.ident_crt,
             m2.numero_crt,
             m1.geo_em_addr,
             m2.vct10_em_addr
      from geo m1,
           rfc m2
      where m1.GEO1_EM_ADDR=m2.geo8_em_addr
      and m2.ident_crt=';
        v_strTempSQL := v_strTempSQL || p_constant.v_RFC_ST;
        v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=';
        v_strTempSQL := v_strTempSQL || v_nPeried;
        v_strTempSQL := v_strTempSQL ||
                        '  and m1.g_cle in (select t1.key from ';
        v_strTempSQL := v_strTempSQL || p_strTableName;
        v_strTempSQL := v_strTempSQL || ' t1 where t1.attr_' || i ||
                        ' is  null)';

        --execute
        execute immediate v_strTempSQL;
      end loop;

    end if;
    -- this is modify geo table col date_modify_geo.
    update geo g set g.date_modify_geo = vnowDate where g.geo_em_addr = 1;
  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end;

  --import trade channel
  procedure sp_ImportTC(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                        p_nAttrCount   in integer,
                        p_strTableName in varchar2,
                        p_strAutoDesc  in varchar2,
                        pIn_vFMUser    in varchar2 default null,
                        p_nSqlCode     out integer) is
    v_strSQL     varchar2(32767) := '';
    v_strTempSQL varchar2(32767) := '';
    v_nRootAddr  integer := 0;

    VAutoDesc varchar(60) := '';
    vnowDate  number;
    VFMUSER   varchar2(60) := '';
    v_nPeried number;
  begin
    p_nSqlCode := 0;
    --==============wfq 2013.2.21=================================
    VFMUSER  := pIn_vFMUser;
    vnowDate := F_ConvertDateToOleDateTime(sysdate);
    --p_strAutoDesc=created automatically

    VAutoDesc := p_strAutoDesc;

    --step 1:
    --delete duplicate sales territory from temporary table
    v_strSQL := 'delete from ' || p_strTableName ||
                ' a where a.rowid<(select max(b.rowid) from ' ||
                p_strTableName || ' b where a.key=b.key)';
    --execute
    execute immediate v_strSQL;
    --Get root node addr, create it if it doesn't exist
    select count(*) into v_nRootAddr from dis where dis.d_cle = chr(1);
    if v_nRootAddr = 0 then
      insert into dis
        (dis.dis_em_addr, dis.d_cle)
      values
        (seq_dis.nextval, chr(1));
    end if;
    select dis.dis_em_addr
      into v_nRootAddr
      from dis
     where ascii(dis.d_cle) = 1;
    --end of step 1

    --step 2: create all relative things that don't exist in database
    --parent
    v_strSQL := 'update ' || p_strTableName ||
                ' t set t.parent = chr(1) where t.parent is null';
    execute immediate v_strSQL;
    --merge into
    v_strSQL := 'merge into dis';
    --using
    v_strSQL := v_strSQL || ' using (select distinct t1.parent from ' ||
                p_strTableName || ' t1 where t1.parent is not null) t';
    --on
    v_strSQL := v_strSQL || ' on (dis.d_cle = t.parent)';
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (dis.dis_em_addr, dis.d_cle, dis.dis2_em_addr, dis.d_desc,user_create_dis,date_create_dis)
                values (seq_dis.nextval, t.parent,' ||
                v_nRootAddr || ' ,''' || VAutoDesc || ''',''' || VFMUSER ||
                ''',''' || vnowDate || ''')';
    --execute
    execute immediate v_strSQL;

    --price table
    if p_oOptions.bTab then
      v_strSQL := 'merge into rms';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.key_prix_1 from ' ||
                  p_strTableName ||
                  ' t1 where t1.key_prix_1 is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (rms.rms_cle=t.key_prix_1)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (rms.rms_em_addr, rms.rms_cle) values (seq_rms.nextval, t.key_prix_1)';
      --execute
      execute immediate v_strSQL;
    end if;

    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into vct';
      --using
      v_strSQL := v_strSQL || ' using (select distinct t1.attr_' || i ||
                  ' from ' || p_strTableName || ' t1 where t1.attr_' || i ||
                  ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (vct.id_crt = ' || p_constant.v_TCData ||
                  ' and vct.num_crt = ' ||
                  (i + p_constant.BaseNumberOfAttr) ||
                  ' and vct.val=t.attr_' || i || ')';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val)';
      v_strSQL := v_strSQL || ' values (seq_vct.nextval,' ||
                  p_constant.v_TCData || ',' ||
                  (i + p_constant.BaseNumberOfAttr) || ',t.attr_' || i;
      v_strSQL := v_strSQL || ')';
      --execute
      execute immediate v_strSQL;
    end loop;
    --end of step 2

    --step 3: create or update sales territory
    --merge into
    v_strSQL := 'merge into dis';
    --using
    v_strSQL := v_strSQL ||
                ' using (select a.*, b.dis_em_addr as parent_addr';
    if p_oOptions.bTab then
      v_strSQL := v_strSQL || ', c.rms_em_addr as prix_addr';
    end if;
    v_strSQL := v_strSQL || ' from ' || p_strTableName ||
                ' a left join dis b on nvl(b.d_cle,'')FM$('')=nvl(a.parent,'')FM$('')';
    if p_oOptions.bTab then
      v_strSQL := v_strSQL || ' left join rms c on c.rms_cle=a.key_prix_1';
    end if;
    v_strSQL := v_strSQL ||
                ' where a.key is not null  ) t';--and a.parent !=chr(1)
    --on
    v_strSQL := v_strSQL || ' on (dis.d_cle = t.key)';
    --when matched
    v_strSQL := v_strSQL || ' when matched then';
    --update
    v_strSQL := v_strSQL || ' update set dis.dis2_em_addr=t.parent_addr,dis.date_modify_dis='||vnowDate||',dis.user_modify_dis='''||VFMUSER||'''';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL ||
                    ' ,dis.d_desc_court=nvl(t.short_desc,case when dis.d_desc_court=''' ||
                    VAutoDesc || ''' then null else dis.d_desc_court end)';
      when 2 then
        v_strSQL := v_strSQL ||
                    ' ,dis.d_desc=nvl(t.description,case when dis.d_desc=''' ||
                    VAutoDesc || ''' then null else dis.d_desc end)';
        v_strSQL := v_strSQL ||
                    ' ,dis.d_desc_court=nvl(t.short_desc,case when dis.d_desc_court=''' ||
                    VAutoDesc || ''' then null else dis.d_desc_court end)';
      when -1 then
        v_strSQL := v_strSQL ||
                    ' ,dis.d_desc=nvl(t.description,case when dis.d_desc=''' ||
                    VAutoDesc || ''' then null else dis.d_desc end)';
      else
        null;
    end case;
    if p_oOptions.bTab then
      v_strSQL := v_strSQL || ' ,dis.rms40_em_addr=t.prix_addr';
    end if;
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (dis.dis_em_addr, dis.d_cle, dis2_em_addr,user_create_dis,date_create_dis';
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', dis.d_desc_court';
      when 2 then
        v_strSQL := v_strSQL || ', dis.d_desc';
        v_strSQL := v_strSQL || ', dis.d_desc_court';
      when -1 then
        v_strSQL := v_strSQL || ', dis.d_desc';
      else
        null;
    end case;
    if p_oOptions.bTab then
      v_strSQL := v_strSQL || ', dis.rms40_em_addr';
    end if;
    v_strSQL := v_strSQL || ') ';

    v_strSQL := v_strSQL || 'values (seq_dis.nextval, t.key, t.parent_addr' ||
                ',''' || VFMUSER || ''',''' || vnowDate || '''';

    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', t.short_desc';
      when 2 then
        v_strSQL := v_strSQL || ',  t.description';
        v_strSQL := v_strSQL || ', t.short_desc';
      when -1 then
        v_strSQL := v_strSQL || ',  t.description';
      else
        null;
    end case;
    if p_oOptions.bTab then
      v_strSQL := v_strSQL || ', t.prix_addr';
    end if;
    v_strSQL := v_strSQL || ') ';
    --execute
    execute immediate v_strSQL;
    --end of step 3

    --step 4: create relationship between sales territory and relative things
    --delete old relationship
    /*    v_strSQL := 'delete from rfc t where t.dis9_em_addr in (select distinct b.dis_em_addr from ' ||
                p_strTableName || ' a left join dis b on b.d_cle=a.key)';
    --execute
    execute immediate v_strSQL;*/

    --attribute
    for i in 1 .. p_nAttrCount loop
      --merge into
      v_strSQL := 'merge into rfc';
      --using
      v_strSQL := v_strSQL ||
                  ' using (select t1.key, a.dis_em_addr, b.vct_em_addr from ' ||
                  p_strTableName ||
                  ' t1 left join dis a on a.d_cle=t1.key left join vct b on (b.val=t1.attr_' || i ||
                  ' and b.id_crt=' || p_constant.v_TCData ||
                  ' and b.num_crt=' || (p_constant.BaseNumberOfAttr + i) ||
                  ') where t1.attr_' || i || ' is not null) t';
      --on
      v_strSQL := v_strSQL || ' on (rfc.ident_crt = ' ||
                  p_constant.v_RFC_TC || ' and rfc.numero_crt=' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ' and rfc.dis9_em_addr=t.dis_em_addr)';
      --when not matched
      v_strSQL := v_strSQL || ' when not matched then';
      --insert
      v_strSQL := v_strSQL ||
                  ' insert (rfc.rfc_em_addr, rfc.ident_crt, rfc.numero_crt, rfc.dis9_em_addr, rfc.vct10_em_addr)';
      v_strSQL := v_strSQL || ' values (seq_rfc.nextval,' ||
                  p_constant.v_RFC_TC || ',' ||
                  (p_constant.BaseNumberOfAttr + i) ||
                  ',t.dis_em_addr, t.vct_em_addr)';
      v_strSQL := v_strSQL ||
                  ' when matched then update  set rfc.vct10_em_addr=t.vct_em_addr';
      --execute
      execute immediate v_strSQL;
    end loop;
    --end of step 4

    if p_oOptions.bAttributeInherit then
      for i in 1 .. p_nAttrCount loop
        v_nPeried    := 48 + i;
        v_strTempSQL := 'insert into rfc(rfc_em_addr,ident_crt,numero_crt,DIS9_EM_ADDR,vct10_em_addr)
      select seq_rfc.nextval,
             m2.ident_crt,
             m2.numero_crt,
             m1.dis_em_addr,
             m2.vct10_em_addr
      from dis m1,
           rfc m2
      where m1.DIS2_EM_ADDR=m2.DIS9_EM_ADDR
      and m2.ident_crt=';
        v_strTempSQL := v_strTempSQL || p_constant.v_RFC_TC;
        v_strTempSQL := v_strTempSQL || '  and m2.numero_crt=';
        v_strTempSQL := v_strTempSQL || v_nPeried;
        v_strTempSQL := v_strTempSQL ||
                        '  and m1.D_CLE in (select t1.key from ';
        v_strTempSQL := v_strTempSQL || p_strTableName;
        v_strTempSQL := v_strTempSQL || ' t1 where t1.attr_' || i ||
                        ' is  null)';
      end loop;
    end if;
    -- this is update dis table col date_modify_dis.
    update dis d set d.date_modify_dis = vnowDate where d.dis_em_addr = 1;
  exception
    when others then
      Fmp_Log.LOGERROR;
      p_nSqlCode := sqlcode;
      raise;
  end;

  --create temporary table for import product/product group
  procedure sp_CreateTmpTableForProduct(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                        p_nAttrCount   in integer,
                                        p_strTableName out varchar2,
                                        p_nSqlCode     in out integer) is
    v_strSQL      varchar2(32767) := '';
    v_nUoMCount   integer := 4;
    v_nPriceCount integer := 3;
  begin
    --preprocess according to switches
    if p_oOptions.bUM then
      if p_oOptions.nUM > 0 then
        v_nUoMCount := p_oOptions.nUM;
      else
        v_nUoMCount := 6;
      end if;
    else
      v_nUoMCount := 4;
    end if;

    if p_oOptions.bPrix then
      v_nPriceCount := 5;
    else
      v_nPriceCount := 3;
    end if;

    --select seq_tb_pimport.Nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename(); -- 'TB' || p_strTableName;
    --begin
    v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
    --LineNumber
    v_strSQL := v_strSQL || 'LineNumber number not null,';
    --key
    v_strSQL := v_strSQL || 'key varchar2(32) not null';
    --description and short description
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', short_desc varchar2(32)';
      when 2 then
        v_strSQL := v_strSQL || ', Description varchar2(64)';
        v_strSQL := v_strSQL || ', short_desc varchar2(32)';
      when -1 then
        v_strSQL := v_strSQL || ', Description varchar2(64)';
      else
        null;
    end case;
    --parent
    v_strSQL := v_strSQL || ', parent varchar2(32)';
    --continuation of and coefficient of continuation of
    if p_oOptions.bSuite then
      v_strSQL := v_strSQL || ', suite varchar2(32)';
      v_strSQL := v_strSQL || ', suite_coef float';
    end if;
    --value unit
    v_strSQL := v_strSQL || ', Value_Unit varchar2(32)';
    --price and price table 1-5
    for i in 1 .. v_nPriceCount loop
      v_strSQL := v_strSQL || ', prix_' || i || ' float';
      if p_oOptions.bTab then
        v_strSQL := v_strSQL || ', key_prix_' || i || ' varchar2(32)';
      end if;
    end loop;
    --UOM ratio and key 1-10
    v_strSQL := v_strSQL || ', key_unite_1 varchar2(32)';
    for i in 2 .. v_nUoMCount loop
      v_strSQL := v_strSQL || ', unite_' || i || ' float';
      v_strSQL := v_strSQL || ', key_unite_' || i || ' varchar2(32)';
    end loop;
    --attribute 1-19
    for i in 1 .. p_nAttrCount loop
      v_strSQL := v_strSQL || ', attr_' || i || ' varchar2(32)';
    end loop;
    --end
    v_strSQL := v_strSQL || ')';

    --execute
    execute immediate v_strSQL;
  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end;

  --create temporary table for import sales territory/trade channel
  procedure sp_CreateTmpTableForSTorTC(p_bIsTradeChannel in boolean,
                                       p_oOptions        in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                       p_nAttrCount      in integer,
                                       p_strTableName    out varchar2,
                                       p_nSqlCode        in out integer) is
    v_strSQL      varchar2(32767) := '';
    v_nPriceCount integer := 1;
  begin

    if p_oOptions.bPrix and p_bIsTradeChannel = false then
      v_nPriceCount := 6;
    else
      v_nPriceCount := 1;
    end if;

    --select seq_tb_pimport.Nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename(); -- 'TB' || p_strTableName;
    --begin
    v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
    --LineNumber
    v_strSQL := v_strSQL || 'LineNumber number not null,';
    --key
    v_strSQL := v_strSQL || 'key varchar2(32) not null';
    --description and short description
    case p_oOptions.nDescription
      when 0 then
        null;
      when 1 then
        v_strSQL := v_strSQL || ', short_desc varchar2(32)';
      when 2 then
        v_strSQL := v_strSQL || ', Description varchar2(64)';
        v_strSQL := v_strSQL || ', short_desc varchar2(32)';
      when -1 then
        v_strSQL := v_strSQL || ', Description varchar2(64)';
      else
        null;
    end case;
    --parent
    v_strSQL := v_strSQL || ', parent varchar2(32)';
    --exchange rate table
    if p_oOptions.bTab then
      for i in 1 .. v_nPriceCount loop
        v_strSQL := v_strSQL || ', key_prix_' || i || ' varchar2(32)';
      end loop;
    end if;
    --attribute 1-19
    for i in 1 .. p_nAttrCount loop
      v_strSQL := v_strSQL || ', attr_' || i || ' varchar2(32)';
    end loop;
    --end
    v_strSQL := v_strSQL || ')';

    --execute
    execute immediate v_strSQL;

  exception
    when others then
      Fmp_Log.LOGERROR;
      p_nSqlCode := sqlcode;
      raise;
  end;

end P_PIMPORT_DIMENSION;
/
