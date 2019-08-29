create or replace package P_PIMPORT_ATTRIBUTE Authid Current_User is

  -- Author  : YPWANG
  -- Created : 11/15/2012 2:47:41 PM
  -- Purpose : process batch command 1,2,3,501, import dimension attribute and time series attribute

  -- Public function and procedure declarations
  --Create temporary table for importing attribute
  procedure sp_CreateTmpTableForAttr(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType, --parameters to indicate switchs
                                     p_strTableName out varchar2,
                                     p_nSqlCode     in out integer --return code, 0: correct, not 0: error
                                     );

  --Put data from temporary table to table nct and table vct
  procedure sp_ImportAttribute(p_nAttrID      in integer, --nct.id_ct
                               p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                               p_strTableName in varchar2, --temporary table name for saving date of import file
                               p_nSqlCode     in out integer --return code, 0: correct, not 0: error
                               );
end P_PIMPORT_ATTRIBUTE;
/
create or replace package body P_PIMPORT_ATTRIBUTE is

  --Create temporary table for importing attribute
  procedure sp_CreateTmpTableForAttr(p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                                     p_strTableName out varchar2,
                                     p_nSqlCode     in out integer) is
    v_strSQL varchar2(5000) := '';
  begin
    Fmp_Log.LOGBEGIN;
    p_nSqlCode := 0;

    --select SEQ_TBMID.nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename();
    --p_strTableName := 'TB' || p_strTableName;
    --begin
    v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
    --field 'Attr_No'
    v_strSQL := v_strSQL || 'Attr_No integer not null,';
    --field 'Attr_Value_Key'
    v_strSQL := v_strSQL || 'Attr_Value_Key varchar2(32),';
    --field 'Attr_value_Desc'
    if p_oOptions.bLib then
      v_strSQL := v_strSQL || 'Attr_Value_Desc varchar2(64),';
    end if;
    --field 'Attr_Name'
    v_strSQL := v_strSQL || 'Attr_Name varchar2(32)';
    --end
    v_strSQL := v_strSQL || ')';

    --execute
    execute immediate v_strSQL;
    Fmp_Log.LOGEND;
  exception
    when others then
      Fmp_Log.LOGERROR;
      p_nSqlCode := sqlcode;
      raise;
  end sp_CreateTmpTableForAttr;

  --Insert attribute information to table nct if this attribute does not exist in this table
  procedure sp_ImportAttribute(p_nAttrID      in integer,
                               p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                               p_strTableName in varchar2,
                               p_nSqlCode     in out integer) is
    v_strSQL varchar2(5000) := '';
  begin
    Fmp_Log.LOGBEGIN;
    p_nSqlCode := 0;
    --merge data to table nct
    --merge into
    v_strSQL := 'merge into nct';
    --using
    v_strSQL := v_strSQL ||
                ' using (select distinct t1.Attr_No, t1.Attr_Name from ' ||
                p_strTableName || ' t1) t';
    --on
    v_strSQL := v_strSQL || ' on (nct.id_ct = ' || p_nAttrID ||
                ' and nct.num_ct = t.Attr_No + ' ||
                p_constant.BaseNumberOfAttr || ')';
    --when matched
    v_strSQL := v_strSQL || ' when matched then';
    --update
    v_strSQL := v_strSQL || ' update set nct.nom = t.Attr_Name';
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (nct.nct_em_addr, nct.id_ct, nct.num_ct, nct.nom) values (seq_nct.nextval,' ||
                p_nAttrID || ', t.Attr_No + ' ||
                p_constant.BaseNumberOfAttr || ', t.Attr_Name)';
    --execute
    execute immediate v_strSQL;

    --delete duplicate data
    v_strSQL := 'delete from ' || p_strTableName ||
                ' where rowid not in (select min(rowid) from ' ||
                p_strTableName || ' group by ' || p_strTableName ||
                '.Attr_No, ' || p_strTableName || '.Attr_Value_Key)';
    --execute
    execute immediate v_strSQL;

    --merge data to table vct
    --merge into
    v_strSQL := 'merge into vct';
    --using
    v_strSQL := v_strSQL || ' using ' || p_strTableName || ' t';
    --on
    v_strSQL := v_strSQL || ' on (vct.id_crt = ' || p_nAttrID ||
                ' and vct.num_crt = t.Attr_No + ' ||
                p_constant.BaseNumberOfAttr ||
                ' and nvl(vct.val,'')FM$('') = nvl(t.Attr_Value_Key,'')FM$(''))';
    --when matched
    if p_oOptions.bLib then
      v_strSQL := v_strSQL || ' when matched then';
      --update
      v_strSQL := v_strSQL || ' update set vct.lib_crt=nvl(trim(t.Attr_Value_Desc),vct.lib_crt)';
    end if;
    --when not matched
    v_strSQL := v_strSQL || ' when not matched then';
    --insert
    v_strSQL := v_strSQL ||
                ' insert (vct.vct_em_addr, vct.id_crt, vct.num_crt, vct.val';
    if p_oOptions.bLib then
      v_strSQL := v_strSQL || ', vct.lib_crt';
    end if;
    v_strSQL := v_strSQL || ')';
    v_strSQL := v_strSQL || ' values (seq_vct.nextval,' || p_nAttrID ||
                ', t.Attr_No + ' || p_constant.BaseNumberOfAttr ||
                ', t.Attr_Value_Key';
    if p_oOptions.bLib then
      v_strSQL := v_strSQL || ', trim(t.Attr_Value_Desc)';
    end if;
    v_strSQL := v_strSQL || ')';
    --execute
    execute immediate v_strSQL;
    Fmp_Log.LOGEND;
  exception
    when others then
      Fmp_Log.LOGERROR;
      p_nSqlCode := sqlcode;
      raise;
  end sp_ImportAttribute;

end P_PIMPORT_ATTRIBUTE;
/
