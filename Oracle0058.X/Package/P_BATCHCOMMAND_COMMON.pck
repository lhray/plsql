create or replace package P_BATCHCOMMAND_COMMON Authid Current_User is

  -- Author  : YPWANG
  -- Created : 11/15/2012 3:31:43 PM
  -- Purpose : common procedures and data types for batch command

  -- Parse options
  procedure sp_ParseOptions(p_strOptions in varchar2, --reference to sp_ProcessImportMasterData.p_strOptions
                            p_oOptions   out P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType, --   reference to sp_ProcessImportMasterData.v_oOptions
                            p_nSqlCode   in out integer);

  --pexport sp_ProcessImportAttribute
  procedure sp_ProcessImportAttribute(p_nCommandNumber  in integer, --batch command number, reserved information
                                      p_nChronology     in integer, --1 Month ,2 Week,3 Day, reserved information
                                      p_strFMUSER       in varchar2, --Current user name, reserved information
                                      p_strOptions      in varchar2, --batch command switchs, use ## as separator
                                      p_strTmpTableName in varchar2, --reference to sp_CreateTmpTableForImport.p_strTableName
                                      p_nSqlCode        out integer --return code, 0: correct, not 0: error
                                      );
  procedure sp_ProcessImportDimension(p_nCommandNumber  in integer, --batch command number, reserved information
                                      p_nChronology     in integer, --1 Month ,2 Week,3 Day, reserved information
                                      p_strFMUSER       in varchar2, --Current user name, reserved information
                                      p_strOptions      in varchar2, --batch command switchs, use ## as separator
                                      p_nAttrCount      in integer, --specify attribute value count in import file
                                      p_strTmpTableName in varchar2, --reference to sp_CreateTmpTableForImport.p_strTableName
                                      p_strAutoDesc     in varchar2, --description for dimension
                                      p_nSqlCode        out integer --return code, 0: correct, not 0: error
                                      );

  --create a temporary table to save import file
  procedure sp_CreateTmpTableForAttr(p_nCommandNumber in integer, --reference to sp_ProcessImportMasterData.p_nCommandNumber
                                     p_nChronology    in integer, --reference to sp_ProcessImportMasterData.p_nChronology
                                     p_strOptions     in varchar2, --reference to sp_ProcessImportMasterData.p_strOptions
                                     p_strTableName   out varchar2, --table name that used to save data of import file
                                     p_nSqlCode       in out integer);

  procedure sp_CreateTmpTableForDimension(p_nCommandNumber in integer, --reference to sp_ProcessImportMasterData.p_nCommandNumber
                                          p_nChronology    in integer, --reference to sp_ProcessImportMasterData.p_nChronology
                                          p_strOptions     in varchar2, --reference to sp_ProcessImportMasterData.p_strOptions
                                          p_nAttrCount     in integer, --specify attribute value count in import file
                                          p_strTableName   out varchar2, --table name that used to save data of import file
                                          p_nSqlCode       in out integer);

  --drop the table that was created by sp_CreateTmpTableForImport
  procedure sp_DropTmpTableForImport(p_strTableName in varchar2, --reference to sp_CreateTmpTableForImport.p_strTableName
                                     p_nSqlCode     in out number);

end P_BATCHCOMMAND_COMMON;
/
create or replace package body P_BATCHCOMMAND_COMMON is

  -- Parse options
  procedure sp_ParseOptions(p_strOptions in varchar2,
                            p_oOptions   out P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType,
                            p_nSqlCode   in out integer) is
    v_strOptions      varchar2(5000) := '';
    v_nOptionsLen     integer := 0;
    v_strSingleOption varchar2(100) := '';
    v_strOptionKey    varchar2(100) := '';
    v_strOptionParam  varchar2(100) := '';
    v_nOffset         integer := 0;
  begin
    v_strOptions  := trim(p_strOptions);
    v_nOptionsLen := length(v_strOptions);

    while v_nOptionsLen > 0 LOOP
      --Pare a single option from options, each option is seperated by '##'
      v_nOffset := instr(v_strOptions, '##', 1, 1);

      /*Initialize the local variable*/
      v_strOptionKey   := null;
      v_strOptionParam := null;

      if v_nOffset = 0 then
        v_strSingleOption := v_strOptions;
        v_nOptionsLen     := 0;
      else
        v_strSingleOption := trim(substr(v_strOptions, 0, v_nOffset - 1));
        v_strOptions      := trim(substr(v_strOptions, v_nOffset + 2));
        v_nOptionsLen     := length(v_strOptions);
      END IF;

      --Parse option key and option parameter from a single option, key and parameter are Separated by ':'
      v_nOffset := INSTR(v_strSingleOption, ':', 1, 1);

      if v_nOffset = 0 then
        v_strOptionKey := v_strSingleOption;
      else
        v_strOptionKey   := trim(substr(v_strSingleOption, 1, v_nOffset - 1));
        v_strOptionParam := trim(substr(v_strSingleOption, v_nOffset + 1));
      end if;

      v_strOptionKey   := rtrim(lower(v_strOptionKey));
      v_strOptionParam := trim(both '"' from v_strOptionParam);

      --Set parameter according to the option key and parameter
      case v_strOptionKey
        when 'key_dis' then
          --use strKeyDis to instead of trade channel when export
          p_oOptions.bKeyDis   := true;
          p_oOptions.strKeyDis := v_strOptionParam;
        when 'key_dis_default' then
          --use strKeyDisDefault to fill blank value of trade channel when export
          p_oOptions.bKeyDisDefault   := true;
          p_oOptions.strKeyDisDefault := v_strOptionParam;
        when 'key_geo' then
          --use strKeyGeo to instead of sales territory when export
          p_oOptions.bKeyGeo   := true;
          p_oOptions.strKeyGeo := v_strOptionParam;
        when 'key_geo_default' then
          --use strKeyGeoDefault to fill blank value of sales territory when export
          p_oOptions.bKeyGeoDefault   := true;
          p_oOptions.strKeyGeoDefault := v_strOptionParam;
        when 'nodis' then
          --Don't output trade channel dimension
          p_oOptions.bNoDis := true;
        when 'nogeo' then
          --Don't output sales territory dimension
          p_oOptions.bNoGeo := true;
        when 'r2p' then
          --for aggregation node:remove aggregation node key column, for detail node: do nothing
          p_oOptions.bR2P := true;
        when 'a_m_j' then
          --Date format YYYY,MM,DD
          p_oOptions.nDateFormat := P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m_j;
        when 'aa_mm' then
          --Date Format YY,MM
          p_oOptions.nDateFormat := P_BATCHCOMMAND_DATA_TYPE.SWITCH_aa_mm;
        when 'am_w' then
          --Allows the specification of the number of the month and of the week for importing/ exporting weekly
          --coefficient tables (pimport/ pexport 2068). This allows the management of the case of a week overlapping
          --over 2 months of a same year or over 2 years. When exporting, this option must be used with -par1val.
          --When importing, this option must be used with a file format that contains only one date and one value by line
          p_oOptions.nDateFormat := P_BATCHCOMMAND_DATA_TYPE.SWITCH_am_w;
        when 'a_m' then
          p_oOptions.nDateFormat := P_BATCHCOMMAND_DATA_TYPE.SWITCH_a_m;
        when 'debut' then
          --add field BEGIN_YY, BEGIN_MM to temporary table
          p_oOptions.bDebut := true;
        when 'sdate' then
          --remove field YY, MM from temporary table
          p_oOptions.bSdate := true;
        when 'sel' then
          --use strKeyGeo to instead of sales territory when export
          p_oOptions.bSel   := true;
          p_oOptions.strSel := v_strOptionParam;
        when 'lib' then
          p_oOptions.bLib := true;
        when 'description' then
          p_oOptions.nDescription := to_number(v_strOptionParam);
        when 'prix' then
          p_oOptions.bPrix := true;
        when 'suite' then
          p_oOptions.bSuite := true;
        when 'tab' then
          p_oOptions.bTab := true;
        when 'Version' then
          p_oOptions.bVersion := true;
          p_oOptions.nVersion := to_number(v_strOptionParam);
        when 'um' then
          p_oOptions.bUM := true;
          if v_strOptionParam is not null then
            p_oOptions.nUM := to_number(v_strOptionParam);
          end if;
        when 'sel_condit' then
          p_oOptions.bSelCondit   := true;
          p_oOptions.strSelCondit := v_strOptionParam;
        when 'attribute_inherit' then
          p_oOptions.bAttributeInherit := true;
          --added begin by zhangxf 20130328 delete null rows
        when 'nobl' then
          p_oOptions.bNobl := true;
        when 'par1val' then
          p_oOptions.bPar1val := true;
        when 'nd' then
          p_oOptions.bNd   := true;
          p_oOptions.strNb := v_strOptionParam;
          --added end
        else
          null;
      end case;
    end loop;

  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_ParseOptions;

  --sp_ProcessImportAttribute
  procedure sp_ProcessImportAttribute(p_nCommandNumber  in integer,
                                      p_nChronology     in integer,
                                      p_strFMUSER       in varchar2,
                                      p_strOptions      in varchar2,
                                      p_strTmpTableName in varchar2,
                                      p_nSqlCode        out integer) is
    v_oOption P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    v_strsql  varchar(5000);
  begin
    p_nSqlCode := 0;
    fmp_log.FMP_SetValue(p_nCommandNumber);
    fmp_log.FMP_SetValue(p_nChronology);
    fmp_log.FMP_SetValue(p_strFMUSER);
    fmp_log.FMP_SetValue(p_strOptions);
    fmp_log.FMP_SetValue(p_strTmpTableName);
    fmp_log.logbegin;

    /*----Delete duplicate records wfq 2013.3.27
    v_strsql := 'delete from ' || p_strTmpTableName ||
                ' a where attr_value_key is null';
    execute immediate v_strsql;

    v_strsql := 'delete from ' || p_strTmpTableName || ' a
        where rowid <
        (
        select max(rowid) from ' || p_strTmpTableName || ' b
        where a.attr_value_key=b.attr_value_key
        )';
    execute immediate v_strsql;
    commit;*/

    sp_ParseOptions(p_strOptions, v_oOption, p_nSqlCode);

    case p_nCommandNumber
      when 1 then
        --product attribute
        P_PIMPORT_ATTRIBUTE.sp_ImportAttribute(p_constant.v_ProductData,
                                               v_oOption,
                                               p_strTmpTableName,
                                               p_nSqlCode);
      when 2 then
        --sales territory attribute
        P_PIMPORT_ATTRIBUTE.sp_ImportAttribute(p_constant.v_STData,
                                               v_oOption,
                                               p_strTmpTableName,
                                               p_nSqlCode);
      when 3 then
        --trade channel attribute
        P_PIMPORT_ATTRIBUTE.sp_ImportAttribute(p_constant.v_TCData,
                                               v_oOption,
                                               p_strTmpTableName,
                                               p_nSqlCode);
      when 501 then
        --time series attribute
        P_PIMPORT_ATTRIBUTE.sp_ImportAttribute(p_constant.v_DetailNodeData,
                                               v_oOption,
                                               p_strTmpTableName,
                                               p_nSqlCode);
      else
        --default
        null;
    end case;
    fmp_log.logend;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end sp_ProcessImportAttribute;

  --sp_ProcessImportDimension
  procedure sp_ProcessImportDimension(p_nCommandNumber  in integer,
                                      p_nChronology     in integer,
                                      p_strFMUSER       in varchar2,
                                      p_strOptions      in varchar2,
                                      p_nAttrCount      in integer,
                                      p_strTmpTableName in varchar2,
                                      p_strAutoDesc     in varchar2,
                                      p_nSqlCode        out integer) is
    v_oOption P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
    v_strsql  varchar(5000);
  begin
    p_nSqlCode := 0;
    fmp_log.FMP_SetValue(p_nCommandNumber);
    fmp_log.FMP_SetValue(p_nChronology);
    fmp_log.FMP_SetValue(p_strFMUSER);
    fmp_log.FMP_SetValue(p_strOptions);
    fmp_log.FMP_SetValue(p_nAttrCount);
    fmp_log.FMP_SetValue(p_strTmpTableName);
    fmp_log.FMP_SetValue(p_strAutoDesc);
    fmp_log.LOGBEGIN;

    ----Delete duplicate records wfq 2013.3.26
    v_strsql := 'delete from ' || p_strTmpTableName ||
                ' a where key is null';
    execute immediate v_strsql;

    v_strsql := 'delete from ' || p_strTmpTableName || ' a
        where rowid <
        (
        select max(rowid) from ' || p_strTmpTableName || ' b
        where a.key=b.key
        )';
    execute immediate v_strsql;
    commit;

    sp_ParseOptions(p_strOptions, v_oOption, p_nSqlCode);

    case p_nCommandNumber
      when 4 then
        --product group
        P_PIMPORT_DIMENSION.sp_ImportProduct(true,
                                             v_oOption,
                                             p_nAttrCount,
                                             p_strTmpTableName,
                                             p_strAutoDesc,
                                             p_strFMUSER,
                                             p_nSqlCode);
      when 5 then
        --product
        P_PIMPORT_DIMENSION.sp_ImportProduct(false,
                                             v_oOption,
                                             p_nAttrCount,
                                             p_strTmpTableName,
                                             p_strAutoDesc,
                                             p_strFMUSER,
                                             p_nSqlCode);
      when 6 then
        --sales territory
        P_PIMPORT_DIMENSION.sp_ImportST(v_oOption,
                                        p_nAttrCount,
                                        p_strTmpTableName,
                                        p_strAutoDesc,
                                        p_strFMUSER,
                                        p_nSqlCode);
      when 7 then
        --trade channel
        P_PIMPORT_DIMENSION.sp_ImportTC(v_oOption,
                                        p_nAttrCount,
                                        p_strTmpTableName,
                                        p_strAutoDesc,
                                        p_strFMUSER,
                                        p_nSqlCode);
      else
        --default
        null;
    end case;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end sp_ProcessImportDimension;

  --create a temporary table to save import file
  procedure sp_CreateTmpTableForAttr(p_nCommandNumber in integer,
                                     p_nChronology    in integer,
                                     p_strOptions     in varchar2,
                                     p_strTableName   out varchar2,
                                     p_nSqlCode       in out integer) is
    v_oOption P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
  begin
    p_nSqlCode := 0;
    sp_ParseOptions(p_strOptions, v_oOption, p_nSqlCode);

    case p_nCommandNumber
      when 1 then
        --product attribute
        P_PIMPORT_ATTRIBUTE.sp_CreateTmpTableForAttr(p_oOptions     => v_oOption,
                                                     p_strTableName => p_strTableName,
                                                     p_nSqlCode     => p_nSqlCode);
      when 2 then
        --sales territory attribute
        P_PIMPORT_ATTRIBUTE.sp_CreateTmpTableForAttr(p_oOptions     => v_oOption,
                                                     p_strTableName => p_strTableName,
                                                     p_nSqlCode     => p_nSqlCode);
      when 3 then
        --trade channel attribute
        P_PIMPORT_ATTRIBUTE.sp_CreateTmpTableForAttr(p_oOptions     => v_oOption,
                                                     p_strTableName => p_strTableName,
                                                     p_nSqlCode     => p_nSqlCode);
      when 501 then
        --time series attribute
        P_PIMPORT_ATTRIBUTE.sp_CreateTmpTableForAttr(p_oOptions     => v_oOption,
                                                     p_strTableName => p_strTableName,
                                                     p_nSqlCode     => p_nSqlCode);
      else
        --default
        null;
    end case;

  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_CreateTmpTableForAttr;

  procedure sp_CreateTmpTableForDimension(p_nCommandNumber in integer, --reference to sp_ProcessImportMasterData.p_nCommandNumber
                                          p_nChronology    in integer, --reference to sp_ProcessImportMasterData.p_nChronology
                                          p_strOptions     in varchar2, --reference to sp_ProcessImportMasterData.p_strOptions
                                          p_nAttrCount     in integer, --specify attribute value count in import file
                                          p_strTableName   out varchar2, --table name that used to save data of import file
                                          p_nSqlCode       in out integer) is
    v_oOption P_BATCHCOMMAND_DATA_TYPE.OptionsRecordType;
  begin
    p_nSqlCode := 0;
    sp_ParseOptions(p_strOptions, v_oOption, p_nSqlCode);

    case p_nCommandNumber
      when 4 then
        --product group
        P_PIMPORT_DIMENSION.sp_CreateTmpTableForProduct(v_oOption,
                                                        p_nAttrCount,
                                                        p_strTableName,
                                                        p_nSqlCode);
      when 5 then
        --product
        P_PIMPORT_DIMENSION.sp_CreateTmpTableForProduct(v_oOption,
                                                        p_nAttrCount,
                                                        p_strTableName,
                                                        p_nSqlCode);
      when 6 then
        --sales territory
        P_PIMPORT_DIMENSION.sp_CreateTmpTableForSTorTC(false,
                                                       v_oOption,
                                                       p_nAttrCount,
                                                       p_strTableName,
                                                       p_nSqlCode);
      when 7 then
        --trade channel
        P_PIMPORT_DIMENSION.sp_CreateTmpTableForSTorTC(true,
                                                       v_oOption,
                                                       p_nAttrCount,
                                                       p_strTableName,
                                                       p_nSqlCode);
      else
        --default
        null;
    end case;

  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end sp_CreateTmpTableForDimension;

  --drop the table that was created by sp_CreateTmpTableForImport
  procedure sp_DropTmpTableForImport(p_strTableName in varchar2, --reference to sp_CreateTmpTableForImport.p_strTableName
                                     p_nSqlCode     in out number) is
    v_strSQL varchar2(5000) := ''; --SQL command string
  begin
    p_nSqlCode := 0;
    v_strSQL   := 'drop table ' || p_strTableName || ' purge';
    execute immediate v_strSQL;

  exception
    when others then
      p_nSqlCode := sqlcode;
      raise;
  end;

end P_BATCHCOMMAND_COMMON;
/
