create or replace package FMP_CommonUtil is

  procedure FMSP_ParseOptions(pIn_vStrOptions   in varchar2,
                              pIn_vSeperator    in varchar2,
                              pInOut_vSeparator in out varchar2,
                              pInOut_vSdlt      in out varchar2,
                              pOut_oOptions     out P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                              pInOut_nSqlCode   in out integer);

  procedure FMSP_GetOptionSeparator(pIn_oOptions    in P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                                    pIn_vSeparator  in varchar2,
                                    pOut_vSeparator out varchar2,
                                    pOut_vSdlt      out varchar2,
                                    pInOut_nSqlCode in out integer);

  procedure FMSP_GetOptionFieldFormats(pIn_vStrOptions in varchar2,
                                       pOut_oOptions   out P_BATCHCOMMAND_DATA_TYPE.OptionsKeyForMatsType,
                                       pInOut_nSqlCode in out integer);

end FMP_CommonUtil;
/
create or replace package body FMP_CommonUtil is

  -- Parse options
  procedure FMSP_ParseOptions(pIn_vStrOptions   in varchar2,
                              pIn_vSeperator    in varchar2,
                              pInOut_vSeparator in out varchar2,
                              pInOut_vSdlt      in out varchar2,
                              pOut_oOptions     out P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                              pInOut_nSqlCode   in out integer) is
    --*****************************************************************
    -- Description: this procedure is get switch info.
    --
    -- Parameters:
    --            pIn_vStrOptions
    --            pIn_vSeperator
    --            pInOut_vSeparator
    --            pInOut_vSdlt
    --            pOut_oOptions
    --            pInOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    v_strOptions      varchar2(4000) := '';
    v_nOptionsLen     integer := 0;
    v_strSingleOption varchar2(100) := '';
    v_strOptionKey    varchar2(100) := '';
    v_strOptionParam  varchar2(100) := '';
    v_nOffset         integer := 0;
  begin
    v_strOptions  := trim(pIn_vStrOptions);
    v_nOptionsLen := length(pIn_vStrOptions);
  
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
        when 'esp' then
          pOut_oOptions.besp := true;
        when 'sdfm' then
          pOut_oOptions.bsdfm := true;
        when 'sdlt' then
          pOut_oOptions.bsdlt := true;
        when 'sep' then
          pOut_oOptions.bsep := true;
        when 'spv' then
          pOut_oOptions.bspv := true;
        when 'stab' then
          pOut_oOptions.bTab := true;
        when 'sv' then
          pOut_oOptions.bsv := true;
        when 'sbs' then
          pOut_oOptions.bSbs := true;
          /*if v_strOptionParam is not null then
            p_oOptions.nUM := to_number(v_strOptionParam);
          end if;*/
        else
          if v_strOptionKey is not null then
            pOut_oOptions.bOther := v_strOptionKey;
          else
            null;
          end if;
      end case;
    end loop;
  
    FMSP_GetOptionSeparator(pIn_oOptions    => pOut_oOptions,
                            pIn_vSeparator  => pIn_vSeperator,
                            pOut_vSeparator => pInOut_vSeparator,
                            pOut_vSdlt      => pInOut_vSdlt,
                            pInOut_nSqlCode => pInOut_nSqlCode);
  
  exception
    when others then
      pInOut_nSqlCode := sqlcode;
      raise;
  end FMSP_ParseOptions;

  procedure FMSP_GetOptionSeparator(pIn_oOptions    in P_BATCHCOMMAND_DATA_TYPE.OptionsFieldForMatsType,
                                    pIn_vSeparator  in varchar2,
                                    pOut_vSeparator out varchar2,
                                    pOut_vSdlt      out varchar2,
                                    pInOut_nSqlCode in out integer) is
    --*****************************************************************
    -- Description: this procedure is get Field option separator String.
    --
    -- Parameters:
    --            pIn_oOptions
    --            pIn_vSeparator
    --            pOut_vSeparator
    --            pInOut_vSdlt
    --            pInOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     LiSang         Created.
    -- **************************************************************
    vSdlt      varchar2(100);
    vSeperator varchar2(30) := 'chr(44)||'; --default ,
    nASCII     varchar2(1000);
  begin
  
    if pIn_oOptions.bTab then
      vSeperator := 'chr(9)||';
    end if;
    if pIn_oOptions.bSbs then
      vSeperator := 'chr(32)||';
    end if;
    if pIn_oOptions.bspv then
      vSeperator := 'chr(59)||';
    end if;
    if pIn_vSeparator is not null then
      nASCII     := ascii(nvl(pIn_vSeparator, ','));
      vSeperator := 'chr(' || nASCII || ')||';
    end if;
    if pIn_oOptions.besp then
      vSeperator := 'chr(32)||';
    elsif pIn_oOptions.bOther is not null then
      nASCII     := ascii(nvl(pIn_oOptions.bOther, ','));
      vSeperator := 'chr(' || nASCII || ')||';
    end if;
  
    if pIn_oOptions.bsdlt then
      vSdlt := 'chr(null)';
    else
      vSdlt := 'chr(34)';
    end if;
  
    pOut_vSdlt      := vSdlt;
    pOut_vSeparator := vSeperator;
  
  exception
    when others then
      pInOut_nSqlCode := sqlcode;
  end;

  procedure FMSP_GetOptionFieldFormats(pIn_vStrOptions in varchar2,
                                       pOut_oOptions   out P_BATCHCOMMAND_DATA_TYPE.OptionsKeyForMatsType,
                                       pInOut_nSqlCode in out integer) as
  
    --*****************************************************************
    -- Description: this procedure is get key option switch.
    --
    -- Parameters:
    --            pIn_vStrOptions
    --            pOut_oOptions
    --            pInOut_nSqlCode
    --
    -- Error Conditions Raised:
    --
    -- Author:   LiSang
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     LiSang         Created.
    -- **************************************************************
  
    v_strOptions      varchar2(4000) := '';
    v_nOptionsLen     integer := 0;
    v_strSingleOption varchar2(100) := '';
    v_strOptionKey    varchar2(100) := '';
    v_strOptionParam  varchar2(100) := '';
    v_nOffset         integer := 0;
  begin
    v_strOptions  := trim(pIn_vStrOptions);
    v_nOptionsLen := length(pIn_vStrOptions);
  
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
        when 'nodis' then
          pOut_oOptions.bnodis := true;
        when 'nogeo' then
          pOut_oOptions.bnogeo := true;
        else
          null;
      end case;
    end loop;
  
  exception
    when others then
      pInOut_nSqlCode := sqlcode;
  end;

end FMP_CommonUtil;
/
