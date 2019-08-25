create or replace function f_GetLOGCODE(f_nCommandNumber in number)
  return number is
  v_nLogCode number;
begin

  case f_nCommandNumber
    when 1 then
      --Duplicated product attribute key
      v_nLogCode := p_Constant.DUP_KEY_PRT_ATTR_LOGCODE;
    when 2 then
      --Duplicated sales territory attribute key
      v_nLogCode := p_Constant.DUP_KEY_ST_ATTR_LOGCODE;
    when 3 then
      --Duplicated trade channel attribute key
      v_nLogCode := p_Constant.DUP_KEY_TC_ATTR_LOGCODE;
    when 4 then
      --Duplicated product group key
      v_nLogCode := p_Constant.DUP_KEY_PRODUCTGROUP_LOGCODE;
    when 5 then
      --Duplicated product key
      v_nLogCode := p_Constant.DUP_KEY_PRODUCT_LOGCODE;
    when 6 then
      --Duplicated sales territory key
      v_nLogCode := p_Constant.DUP_KEY_SALESTERRITORY_LOGCODE;
    when 7 then
      --Duplicated trade channel key
      v_nLogCode := p_Constant.DUP_KEY_TRADECHANNEL_LOGCODE;
    when 501 then
      --Duplicated time series attribute key
      v_nLogCode := p_Constant.DUP_KEY_TS_ATT_LOGCODE;
    else
      --default
      null;
  end case;

  return v_nLogCode;
end;
/
