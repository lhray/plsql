create or replace package FMP_ExportForecast is

  -- Author  : JHZUO
  -- Created : 1/13/2013 5:30:21 PM
  -- Purpose :
  procedure fmsp_ProcessExportForecast(p_nObjectCode  in number,
                                       p_strFMUSER    in varchar2,
                                       P_StrOption    in varchar2,
                                       p_strSeparator in varchar2,
                                       p_nDecimals    in number,
                                       p_strTableName out varchar2,
                                       p_nSqlCode     out integer);

  procedure sp_CreateTempTableNote(p_nObjectCode  in number,
                                   p_strFMUSER    in varchar2,
                                   P_StrOption    in varchar2,
                                   p_strTableName out varchar2,
                                   p_nSqlCode     in out integer);

  procedure sp_CreateTempTableParameter(p_nObjectCode      in number,
                                        p_strFMUSER        in varchar2,
                                        P_StrOption        in varchar2,
                                        p_strTableName     out varchar2,
                                        p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                        p_nSqlCode         in out integer);

  procedure fmsp_CreateTempTabExpForecast(p_nObjectCode      in number,
                                          p_strFMUSER        in varchar2,
                                          P_StrOption        in varchar2,
                                          p_strTableName     out varchar2,
                                          p_nBestfitRuleFlag in number,
                                          p_nSqlCode         in out integer);

  procedure sp_GetSelectField(p_strTableName in varchar2,
                              P_OrderFlag    in Number,
                              p_ColumnId     in Number,
                              p_strSQLField  out varchar2,
                              p_nSqlCode     out integer);
  procedure sp_PutDataToTmpTableParameter(p_nObjectCode      in number,
                                          p_strFMUSER        in varchar2,
                                          P_StrOption        in varchar2,
                                          p_strTableName     in out varchar2,
                                          p_nBestfitRuleFlag in number,
                                          p_strSeparator     in varchar2,
                                          p_nDecimals        in number, --Decimals config
                                          p_nSqlCode         out integer);

  procedure sp_PutDataToTmpTableNote(p_nObjectCode  in number,
                                     p_strFMUSER    in varchar2,
                                     P_StrOption    in varchar2,
                                     p_strSeparator in varchar2,
                                     p_strTableName in out varchar2,
                                     p_nSqlCode     out integer);
end FMP_exportForecast;
/
create or replace package body FMP_ExportForecast is

  --*****************************************************************
  -- Description: Export Forecast Operations.
  --
  -- Author:      <zjh>
  -- Revise
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        14-JAN-2013     zjh           Created.

  -- **************************************************************
  gvDebugText constant varchar2(30) := 'pexportFCST';
  procedure fmsp_ProcessExportForecast(p_nObjectCode  in number,
                                       p_strFMUSER    in varchar2,
                                       P_StrOption    in varchar2,
                                       p_strSeparator in varchar2,
                                       p_nDecimals    in number, --Decimals config
                                       p_strTableName out varchar2,
                                       p_nSqlCode     out integer)
  --*****************************************************************
    -- Description: ProcessExportForecast main procedure
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strSeparator
    --       p_strTableName
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   is
    v_nSqlCode         number := 0;
    v_strTableName     varchar2(100) := '';
    v_nBestfitRuleFlag number := 0;
    v_strSeparator     varchar2(100) := '';
    v_nDecimals        number := 0;
  begin
    fmp_log.FMP_SetValue(p_nObjectCode);
    fmp_log.FMP_SetValue(p_strFMUSER);
    fmp_log.FMP_SetValue(P_StrOption);
    fmp_log.FMP_SetValue(p_strSeparator);
    fmp_log.FMP_SetValue(p_nDecimals);
    fmp_log.LOGBEGIN;
  
    v_nBestfitRuleFlag := 0;
  
    v_strSeparator := p_strSeparator;
    if p_strSeparator = 'ESP' then
      v_strSeparator := ' ';
    end if;
    if v_strSeparator is null then
      v_strSeparator := ',';
    end if;
  
    if p_nObjectCode in (1051, 1052) then
    
      --create temp table forecast note
      sp_CreateTempTableNote(p_nObjectCode,
                             p_strFMUSER,
                             P_StrOption,
                             v_strTableName,
                             v_nSqlCode);
    
      --get forecast note information to result table
      sp_PutDataToTmpTableNote(p_nObjectCode,
                               p_strFMUSER,
                               P_StrOption,
                               v_strSeparator,
                               v_strTableName,
                               v_nSqlCode);
    end if;
    if p_nObjectCode not in (1051, 1052) then
      v_nBestfitRuleFlag := 0;
    
      ----create temp table forecast parameter
      fmsp_CreateTempTabExpForecast(p_nObjectCode,
                                    p_strFMUSER,
                                    P_StrOption,
                                    v_strTableName,
                                    v_nBestfitRuleFlag,
                                    v_nSqlCode);
    
      v_nDecimals := p_nDecimals;
      --get forecast parameter information to result table
      sp_PutDataToTmpTableParameter(p_nObjectCode,
                                    p_strFMUSER,
                                    P_StrOption,
                                    v_strTableName,
                                    v_nBestfitRuleFlag,
                                    v_strSeparator,
                                    v_nDecimals, --Decimals config
                                    v_nSqlCode);
    end if;
    p_nSqlCode     := v_nSqlCode;
    p_strTableName := v_strTableName;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
    
  end fmsp_ProcessExportForecast;

  procedure sp_CreateTempTableNote(p_nObjectCode  in number,
                                   p_strFMUSER    in varchar2,
                                   P_StrOption    in varchar2,
                                   p_strTableName out varchar2,
                                   p_nSqlCode     in out integer)
  --*****************************************************************
    -- Description: CreateTempTableNote procedure
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strTableName
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   as
    v_strSQL    varchar2(30000) := '';
    v_StrOption varchar2(3000);
  begin
    if p_nObjectCode in (1051, 1052) then
      v_StrOption := upper(trim(P_StrOption));
    
      p_strTableName := fmf_gettmptablename();
      --begin
      v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
      --detail Notes
      if p_nObjectCode = 1051 then
        --field 'Product key'
        v_strSQL := v_strSQL || 'product varchar2(60),';
      end if;
      --aggregate Notes
      if p_nObjectCode = 1052 then
        --field 'Aggregate key'
        v_strSQL := v_strSQL || 'AggNode varchar2(184),';
        --field 'Product key'
        v_strSQL := v_strSQL || 'product varchar2(60),';
      end if;
    
      --field 'Sales Territory key'
      v_strSQL := v_strSQL || 'sales varchar2(60),';
    
      --field 'Trade channel key'*/
      v_strSQL := v_strSQL || 'trade varchar2(60),';
    
      --field 'Model - see table at the bottom '
      v_strSQL := v_strSQL || 'F_Note CLOB ';
      v_strSQL := v_strSQL || ')';
    
      --execute
      execute immediate v_strSQL;
    end if;
    p_nSqlCode := 0;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end;

  procedure sp_CreateTempTableParameter(p_nObjectCode      in number,
                                        p_strFMUSER        in varchar2,
                                        P_StrOption        in varchar2,
                                        p_strTableName     out varchar2,
                                        p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                        p_nSqlCode         in out integer)
  --*****************************************************************
    -- Description: CreateTempTableParameter procedure
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strSeparator
    --       p_strTableName
    --       p_nBestfitRuleFlag
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   as
    v_strSQL    varchar2(5000) := ''; --SQL command string
    v_StrOption varchar2(5000) := '';
  begin
    v_StrOption := upper(P_StrOption);
    if P_StrOption IS NULL then
      v_StrOption := 'A';
    end if;
  
    p_strTableName := fmf_gettmptablename();
    --begin
    v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
    --detail level1,2,3,4
    if p_nObjectCode in (51, 2072, 2076, 2080) then
      --field 'Product key'
      v_strSQL := v_strSQL || 'product varchar2(60) not null,';
    end if;
  
    --aggregate level1,2,3,4
    if p_nObjectCode in (52, 2074, 2078, 2082) then
      --field 'Aggregate key'
      v_strSQL := v_strSQL || 'AggNode varchar2(184),';
      --field 'Product key'
      v_strSQL := v_strSQL || 'product varchar2(60),';
    end if;
  
    --if p_oOptions.bmTotal then
    if instr(v_StrOption, 'MTOTAL') > 0 then
    
      --field 'Sales Territory key'
      v_strSQL := v_strSQL || 'sales varchar2(60),';
      --if p_oOptions.bNoDis then
    
      --field 'Trade channel key'
      v_strSQL := v_strSQL || 'trade varchar2(60),';
    
      --field 'Model - see table at the bottom '
      v_strSQL := v_strSQL || 'F_Model number,';
      --field 'Reference node '
      v_strSQL := v_strSQL || 'Reference_node varchar2(92),';
      --field 'Max number of periods'
      v_strSQL := v_strSQL || 'Max_periods number,';
      --field 'Start History for forecast'
      v_strSQL := v_strSQL || 'Start_History number,';
      --field 'End History for forecast'
      v_strSQL := v_strSQL || 'End_History number,';
      --field 'Horizon'
      v_strSQL := v_strSQL || 'Horizon number,';
      --field 'End Forecast date'
      v_strSQL := v_strSQL || 'End_Forecast_date number,';
      --field 'Type of trend'
      v_strSQL := v_strSQL || 'trend_type number,';
      --field 'Smoothing coefficient'
      v_strSQL := v_strSQL || 'Smoothing_coefficient number,';
      --field 'Autoadaptation'
      v_strSQL := v_strSQL || 'Autoadaptation number,';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table varchar2(30),';
      --field 'Target 1'
      v_strSQL := v_strSQL || 'Target_1 number,';
      --field 'Value Target 1'
      v_strSQL := v_strSQL || 'Value_Target_1 number,';
      --field 'Target profile'
      v_strSQL := v_strSQL || 'Target_profile number,';
      --field 'Target Start date'
      v_strSQL := v_strSQL || 'Target_Start_date number,';
      --field 'Target end date'
      v_strSQL := v_strSQL || 'Target_End_date number,';
      --field 'Target 2'
      v_strSQL := v_strSQL || 'Target_2 number,';
      --field 'Value Target 2'
      v_strSQL := v_strSQL || 'Value_Target_2 number,';
      --field 'Corrected by Ext Event'
      v_strSQL := v_strSQL || 'Corrected_Ext_Event number,';
      --field 'Forecast retained on split (As forced when splitting)'
      v_strSQL := v_strSQL || 'retained_split number,';
      --field 'Calculated seasonal correction'
      v_strSQL := v_strSQL || 'Calculated_seasonal_correction number,';
      --field 'Monthly Seasonality (Month 1)'
      v_strSQL := v_strSQL || 'Seasonality_Month_1 number,';
      --field 'Monthly Seasonality (Month 2)'
      v_strSQL := v_strSQL || 'Seasonality_Month_2 number,';
      --field 'Monthly Seasonality (Month 3)'
      v_strSQL := v_strSQL || 'Seasonality_Month_3 number,';
      --field 'Monthly Seasonality (Month 4)'
      v_strSQL := v_strSQL || 'Seasonality_Month_4 number,';
      --field 'Monthly Seasonality (Month 5)'
      v_strSQL := v_strSQL || 'Seasonality_Month_5 number,';
      --field 'Monthly Seasonality (Month 6)'
      v_strSQL := v_strSQL || 'Seasonality_Month_6 number,';
      --field 'Monthly Seasonality (Month 7)'
      v_strSQL := v_strSQL || 'Seasonality_Month_7 number,';
      --field 'Monthly Seasonality (Month 8)'
      v_strSQL := v_strSQL || 'Seasonality_Month_8 number,';
      --field 'Monthly Seasonality (Month 9)'
      v_strSQL := v_strSQL || 'Seasonality_Month_9 number,';
      --field 'Monthly Seasonality (Month 10)'
      v_strSQL := v_strSQL || 'Seasonality_Month_10 number,';
      --field 'Monthly Seasonality (Month 11)'
      v_strSQL := v_strSQL || 'Seasonality_Month_11 number,';
      --field 'Monthly Seasonality (Month 12)'
      v_strSQL := v_strSQL || 'Seasonality_Month_12 number,';
      --field 'End history for Forecast'
      v_strSQL := v_strSQL || 'history_end number,';
      --field 'Sales (-12M)'
      v_strSQL := v_strSQL || 'F_Sales number,';
      --field 'Forecast (+12M)'
      v_strSQL := v_strSQL || 'Forecast number,';
      --field 'Forecast (+12M): target column'
      v_strSQL := v_strSQL || 'Forecast_target_column number,';
      --field '(+12M)/(-12M)(%)'
      v_strSQL := v_strSQL || 'Percent_12M number,';
      --field '(+12M)/(-12M)(%) : target column'
      v_strSQL := v_strSQL || 'Percent_12M_target_column number,';
      --field 'Previous financial year'
      v_strSQL := v_strSQL || 'Previous_financial_year number,';
      --field 'Current year'
      v_strSQL := v_strSQL || 'Current_year number,';
      --field 'Current year: target column'
      v_strSQL := v_strSQL || 'Current_year_target_column number,';
      --field 'Current year/Previous (%)'
      v_strSQL := v_strSQL || 'Precent_Previous_Current_year number,';
      --field 'Current year/Previous (%): target column'
      v_strSQL := v_strSQL || 'Precent_Previ_Cur_year_target number,';
      --field 'Next financial year'
      v_strSQL := v_strSQL || 'Next_financial_year number,';
      --field 'Next financial year: target column'
      v_strSQL := v_strSQL || 'Next_financial_year_target number,';
      --field 'Next fin. year/Current year (%)'
      v_strSQL := v_strSQL || 'Percent_next_fin_year number,';
      --field 'Next fin. year/Current year (%): target column'
      v_strSQL := v_strSQL || 'Percent_next_fin_year_target number,';
      --field 'Sales to date'
      v_strSQL := v_strSQL || 'Sales_date number,';
      --field 'Sales to date (%)'
      v_strSQL := v_strSQL || 'Percent_Sales_date number,';
      --field 'Sales to date (%): target column'
      v_strSQL := v_strSQL || 'Percent_Sales_date_target number,';
      --field 'Balance of sales to be achieved'
      v_strSQL := v_strSQL || 'Balance_sales_achieve number,';
      --field 'Balance of sales to be achieved: target column'
      v_strSQL := v_strSQL || 'Balance_sales_achieve_target number,';
      --field 'Balance of sales to be achieved (%)'
      v_strSQL := v_strSQL || 'Percent_Balance_sales_achieve number,';
      --field 'Balance of sales to be achieved (%): target column'
      v_strSQL := v_strSQL || 'Percent_Balan_achieve_target number,';
      --field 'Mean'
      v_strSQL := v_strSQL || 'F_Mean number,';
      --field 'Trend'
      v_strSQL := v_strSQL || 'Trend number,';
      --field 'Alarm'
      v_strSQL := v_strSQL || 'Alarm number,';
      --field 'Standard deviation'
      v_strSQL := v_strSQL || 'Standard_deviation number,';
      --field 'Absolute mean deviation'
      v_strSQL := v_strSQL || 'Absolute_mean_deviation number,';
      --field '% Deviation/Average'
      v_strSQL := v_strSQL || 'Percent_Deviation_Average number,';
      --field '% Forecast deviation'
      v_strSQL := v_strSQL || 'Percent_Forecast_deviation number,';
      --field '% Forecast deviation(-6M)'
      v_strSQL := v_strSQL || 'Percent_Forecast_deviation_6M number,';
      --field 'Continuation of Factor (%)'
      v_strSQL := v_strSQL || 'Percent_Continuation_Factor number,';
      --field 'Offset-Periods'
      v_strSQL := v_strSQL || 'Offset_Periods number,';
      --field 'Start date for forecast'
      v_strSQL := v_strSQL || 'forecast_Start_date number,';
      --field 'Start date for seasonality'
      v_strSQL := v_strSQL || 'seasonality_Start_date number,';
      --field 'End date for seasonality'
      v_strSQL := v_strSQL || 'seasonality_End_date number,';
      --field 'Historical smoothing Filter'
      v_strSQL := v_strSQL || 'Historical_smoothing_Filter number,';
      --field 'Start date for continued history'
      v_strSQL := v_strSQL || 'continued_history_Start_date  number,';
      --field 'End date for continued history'
      v_strSQL := v_strSQL || 'continued_history_End_date  number,';
      --field 'External Data table'
      v_strSQL := v_strSQL || 'External_Data_table  varchar2(45),';
      --field 'Display horizon'
      v_strSQL := v_strSQL || 'Display_horizon  number,';
      --field 'Short term Forecast - Forecast horizon'
      v_strSQL := v_strSQL || 'Short_term_horizon  number,';
      --field 'Short term Forecast - Pds of history'
      v_strSQL := v_strSQL || 'Short_term_horizon_Pds  number,';
      --field 'Short term Forecast - Trend'
      v_strSQL := v_strSQL || 'Short_term_Forecast_Trend  number,';
      --field 'Short term Forecast - Seasonality '
      v_strSQL := v_strSQL || 'Short_term_Forecast_Seasonal  number,';
      --field 'Managing Extremities (0=no, 1=yes)'
      v_strSQL := v_strSQL || 'Managing_Extremities  number,';
      --field 'Max no of periods for seasonality'
      v_strSQL := v_strSQL || 'Max_periods_seasonality  number,';
      --field 'Best Fit Model (0 : unchecked; 1 : checked)'
      v_strSQL := v_strSQL || 'Best_Fit_Model  number,';
      --field 'Smoothing coefficient (trend) (only with the Winters mode)'
      v_strSQL := v_strSQL || 'Smoothing_coefficient_trend  number,';
      --field 'Trend (only with the Winters mode)'
      v_strSQL := v_strSQL || 'Trend_Winters  number';
    
    end if;
  
    if instr(v_StrOption, 'MTOTAL') = 0 then
      --field 'Sales Territory key'
      v_strSQL := v_strSQL || 'sales varchar2(60),';
      --field 'Trade channel key'
      v_strSQL := v_strSQL || 'trade varchar2(60),';
    
      --field 'Model - see table at the bottom '
      v_strSQL := v_strSQL || 'F_Model number,';
      --field 'Reference node '
      v_strSQL := v_strSQL || 'Reference_node varchar2(92),';
    
      /*        --field 'Start History for forecast'
      v_strSQL := v_strSQL || 'Start_History number,';
      --field 'End History for forecast'
      v_strSQL := v_strSQL || 'End_History number,';*/
      --field 'Horizon'
      v_strSQL := v_strSQL || 'Horizon number,';
      --field 'End Forecast date'
      v_strSQL := v_strSQL || 'End_Forecast_date number,';
      --field 'Type of trend'
      v_strSQL := v_strSQL || 'trend_type number,';
      --field 'Max number of periods'
      v_strSQL := v_strSQL || 'Max_periods number,';
      --field 'Smoothing coefficient'
      v_strSQL := v_strSQL || 'Smoothing_coefficient number,';
      --field 'Autoadaptation'
      v_strSQL := v_strSQL || 'Autoadaptation number,';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table1 varchar2(30),';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table2 varchar2(30),';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table3 varchar2(30),';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table4 varchar2(30),';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table5 varchar2(30),';
      --field 'Trading Day table'
      v_strSQL := v_strSQL || 'Trading_Day_table6 varchar2(30),';
      --field 'Target 1'
      v_strSQL := v_strSQL || 'Target_1 number,';
      --field 'Value Target 1'
      v_strSQL := v_strSQL || 'Value_Target_1 number,';
      /*       --field 'Target profile'
      v_strSQL := v_strSQL || 'Target_profile number,';
      --field 'Target Start date'
      v_strSQL := v_strSQL || 'Target_Start_date number,';
      --field 'Target end date'
      v_strSQL := v_strSQL || 'Target_End_date number,';
      --field 'Target 2'
      v_strSQL := v_strSQL || 'Target_2 number,';
      --field 'Value Target 2'
      v_strSQL := v_strSQL || 'Value_Target_2 number,';
      --field 'Corrected by Ext Event'
      v_strSQL := v_strSQL || 'Corrected_Ext_Event number,';
      --field 'Forecast retained on split (As forced when splitting)'
      v_strSQL := v_strSQL || 'retained_split number,';*/
      --field 'Calculated seasonal correction'
      v_strSQL := v_strSQL || 'Calculated_seasonal_correction number,';
      --field 'Monthly Seasonality (Month 1)'
      v_strSQL := v_strSQL || 'Seasonality_Month_1 number,';
      --field 'Monthly Seasonality (Month 2)'
      v_strSQL := v_strSQL || 'Seasonality_Month_2 number,';
      --field 'Monthly Seasonality (Month 3)'
      v_strSQL := v_strSQL || 'Seasonality_Month_3 number,';
      --field 'Monthly Seasonality (Month 4)'
      v_strSQL := v_strSQL || 'Seasonality_Month_4 number,';
      --field 'Monthly Seasonality (Month 5)'
      v_strSQL := v_strSQL || 'Seasonality_Month_5 number,';
      --field 'Monthly Seasonality (Month 6)'
      v_strSQL := v_strSQL || 'Seasonality_Month_6 number,';
      --field 'Monthly Seasonality (Month 7)'
      v_strSQL := v_strSQL || 'Seasonality_Month_7 number,';
      --field 'Monthly Seasonality (Month 8)'
      v_strSQL := v_strSQL || 'Seasonality_Month_8 number,';
      --field 'Monthly Seasonality (Month 9)'
      v_strSQL := v_strSQL || 'Seasonality_Month_9 number,';
      --field 'Monthly Seasonality (Month 10)'
      v_strSQL := v_strSQL || 'Seasonality_Month_10 number,';
      --field 'Monthly Seasonality (Month 11)'
      v_strSQL := v_strSQL || 'Seasonality_Month_11 number,';
      --field 'Monthly Seasonality (Month 12)'
      v_strSQL := v_strSQL || 'Seasonality_Month_12 number,';
      --field 'End history for Forecast'
      v_strSQL := v_strSQL || 'history_end number,';
      /*        --field 'Sales (-12M)'
      v_strSQL := v_strSQL || 'Sales number,';
      --field 'Forecast (+12M)'
      v_strSQL := v_strSQL || 'Forecast number,';
      --field 'Forecast (+12M): target column'
      v_strSQL := v_strSQL || 'Forecast_target_column number,';
      --field '(+12M)/(-12M)(%)'
      v_strSQL := v_strSQL || 'Percent_12M number,';
      --field '(+12M)/(-12M)(%) : target column'
      v_strSQL := v_strSQL || 'Percent_12M_target_column number,';
      --field 'Previous financial year'
      v_strSQL := v_strSQL || 'Previous_financial_year number,';
      --field 'Current year'
      v_strSQL := v_strSQL || 'Current_year number,';
      --field 'Current year: target column'
      v_strSQL := v_strSQL || 'Current_year_target_column number,';
      --field 'Current year/Previous (%)'
      v_strSQL := v_strSQL || 'Precent_Previous_Current_year number,';
      --field 'Current year/Previous (%): target column'
      v_strSQL := v_strSQL || 'Precent_Previ_Cur_year_target number,';
      --field 'Next financial year'
      v_strSQL := v_strSQL || 'Next_financial_year number,';
      --field 'Next financial year: target column'
      v_strSQL := v_strSQL || 'Next_financial_year_target number,';
      --field 'Next fin. year/Current year (%)'
      v_strSQL := v_strSQL || 'Percent_next_fin_year number,';
      --field 'Next fin. year/Current year (%): target column'
      v_strSQL := v_strSQL || 'Percent_next_fin_year_target number,';
      --field 'Sales to date'
      v_strSQL := v_strSQL || 'Sales_date number,';
      --field 'Sales to date (%)'
      v_strSQL := v_strSQL || 'Percent_Sales_date number,';
      --field 'Sales to date (%): target column'
      v_strSQL := v_strSQL || 'Percent_Sales_date_target number,';
      --field 'Balance of sales to be achieved'
      v_strSQL := v_strSQL || 'Balance_sales_achieve number,';
      --field 'Balance of sales to be achieved: target column'
      v_strSQL := v_strSQL || 'Balance_sales_achieve_target number,';
      --field 'Balance of sales to be achieved (%)'
      v_strSQL := v_strSQL || 'Percent_Balance_sales_achieve number,';
      --field 'Balance of sales to be achieved (%): target column'
      v_strSQL := v_strSQL || 'Percent_Balan_achieve_target number,';*/
      --field 'Mean'
      v_strSQL := v_strSQL || 'F_Mean number,';
      --field 'Trend'
      v_strSQL := v_strSQL || 'Trend number,';
      --field 'Sales (-12M)'
      v_strSQL := v_strSQL || 'F_Sales number,';
      --field 'Previous financial year'
      v_strSQL := v_strSQL || 'Previous_financial_year number,';
      --field 'Forecast (+12M)'
      v_strSQL := v_strSQL || 'Forecast number,';
      --field 'Current year'
      v_strSQL := v_strSQL || 'Current_year number,';
      --field 'Sales to date'
      v_strSQL := v_strSQL || 'Sales_date number,';
      v_strSQL := v_strSQL || 'Next_financial_year number,';
      --field 'Next financial year: target column'
      --field 'Alarm'
      v_strSQL := v_strSQL || 'Alarm number,';
      /*
      --field 'Standard deviation'
      v_strSQL := v_strSQL || 'Standard_deviation number,';*/
      --field 'Absolute mean deviation'
      v_strSQL := v_strSQL || 'Absolute_mean_deviation number,';
      --field '% Forecast deviation'
      v_strSQL := v_strSQL || 'Percent_Forecast_deviation number,';
    
      --field '% Deviation/Average'
      v_strSQL := v_strSQL || 'Percent_Deviation_Average number,';
    
      /*
      --field '% Forecast deviation(-6M)'
      v_strSQL := v_strSQL || 'Percent_Forecast_deviation_6M number,';*/
      --field 'Continuation of Factor (%)'
      v_strSQL := v_strSQL || 'Percent_Continuation_Factor number,';
      --field 'Offset-Periods'
      v_strSQL := v_strSQL || 'Offset_Periods number,';
      --field 'Start date for forecast'
      v_strSQL := v_strSQL || 'forecast_Start_date number,';
      --field 'Start date for seasonality'
      v_strSQL := v_strSQL || 'seasonality_Start_date number,';
      --field 'End date for seasonality'
      v_strSQL := v_strSQL || 'seasonality_End_date number,';
      --field 'Historical smoothing Filter'
      v_strSQL := v_strSQL || 'Historical_smoothing_Filter number,';
      --field 'Start date for continued history'
      v_strSQL := v_strSQL || 'continued_history_Start_date  number,';
      --field 'End date for continued history'
      v_strSQL := v_strSQL || 'continued_history_End_date  number,';
      --field 'External Data table'
      v_strSQL := v_strSQL || 'External_Data_table  varchar2(45),';
      --field 'Display horizon'
      v_strSQL := v_strSQL || 'Display_horizon  number,';
      --field 'Short term Forecast - Forecast horizon'
      v_strSQL := v_strSQL || 'Short_term_horizon  number,';
      --field 'Short term Forecast - Pds of history'
      v_strSQL := v_strSQL || 'Short_term_horizon_Pds  number,';
      --field 'Short term Forecast - Trend'
      v_strSQL := v_strSQL || 'Short_term_Forecast_Trend  number,';
      --field 'Short term Forecast - Seasonality '
      v_strSQL := v_strSQL || 'Short_term_Forecast_Seasonal  number,';
      --field 'Managing Extremities (0=no, 1=yes)'
      v_strSQL := v_strSQL || 'Managing_Extremities  number,';
      --field 'Max no of periods for seasonality'
      v_strSQL := v_strSQL || 'Max_periods_seasonality  number,';
      --field 'Best Fit Model (0 : unchecked; 1 : checked)'
      v_strSQL := v_strSQL || 'Best_Fit_Model  number,';
      --field 'Smoothing coefficient (trend) (only with the Winters mode)'
      v_strSQL := v_strSQL || 'Smoothing_coefficient_trend  number,';
      --field 'Trend (only with the Winters mode)'
      v_strSQL := v_strSQL || 'Trend_Winters  number';
      -- v_strSQL := v_strSQL || 'Trend_Winters  number,';
      --field 'R2'
      -- v_strSQL := v_strSQL || 'R2 number';
    end if;
  
    --0 is  -hasbestfitrule 1 is none
    if p_nBestfitRuleFlag = 0 then
      v_strSQL := v_strSQL || ',SZBESTFITRULENAME varchar2(60) ';
      v_strSQL := v_strSQL || ',SZBESTFITRULEDESC varchar2(120) ';
    end if;
    v_strSQL := v_strSQL || ')';
  
    --execute
    execute immediate v_strSQL;
    commit;
    p_nSqlCode := 0;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
      raise;
  end sp_CreateTempTableParameter;

  procedure fmsp_CreateTempTabExpForecast(p_nObjectCode      in number,
                                          p_strFMUSER        in varchar2,
                                          P_StrOption        in varchar2,
                                          p_strTableName     out varchar2,
                                          p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                          p_nSqlCode         in out integer)
  --*****************************************************************
    -- Description: CreateTempTabExpForecast procedure
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strSeparator
    --       p_strTableName
    --       p_nBestfitRuleFlag
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
  
   as
    v_nSqlCode         number;
    v_strTableName     varchar2(100) := '';
    v_nBestfitRuleFlag number := 1;
  begin
    if p_nObjectCode in (1051, 1052) then
      sp_CreateTempTableNote(p_nObjectCode,
                             p_strFMUSER,
                             P_StrOption,
                             v_strTableName,
                             v_nSqlCode);
    
    elsif p_nObjectCode not in (1051, 1052) then
      v_nBestfitRuleFlag := p_nBestfitRuleFlag;
      sp_CreateTempTableParameter(p_nObjectCode,
                                  p_strFMUSER,
                                  P_StrOption,
                                  v_strTableName,
                                  v_nBestfitRuleFlag, --0 is  -hasbestfitrule 1 is none
                                  v_nSqlCode);
    end if;
  
    p_strTableName := v_strTableName;
    p_nSqlCode     := v_nSqlCode;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
  end;

  procedure sp_GetSelectField(p_strTableName in varchar2,
                              P_OrderFlag    in Number,
                              p_ColumnId     in Number,
                              p_strSQLField  out varchar2,
                              p_nSqlCode     out integer)
  --*****************************************************************
    -- Description: sp_GetSelectField procedure
    --
    -- Parameters:
    --       p_strTableName
    --       P_OrderFlag
    --       p_ColumnId
    --       p_strSQLField
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   is
    p_nCount Number := 0;
  
  begin
  
    --Get Select Field sql
    p_strSQLField := '';
    if P_OrderFlag = 0 then
      for v_cur in (select t.*
                      from user_tab_columns t
                     where t.TABLE_NAME = upper(p_strTableName)
                       and t.COLUMN_ID >= p_ColumnId
                     order by t.COLUMN_ID) loop
        p_strSQLField := p_strSQLField || 't.' || v_cur.column_name || ',';
      
      end loop;
    elsif P_OrderFlag = 1 then
      select count(1)
        into p_nCount
        from user_tab_columns t
       where t.TABLE_NAME = upper(p_strTableName);
      p_nCount := p_nCount - 1;
    
      for v_cur in (select t.*
                      from user_tab_columns t
                     where t.TABLE_NAME = upper(p_strTableName)
                       and t.COLUMN_ID <= p_nCount
                     order by t.COLUMN_ID) loop
        p_strSQLField := p_strSQLField || 't.' || v_cur.column_name || ',';
      end loop;
    end if;
    p_strSQLField := substr(trim(p_strSQLField),
                            1,
                            length(trim(p_strSQLField)) - 1);
  
  end;

  procedure sp_PutDataToTmpTableParameter(p_nObjectCode      in number,
                                          p_strFMUSER        in varchar2,
                                          P_StrOption        in varchar2,
                                          p_strTableName     in out varchar2,
                                          p_nBestfitRuleFlag in number,
                                          p_strSeparator     in varchar2,
                                          p_nDecimals        in number, --Decimals config
                                          p_nSqlCode         out integer)
  --*****************************************************************
    -- Description: save forecast parameter data to  temp table
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strTableName
    --       p_nBestfitRuleFlag
    --       p_strSeparator
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
   is
    v_strSQL         varchar2(30000) := '';
    v_strheader      varchar2(30000) := '';
    v_strCreateTable varchar2(30000) := '';
    v_strSQL0        varchar2(30000) := '';
    v_strSQL1        varchar2(30000) := '';
    v_strSQL2        varchar2(30000) := '';
    v_strSQL3        varchar2(30000) := '';
    v_strSQL4        varchar2(30000) := '';
    v_strSQL5        varchar2(30000) := '';
    v_strSQLField    varchar2(30000) := '';
    v_selectFieldAll varchar2(30000) := '';
  
    v_strSQLNOMTotal1 varchar2(30000) := '';
  
    v_strSQLNOMTotal3 varchar2(30000) := '';
  
    v_strSQLNOMTotal5 varchar2(30000) := '';
  
    v_strSQLMOD_BDG varchar2(30000) := '';
    v_strSQLColumn0 varchar2(30000) := '';
    v_strSQLColumn1 varchar2(30000) := '';
    v_strSQLColumn2 varchar2(30000) := '';
    v_strSQLColumn3 varchar2(30000) := '';
    v_strSQLColumn4 varchar2(30000) := '';
    v_strSQLColumn5 varchar2(30000) := '';
  
    v_strSQLColumnNoMTotal1 varchar2(30000) := '';
    v_strSQLColumnNoMTotal3 varchar2(30000) := '';
    v_strSQLColumnNoMTotal5 varchar2(30000) := '';
  
    v_strFrom  varchar2(30000) := '';
    v_strWhere varchar2(30000) := '';
  
    v_view          varchar2(100) := '';
    v_strTableName1 varchar2(100) := '';
    v_strTableName2 varchar2(100) := '';
    v_strTableName3 varchar2(100) := '';
    v_strTableName4 varchar2(100) := '';
    v_strTableName5 varchar2(100) := '';
    v_strTableName6 varchar2(100) := '';
    v_strTableName7 varchar2(100) := '';
    v_strTableName8 varchar2(100) := '';
    v_strTableName9 varchar2(100) := '';
    v_strselvalue   varchar2(1000) := '';
  
    v_StrOption       varchar2(3000);
    v_g_FMRT_Switches FMP_Batch.g_FMRT_Switches;
    pIn_nNodeType     number;
    pIn_vSQLField     varchar2(3000);
    v_strNodeName     varchar2(3000) := '';
  
    v_delimiter       varchar2(100);
    v_strSeparator    varchar2(100);
    v_nNumMod         number;
    v_nSelOrAggRuleID number;
  
    v_strDecimalsValue varchar2(4000) := '';
    picount            number;
  begin
  
    fmp_log.FMP_SetValue(p_nObjectCode);
    fmp_log.FMP_SetValue(p_strFMUSER);
    fmp_log.FMP_SetValue(P_StrOption);
    fmp_log.FMP_SetValue(p_strTableName);
    fmp_log.FMP_SetValue(p_nBestfitRuleFlag);
    fmp_log.FMP_SetValue(p_strSeparator);
    fmp_log.FMP_SetValue(p_nDecimals);
    fmp_log.LOGBEGIN;
  
    v_StrOption        := upper(P_StrOption);
    v_strSeparator     := p_strSeparator;
    v_strDecimalsValue := '';
    if p_nDecimals > 0 then
      for picount in 1 .. p_nDecimals loop
        v_strDecimalsValue := v_strDecimalsValue || '0';
      end loop;
    end if;
  
    if v_strSeparator is null then
      v_strSeparator := ',';
    end if;
  
    --delimiter
    if v_g_FMRT_Switches.sdlt then
      v_delimiter := '';
    else
      v_delimiter := '"';
    end if;
  
    if v_g_FMRT_Switches.version = 1 then
      v_nNumMod := 49;
    elsif v_g_FMRT_Switches.version = 2 then
      v_nNumMod := 53;
    elsif v_g_FMRT_Switches.version = 3 then
      v_nNumMod := 54;
    elsif v_g_FMRT_Switches.version = 4 then
      v_nNumMod := 55;
    elsif v_g_FMRT_Switches.version is null then
      v_nNumMod := 49;
    else
      null;
    end if;
  
    -- if  v_StrOption is null set 'a',for instr function
    if v_StrOption IS NULL then
      v_StrOption := 'A';
    end if;
  
    if p_nObjectCode in (51, 2072, 2076, 2080) then
      --get deteil node product key , sales key ,trade key
      v_strheader := ' select
                             g.f_cle PRODUCT,
                             g.g_cle SALES,
                             g.d_cle TRADE, ';
      --v_view      := ' v_pvt_three_key g ';
      v_view := ' v_bdg_pvt_three_key g ';
    elsif p_nObjectCode in (52, 2074, 2078, 2082) then
      --get aggre node key, product key , sales key ,trade key
      v_strheader := ' select g.sel_cle AGGNODE,
                             g.f_cle PRODUCT,
                             g.g_cle SALES,
                             g.d_cle TRADE, ';
      --v_view      := ' v_sel_threeKey g ';
      v_view := ' v_bdg_sel_three_key g ';
    end if;
  
    v_strTableName1 := fmf_gettmptablename();
  
    -- create temp table of mod_forecast and SUPPLIER correlation of the field
    v_strCreateTable := 'CREATE TABLE ' || v_strTableName1 || ' AS ';
    v_strSQL0        := ' SELECT ';
    v_strSQLMOD_BDG  := ' t.mod_em_addr,
                          t.bdg_em_addr, ';
    v_strSQL1        := '
             t.TYPE_PARAM F_MODEL,
             n.REFERENCE_NODE   REFERENCE_NODE,
             t.NBPERIODE  Max_periods,
             cast(decode(to_number(to_char(t.DEBUT_UTIL_ANNEE)||to_char(t.DEBUT_UTIL_PERIODE)),0,null,to_number(to_char(t.DEBUT_UTIL_ANNEE)||case when t.DEBUT_UTIL_PERIODE<10 then ''' || '0' ||
                        '''||t.DEBUT_UTIL_PERIODE else to_char(t.DEBUT_UTIL_PERIODE) end )) as number) START_HISTORY,
             cast(decode(to_number(to_char(t.DATE_FIN_HISTO_ANNEE)||to_char(t.DATE_FIN_HISTO_PERIODE)),0,null,to_char(t.DATE_FIN_HISTO_ANNEE)|| case when t.DATE_FIN_HISTO_PERIODE<10 then ''' || '0' ||
                        '''||t.DATE_FIN_HISTO_PERIODE else to_char(t.DATE_FIN_HISTO_PERIODE) end) as number) End_History,
             t.Horizon HORIZON,
             cast(decode(to_number(to_char(t.DATE_FIN_PREV_ANNEE)||to_char(t.Date_fin_prev_periode)),0,null,to_number(to_char(t.DATE_FIN_PREV_ANNEE)||
                        case when t.Date_fin_prev_periode >0 and t.Date_fin_prev_periode<10 then ''' || '0' ||
                        '''||to_char(t.Date_fin_prev_periode) else to_char(t.Date_fin_prev_periode) end )) as number) END_FORECAST_DATE,
             t.TYPE_ID  trend_type,
             t.ALPHA_INIT SMOOTHING_COEFFICIENT,
             t.ADAPT_ALPHA  AUTOADAPTATION,';
  
    v_strSQLColumn1 := '  t.F_MODEL,
                          t.REFERENCE_NODE,
                          t.MAX_PERIODS,
                          t.START_HISTORY,
                          t.END_HISTORY,
                          t.HORIZON,
                          t.END_FORECAST_DATE,
                          t.TREND_TYPE,
                          t.SMOOTHING_COEFFICIENT,
                          t.AUTOADAPTATION,';
  
    --SCL.SCL_CLE/MOD SCL Trading_Day_table,
    v_strSQL2 := ' s.SCL_CLE Trading_Day_table,';
    v_strSQL3 := '
             t.OBJECTIF TARGET_1 ,
             t.VALOBJECTIF Value_Target_1 ,
             t.TYPE_OBJECTIF  Target_profile ,
             CAST(decode(to_number(to_char(t.DATE_DEB_OBJ_ANNEE)||to_char(t.DATE_DEB_OBJ_PERIODE)),0,null,to_number(to_char(t.DATE_DEB_OBJ_ANNEE)||case when t.DATE_DEB_OBJ_PERIODE<10 then ''' || '0' ||
                 '''||t.DATE_DEB_OBJ_PERIODE else to_char(t.DATE_DEB_OBJ_PERIODE) end)) AS number) TARGET_START_DATE,
             CAST(decode(to_number(to_char(t.DATE_FIN_OBJ_ANNEE)||to_char(t.DATE_FIN_OBJ_PERIODE)),0,null,to_number(to_char(t.DATE_FIN_OBJ_ANNEE)||case when t.DATE_FIN_OBJ_PERIODE<10 then ''' || '0' ||
                 '''||t.DATE_FIN_OBJ_PERIODE else to_char(t.DATE_FIN_OBJ_PERIODE) end)) AS number) Target_End_date,
             t.OBJECTIF2 TARGET_2,
             t.VALOBJECTIF2  Value_Target_2 ,
             t.AVEC_AS CORRECTED_EXT_EVENT ,
             t.MAJ_BATCH RETAINED_SPLIT,
             t.FORCE_SAIS  CALCULATED_SEASONAL_CORRECTION ,
             CAST(t.COEF_SAIS1 AS NUMBER(10,2)) Seasonality_Month_1 ,
             CAST(t.COEF_SAIS2 AS NUMBER(10,2))  Seasonality_Month_2 ,
             CAST(t.COEF_SAIS3 AS NUMBER(10,2))  seasonality_month_3 ,
             CAST(t.COEF_SAIS4 AS NUMBER(10,2))  seasonality_month_4 ,
             CAST(t.COEF_SAIS5 AS NUMBER(10,2))  seasonality_month_5 ,
             CAST(t.COEF_SAIS6 AS NUMBER(10,2))  seasonality_month_6 ,
             CAST(t.COEF_SAIS7 AS NUMBER(10,2))  seasonality_month_7 ,
             CAST(t.COEF_SAIS8 AS NUMBER(10,2))  seasonality_month_8 ,
             CAST(t.COEF_SAIS9 AS NUMBER(10,2))  seasonality_month_9 ,
             CAST(t.COEF_SAIS10 AS NUMBER(10,2)) seasonality_month_10 ,
             CAST(t.COEF_SAIS11 AS NUMBER(10,2)) seasonality_month_11 ,
             CAST(t.COEF_SAIS12 AS NUMBER(10,2)) seasonality_month_12 ,
             CAST(decode(to_number(to_char(t.date_prev_annee)||to_char(t.date_prev_periode)),0,null,to_number(to_char(t.date_prev_annee)||case when t.date_prev_periode<10 then ''' || '0' ||
                 '''||t.date_prev_periode else to_char(t.date_prev_periode) end)) as number) HISTORY_END,
             t.totmoins12 F_SALES,
             t.totplus12 Forecast,
             t.totplus12_obj Forecast_target_column ,
             t.RATIO_12 Percent_12M ,
             t.RATIO_12_OBJ Percent_12M_target_column ,
             t.TOTANPREC Previous_financial_year ,
             t.prevancours Current_year ,
             t.prevancours_obj Current_year_target_column ,
             t.RATIO_COUR_PREC Precent_Previous_Current_year ,
             t.RATIO_COUR_PREC_OBJ Precent_Previ_Cur_year_target ,
             t.PREVANSUIV Next_financial_year ,
             t.PREVANSUIV_OBJ Next_financial_year_target ,
             t.RATIO_SUIV_COUR Percent_next_fin_year ,
             t.RATIO_SUIV_COUR_OBJ Percent_next_fin_year_target ,
             t.TOTANCOURS Sales_date ,
             t.RATIO_REAL Percent_Sales_date ,
             t.RATIO_REAL_OBJ Percent_Sales_date_target ,
             t.reste_a_faire Balance_sales_achieve ,
             t.reste_a_faire_obj Balance_sales_achieve_target ,
             t.ratio_a_faire Percent_Balance_sales_achieve ,
             t.ratio_a_faire_obj Percent_Balan_achieve_target ,
             t.MOYENNE F_Mean ,
             t.TENDANCE Trend ,
             t.AWS Alarm ,
             t.NF Standard_deviation ,
             t.MAD Absolute_mean_deviation ,
             t.ERR2 Percent_Deviation_Average ,
             t.ERR1 Percent_Forecast_deviation ,
             t.ERR_PRV_6M Percent_Forecast_deviation_6M ,
             t.TAUX_SUITE_DE Percent_Continuation_Factor ,
             t.DECALAGE Offset_Periods ,
             CAST(decode(to_number(to_char(t.DATE_DEB_PREV_ANNEE)||to_char(t.DATE_DEB_PREV_PERIODE)),0,null,to_number(to_char(t.DATE_DEB_PREV_ANNEE)||case when t.DATE_DEB_PREV_PERIODE<10 then ''' || '0' ||
                 '''||t.DATE_DEB_PREV_PERIODE else to_char(t.DATE_DEB_PREV_PERIODE) end)) as number) forecast_Start_date,
             CAST(decode(to_number(to_char(t.debut_util_saison_annee)||to_char(t.debut_util_saison_PERIODE)),0,null,to_number(to_char(t.debut_util_saison_annee)||case when t.debut_util_saison_PERIODE<10 then ''' || '0' ||
                 '''||t.debut_util_saison_PERIODE else to_char(t.debut_util_saison_PERIODE) end)) as number) seasonality_Start_date,
             CAST(decode(to_number(to_char(t.fin_util_saison_annee)||to_char(t.fin_util_saison_PERIODE)),0,null,to_number(to_char(t.fin_util_saison_annee)||case when t.fin_util_saison_PERIODE<10 then ''' || '0' ||
                 '''||t.fin_util_saison_PERIODE else to_char(t.fin_util_saison_PERIODE) end)) as number)  seasonality_End_date,
             t.FILTRAGE Historical_smoothing_Filter ,
             CAST(decode(to_number(to_char(n.STARTYEAR)||to_char(n.STARTPERIOD)),0,null,to_number(to_char(n.STARTYEAR)||case when n.STARTPERIOD<10 then ''' || '0' ||
                 '''||n.STARTPERIOD else to_char(n.STARTPERIOD) end )) as number) continued_history_Start_date,
             CAST(decode(to_number(to_char(n.ENDYEAR)||to_char(n.ENDPERIOD)),0,null,to_number(to_char(n.ENDYEAR)||case when n.ENDPERIOD<10 then ''' || '0' ||
                 '''||n.ENDPERIOD else to_char(n.ENDPERIOD) end)) AS NUMBER) continued_history_End_date,';
  
    v_strSQLColumn3 := 't.TARGET_1,
                        t.VALUE_TARGET_1,
                        t.TARGET_PROFILE,
                        t.TARGET_START_DATE,
                        t.TARGET_END_DATE,
                        t.TARGET_2,
                        t.VALUE_TARGET_2,
                        t.CORRECTED_EXT_EVENT,
                        t.RETAINED_SPLIT,
                        t.CALCULATED_SEASONAL_CORRECTION,
                        t.SEASONALITY_MONTH_1,
                        t.SEASONALITY_MONTH_2,
                        t.SEASONALITY_MONTH_3,
                        t.SEASONALITY_MONTH_4,
                        t.SEASONALITY_MONTH_5,
                        t.SEASONALITY_MONTH_6,
                        t.SEASONALITY_MONTH_7,
                        t.SEASONALITY_MONTH_8,
                        t.SEASONALITY_MONTH_9,
                        t.SEASONALITY_MONTH_10,
                        t.SEASONALITY_MONTH_11,
                        t.SEASONALITY_MONTH_12,
                        t.HISTORY_END,
                        t.F_SALES,
                        t.FORECAST,
                        t.FORECAST_TARGET_COLUMN,
                        t.PERCENT_12M,
                        t.PERCENT_12M_TARGET_COLUMN,
                        t.PREVIOUS_FINANCIAL_YEAR,
                        t.CURRENT_YEAR,
                        t.CURRENT_YEAR_TARGET_COLUMN,
                        t.PRECENT_PREVIOUS_CURRENT_YEAR,
                        t.PRECENT_PREVI_CUR_YEAR_TARGET,
                        t.NEXT_FINANCIAL_YEAR,
                        t.NEXT_FINANCIAL_YEAR_TARGET,
                        t.PERCENT_NEXT_FIN_YEAR,
                        t.PERCENT_NEXT_FIN_YEAR_TARGET,
                        t.SALES_DATE,
                        t.PERCENT_SALES_DATE,
                        t.PERCENT_SALES_DATE_TARGET,
                        t.BALANCE_SALES_ACHIEVE,
                        t.BALANCE_SALES_ACHIEVE_TARGET,
                        t.PERCENT_BALANCE_SALES_ACHIEVE,
                        t.PERCENT_BALAN_ACHIEVE_TARGET,
                        t.F_MEAN,
                        t.TREND,
                        t.ALARM,
                        t.STANDARD_DEVIATION,
                        t.ABSOLUTE_MEAN_DEVIATION,
                        t.PERCENT_DEVIATION_AVERAGE,
                        t.PERCENT_FORECAST_DEVIATION,
                        t.PERCENT_FORECAST_DEVIATION_6M,
                        t.PERCENT_CONTINUATION_FACTOR,
                        t.OFFSET_PERIODS,
                        t.FORECAST_START_DATE,
                        t.SEASONALITY_START_DATE,
                        t.SEASONALITY_END_DATE,
                        t.HISTORICAL_SMOOTHING_FILTER,
                        t.CONTINUED_HISTORY_START_DATE,
                        t.CONTINUED_HISTORY_END_DATE,';
  
    v_strSQL4 := ' j.SCL_CLE External_Data_table,'; --SCL   External_Data_table
    v_strSQL5 := '
         t.HORIZON_AFFICHAGE Display_horizon              ,
         t.HORIZONFUTUR  Short_term_horizon           ,
         t.HORIZONPASSE   Short_term_horizon_Pds       ,
         t.CHOIXFONCTION   Short_term_Forecast_Trend    ,
         t.SAISONNALITE   Short_term_Forecast_Seasonal ,
         t.GESTIONDESBORDS  Managing_Extremities         ,
         t.MAX_NBPERIODE_SAIS   Max_periods_seasonality      ,
         t.BESTFIT   Best_Fit_Model         ,
         t.TAUX_EXPLIT   Smoothing_coefficient_trend,
         t.HAUTEUR_REAPROVIS  Trend_Winters,
         t.COEF_CORREL_R2     COEF_CORREL_R2';
    if p_nBestfitRuleFlag = 0 then
      v_strSQL5 := v_strSQL5 ||
                   ',t.SZBESTFITRULENAME SZBESTFITRULENAME
                    ,t.SZBESTFITRULEDESC SZBESTFITRULEDESC ';
    
    end if;
    v_strSQLColumn5 := '  t.DISPLAY_HORIZON,
                          t.SHORT_TERM_HORIZON,
                          t.SHORT_TERM_HORIZON_PDS,
                          t.SHORT_TERM_FORECAST_TREND,
                          t.SHORT_TERM_FORECAST_SEASONAL,
                          t.MANAGING_EXTREMITIES,
                          t.MAX_PERIODS_SEASONALITY,
                          t.BEST_FIT_MODEL,
                          t.SMOOTHING_COEFFICIENT_TREND,
                          t.TREND_WINTERS,
                          t.COEF_CORREL_R2';
  
    if p_nBestfitRuleFlag = 0 then
      v_strSQLColumn5 := v_strSQLColumn5 ||
                         ',t.SZBESTFITRULENAME
                    ,t.SZBESTFITRULEDESC ';
    
    end if;
  
    v_strFrom  := ' from mod_forecast t left join
                    (
                      select m.fils_bdg,g.b_cle REFERENCE_NODE,m.STARTYEAR,m.startperiod,m.endyear,m.endperiod,m.bdg51_em_addr
                      from SUPPLIER m, bdg g
                      where m.id_supplier=83 and m.fils_bdg=g.bdg_em_addr(+)
                     ) n on t.BDG_EM_ADDR = n.BDG51_EM_ADDR';
    v_strWhere := ' where    t.num_mod=' || v_nNumMod;
  
    -- mtotal switch
    if instr(v_StrOption, 'MTOTAL') > 0 then
      v_strSQL := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                  v_strSQL1 || v_strSQL3 || v_strSQL5 || v_strFrom ||
                  v_strWhere;
    
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
      execute immediate v_strSQL;
      --For "Model Based on¡­ %", some nodes' Model is Continuation of*, but Based on is empty. it should be empty, not 100%.
      --but the based on is not NULL,and the 5 is null ,it should be 100 
      fmsp_execsql(pIn_cSql => 'update   ' || v_strTableName1 ||
                               ' t set t.Percent_Continuation_Factor=100 
      where t.reference_node is not null and t.f_model in (8, 17, 18, 19, 21, 25) and t.Percent_Continuation_Factor is null');
      v_strTableName2 := fmf_gettmptablename();
    
      --create temp table for view v_scl_tradingdaytable
      v_strCreateTable := 'CREATE TABLE ' || v_strTableName2 || ' AS ';
    
      v_strFrom := ' from ' || v_strTableName1 ||
                   ' t, v_scl_tradingdaytable s';
    
      v_strWhere := ' where t.MOD_EM_ADDR = s.MOD42_EM_ADDR(+) ';
    
      v_strSQL := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                  v_strSQLColumn1 || v_strSQL2 || v_strSQLColumn3 ||
                  v_strSQLColumn5 || v_strFrom || v_strWhere;
    
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
    
      execute immediate v_strSQL;
    
      v_strTableName3 := fmf_gettmptablename();
    
      --create temp table for view v_scl_external_data
      v_strCreateTable := 'CREATE TABLE ' || v_strTableName3 || ' AS ';
    
      v_strFrom := ' from ' || v_strTableName2 ||
                   ' t, v_scl_external_data j';
    
      v_strWhere := ' where t.MOD_EM_ADDR = j.MOD42_EM_ADDR(+) ';
      v_strSQL2  := 't.Trading_Day_table,';
      v_strSQL   := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                    v_strSQLColumn1 || v_strSQL2 || v_strSQLColumn3 ||
                    v_strSQL4 || v_strSQLColumn5 || v_strFrom || v_strWhere;
    
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
    
      execute immediate v_strSQL;
    
      --from  part sql
      v_strFrom := ' FROM ' || v_strTableName3 || ' t, ' || v_view;
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        v_strWhere := 'Where t.bdg_em_addr=g.pvt_em_addr';
      
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        v_strWhere := 'Where t.bdg_em_addr=g.sel_em_addr';
      end if;
    
      v_strSQL2 := 't.Trading_Day_table,';
      v_strSQL4 := 't.External_Data_table,';
      /*      v_strSQL  := 'INSERT INTO ' || p_strTableName;
      v_strSQL  := v_strSQL || v_strheader || v_strSQLColumn1 || v_strSQL2 ||
                   v_strSQLColumn3 || v_strSQL4 || v_strSQLColumn5 ||
                   v_strFrom || v_strWhere;*/
    
      --create temp table
      v_strTableName4 := fmf_gettmptablename();
      v_strSQL        := 'Create Table ' || v_strTableName4 || ' AS ';
      v_strSQL        := v_strSQL || v_strheader || v_strSQLColumn1 ||
                         v_strSQL2 || v_strSQLColumn3 || v_strSQL4 ||
                         v_strSQLColumn5 || ',t.bdg_em_addr ' || v_strFrom ||
                         v_strWhere;
    
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
      execute immediate v_strSQL;
    
      --insert into
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        v_strWhere    := 'Where g.pvt_em_addr=t.bdg_em_addr(+) and t.bdg_em_addr is null ';
        v_strNodeName := ' g.pvt_em_addr ';
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        v_strWhere    := 'Where g.sel_em_addr=t.bdg_em_addr(+) and t.bdg_em_addr is null ';
        v_strNodeName := ' g.sel_em_addr ';
      end if;
    
      /*      --insert no modforecast data detailnode and aggndoe
        if p_nBestfitRuleFlag <> 0 then
          v_strSQL := 'insert into ' || v_strTableName4 || v_strheader ||
                      '0, null, 108, null, null, 72, 0, 1, 0.06, 1, null, 0, null, 0, 0, 0, 0, null, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, null,
                      null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                      null, null, null, null, null, null, null, 0, null, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0' || ',' ||
                      v_strNodeName || v_strFrom || v_strWhere;
        elsif p_nBestfitRuleFlag = 0 then
          v_strSQL := 'insert into ' || v_strTableName4 || v_strheader ||
                      '0, null, 108, null, null, 72, null, 1, 0.06, 1, null, 0, null, 0, null, null, 0, null, 1, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                       null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                       null, null, null, null, null, null, null, null, null, 0, null, null, null, 0, null, null, null, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0,null,
                       null,null' || ',' || v_strNodeName ||
                      v_strFrom || v_strWhere;
        end if;
      
        fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
        execute immediate v_strSQL;
        commit;
      */
      pIn_vSQLField := '';
    
      --Parse P_StrOption to g_FMRT_Switches type object
      FMP_Batch.FMSP_Parse(v_StrOption, v_g_FMRT_Switches, p_nSqlCode);
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        -- detail node
        pIn_nNodeType := 1;
        sp_GetSelectField(v_strTableName4, 0, 4, pIn_vSQLField, p_nSqlCode);
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        --aggre node
        pIn_nNodeType := 2;
        sp_GetSelectField(v_strTableName4, 0, 5, pIn_vSQLField, p_nSqlCode);
      end if;
    
      --batch swicth
      FMP_Batch.FMSP_ExpNode(pIn_nNodeType,
                             v_strTableName4,
                             v_g_FMRT_Switches,
                             pIn_vSQLField,
                             p_strFMUSER,
                             v_strTableName5,
                             p_nSqlCode);
      p_strTableName := v_strTableName5;
    
      sp_GetSelectField(v_strTableName5, 1, 0, v_strSQLField, p_nSqlCode);
      v_strTableName6 := fmf_gettmptablename();
      v_strSQL        := 'Create Table ' || v_strTableName6 ||
                         ' AS select ' || v_strSQLField || ' From ' ||
                         v_strTableName5 || ' t';
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
      execute immediate v_strSQL;
    
      --v_strSeparator
      v_strTableName7 := fmf_gettmptablename();
    
      -- Multiple fields will be merged into one field
      v_strSQL := 'Create Table ' || v_strTableName7 || ' AS Select ';
    
      /*
      for v_cur_name in (select t.*
                           from user_tab_columns t
                          where t.TABLE_NAME = upper(v_strTableName6)
                          order by t.COLUMN_ID) loop
        if v_cur_name.data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then
          v_selectFieldAll := v_selectFieldAll || '''' || v_delimiter || '''' || '||' ||
                              v_cur_name.column_name || '||' || '''' ||
                              v_delimiter || '''' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        else
          v_selectFieldAll := v_selectFieldAll || v_cur_name.column_name || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        end if;
      end loop;*/
    
      for v_cur_name in (select t.*
                           from user_tab_columns t
                          where t.TABLE_NAME = upper(v_strTableName6)
                          order by t.COLUMN_ID) loop
        if v_cur_name.data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then
          v_selectFieldAll := v_selectFieldAll || '''' || v_delimiter || '''' || '||' ||
                              v_cur_name.column_name || '||' || '''' ||
                              v_delimiter || '''' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('SMOOTHING_COEFFICIENT', 'SMOOTHING_COEFFICIENT_TREND') then
          v_selectFieldAll := v_selectFieldAll || ' case when ' ||
                              v_cur_name.column_name || ' =0 then ''' || '0' ||
                              ''' when ' || v_cur_name.column_name ||
                              ' <0 then to_char(' || v_cur_name.column_name ||
                              ',''' || 'SFM9999999990.000' ||
                              ''') else to_char(' || v_cur_name.column_name ||
                              ',''' || 'FM9999999990.000' || ''') end ' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('TARGET_1',
               'VALUE_TARGET_1',
               'F_SALES',
               'FORECAST',
               'FORECAST_TARGET_COLUMN',
               'PREVIOUS_FINANCIAL_YEAR',
               'CURRENT_YEAR',
               'CURRENT_YEAR_TARGET_COLUMN',
               'NEXT_FINANCIAL_YEAR',
               'NEXT_FINANCIAL_YEAR_TARGET',
               'SALES_DATE',
               'BALANCE_SALES_ACHIEVE',
               'BALANCE_SALES_ACHIEVE_TARGET',
               'F_MEAN',
               'TREND',
               'ALARM',
               'STANDARD_DEVIATION',
               'ABSOLUTE_MEAN_DEVIATION',
               'PERCENT_DEVIATION_AVERAGE',
               'PERCENT_FORECAST_DEVIATION',
               'PERCENT_FORECAST_DEVIATION_6M',
               'TREND_WINTERS',
               'COEF_CORREL_R2') then
          if p_nDecimals = 0 then
            v_selectFieldAll := v_selectFieldAll || '  round(' ||
                                v_cur_name.column_name || ')  ' || '||' || '''' ||
                                v_strSeparator || '''' || '||';
          else
            /*v_selectFieldAll := v_selectFieldAll || 'case when ' ||
            v_cur_name.column_name || '=''' || '0' ||
            ''' then ' || '''0' || ''' when ' ||
            v_cur_name.column_name ||
            ' <0 then to_char(' ||
            v_cur_name.column_name || ',''' ||
            'SFM9999999990.' || v_strDecimalsValue ||
            ''') else ' || '  to_char(' ||
            v_cur_name.column_name || ',''' ||
            'FM9999999990.' || v_strDecimalsValue ||
            ''') end ' || '||' || '''' ||
            v_strSeparator || '''' || '||';*/
          
            v_selectFieldAll := v_selectFieldAll || 'case when ' ||
                                v_cur_name.column_name ||
                                ' <0 then to_char(' ||
                                v_cur_name.column_name || ',''' ||
                                'SFM9999999990.' || v_strDecimalsValue ||
                                ''') else ' || '  to_char(' ||
                                v_cur_name.column_name || ',''' ||
                                'FM9999999990.' || v_strDecimalsValue ||
                                ''') end ' || '||' || '''' ||
                                v_strSeparator || '''' || '||';
          end if;
        
        elsif v_cur_name.column_name in
              ('PERCENT_12M',
               'PERCENT_12M_TARGET_COLUMN',
               'PRECENT_PREVIOUS_CURRENT_YEAR',
               'PRECENT_PREVI_CUR_YEAR_TARGET',
               'PERCENT_NEXT_FIN_YEAR',
               'PERCENT_NEXT_FIN_YEAR_TARGET',
               'PERCENT_SALES_DATE',
               'PERCENT_SALES_DATE_TARGET',
               'PERCENT_BALANCE_SALES_ACHIEVE',
               'PERCENT_BALAN_ACHIEVE_TARGET') then
          v_selectFieldAll := v_selectFieldAll || ' case when ' ||
                              v_cur_name.column_name || '<0 then to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'SFM9999999990.0' || ''') else to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9999999990.0' || ''') end ' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        
        elsif v_cur_name.column_name in
              ('PERCENT_CONTINUATION_FACTOR',
               'SEASONALITY_MONTH_1',
               'SEASONALITY_MONTH_2',
               'SEASONALITY_MONTH_3',
               'SEASONALITY_MONTH_4',
               'SEASONALITY_MONTH_5',
               'SEASONALITY_MONTH_6',
               'SEASONALITY_MONTH_7',
               'SEASONALITY_MONTH_8',
               'SEASONALITY_MONTH_9',
               'SEASONALITY_MONTH_10',
               'SEASONALITY_MONTH_11',
               'SEASONALITY_MONTH_12') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9999999990.00' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        else
          v_selectFieldAll := v_selectFieldAll || v_cur_name.column_name || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        end if;
      end loop;
    
      v_selectFieldAll := trim(v_selectFieldAll);
      v_selectFieldAll := substr(v_selectFieldAll,
                                 1,
                                 length(v_selectFieldAll) - 7);
    
      v_strSQL := v_strSQL || v_selectFieldAll || ' AS FORECASTINFO From ' ||
                  v_strTableName6;
    
      fmp_log.LOGDEBUG(pIn_vText => gvDebugText, pIn_cSqlText => v_strSQL);
      execute immediate v_strSQL;
    
      v_strTableName8 := fmf_gettmptablename();
    
      --Sel switch
      if instr(v_StrOption, 'SEL') > 0 then
        v_strselvalue := substr(v_StrOption, instr(v_StrOption, 'SEL') + 4);
        if instr(v_strselvalue, '##') > 0 then
          v_strselvalue := substr(v_strselvalue,
                                  1,
                                  instr(v_strselvalue, '##') - 1);
        end if;
      
        if p_nObjectCode in (51, 2072, 2076, 2080) then
          select sel.sel_em_addr
            into v_nSelOrAggRuleID
            from sel
           where sel.sel_cle = v_strselvalue;
        elsif p_nObjectCode in (52, 2074, 2078, 2082) then
          select prv.prv_em_addr
            into v_nSelOrAggRuleID
            from prv
           where prv.prv_cle = v_strselvalue;
        end if;
      
        v_strSQL := 'Create Table ' || v_strTableName8 || ' AS ';
      
        if p_nObjectCode in (51, 2072, 2076, 2080) then
          --detail node
          v_strSQL := v_strSQL ||
                      ' select m.pvt14_em_addr node_em_addr
                                from rsp m left join pvt n
                                on m.pvt14_em_addr=n.pvt_em_addr
                                where m.sel13_em_addr=' ||
                      v_nSelOrAggRuleID;
        end if;
      
        if p_nObjectCode in (52, 2074, 2078, 2082) then
          --agg node
          v_strSQL := v_strSQL ||
                      ' select m.sel16_em_addr node_em_addr
                                from prvsel m left outer join sel n
                                on m.sel16_em_addr=n.sel_em_addr
                                where n.sel_bud=71
                                and m.prv15_em_addr=' ||
                      v_nSelOrAggRuleID;
        end if;
      
        /*        execute immediate 'truncate table t_test';
        insert into t_test values (v_strSQL);
        commit;*/
      
        execute immediate v_strSQL;
      
        v_strTableName9 := fmf_gettmptablename();
      
        -- create result temp table
        v_strSQL := ' Create Table ' || v_strTableName9 || ' AS Select ' ||
                    v_selectFieldAll || ' AS FORECASTINFO From ' ||
                    v_strTableName8 || ' m,' || v_strTableName5 ||
                    ' n where m.node_em_addr=n.bdg_em_addr';
      
        /*        execute immediate 'truncate table t_test';
        insert into t_test values (v_strSQL);
        commit;*/
      
        execute immediate v_strSQL;
        p_strTableName := v_strTableName9;
      
        execute immediate 'drop table ' || v_strTableName7;
      
      else
        p_strTableName := v_strTableName7;
        --execute immediate 'drop table ' || v_strTableName9;
      end if;
      -- drop all temp table
      execute immediate 'drop table ' || v_strTableName1;
      execute immediate 'drop table ' || v_strTableName2;
      execute immediate 'drop table ' || v_strTableName3;
      execute immediate 'drop table ' || v_strTableName4;
      execute immediate 'drop table ' || v_strTableName5;
      execute immediate 'drop table ' || v_strTableName6;
    end if;
  
    v_strSQLNOMTotal1 := '  t.TYPE_PARAM        F_Model,
                            cast(decode(n.PERE_BDG,0,null,n.PERE_BDG) as varchar2(100)) REFERENCE_NODE,
                            t.HORIZON           Horizon,
                            --CAST(decode(to_number(to_char(DATE_FIN_PREV_ANNEE)||to_char(Date_fin_prev_periode)),0,null,to_number(to_char(DATE_FIN_PREV_ANNEE)||to_char(Date_fin_prev_periode))) AS NUMBER) End_Forecast_date,
                            t.TYPE_ID           trend_type,
                            t.NBPERIODE         Max_periods,
                            t.ALPHA_INIT        Smoothing_coefficient,
                            t.ADAPT_ALPHA       Autoadaptation,';
  
    v_strSQLColumnNoMTotal1 := 't.F_Model,
                                t.REFERENCE_NODE,
                                t.Horizon,
                                --t.End_Forecast_date,
                                t.trend_type,
                                t.Max_periods,
                                t.Smoothing_coefficient,
                                t.Autoadaptation,';
  
    /*
    
        v_strSQLNOMTotal3 := '      cast(null as varchar2(200))  Trading_Day_table2,
                                    cast(null as varchar2(200))  Trading_Day_table3,
                                    cast(null as varchar2(200))  Trading_Day_table4,
                                    cast(null as varchar2(200))  Trading_Day_table5,
                                    cast(null as varchar2(200))  Trading_Day_table6,
    
    */
  
    v_strSQLNOMTotal3 := '
                                t.OBJECTIF          Target_1,
                                t.VALOBJECTIF       Value_Target_1,
                                t.AVEC_AS           Corrected_by_Ext_Event,
                                t.FORCE_SAIS        Calculated_seasonal_correction,
                                t.COEF_SAIS1        Seasonality_Month_1,
                                t.COEF_SAIS2        Seasonality_Month_2,
                                t.COEF_SAIS3        seasonality_month_3,
                                t.COEF_SAIS4        seasonality_month_4,
                                t.COEF_SAIS5        seasonality_month_5,
                                t.COEF_SAIS6        seasonality_month_6,
                                t.COEF_SAIS7        seasonality_month_7,
                                t.COEF_SAIS8        seasonality_month_8,
                                t.COEF_SAIS9        seasonality_month_9,
                                t.COEF_SAIS10        seasonality_month_10,
                                t.COEF_SAIS11        seasonality_month_11,
                                t.COEF_SAIS12        seasonality_month_12,
                                CAST(decode(to_number(to_char(t.date_prev_annee)||to_char(date_prev_periode)),0,null,to_number(to_char(t.date_prev_annee)||case when t.date_prev_periode<10 then ''' || '0' ||
                         '''||t.date_prev_periode else to_char(t.date_prev_periode) end)) AS number)  history_end,
                                t.MOYENNE           F_Mean,
                                t.TENDANCE          Trend,
                                t.totmoins12        F_Sales,
                                t.TOTANPREC         Previous_financial_year,
                                t.totplus12         Forecast,
                                t.prevancours       Current_year,
                                t.TOTANCOURS        Sales_date,
                                /*t.PREVANSUIV_OBJ    Next_financial_year,*/
                                t.PREVANSUIV        Next_financial_year,
                                t.AWS               Alarm,
                                t.MAD               Absolute_mean_deviation,
                                t.ERR1              Percent_Forecast_deviation,
                                t.ERR2              Percent_Deviation_Average,
                                case when t.type_param in (8, 17, 18, 19, 21, 25) and t.TAUX_SUITE_DE is null
                                then 100 else t.TAUX_SUITE_DE end      Percent_Continuation_Factor,
                                t.DECALAGE          Offset_Periods,
                                CAST(decode(to_number(to_char(DATE_DEB_PREV_ANNEE)||to_char(DATE_DEB_PREV_PERIODE)),0,null,to_number(to_char(DATE_DEB_PREV_ANNEE)||case when t.DATE_DEB_PREV_PERIODE<10 then ''' || '0' ||
                         '''||t.DATE_DEB_PREV_PERIODE else to_char(t.DATE_DEB_PREV_PERIODE) end)) AS number) forecast_Start_date,
                                CAST(decode(to_number(to_char(debut_util_saison_annee)||to_char(debut_util_saison_PERIODE)),0,null,to_number(to_char(debut_util_saison_annee)||case when t.debut_util_saison_PERIODE<10 then ''' || '0' ||
                         '''||t.debut_util_saison_PERIODE else to_char(t.debut_util_saison_PERIODE) end)) AS number) seasonality_Start_date,
                                CAST(decode(to_number(to_char(fin_util_saison_annee)||to_char(fin_util_saison_PERIODE)),0,null,to_number(to_char(fin_util_saison_annee)||case when t.fin_util_saison_PERIODE<10 then ''' || '0' ||
                         '''||t.fin_util_saison_PERIODE else to_char(t.fin_util_saison_PERIODE) end)) AS number) seasonality_End_date,
                                t.FILTRAGE          Historical_smoothing_Filter,
                                CAST(decode(to_number(to_char(n.STARTYEAR)||to_char(n.STARTPERIOD)),0,null,to_number(to_char(n.STARTYEAR)||case when n.STARTPERIOD<10 then ''' || '0' ||
                         '''||n.STARTPERIOD else to_char(n.STARTPERIOD) end)) AS number)    continued_history_Start_date,
                                CAST(Decode(TO_NUMBER(to_char(n.ENDYEAR)||to_char(n.ENDPERIOD)),0,NULL,to_number(to_char(n.ENDYEAR)||case when n.ENDPERIOD<10 then ''' || '0' ||
                         '''||n.ENDPERIOD else to_char(n.ENDPERIOD) end)) AS number)            continued_history_End_date,  ';
  
    /*
    v_strSQLColumnNoMTotal3 := 't.Trading_Day_table2,
                              t.Trading_Day_table3,
                              t.Trading_Day_table4,
                              t.Trading_Day_table5,
                              t.Trading_Day_table6,
                              */
    v_strSQLColumnNoMTotal3 := '
                                t.Target_1,
                                t.Value_Target_1,
                                t.Corrected_by_Ext_Event,
                                t.Calculated_seasonal_correction,
                                t.Seasonality_Month_1,
                                t.Seasonality_Month_2,
                                t.seasonality_month_3,
                                t.seasonality_month_4,
                                t.seasonality_month_5,
                                t.seasonality_month_6,
                                t.seasonality_month_7,
                                t.seasonality_month_8,
                                t.seasonality_month_9,
                                t.seasonality_month_10,
                                t.seasonality_month_11,
                                t.seasonality_month_12,
                                t.history_end,
                                t.F_Mean,
                                t.Trend,
                                t.F_Sales,
                                t.Previous_financial_year,
                                t.Forecast,
                                t.Current_year,
                                t.Sales_date,
                                t.Next_financial_year,
                                t.Alarm,
                                t.Absolute_mean_deviation,
                                t.Percent_Forecast_deviation,
                                t.Percent_Deviation_Average,
                                t.Percent_Continuation_Factor,
                                t.Offset_Periods,
                                t.forecast_Start_date,
                                t.seasonality_Start_date,
                                t.seasonality_End_date,
                                t.Historical_smoothing_Filter,
                                t.continued_history_Start_date,
                                t.continued_history_End_date,';
  
    v_strSQLNOMTotal5 := '      t.HORIZON_AFFICHAGE    Display_horizon,
                                t.HORIZONFUTUR         Short_term_horizon,
                                t.HORIZONPASSE         Short_term_horizon_Pds,
                                t.CHOIXFONCTION        Short_term_Forecast_Trend,
                                t.SAISONNALITE         Short_term_Forecast_Seasonal,
                                t.GESTIONDESBORDS      Managing_Extremities,
                                t.MAX_NBPERIODE_SAIS   Max_periods_seasonality,
                                t.BESTFIT              Best_Fit_Model,
                                t.TAUX_EXPLIT          Smoothing_coefficient_trend,
                                t.HAUTEUR_REAPROVIS    Trend_Winters,
                                t.COEF_CORREL_R2       COEF_CORREL_R2';
  
    if p_nBestfitRuleFlag = 0 then
      v_strSQLNOMTotal5 := v_strSQLNOMTotal5 ||
                           ',t.SZBESTFITRULENAME SZBESTFITRULENAME
                    ,t.SZBESTFITRULEDESC SZBESTFITRULEDESC ';
    
    end if;
    v_strSQLColumnNoMTotal5 := 't.Display_horizon,
                                t.Short_term_horizon,
                                t.Short_term_horizon_Pds,
                                t.Short_term_Forecast_Trend,
                                t.Short_term_Forecast_Seasonal,
                                t.Managing_Extremities,
                                t.Max_periods_seasonality,
                                t.Best_Fit_Model,
                                t.Smoothing_coefficient_trend,
                                t.Trend_Winters,
                                t.COEF_CORREL_R2';
  
    if p_nBestfitRuleFlag = 0 then
      v_strSQLColumnNoMTotal5 := v_strSQLColumnNoMTotal5 ||
                                 ',t.SZBESTFITRULENAME
                    ,t.SZBESTFITRULEDESC  ';
    
    end if;
  
    -- no include mtotal switch
    if instr(v_StrOption, 'MTOTAL') = 0 then
      v_strSQL := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                  v_strSQLNOMTotal1 || v_strSQLNOMTotal3 ||
                  v_strSQLNOMTotal5 || v_strFrom || v_strWhere;
    
      /*      execute immediate 'truncate table  t_test ';
      insert into t_test values (v_strSQL);
      commit;*/
      execute immediate v_strSQL;
    
      /*execute immediate ' update ' || v_strTableName1 ||
                        ' t set t.Trading_Day_table2=null,
                          t.Trading_Day_table3=null,
                          t.Trading_Day_table4=null,
                          t.Trading_Day_table5=null,
                          t.Trading_Day_table6=null';
      
      commit;*/
    
      --  connected view v_scl_tradingdaytable
      v_strTableName2  := fmf_gettmptablename();
      v_strCreateTable := 'CREATE TABLE ' || v_strTableName2 || ' AS ';
    
      v_strFrom := ' from ' || v_strTableName1 ||
                   ' t, v_scl_tradingdaytable s';
    
      v_strWhere := ' where t.MOD_EM_ADDR = s.MOD42_EM_ADDR(+) ';
      v_strSQL2  := ' s.SCL_CLE Trading_Day_table, s.SCL_CLE Trading_Day_table2, s.SCL_CLE Trading_Day_table3, s.SCL_CLE Trading_Day_table4, s.SCL_CLE Trading_Day_table5, s.SCL_CLE Trading_Day_table6,';
      v_strSQL   := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                    v_strSQLColumnNoMTotal1 || v_strSQL2 ||
                    v_strSQLColumnNoMTotal3 || v_strSQLColumnNoMTotal5 ||
                    v_strFrom || v_strWhere;
    
      /*      execute immediate 'truncate table t_test';
      insert into t_test values (v_strSQL);
      commit;*/
      execute immediate v_strSQL;
    
      v_strTableName3 := fmf_gettmptablename();
    
      --connected view v_scl_external_data
      v_strCreateTable := 'CREATE TABLE ' || v_strTableName3 || ' AS ';
    
      v_strFrom := ' from ' || v_strTableName2 ||
                   ' t, v_scl_external_data j';
    
      v_strWhere := ' where t.MOD_EM_ADDR = j.MOD42_EM_ADDR(+) ';
      --v_strSQL2  := 't.Trading_Day_table,';
      v_strSQL2 := 't.Trading_Day_table,t.Trading_Day_table2,t.Trading_Day_table3,t.Trading_Day_table4,t.Trading_Day_table5,t.Trading_Day_table6,';
      v_strSQL  := v_strCreateTable || v_strSQL0 || v_strSQLMOD_BDG ||
                   v_strSQLColumnNoMTotal1 || v_strSQL2 ||
                   v_strSQLColumnNoMTotal3 || v_strSQL4 ||
                   v_strSQLColumnNoMTotal5 || v_strFrom || v_strWhere;
    
      /*      execute immediate 'truncate table  t_test ';
      insert into t_test values (v_strSQL);
      commit;*/
    
      execute immediate v_strSQL;
    
      v_strFrom := ' FROM ' || v_strTableName3 || ' t, ' || v_view;
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        --detail node
        v_strWhere := 'Where t.bdg_em_addr=g.pvt_em_addr';
      
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        --aggre node
        v_strWhere := 'Where t.bdg_em_addr=g.sel_em_addr';
      end if;
    
      --v_strSQL2 := 't.Trading_Day_table,';
      v_strSQL2 := 't.Trading_Day_table,t.Trading_Day_table2,t.Trading_Day_table3,t.Trading_Day_table4,t.Trading_Day_table5,t.Trading_Day_table6,';
      v_strSQL4 := 't.External_Data_table,';
      /*      v_strSQL  := 'INSERT INTO ' || p_strTableName;
      v_strSQL  := v_strSQL || v_strheader || v_strSQLColumnNoMTotal1 ||
                   v_strSQL2 || v_strSQLColumnNoMTotal3 || v_strSQL4 ||
                   v_strSQLColumnNoMTotal5 || v_strFrom || v_strWhere;*/
    
      v_strTableName4 := fmf_gettmptablename();
      v_strSQL        := 'Create Table ' || v_strTableName4 || ' AS ';
      v_strSQL        := v_strSQL || v_strheader || v_strSQLColumnNoMTotal1 ||
                         v_strSQL2 || v_strSQLColumnNoMTotal3 || v_strSQL4 ||
                         v_strSQLColumnNoMTotal5 || ',t.bdg_em_addr ' ||
                         v_strFrom || v_strWhere;
    
      /*    execute immediate 'truncate table  t_test ';
      insert into t_test values (v_strSQL);
      commit;*/
      execute immediate v_strSQL;
    
      --insert into
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        --detail node
        v_strWhere    := 'Where g.pvt_em_addr=t.bdg_em_addr(+) and t.bdg_em_addr is null ';
        v_strNodeName := ' g.pvt_em_addr ';
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        --aggr node
        v_strWhere    := 'Where g.sel_em_addr=t.bdg_em_addr(+) and t.bdg_em_addr is null ';
        v_strNodeName := ' g.sel_em_addr ';
      end if;
    
      --insert no modforecast data detailnode and aggndoe
      if p_nBestfitRuleFlag <> 0 then
        v_strSQL := 'insert into ' || v_strTableName4 || v_strheader ||
                    '0, null, 72, 0, 1, 108, 0.06, 1, null, null, null, null, null, null, 0, null, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, 0, null, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0' || ',' ||
                    v_strNodeName || v_strFrom || v_strWhere;
      elsif p_nBestfitRuleFlag = 0 then
        v_strSQL := 'insert into ' || v_strTableName4 || v_strheader ||
                    '0, null, 72, null, 1, 108, 0.06, 1, null, null, null, null, null, null, 0, null, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, null, null, null, null, null, null, null, null, null, null, null, null, null, 0, null, 0, 0, 0, 0, 0, null, 0, 0, 0, 0, 0, 0, 0, 0, 0.06, 0,null,null,null' || ',' ||
                    v_strNodeName || v_strFrom || v_strWhere;
      
      end if;
    
      /*      execute immediate 'truncate table  t_test ';
      insert into t_test values (v_strSQL);
      commit;*/
      execute immediate v_strSQL;
      commit;
    
      pIn_vSQLField := '';
      FMP_Batch.FMSP_Parse(v_StrOption, v_g_FMRT_Switches, p_nSqlCode);
    
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        --detail node
        pIn_nNodeType := 1;
        sp_GetSelectField(v_strTableName4, 0, 4, pIn_vSQLField, p_nSqlCode);
      elsif p_nObjectCode in (52, 2074, 2078, 2082) then
        -- aggre node
        pIn_nNodeType := 2;
        sp_GetSelectField(v_strTableName4, 0, 5, pIn_vSQLField, p_nSqlCode);
      end if;
    
      --batch swicth
      FMP_Batch.FMSP_ExpNode(pIn_nNodeType,
                             v_strTableName4,
                             v_g_FMRT_Switches,
                             pIn_vSQLField,
                             p_strFMUSER,
                             v_strTableName5,
                             p_nSqlCode);
    
      sp_GetSelectField(v_strTableName5, 1, 0, v_strSQLField, p_nSqlCode);
      v_strTableName6 := fmf_gettmptablename();
      v_strSQL        := 'Create Table ' || v_strTableName6 ||
                         ' AS select ' || v_strSQLField || ' From ' ||
                         v_strTableName5 || ' t';
    
      /*      execute immediate 'truncate table t_test';
      insert into t_test values (v_strSQL);
      commit;*/
      execute immediate v_strSQL;
    
      --v_strSeparator
      v_strTableName7 := fmf_gettmptablename();
      -- Multiple fields will be merged into one field
      v_strSQL := 'Create Table ' || v_strTableName7 || ' AS Select ';
    
      /*for v_cur_name in (select t.*
                           from user_tab_columns t
                          where t.TABLE_NAME = upper(v_strTableName6)
                          order by t.COLUMN_ID) loop
        if v_cur_name.data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then
          v_selectFieldAll := v_selectFieldAll || '''' || v_delimiter || '''' || '||' ||
                              v_cur_name.column_name || '||' || '''' ||
                              v_delimiter || '''' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('SMOOTHING_COEFFICIENT', 'SMOOTHING_COEFFICIENT_TREND') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9990.000' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('SEASONALITY_MONTH_1',
               'SEASONALITY_MONTH_2',
               'SEASONALITY_MONTH_3',
               'SEASONALITY_MONTH_4',
               'SEASONALITY_MONTH_5',
               'SEASONALITY_MONTH_6',
               'SEASONALITY_MONTH_7',
               'SEASONALITY_MONTH_8',
               'SEASONALITY_MONTH_9',
               'SEASONALITY_MONTH_10',
               'SEASONALITY_MONTH_11',
               'SEASONALITY_MONTH_12') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM999999.00' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        else
          v_selectFieldAll := v_selectFieldAll || v_cur_name.column_name || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        end if;
      end loop;*/
    
      for v_cur_name in (select t.*
                           from user_tab_columns t
                          where t.TABLE_NAME = upper(v_strTableName6)
                          order by t.COLUMN_ID) loop
        if v_cur_name.data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2') then
          v_selectFieldAll := v_selectFieldAll || '''' || v_delimiter || '''' || '||' ||
                              v_cur_name.column_name || '||' || '''' ||
                              v_delimiter || '''' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('SMOOTHING_COEFFICIENT', 'SMOOTHING_COEFFICIENT_TREND') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9999999990.000' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        elsif v_cur_name.column_name in
              ('TARGET_1',
               'VALUE_TARGET_1',
               'F_SALES',
               'FORECAST',
               'FORECAST_TARGET_COLUMN',
               'PREVIOUS_FINANCIAL_YEAR',
               'CURRENT_YEAR',
               'CURRENT_YEAR_TARGET_COLUMN',
               'NEXT_FINANCIAL_YEAR',
               'NEXT_FINANCIAL_YEAR_TARGET',
               'SALES_DATE',
               'BALANCE_SALES_ACHIEVE',
               'BALANCE_SALES_ACHIEVE_TARGET',
               'F_MEAN',
               'TREND',
               'ALARM',
               'STANDARD_DEVIATION',
               'ABSOLUTE_MEAN_DEVIATION',
               'PERCENT_DEVIATION_AVERAGE',
               'PERCENT_FORECAST_DEVIATION',
               'PERCENT_FORECAST_DEVIATION_6M',
               'TREND_WINTERS',
               'COEF_CORREL_R2') then
          if p_nDecimals = 0 then
            v_selectFieldAll := v_selectFieldAll || '  round(' ||
                                v_cur_name.column_name || ')  ' || '||' || '''' ||
                                v_strSeparator || '''' || '||';
          
          else
          
            /*            v_selectFieldAll := v_selectFieldAll || 'case when ' ||
            v_cur_name.column_name || '=0 then ' ||
            '''0' || ''' else ' || '  to_char(' ||
            v_cur_name.column_name || ',''' ||
            'FM9999999999.' || v_strDecimalsValue ||
            ''') end ' || '||' || '''' ||
            v_strSeparator || '''' || '||';*/
          
            v_selectFieldAll := v_selectFieldAll || '  to_char(' ||
                                v_cur_name.column_name || ',''' ||
                                'FM9999999990.' || v_strDecimalsValue ||
                                ''')  ' || '||' || '''' || v_strSeparator || '''' || '||';
          
          end if;
        
        elsif v_cur_name.column_name in
              ('PERCENT_12M',
               'PERCENT_12M_TARGET_COLUMN',
               'PRECENT_PREVIOUS_CURRENT_YEAR',
               'PRECENT_PREVI_CUR_YEAR_TARGET',
               'PERCENT_NEXT_FIN_YEAR',
               'PERCENT_NEXT_FIN_YEAR_TARGET',
               'PERCENT_SALES_DATE',
               'PERCENT_SALES_DATE_TARGET',
               'PERCENT_BALANCE_SALES_ACHIEVE',
               'PERCENT_BALAN_ACHIEVE_TARGET') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9999999990.0' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        
        elsif v_cur_name.column_name in
              ('PERCENT_CONTINUATION_FACTOR',
               'SEASONALITY_MONTH_1',
               'SEASONALITY_MONTH_2',
               'SEASONALITY_MONTH_3',
               'SEASONALITY_MONTH_4',
               'SEASONALITY_MONTH_5',
               'SEASONALITY_MONTH_6',
               'SEASONALITY_MONTH_7',
               'SEASONALITY_MONTH_8',
               'SEASONALITY_MONTH_9',
               'SEASONALITY_MONTH_10',
               'SEASONALITY_MONTH_11',
               'SEASONALITY_MONTH_12') then
          v_selectFieldAll := v_selectFieldAll || ' to_char(' ||
                              v_cur_name.column_name || ',''' ||
                              'FM9999999990.00' || ''')' || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        else
          v_selectFieldAll := v_selectFieldAll || v_cur_name.column_name || '||' || '''' ||
                              v_strSeparator || '''' || '||';
        end if;
      end loop;
    
      v_selectFieldAll := trim(v_selectFieldAll);
      v_selectFieldAll := substr(v_selectFieldAll,
                                 1,
                                 length(v_selectFieldAll) - 7);
    
      v_strSQL := v_strSQL || v_selectFieldAll || ' AS FORECASTINFO From ' ||
                  v_strTableName6;
    
      /*      execute immediate 'truncate table t_test';
      insert into t_test values (v_strSQL);
      commit;*/
    
      execute immediate v_strSQL;
    
      v_strTableName8 := fmf_gettmptablename();
      if instr(v_StrOption, 'SEL') > 0 then
        --sel swicth
        v_strselvalue := substr(v_StrOption, instr(v_StrOption, 'SEL') + 4);
        if instr(v_strselvalue, '##') > 0 then
          v_strselvalue := substr(v_strselvalue,
                                  1,
                                  instr(v_strselvalue, '##') - 1);
        end if;
      
        if p_nObjectCode in (51, 2072, 2076, 2080) then
          select sel.sel_em_addr
            into v_nSelOrAggRuleID
            from sel
           where sel.sel_cle = v_strselvalue;
        elsif p_nObjectCode in (52, 2074, 2078, 2082) then
          select prv.prv_em_addr
            into v_nSelOrAggRuleID
            from prv
           where prv.prv_cle = v_strselvalue;
        end if;
      
        v_strSQL := 'Create Table ' || v_strTableName8 || ' AS ';
      
        if p_nObjectCode in (51, 2072, 2076, 2080) then
          v_strSQL := v_strSQL ||
                      ' select m.pvt14_em_addr node_em_addr
                                from rsp m left join pvt n
                                on m.pvt14_em_addr=n.pvt_em_addr
                                where m.sel13_em_addr=' ||
                      v_nSelOrAggRuleID;
        end if;
      
        if p_nObjectCode in (52, 2074, 2078, 2082) then
          v_strSQL := v_strSQL ||
                      ' select m.sel16_em_addr node_em_addr
                                from prvsel m left outer join sel n
                                on m.sel16_em_addr=n.sel_em_addr
                                where n.sel_bud=71
                                and m.prv15_em_addr=' ||
                      v_nSelOrAggRuleID;
        end if;
      
        /*      execute immediate 'truncate table t_test';
        insert into t_test values (v_strSQL);
        commit;*/
      
        execute immediate v_strSQL;
      
        v_strTableName9 := fmf_gettmptablename();
        v_strSQL        := ' Create Table ' || v_strTableName9 ||
                           ' AS SELECT ' || v_selectFieldAll ||
                           ' AS FORECASTINFO From ' || v_strTableName8 ||
                           ' m,' || v_strTableName5 ||
                           ' n where m.node_em_addr=n.bdg_em_addr';
      
        /*execute immediate 'truncate table t_test';
        insert into t_test values (v_strSQL);
        commit;*/
        execute immediate v_strSQL;
        p_strTableName := v_strTableName9;
        execute immediate 'drop table ' || v_strTableName7;
      else
        p_strTableName := v_strTableName7;
        --execute immediate 'drop table ' || v_strTableName9;
      end if;
      -- drop temp table
      execute immediate 'drop table ' || v_strTableName1;
      execute immediate 'drop table ' || v_strTableName2;
      execute immediate 'drop table ' || v_strTableName3;
      execute immediate 'drop table ' || v_strTableName4;
      execute immediate 'drop table ' || v_strTableName5;
      execute immediate 'drop table ' || v_strTableName6;
    end if;
  
    p_nSqlCode := 0;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
  end sp_PutDataToTmpTableParameter;

  procedure sp_PutDataToTmpTableNote(p_nObjectCode  in number,
                                     p_strFMUSER    in varchar2,
                                     P_StrOption    in varchar2,
                                     p_strSeparator in varchar2,
                                     p_strTableName in out varchar2,
                                     p_nSqlCode     out integer)
  --*****************************************************************
    -- Description: save forecast Note data to  temp table
    --
    -- Parameters:
    --       p_nObjectCode
    --       p_strFMUSER
    --       P_StrOption
    --       p_strSeparator
    --       p_strTableName
    --       p_nSqlCode
    -- Error Conditions Raised:
    --
    -- Author:      junhua zuo
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     junhua zuo     Created.
    -- **************************************************************
  
   is
    v_strSQL          varchar2(30000) := '';
    v_StrOption       varchar2(3000);
    v_strTableName1   varchar2(100) := '';
    v_strTableName2   varchar2(100) := '';
    v_delimiter       varchar2(100) := '';
    v_strSeparator    varchar2(100) := '';
    v_g_FMRT_Switches FMP_Batch.g_FMRT_Switches;
    v_nNumMod         number;
  begin
    v_strSQL        := '';
    v_strTableName1 := p_strTableName;
    v_strSeparator  := p_strSeparator;
    FMP_Batch.FMSP_Parse(v_StrOption, v_g_FMRT_Switches, p_nSqlCode);
    if v_g_FMRT_Switches.sdlt then
      v_delimiter := '';
    else
      v_delimiter := '"';
    end if;
  
    if v_g_FMRT_Switches.version = 1 then
      v_nNumMod := 49;
    elsif v_g_FMRT_Switches.version = 2 then
      v_nNumMod := 53;
    elsif v_g_FMRT_Switches.version = 3 then
      v_nNumMod := 54;
    elsif v_g_FMRT_Switches.version = 4 then
      v_nNumMod := 55;
    elsif v_g_FMRT_Switches.version is null then
      v_nNumMod := 49;
    else
      null;
    end if;
    --v_delimiter := '';
    /*    if p_nObjectCode in (1051, 1052) then
      v_StrOption := upper(trim(P_StrOption));
    
      p_strTableName := fmf_gettmptablename();
      --begin
      v_strSQL := 'CREATE TABLE ' || p_strTableName || '(';
      --detail Notes
      if p_nObjectCode = 1051 then
        --field 'Product key'
        v_strSQL := v_strSQL || 'product varchar2(60) not null,';
      end if;
      --aggregate Notes
      if p_nObjectCode = 1052 then
        --field 'Aggregate key'
        v_strSQL := v_strSQL || 'AggNode varchar2(184),';
        --field 'Product key'
        v_strSQL := v_strSQL || 'product varchar2(60),';
      end if;
    
      --field 'Sales Territory key'
      v_strSQL := v_strSQL || 'sales varchar2(60),';
    
      --field 'Trade channel key'*\
      v_strSQL := v_strSQL || 'trade varchar2(60),';
    
      --field 'Model - see table at the bottom '
      v_strSQL := v_strSQL || 'F_Note CLOB ';
      v_strSQL := v_strSQL || ')';
    
      --execute
      execute immediate v_strSQL;
    end if;*/
    if p_nObjectCode = 1051 then
      /*v_strSQL := 'create or replace view v_pvt_three_key
                    as
                    select t.pvt_em_addr,t.pvt_cle,t1.f_cle,t2.g_cle,t3.d_cle
                    from pvt t left outer join fam t1
                    on t.fam4_em_addr=t1.fam_em_addr
                    left outer join geo t2
                    on t.geo5_em_addr=t2.geo_em_addr
                    left outer join dis t3
                    on t.dis6_em_addr=t3.dis_em_addr';
      
      execute immediate v_strSQL;*/
      --get detail node of product key , sales key, trade key
      v_strSQL := 'insert into ' || v_strTableName1 || '
         select n.f_cle,n.g_cle,n.d_cle,m.texte
        from serinote m,
             bdg t,
             v_pvt_three_key n
        where m.bdg3_em_addr=t.bdg_em_addr
        and t.B_CLE=n.PVT_CLE
        and t.ID_BDG=80
        and   m.NOPAGE=0 and m.NUM_MOD=' || v_nNumMod;
      execute immediate v_strSQL;
      commit;
    
      /*
        -- no forecast information of detail node fill up an empty space
        v_strSQL := 'insert into ' || v_strTableName1 || '
           select n.f_cle,n.g_cle,n.d_cle,null
          from (select mm.* from serinote mm where mm.NOPAGE=0 and mm.NUM_MOD=' ||
                    v_nNumMod || ') m,
               v_pvt_three_key n
          where n.pvt_em_addr=m.bdg3_em_addr(+)
          and m.bdg3_em_addr is null ';
      
        execute immediate v_strSQL;
        commit;
      */
    elsif p_nObjectCode = 1052 then
    
      /*v_strSQL := 'create or replace view v_sel_threeKey
          as
          select n.sel_em_addr,s.sel_cle,f.f_cle,g.g_cle,d.d_cle
          from v_aggnodetodimension n
          left outer join  fam f
          on n.fam4_em_addr=f.fam_em_addr
          left outer join  geo g
          on n.geo5_em_addr=g.geo_em_addr
          left outer join  dis d
          on n.dis6_em_addr=d.dis_em_addr
          left outer join sel s
          on n.sel_em_addr=s.sel_em_addr';
      
      execute immediate v_strSQL;
      commit;*/
      --get aggre node of aggre node key,product key , sales key, trade key
      /*      v_strSQL := 'insert into ' || v_strTableName1 || '
         select n.sel_cle,n.f_cle,n.g_cle,n.d_cle,m.texte
        from serinote m,
             v_sel_threeKey n
        where m.BDG3_EM_ADDR=n.SEL_EM_ADDR
        and   m.NOPAGE=0 and m.NUM_MOD=49' || v_nNumMod;
      execute immediate v_strSQL;
      commit;*/
    
      v_strSQL := 'insert into ' || v_strTableName1 || '
         --select n.sel_cle,n.f_cle,n.g_cle,n.d_cle,m.texte
         --Will enter newline replacement is empty
        select n.sel_cle,n.f_cle,n.g_cle,n.d_cle,replace(m.texte, chr(13)||chr(10), ''##' || ''')
        from serinote m,
             bdg t,
             v_sel_threeKey n
        where m.BDG3_EM_ADDR=t.bdg_em_addr
        and t.B_CLE=n.SEL_CLE
        and t.ID_BDG=71
        and   m.NOPAGE=0 and m.NUM_MOD=' || v_nNumMod;
      execute immediate v_strSQL;
      commit;
    
      /*
        -- no forecast information of aggr node fill up an empty space
        v_strSQL := 'insert into ' || v_strTableName1 || '
           select n.sel_cle,n.f_cle,n.g_cle,n.d_cle,m.texte
           from (select mm.* from serinote mm where mm.NOPAGE=0 and mm.NUM_MOD=' ||
                    v_nNumMod || ') m,
                v_sel_threeKey n
          where n.sel_em_addr=m.bdg3_em_addr(+)
          and   m.bdg3_em_addr is null';
        execute immediate v_strSQL;
        commit;
      */
    end if;
  
    --v_strSeparator
    v_strTableName2 := fmf_gettmptablename();
    v_strSQL        := 'Create Table ' || v_strTableName2 || ' AS Select ';
  
    --Multiple fields will be merged into one field
    for v_cur_name in (select t.*
                         from user_tab_columns t
                        where t.TABLE_NAME = upper(v_strTableName1)
                        order by t.COLUMN_ID) loop
      if v_cur_name.data_type in ('CHAR', 'VARCHAR2', 'NVARCHAR2', 'CLOB') then
        v_strSQL := v_strSQL || '''' || v_delimiter || '''' || '||' ||
                    v_cur_name.column_name || '||' || '''' || v_delimiter || '''' || '||' || '''' ||
                    v_strSeparator || '''' || '||';
      else
        v_strSQL := v_strSQL || v_cur_name.column_name || '||' || '''' ||
                    v_strSeparator || '''' || '||';
      end if;
    end loop;
  
    v_strSQL := trim(v_strSQL);
    v_strSQL := substr(v_strSQL, 1, length(v_strSQL) - 7);
  
    v_strSQL := v_strSQL || ' AS FORECASTNOTE From ' || v_strTableName1;
  
    /*execute immediate 'truncate table t_test';
    insert into t_test values (v_strSQL);
    commit;*/
  
    execute immediate v_strSQL;
  
    p_strTableName := v_strTableName2;
  
    --drop temp table
    execute immediate 'drop table ' || v_strTableName1;
  
    p_nSqlCode := 0;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;
    
  end sp_PutDataToTmpTableNote;
end FMP_exportForecast;
/
