create or replace package P_PIMPORT_FORECAST Authid Current_User is

  -- Author  : junhuazuo
  -- Created : 12/5/2012 2:35:40 PM

  -- Public function and procedure declarations
  --Create temporary table for importing forcaste
  procedure sp_PimportForecastparameter(p_nObjectCode      in number,
                                        P_FMUSER           in varchar2,
                                        P_StrOption        in varchar2, --## as separator
                                        p_strTableName     in varchar2,
                                        p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                        p_nSqlCode         out integer);
  procedure sp_CreateTmpTableForForecast(p_nObjectCode      in number,
                                         P_StrOption        in varchar2, --## as separator
                                         p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                         p_strTableName     out varchar2,
                                         p_nSqlCode         out integer); --return code, 0: correct, not 0: error
  procedure SP_OptionHandle(P_StrOption in varchar2, --## as separator
                            P_TableName in varchar2,
                            p_period    in number,
                            P_strUnit   in varchar2, --set Unit
                            P_version   in out number,
                            p_SqlCode   out number);

  procedure sp_DetailNode_note(p_nObjectCode in number,
                               P_TableName   in varchar2,
                               P_StrOption   in varchar2,
                               P_FMUSER      in varchar2,
                               p_nSqlCode    out number);

  procedure sp_OtherSwitch(p_nObjectCode in number,
                           P_TableName   in varchar2,
                           P_StrOption   in varchar2,
                           P_FMUSER      in varchar2,
                           p_nSqlCode    out number);

  procedure sp_init_three_key(P_TableName in varchar2,
                              P_StrOption in varchar2,
                              P_FMUSER    in varchar2,
                              p_nSqlCode  out number);
  procedure sp_initial_datas(p_nObjectCode      in number,
                             P_TableName        in varchar2,
                             P_StrOption        in varchar2,
                             P_FMUSER           in varchar2,
                             p_nBestfitRuleFlag in number,
                             p_nSqlCode         out number);
  procedure sp_CreateTmpTableModForecast(p_strTableName out varchar2,
                                         p_nSqlCode     out integer);
  procedure sp_SaveDatatoModForecast(p_strTableName in varchar2,
                                     p_nSqlCode     out integer);
end P_PIMPORT_FORECAST;
/
create or replace package body P_PIMPORT_FORECAST is
  procedure sp_PimportForecastparameter(p_nObjectCode      in number,
                                        P_FMUSER           in varchar2,
                                        P_StrOption        in varchar2, --## as separator
                                        p_strTableName     in varchar2,
                                        p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                        p_nSqlCode         out integer) is
    v_strTableName      varchar2(40) := '';
    v_StrOption         varchar2(2000);
    v_dis_default_value varchar2(200) := '';
    v_strsql            varchar2(2000);
    v_version           varchar2(100) := '';
    v_nSqlCode          number := 0;
    i                   number;
  begin
    v_StrOption := upper(P_StrOption);
    if v_StrOption is null then
      v_StrOption := 'a';
    end if;
    Fmp_Log.FMP_SetValue(p_nObjectCode);
    Fmp_Log.FMP_SetValue(P_FMUSER);
    Fmp_Log.FMP_SetValue(P_StrOption);
    Fmp_Log.FMP_SetValue(p_strTableName);
    Fmp_Log.FMP_SetValue(p_nBestfitRuleFlag);
    Fmp_Log.LOGBEGIN;
    p_nSqlCode     := 0;
    v_strTableName := p_strTableName;

    /*    if instr(v_StrOption, 'KEY_DIS_DEFAULT') > 0 then
      v_dis_default_value := substr(v_StrOption,
                                    instr(v_StrOption, 'KEY_DIS_DEFAULT') + 16);
      if instr(v_dis_default_value, '##') > 0 then
        v_dis_default_value := substr(v_dis_default_value,
                                      1,
                                      instr(v_dis_default_value, '##') - 1);
      end if;
      --trade channel default
      v_strsql := 'update ' || v_strTableName || ' set trade=''' ||
                  v_dis_default_value || ''' where trade is null';
      execute immediate v_strsql;
      commit;
    end if;*/

    --switch options
    SP_OptionHandle(v_StrOption, --## as separator
                    v_strTableName,
                    0,
                    '', --set Unit
                    v_version,
                    v_nSqlCode);

    if p_nObjectCode not in (1051, 1052) then
      if instr(v_StrOption, 'NODIS') > 0 then
        v_strsql := 'alter table ' || v_strTableName ||
                    ' add(trade varchar2(60))';
        execute immediate v_strsql;
      end if;

      if instr(v_StrOption, 'FAM_N0CRT') > 0 then
        v_strsql := 'alter table ' || v_strTableName ||
                    ' add(productold varchar2(60))';
        execute immediate v_strsql;
      end if;

      if instr(v_StrOption, '2KEYS') > 0 then
        v_strsql := 'alter table ' || v_strTableName ||
                    ' add(IFPVT NUMBER)';
        execute immediate v_strsql;
      end if;

    end if;

    if p_nObjectCode not in (1051, 1052) then
      /*     if p_nObjectCode = 51 then
        v_strTableName := 'tb_temp_fore_detailnode_bak';
      end if;
      if p_nObjectCode = 52 then
        v_strTableName := 'TB_TEMP_FORE_AGGRENODE';
      end if;*/
      if instr(v_StrOption, 'DDP') > 0 or instr(v_StrOption, 'DFP') > 0 or
         instr(v_StrOption, 'OBJ') > 0 then
        sp_OtherSwitch(p_nObjectCode,
                       v_strTableName,
                       v_StrOption,
                       P_FMUSER,
                       p_nSqlCode);
        return;
      end if;

      sp_initial_datas(p_nObjectCode,
                       v_strTableName,
                       v_StrOption,
                       P_FMUSER,
                       p_nBestfitRuleFlag,
                       p_nSqlCode);
    end if;

    if p_nObjectCode in (1051, 1052) then
      --insert notes infomation
      --v_strTableName := 'TB_20270';

      /*      execute immediate 'select count(1)  from ' || p_strTableName
        into i;
      insert into t_count values (i);
      commit;*/

      sp_DetailNode_note(p_nObjectCode,
                         v_strTableName,
                         v_StrOption,
                         P_FMUSER,
                         p_nSqlCode);
    end if;
    p_nSqlCode := 0;
    Fmp_Log.LOGEND;
  end sp_PimportForecastparameter;

  --Create temporary table for importing attribute
  procedure sp_CreateTmpTableForForecast(p_nObjectCode in number,
                                         --p_oOptions     in P_BATCHCOMMAND_DATA_TYPE.OptionsForcastRecordType,
                                         P_StrOption        in varchar2, --## as separator
                                         p_nBestfitRuleFlag in number, --0 is  -hasbestfitrule 1 is none
                                         p_strTableName     out varchar2,
                                         p_nSqlCode         out integer) is
    v_strSQL    varchar2(5000) := '';
    v_StrOption varchar2(400) := '';
  begin
    Fmp_Log.FMP_SetValue(p_nObjectCode);
    Fmp_Log.FMP_SetValue(P_StrOption);
    Fmp_Log.FMP_SetValue(p_nBestfitRuleFlag);
    Fmp_Log.LOGBEGIN;
    p_nSqlCode  := 0;
    v_StrOption := upper(P_StrOption);
    if P_StrOption is null then
      v_StrOption := 'a';
    end if;
    if p_nObjectCode not in (1051, 1052) then
      v_StrOption := upper(trim(P_StrOption));
      --v_StrOption := 'mtotal##nodis##sdlt##spv##p2r';
      --select seq_tb_pimport.Nextval into p_strTableName from dual;
      p_strTableName := fmf_gettmptablename(); -- 'TB_' || p_strTableName;
      
      --add log
     -- Fmp_Log.logInfo(pIn_cSqlText => '1_'||p_strTableName);
      
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

      v_strSQL := v_strSQL || 'sales varchar2(60),';

      --if p_oOptions.bNoDis then
      if (instr(v_StrOption, 'NODIS') <= 0) or
         (instr(v_StrOption, 'NODIS') is null) then
        --field 'Trade channel key'
        v_strSQL := v_strSQL || 'trade varchar2(60),';
      end if;

      if instr(v_StrOption, 'DDP') > 0 or instr(v_StrOption, 'DFP') > 0 or
         instr(v_StrOption, 'OBJ') > 0 then

        if instr(v_StrOption, 'DDP') > 0 and instr(v_StrOption, 'DFP') > 0 then
          v_strSQL := v_strSQL || ' forecast_start_date varchar2(8),';
          v_strSQL := v_strSQL || ' forecast_end_date   varchar2(8)';
        elsif instr(v_StrOption, 'DDP') > 0 and
              instr(v_StrOption, 'DFP') <= 0 then
          v_strSQL := v_strSQL || ' forecast_start_date varchar2(8)';
        elsif instr(v_StrOption, 'DDP') <= 0 and
              instr(v_StrOption, 'DFP') > 0 then
          v_strSQL := v_strSQL || ' forecast_end_date   varchar2(8)';
        else
          null;
        end if;

        if instr(v_StrOption, 'OBJ') > 0 then
          v_strSQL := v_strSQL || ' target_type  number,'; --0,1,2,...13
          v_strSQL := v_strSQL || ' target_value varchar2(60)';
        end if;
        v_strSQL := v_strSQL || ' )';

        execute immediate v_strSQL;
        return;
      end if;

      --if p_oOptions.bmTotal then
      if instr(v_StrOption, 'MTOTAL') > 0 then

        /*--field 'Sales Territory key'
        v_strSQL := v_strSQL || 'sales varchar2(60),';
        --if p_oOptions.bNoDis then
        if instr(v_StrOption, 'NODIS') <= 0 then
          --field 'Trade channel key'
          v_strSQL := v_strSQL || 'trade varchar2(60),';
        end if;*/
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
        v_strSQL := v_strSQL || 'Trend_Winters  number,';
        --field 'R2'
        v_strSQL := v_strSQL || 'R2 number';

      end if;
      if (instr(v_StrOption, 'MTOTAL') <= 0) or
         (instr(v_StrOption, 'MTOTAL') is null) then
        --field 'Product key'
        --v_strSQL := v_strSQL || 'product varchar2(60) not null,';
        --field 'Sales Territory key'
        /*v_strSQL := v_strSQL || 'sales varchar2(60),';
        --if p_oOptions.bNoDis then
        if (instr(v_StrOption, 'NODIS') <= 0) or
           (instr(v_StrOption, 'NODIS') is null) then
          --field 'Trade channel key'
          v_strSQL := v_strSQL || 'trade varchar2(60),';
        end if;*/
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
        --v_strSQL := v_strSQL || 'End_Forecast_date number,';
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
        --field 'Corrected_by_Ext_Event'
        v_strSQL := v_strSQL || 'Corrected_by_Ext_Event number,';
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
        --v_strSQL := v_strSQL || 'Trend_Winters  number';
        v_strSQL := v_strSQL || 'Trend_Winters  number,';
        --field 'R2'
        v_strSQL := v_strSQL || 'R2 number';
      end if;

      if p_nBestfitRuleFlag = 0 then
        v_strSQL := v_strSQL || ',SZBESTFITRULENAME varchar2(60) ';
        v_strSQL := v_strSQL || ',SZBESTFITRULEDESC varchar2(120) ';
        --v_strSQL := v_strSQL || ',SZBESTFITRULENAME varchar2(60) ';
        --v_strSQL := v_strSQL || ',SZBESTFITRULEDESC varchar2(120) ';
      end if;
      v_strSQL := v_strSQL || ')';
      /*      insert into t_test values (v_strSQL);
      commit;*/
      --execute
      execute immediate v_strSQL;
      /*    execute immediate 'truncate table t_temp_zjh';
      execute immediate 'insert into t_temp_zjh(f) values(''' || v_strSQL ||
                        ''')';*/
      commit;
    end if;

    if p_nObjectCode in (1051, 1052) then
      v_StrOption := upper(trim(P_StrOption));
      --v_StrOption := 'mtotal##nodis##sdlt##spv##p2r';
      --select seq_tb_pimport.Nextval into p_strTableName from dual;
      p_strTableName := fmf_gettmptablename(); -- 'TB_' || p_strTableName;
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

      --if p_oOptions.bmTotal then
      /*if instr(v_StrOption, 'MTOTAL') > 0 then*/

      --field 'Sales Territory key'
      v_strSQL := v_strSQL || 'sales varchar2(60),';
      --if p_oOptions.bNoDis then
      /*      if instr(v_StrOption, 'NODIS') <= 0 then
      --field 'Trade channel key'*/
      v_strSQL := v_strSQL || 'trade varchar2(60),';
      /*      end if;*/
      --field 'Model - see table at the bottom '
      v_strSQL := v_strSQL || 'F_Note CLOB ';
      /*end if;*/
      v_strSQL := v_strSQL || ')';

      --execute

      execute immediate v_strSQL;
      /*    execute immediate 'truncate table t_temp_zjh';
      execute immediate 'insert into t_temp_zjh(f) values(''' || v_strSQL ||
                        ''')';*/

    end if;
    Fmp_Log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      --DBMS_OUTPUT.PUT_LINE(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
      Fmp_Log.LOGERROR;
      --raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end sp_CreateTmpTableForForecast;

  -- Option handling
  procedure SP_OptionHandle(P_StrOption in varchar2, --## as separator
                            P_TableName in varchar2,
                            p_period    in number,
                            P_strUnit   in varchar2, --set Unit
                            P_version   in out number,
                            p_SqlCode   out number) as

    v_Option    varchar2(5000);
    v_StrOption varchar2(5000);
    v_next      integer;
    v_position  integer;
    v_length    int := 0;
    v_Key       varchar2(50);
    v_value     varchar2(50);
    v_strsql    varchar2(5000);
    v_strset    varchar2(5000);

    P_Fieldpvt varchar2(1000);
    p_PCount   int := 0;
    p_SCount   int := 0;
    p_TCount   int := 0;

    V_setunit varchar2(10);
  begin
    p_SqlCode   := 0;
    v_StrOption := trim(upper(P_StrOption));
    v_length    := length(v_StrOption);
    V_setunit   := P_strUnit;

    while v_length > 0 LOOP
      --'##' Separated values
      v_next := instr(v_StrOption, '##', 1, 1);

      IF v_next = 0 then
        v_Option := v_StrOption;
        v_length := 0;
      end if;

      if v_next > 1 then
        v_Option    := trim(substr(v_StrOption, 0, v_next - 1));
        v_StrOption := trim(substr(v_StrOption, v_next + 2));
        v_length    := length(v_StrOption);
      END IF;

      --':' Separated values
      v_position := INSTR(v_Option, ':', 1, 1);
      if v_position = 0 then
        v_Key := v_Option;
      else
        v_Key := trim(substr(v_Option, 1, v_position - 1));
      end if;

      v_Key   := rtrim(v_Key);
      v_value := trim(substr(v_Option, v_position + 1));

      case v_Key
      --=========================================Date formats==========================================
        when 'A_M' then
          --YYYY,MM/WW
          v_strsql := 'update ' || P_TableName || ' set yydate=YY||MM ';
          execute immediate v_strsql;

        when 'A_M_J' then
          --YYYY,MM,DD
          v_strsql := 'update ' || P_TableName || ' set yydate=YY||MM ';
          execute immediate v_strsql;

        when 'AA_MM' then
          --YY,MM
          v_strsql := 'update ' || P_TableName ||
                      ' set yydate=''20''||yydate ';
          execute immediate v_strsql;

      ---------------------------------------------------------------
        when 'NODIS' then
          v_strsql := 'update ' || P_TableName || ' set trade=null';
          execute immediate v_strsql;

        when 'NOGEO' then
          v_strsql := 'update ' || P_TableName || ' set sales=null';
          execute immediate v_strsql;

      --====================================Key formats==============================================
        when 'KEY_DIS' then
          --trade channel
          v_strsql := 'update ' || P_TableName || ' set trade=''' ||
                      v_value || '''';
          execute immediate v_strsql;

        when 'KEY_DIS_DEFAULT' then
          --trade channel default
          v_strsql := 'update ' || P_TableName || ' set trade=''' ||
                      v_value || ''' where trade is null';
          execute immediate v_strsql;

        when 'KEY_GEO' then
          --sales territory
          v_strsql := 'update ' || P_TableName || ' set sales=''' ||
                      v_value || '''';
          execute immediate v_strsql;

        when 'KEY_GEO_DEFAULT' then
          --sales territory default
          v_strsql := 'update ' || P_TableName || ' set sales=''' ||
                      v_value || ''' where sales is null';
          execute immediate v_strsql;

      ------------------------------------------------------------------
        when 'PRO' then
          --product (delete is not product in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select f_cle from fam f where f.f_cle=t.product)';
          execute immediate v_strsql;

        when 'GEO' then
          --sales territory (delete is not sales territory in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select g_cle from geo g where g.g_cle=t.sales )';
          execute immediate v_strsql;

        when 'DIS' then
          --trade channel (delete is not trade channel in the database)
          v_strsql := 'delete ' || P_TableName ||
                      ' t where not exists (select d_cle from dis d where d.d_cle=t.trade)';
          execute immediate v_strsql;

      ---------------------------------------------------------------

        when 'PRO_N0CRT' then
          --product  attribute

          v_value  := 48 + v_value;
          v_strsql := 'update ' || P_TableName ||
                      ' set product = (select max(f_cle) from v_productattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = product )
             where exists
             (select 1 from v_productattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = product)';

          execute immediate v_strsql;

        when 'FAM_N0CRT' then

          v_value := 48 + v_value;

          --product to productold
          v_strsql := 'update ' || P_TableName || ' set productold=product';
          execute immediate v_strsql;

          --product group attribute ,aggregate TS ,Find attrvalue by product group
          v_strsql := 'update ' || P_TableName ||
                      ' set product = (select max(f_cle) from v_productattrvalue P, vct v where p.nlevel > 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = product )
             where exists
             (select 1 from v_productattrvalue P, vct v where p.nlevel > 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = product)';

          execute immediate v_strsql;

          --update AGGNode
          v_strsql := 'update ' || P_TableName ||
                      ' set AggNode=replace(AggNode,productold,product)';
          execute immediate v_strsql;

        when 'GEO_N0CRT' then

          --sales territory attribute
          v_value := 48 + v_value;

          v_strsql := 'update ' || P_TableName ||
                      ' set sales = (select max(g_cle) from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = sales )
             where exists
             (select 1 from v_Saleterritoryattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = sales)';

          execute immediate v_strsql;

        when 'DIS_N0CRT' then
          --trade channel attribute
          v_value := 48 + v_value;

          v_strsql := 'update ' || P_TableName ||
                      ' set trade = (select max(d_cle) from v_tradechannelattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value ||
                      ' = v.vct_em_addr  and v.val = trade )
             where exists
             (select 1 from v_tradechannelattrvalue P, vct v where p.nlevel = 1 and p.C' ||
                      v_value || ' = v.vct_em_addr  and v.val = trade)';

          execute immediate v_strsql;

          v_strsql := 'update ' || P_TableName ||
                      ' set trade=null,ifpvt=2 ';
          execute immediate v_strsql;
        when '2KEYS' then
          v_strsql := 'update ' || P_TableName || ' set trade=null ';
          execute immediate v_strsql;
        when 'MTOTAL' then
          null;

      end case;

    END LOOP;
    commit;

  exception
    when others then
      --rollback;
      p_SqlCode := sqlcode;
  end;

  procedure sp_init_three_key(P_TableName in varchar2,
                              P_StrOption in varchar2,
                              P_FMUSER    in varchar2,
                              p_nSqlCode  out number) is
    v_strsql  varchar2(8000);
    v_famID   int;
    v_geoID   int;
    v_disID   int;
    v_nowDate number;
    nCount    number := 0;
  begin
    v_nowDate := F_ConvertDateToOleDateTime(sysdate);
    --v_Desc    := P_Desc; --'created automatically';
    --Add new Product
    nCount := 0;
    select count(1) into nCount from fam t where rownum < 2;

    if nCount > 0 then
      v_strsql := 'select fam_em_addr from fam where ID_fam=1 and F_cle is null';
      execute immediate v_strsql
        into v_famID;

      v_strsql := 'insert into fam(fam_em_addr,id_fam,f_cle,user_create_fam,date_create_fam,fam0_em_addr)';
      v_strsql := v_strsql ||
                  ' select seq_fam.nextval,f.* from (select distinct 80,product' ||
                  ',''' || P_FMUSER || ''',' || v_nowDate || ',' ||
                 /* v_famID || ' from  TB_TEMP_FORE_AGGRENODE t' ||*/
                  v_famID || ' from  ' || P_TableName || ' t' ||
                  '  where t.product is not null and not exists (select  1 from fam f where
                  t.product =f.f_cle ) ) f';
      execute immediate v_strsql;
      commit;
    end if;

    --Add new Sales SalesTerritory
    nCount := 0;
    select count(1) into nCount from geo t where rownum < 2;
    if nCount > 0 then
      v_strsql := 'select geo_em_addr from geo where ascii(g_cle)=1';
      execute immediate v_strsql
        into v_geoID;

      v_strsql := 'insert into geo(geo_em_addr,g_cle,user_create_geo,date_create_geo,geo1_em_addr)
        select seq_geo.nextval,g.* from (select distinct sales' ||
                  ',''' || P_FMUSER || ''',' || v_nowDate || ',' || v_geoID ||
                  ' from ' || P_TableName || ' t' ||
                  '  where t.sales is not null and not exists (select  1 from geo g where
                  t.sales =g.g_cle )) g';
      execute immediate v_strsql;
      commit;
    end if;

    --Add new Trade TradeTrade
    nCount := 0;
    select count(1) into nCount from dis t where rownum < 2;
    if nCount > 0 then
      v_strsql := 'select dis_em_addr from dis where ascii(d_cle)=1';
      execute immediate v_strsql
        into v_disID;

      if (instr(UPPER(P_StrOption), 'NODIS') <= 0) then

        v_strsql := 'insert into dis(dis_em_addr,d_cle,user_create_dis,date_create_dis,dis2_em_addr)
        select seq_dis.nextval,d.* from (select distinct trade' ||
                    ',''' || P_FMUSER || ''',' || v_nowDate || ',' ||
                    v_disID || ' from ' || P_TableName || ' t' ||
                    '  where t.trade is not null and not exists (select  1 from dis d where
                  t.trade =d.d_cle )) d';
        execute immediate v_strsql;
        commit;
      end if;

    end if;
  end sp_init_three_key;

  --Generate detail node
  procedure sp_DetailNode(p_nObjectCode in number,
                          P_TableName   in varchar2,
                          P_StrOption   in varchar2,
                          P_FMUSER      in varchar2,
                          p_nSqlCode    out number) as
    v_strsql       varchar2(32767);
    v_nowDate      number;
    v_strTableName varchar2(30) := '';
    v_nNumMod      number;
  begin
    p_nSqlCode := 0;
    if p_nObjectCode in (51, 52) then
      v_nNumMod := 49;
    elsif p_nObjectCode in (2074, 2072) then
      v_nNumMod := 53;
    elsif p_nObjectCode in (2078, 2076) then
      v_nNumMod := 54;
    elsif p_nObjectCode in (2082, 2080) then
      v_nNumMod := 55;
    end if;
    v_nowDate := F_ConvertDateToOleDateTime(sysdate);
    --add new Detail Node
    --select seq_tb_pimport.Nextval into v_strTableName from dual;
    v_strTableName := fmf_gettmptablename(); -- 'tb_node_' || v_strTableName;
    --insert into  detail node

    if instr(upper(P_StrOption), 'NODIS') <= 0 or
       instr(upper(P_StrOption), 'NODIS') is null then

      v_strsql := 'create table ' || v_strTableName || ' as ';
      v_strsql := v_strsql ||
                  ' select  seq_pvt.nextval as pvt_em_addr,f.fam_em_addr as fam4_em_addr,g.geo_em_addr as geo5_em_addr,d.dis_em_addr as dis6_em_addr,f.fam_em_addr as adr_pro,g.geo_em_addr as adr_geo,d.dis_em_addr as adr_dis,''' ||
                  P_FMUSER || ''' as user_create_pvt,' || v_nowDate ||
                  ' as date_create_pvt,';

      v_strsql := v_strsql ||
                  ' replace((case when (a.sales is not null and';
      v_strsql := v_strsql || '       a.trade is not null) then';
      v_strsql := v_strsql || ' a.product ||''' || '-' ||
                  ''' || a.sales ||''' || '-' || '''|| a.trade';
      v_strsql := v_strsql ||
                  ' when (a.sales is not null and a.trade is null) then ';
      v_strsql := v_strsql || ' a.product ||''' || '-' || '''|| a.sales';
      v_strsql := v_strsql ||
                  ' when (a.sales is null and a.trade is not null) then ';
      v_strsql := v_strsql || '      a.product ||''' || '-' ||
                  '''|| a.trade';
      v_strsql := v_strsql ||
                  '     when (a.sales is null and a.trade is null) then ';
      v_strsql := v_strsql || '      a.product ';
      v_strsql := v_strsql || '   end),''' || '"' || ''',''' ||
                  ''') as pvt_cle';
    else
      v_strsql := 'create table ' || v_strTableName || ' as ';
      v_strsql := v_strsql ||
                  ' select  seq_pvt.nextval as pvt_em_addr,f.fam_em_addr as fam4_em_addr,g.geo_em_addr as geo5_em_addr,0 as dis6_em_addr,f.fam_em_addr as adr_pro,g.geo_em_addr as adr_geo,0 as adr_dis,''' ||
                  P_FMUSER || ''' as user_create_pvt,' || v_nowDate ||
                  ' as date_create_pvt,';

      v_strsql := v_strsql || ' replace((case when (a.sales is not null';
      v_strsql := v_strsql || '     ) then';
      v_strsql := v_strsql || ' a.product ||''' || '-' || ''' || a.sales ';
      v_strsql := v_strsql || ' when (a.sales is  null ) then ';
      v_strsql := v_strsql || ' a.product ';
      v_strsql := v_strsql || '   end),''' || '"' || ''',''' ||
                  ''') as pvt_cle';
    end if;

    v_strsql := v_strsql || ' from ' || P_TableName || ' a';
    v_strsql := v_strsql || ' left join fam f on a.product=f.f_cle';
    v_strsql := v_strsql || ' left join geo g on a.sales=g.g_cle ';

    if instr(upper(P_StrOption), 'NODIS') <= 0 then
      v_strsql := v_strsql || ' left join dis d on a.trade=d.d_cle';
    end if;

    execute immediate v_strsql;
    commit;

    v_strsql := 'insert into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr,adr_pro,adr_geo,adr_dis' ||
                ',user_create_pvt,date_create_pvt' || ',pvt_cle)  ';
    v_strsql := v_strsql || ' select n.* ';
    v_strsql := v_strsql || ' from ' || v_strTableName || ' n,';
    v_strsql := v_strsql || ' pvt m ';
    v_strsql := v_strsql ||
                'where n.pvt_cle=m.pvt_cle(+) and m.pvt_cle is null';

    execute immediate v_strsql;
    commit;

    /*    v_strsql := v_strsql ||
    ' create table temp_bdg(bdg_em_addr NUMBER(19),ID_bdg INTEGER,b_cle NVARCHAR2(184)) nologging as ';*/
    v_strsql := '';
    v_strsql := v_strsql || ' insert into bdg(bdg_em_addr,ID_bdg,b_cle) ';
    v_strsql := v_strsql ||
                ' select seq_bdg.nextval bdg_em_addr,80 ID_bdg,t.pvt_cle b_cle';
    v_strsql := v_strsql || ' from ' || v_strTableName ||
                ' t left outer join bdg p';
    v_strsql := v_strsql || ' on p.ID_BDG=80';
    v_strsql := v_strsql || ' and t.pvt_cle=p.B_CLE';
    v_strsql := v_strsql || ' where p.B_CLE is null';
    execute immediate v_strsql;
    commit;

    --insert detail node mod table
    /*    v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr)  ';
    v_strsql := v_strsql || ' select seq_mod.nextval,t.bdg_em_addr ';
    v_strsql := v_strsql || ' from temp_bdg t ';
    v_strsql := v_strsql ||
                ' where not exists (select 1 from mod n where n.bdg30_em_addr=t.bdg_em_addr)';*/

    v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr,num_mod)  ';
    v_strsql := v_strsql || ' select seq_mod.nextval,t.bdg_em_addr,' ||
                v_nNumMod;
    v_strsql := v_strsql || ' from bdg t ';
    v_strsql := v_strsql || ' where t.id_bdg=80 ';
    v_strsql := v_strsql ||
                ' and not exists (select 1 from mod n where n.bdg30_em_addr=t.bdg_em_addr and n.num_mod=' ||
                v_nNumMod || ') ';

    execute immediate v_strsql;
    commit;

    execute immediate 'drop table ' || v_strTableName || ' purge';
    commit;
  exception
    when others then
      p_nSqlCode := sqlcode;
  end;

  --Generate detail node
  procedure sp_DetailNode_note(p_nObjectCode in number,
                               P_TableName   in varchar2,
                               P_StrOption   in varchar2,
                               P_FMUSER      in varchar2,
                               p_nSqlCode    out number) as
    v_nObjectCode  number := 0;
    v_strsql       varchar2(32767);
    v_nowDate      number;
    v_strTableName varchar2(30) := '';
    v_strFMUSER    varchar2(30) := '';
    v_nNumMod      number;
    v_StrOption    varchar2(32767);
    v_strDel       varchar2(1000) := '';
    v_nIDFlag      number := 0;
  begin
    if p_nObjectCode = 1051 then
      v_nIDFlag := 80;
    elsif p_nObjectCode = 1052 then
      v_nIDFlag := 71;
    end if;

    v_StrOption := upper(P_StrOption);
    if instr(v_StrOption, 'VERSION:') >= 1 then
      v_nNumMod := substr(v_StrOption,
                          instr(v_StrOption, 'VERSION:') + 8,
                          1);

      /*      select ascii(substr(v_StrOption,
                        instr(v_StrOption, 'VERSION:') + 8,
                        1))
      into v_nNumMod
      from dual;*/

      if v_nNumMod = 1 then
        v_nNumMod := 49;
      elsif v_nNumMod = 2 then
        v_nNumMod := 53;
      elsif v_nNumMod = 3 then
        v_nNumMod := 54;
      elsif v_nNumMod = 4 then
        v_nNumMod := 55;
      elsif v_nNumMod = 0 then
        v_nNumMod := 50;
      else
        null;
      end if;

    else
      v_nNumMod := 49;
    end if;
    p_nSqlCode    := 0;
    v_nObjectCode := p_nObjectCode;
    v_strFMUSER   := P_FMUSER;
    v_nowDate     := F_ConvertDateToOleDateTime(sysdate);
    --add new Detail Node
    --select seq_tb_pimport.Nextval into v_strTableName from dual;
    v_strTableName := fmf_gettmptablename(); --'TB_note_' || v_strTableName;
    --insert into  detail node
    /*    v_strsql := 'create table ' || v_strTableName ||
                '(pvt_em_addr NUMBER(19) not null, ';
    v_strsql := v_strsql || ' fam4_em_addr     INTEGER,';
    v_strsql := v_strsql || ' geo5_em_addr     INTEGER,';
    v_strsql := v_strsql || ' dis6_em_addr     INTEGER,';
    v_strsql := v_strsql || ' adr_pro          INTEGER,';
    v_strsql := v_strsql || ' adr_geo          INTEGER,';
    v_strsql := v_strsql || ' adr_dis          INTEGER,';
    v_strsql := v_strsql || ' user_create_pvt  NVARCHAR2(60),';
    v_strsql := v_strsql || ' date_create_pvt  NUMBER,';
    v_strsql := v_strsql || ' pvt_cle          NVARCHAR2(184),';
    v_strsql := v_strsql || ' F_Note NVARCHAR2(976)) as ';*/
    if P_FMUSER is null then
      v_strFMUSER := '0';
    end if;

    if v_nObjectCode = 1051 then
      -- detail node forecast note
      v_strsql := 'create table ' || v_strTableName || ' as ';
      v_strsql := v_strsql ||
                  ' select  seq_pvt.nextval pvt_em_addr,f.fam_em_addr fam4_em_addr,g.geo_em_addr geo5_em_addr,d.dis_em_addr dis6_em_addr,f.fam_em_addr adr_pro,g.geo_em_addr adr_geo,d.dis_em_addr adr_dis,''' ||
                  v_strFMUSER || ''' user_create_pvt,' || v_nowDate ||
                  ' date_create_pvt,';
      --v_strsql :=v_strsql|| P_Fieldpvt;

      v_strsql := v_strsql ||
                  ' replace((case when (a.sales is not null and';
      v_strsql := v_strsql || '       a.trade is not null) then';
      v_strsql := v_strsql || ' a.product ||''' || '-' ||
                  ''' || a.sales ||''' || '-' || '''|| a.trade';
      v_strsql := v_strsql ||
                  ' when (a.sales is not null and a.trade is null) then ';
      v_strsql := v_strsql || ' a.product ||''' || '-' || '''|| a.sales';
      v_strsql := v_strsql ||
                  ' when (a.sales is null and a.trade is not null) then ';
      v_strsql := v_strsql || '      a.product ||''' || '-' ||
                  '''|| a.trade';
      v_strsql := v_strsql ||
                  '     when (a.sales is null and a.trade is null) then ';
      v_strsql := v_strsql || '      a.product ';
      v_strsql := v_strsql || '   end),''' || '"' || ''',''' ||
                  ''') as pvt_cle,a.F_Note';
      v_strsql := v_strsql || ' from ' || P_TableName || ' a';
      v_strsql := v_strsql || ' left join fam f on a.product=f.f_cle';
      v_strsql := v_strsql || ' left join geo g on a.sales=g.g_cle';
      v_strsql := v_strsql || ' left join dis d on a.trade=d.d_cle';

      /*      execute immediate 'truncate table t_test_log';

      insert into t_test_log values (v_strsql);
      commit;*/

      execute immediate v_strsql;

      v_strsql := 'delete from ' || v_strTableName ||
                  ' t where t.rowid<(select max(n.rowid) from ' ||
                  v_strTableName || ' n where n.pvt_cle=t.pvt_cle)';
      execute immediate v_strsql;
      commit;
      --insert three_key information
      sp_init_three_key(P_TableName, P_StrOption, P_FMUSER, p_nSqlCode);

      --v_strsql := '';
      v_strsql := 'insert into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr,adr_pro,adr_geo,adr_dis' ||
                  ',user_create_pvt,date_create_pvt' || ',pvt_cle)  ';
      v_strsql := v_strsql ||
                  ' select n.pvt_em_addr,n.fam4_em_addr,n.geo5_em_addr,n.dis6_em_addr,n.adr_pro,n.adr_geo,n.adr_dis,n.user_create_pvt,n.date_create_pvt,n.pvt_cle ';
      v_strsql := v_strsql || ' from ' || v_strTableName || ' n,';
      v_strsql := v_strsql || ' pvt m ';
      v_strsql := v_strsql ||
                  'where n.pvt_cle=m.pvt_cle(+) and m.pvt_cle is null';

      execute immediate v_strsql;
      commit;

      /*    v_strsql := v_strsql ||
      ' create table temp_bdg_note(bdg_em_addr NUMBER(19),ID_bdg INTEGER,b_cle NVARCHAR2(184)) nologging as ';*/
      --v_strsql := '';
      v_strsql := 'insert into bdg(bdg_em_addr,ID_bdg,b_cle) ';
      v_strsql := v_strsql ||
                  ' select seq_bdg.nextval bdg_em_addr,80 ID_bdg,t.pvt_cle b_cle';
      v_strsql := v_strsql || ' from ' || v_strTableName ||
                  ' t left outer join bdg p';
      v_strsql := v_strsql || ' on p.ID_BDG=80';
      v_strsql := v_strsql || ' and t.pvt_cle=p.B_CLE';
      v_strsql := v_strsql || ' where p.B_CLE is null';
      execute immediate v_strsql;
      commit;

    end if;
    if v_nObjectCode = 1052 then
      -- aggregate node forecast note

      --aggregate node
      /*v_strsql := 'CREATE TABLE TB_TEMP_FORE_AGGRENODE AS ';*/
      v_strsql := 'CREATE TABLE ' || v_strTableName || ' AS ';
      v_strsql := v_strsql || ' select n.AggNode as new_AggNode,n.*';
      v_strsql := v_strsql || ' from sel t , ' || P_TableName;
      v_strsql := v_strsql || ' n ';
      v_strsql := v_strsql || ' where t.sel_bud=71';
      v_strsql := v_strsql || ' and t.sel_cle=n.AggNode ';
      execute immediate v_strsql;

      --switch p2r
      if instr(upper(P_StrOption), 'P2R') > 0 then
        /*v_strsql := 'insert into TB_TEMP_FORE_AGGRENODE ';*/
        v_strsql := 'insert into ' || v_strTableName;
        v_strsql := v_strsql || ' select t2.*';
        v_strsql := v_strsql || ' from sel t1,';
        v_strsql := v_strsql || ' (select replace(case';
        v_strsql := v_strsql || ' when (t.sales is not null and';
        v_strsql := v_strsql || '       t.trade is not null) then';
        v_strsql := v_strsql || ' t.product ||''' || '-' ||
                    ''' || t.sales ||''' || '-' || '''|| t.trade';
        v_strsql := v_strsql ||
                    ' when (t.sales is not null and t.trade is null) then ';
        v_strsql := v_strsql || ' t.product ||''' || '-' || '''|| t.sales';
        v_strsql := v_strsql ||
                    ' when (t.sales is null and t.trade is not null) then ';
        v_strsql := v_strsql || '      t.product ||''' || '-' ||
                    '''|| t.trade';
        v_strsql := v_strsql ||
                    '     when (t.sales is null and t.trade is null) then ';
        v_strsql := v_strsql || '      t.product ';
        v_strsql := v_strsql || '   end,';
        v_strsql := v_strsql || '''' || '"' || ''',';
        v_strsql := v_strsql || '''' || ''') as new_AggNode,';
        v_strsql := v_strsql || ' t.*';
        /*v_strsql := v_strsql || ' from TB_TEMP_FORE_AGGRENODE t ';*/
        v_strsql := v_strsql || ' from ' || P_TableName || ' t';
        v_strsql := v_strsql || ' where t.AggNode is null) t2';
        v_strsql := v_strsql || ' where t1.sel_bud = 71';
        v_strsql := v_strsql || ' and replace(t1.sel_cle, ''' || '"' ||
                    ''', ''' || ''') = t2.new_AggNode';

        --dbms_output.put_line(v_strsql);
        execute immediate v_strsql;
        commit;
      end if;

      --init_three_key aggregate node procedure
      sp_init_three_key(v_strTableName, P_StrOption, P_FMUSER, p_nSqlCode);

      v_strsql := '';
      v_strsql := v_strsql ||
                  ' insert into bdg(bdg_em_addr,ID_bdg,b_cle)  ';
      v_strsql := v_strsql ||
                  ' select seq_bdg.nextval bdg_em_addr,71 ID_bdg,t.new_AggNode b_cle';
      v_strsql := v_strsql || ' from ' || v_strTableName ||
                  ' t left outer join bdg p';
      v_strsql := v_strsql || ' on p.ID_BDG=71';
      v_strsql := v_strsql || ' and t.new_AggNode=p.B_CLE';
      v_strsql := v_strsql || ' where p.B_CLE is null';
      execute immediate v_strsql;
      commit;

    end if;

    --insert aggregate node mod table
    v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr,num_mod) ';
    v_strsql := v_strsql || ' select seq_mod.nextval,t.bdg_em_addr, ' ||
                v_nNumMod;
    v_strsql := v_strsql || ' from bdg t ';
    v_strsql := v_strsql || ' where t.id_bdg=' || v_nIDFlag;
    v_strsql := v_strsql ||
                ' and not exists(select 1 from mod n where n.bdg30_em_addr=t.bdg_em_addr and n.num_mod=' ||
                v_nNumMod || ') ';

    execute immediate v_strsql;
    commit;

    v_strDel := 'DELETE from SERINOTE t1 where exists (select 1 from (select n.BDG_EM_ADDR as BDG_EM_ADDR from ' ||
                v_strTableName || ' m,bdg n ';
    v_strDel := v_strDel || ' where n.ID_BDG=' || v_nIDFlag;
    if v_nObjectCode = 1051 then
      v_strDel := v_strDel ||
                  ' and m.pvt_cle=n.b_cle) t2 where t1.BDG3_EM_ADDR=t2.BDG_EM_ADDR and t1.NUM_MOD=' ||
                  v_nNumMod || ')';
    elsif v_nObjectCode = 1052 then
      v_strDel := v_strDel ||
                  ' and m.NEW_AggNode=n.b_cle) t2 where t1.BDG3_EM_ADDR=t2.BDG_EM_ADDR and t1.NUM_MOD=' ||
                  v_nNumMod || ')';
    end if;

    execute immediate v_strDel;
    commit;

    --insert SERINOTE tables data
    v_strsql := ' insert into SERINOTE(SERINOTE_EM_ADDR,NOPAGE,TEXTE,BDG3_EM_ADDR,NUM_MOD) ';
    v_strsql := v_strsql ||
                ' select SEQ_SERINOTE.nextval,0,m.F_Note,n.BDG_EM_ADDR,' ||
                v_nNumMod;
    v_strsql := v_strsql || ' from ' || v_strTableName || ' m ,bdg n';
    v_strsql := v_strsql || ' where n.ID_BDG=' || v_nIDFlag;
    if v_nObjectCode = 1051 then
      v_strsql := v_strsql || ' and m.pvt_cle=n.b_cle';
    elsif v_nObjectCode = 1052 then
      v_strsql := v_strsql || ' and m.NEW_AggNode=n.b_cle';
    end if;

    execute immediate v_strsql;
    commit;

    execute immediate 'drop table ' || v_strTableName || ' purge';
    commit;

    execute immediate 'drop table ' || P_TableName || ' purge';
    commit;
  exception
    when others then
      p_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  --switch ddp_dfp_obj
  procedure sp_OtherSwitch(p_nObjectCode in number,
                           P_TableName   in varchar2,
                           P_StrOption   in varchar2,
                           P_FMUSER      in varchar2,
                           p_nSqlCode    out number) as
    v_nObjectCode  number := 0;
    v_strsql       varchar2(32767);
    v_nowDate      number;
    v_strTableName varchar2(30) := '';
    v_strFMUSER    varchar2(30) := '';
    v_nNumMod      number;
    v_StrOption    varchar2(32767);
    v_strDel       varchar2(1000) := '';
    v_nIDFlag      number := 0;
  begin
    if p_nObjectCode in (51, 2072, 2076, 2080) then
      v_nIDFlag := 80;
    elsif p_nObjectCode in (52, 2074, 2078, 2082) then
      v_nIDFlag := 71;
    end if;

    v_StrOption := upper(P_StrOption);
    if instr(v_StrOption, 'VERSION:') >= 1 then
      v_nNumMod := substr(v_StrOption,
                          instr(v_StrOption, 'VERSION:') + 8,
                          1);

      if v_nNumMod = 1 then
        v_nNumMod := 49;
      elsif v_nNumMod = 2 then
        v_nNumMod := 53;
      elsif v_nNumMod = 3 then
        v_nNumMod := 54;
      elsif v_nNumMod = 4 then
        v_nNumMod := 55;
      elsif v_nNumMod = 0 then
        v_nNumMod := 50;
      else
        null;
      end if;
    else
      v_nNumMod := 49;
    end if;
    p_nSqlCode    := 0;
    v_nObjectCode := p_nObjectCode;
    v_strFMUSER   := P_FMUSER;
    v_nowDate     := F_ConvertDateToOleDateTime(sysdate);

    v_strTableName := fmf_gettmptablename();

    if P_FMUSER is null then
      v_strFMUSER := '0';
    end if;

    if v_nObjectCode in (51, 2072, 2076, 2080) then

      v_strsql := 'create table ' || v_strTableName || ' as ';
      v_strsql := v_strsql ||
                  ' select  seq_pvt.nextval pvt_em_addr,f.fam_em_addr fam4_em_addr,g.geo_em_addr geo5_em_addr,d.dis_em_addr dis6_em_addr,f.fam_em_addr adr_pro,g.geo_em_addr adr_geo,d.dis_em_addr adr_dis,''' ||
                  v_strFMUSER || ''' user_create_pvt,' || v_nowDate ||
                  ' date_create_pvt,';

      v_strsql := v_strsql ||
                  ' replace((case when (a.sales is not null and';
      v_strsql := v_strsql || '       a.trade is not null) then';
      v_strsql := v_strsql || ' a.product ||''' || '-' ||
                  ''' || a.sales ||''' || '-' || '''|| a.trade';
      v_strsql := v_strsql ||
                  ' when (a.sales is not null and a.trade is null) then ';
      v_strsql := v_strsql || ' a.product ||''' || '-' || '''|| a.sales';
      v_strsql := v_strsql ||
                  ' when (a.sales is null and a.trade is not null) then ';
      v_strsql := v_strsql || '      a.product ||''' || '-' ||
                  '''|| a.trade';
      v_strsql := v_strsql ||
                  '     when (a.sales is null and a.trade is null) then ';
      v_strsql := v_strsql || '      a.product ';
      v_strsql := v_strsql || '   end),''' || '"' || ''',''' ||
                  ''') as pvt_cle,a.*';
      v_strsql := v_strsql || ' from ' || P_TableName || ' a';
      v_strsql := v_strsql || ' left join fam f on a.product=f.f_cle';
      v_strsql := v_strsql || ' left join geo g on a.sales=g.g_cle';
      v_strsql := v_strsql || ' left join dis d on a.trade=d.d_cle';

      /*      execute immediate 'truncate table t_test_log';

      insert into t_test_log values (v_strsql);
      commit;*/

      execute immediate v_strsql;

      v_strsql := 'delete from ' || v_strTableName ||
                  ' t where t.rowid<(select max(n.rowid) from ' ||
                  v_strTableName || ' n where n.pvt_cle=t.pvt_cle)';
      execute immediate v_strsql;
      commit;
      --insert three_key information
      sp_init_three_key(P_TableName, P_StrOption, P_FMUSER, p_nSqlCode);

      v_strsql := 'insert into pvt(pvt_em_addr,fam4_em_addr,geo5_em_addr,dis6_em_addr,adr_pro,adr_geo,adr_dis' ||
                  ',user_create_pvt,date_create_pvt' || ',pvt_cle)  ';
      v_strsql := v_strsql ||
                  ' select n.pvt_em_addr,n.fam4_em_addr,n.geo5_em_addr,n.dis6_em_addr,n.adr_pro,n.adr_geo,n.adr_dis,n.user_create_pvt,n.date_create_pvt,n.pvt_cle ';
      v_strsql := v_strsql || ' from ' || v_strTableName || ' n,';
      v_strsql := v_strsql || ' pvt m ';
      v_strsql := v_strsql ||
                  'where n.pvt_cle=m.pvt_cle(+) and m.pvt_cle is null';

      execute immediate v_strsql;
      commit;

      v_strsql := 'insert into bdg(bdg_em_addr,ID_bdg,b_cle) ';
      v_strsql := v_strsql ||
                  ' select seq_bdg.nextval bdg_em_addr,80 ID_bdg,t.pvt_cle b_cle';
      v_strsql := v_strsql || ' from ' || v_strTableName ||
                  ' t left outer join bdg p';
      v_strsql := v_strsql || ' on p.ID_BDG=80';
      v_strsql := v_strsql || ' and t.pvt_cle=p.B_CLE';
      v_strsql := v_strsql || ' where p.B_CLE is null';
      execute immediate v_strsql;
      commit;

    end if;
    if v_nObjectCode in (52, 2074, 2078, 2082) then
      v_strsql := 'CREATE TABLE ' || v_strTableName || ' AS ';
      v_strsql := v_strsql || ' select n.AggNode as new_AggNode,n.*';
      v_strsql := v_strsql || ' from sel t , ' || P_TableName;
      v_strsql := v_strsql || ' n ';
      v_strsql := v_strsql || ' where t.sel_bud=71';
      v_strsql := v_strsql || ' and t.sel_cle=n.AggNode ';
      execute immediate v_strsql;

      --switch p2r
      if instr(upper(P_StrOption), 'P2R') > 0 then
        v_strsql := 'insert into ' || v_strTableName;
        v_strsql := v_strsql || ' select t2.*';
        v_strsql := v_strsql || ' from sel t1,';
        v_strsql := v_strsql || ' (select replace(case';
        v_strsql := v_strsql || ' when (t.sales is not null and';
        v_strsql := v_strsql || '       t.trade is not null) then';
        v_strsql := v_strsql || ' t.product ||''' || '-' ||
                    ''' || t.sales ||''' || '-' || '''|| t.trade';
        v_strsql := v_strsql ||
                    ' when (t.sales is not null and t.trade is null) then ';
        v_strsql := v_strsql || ' t.product ||''' || '-' || '''|| t.sales';
        v_strsql := v_strsql ||
                    ' when (t.sales is null and t.trade is not null) then ';
        v_strsql := v_strsql || '      t.product ||''' || '-' ||
                    '''|| t.trade';
        v_strsql := v_strsql ||
                    '     when (t.sales is null and t.trade is null) then ';
        v_strsql := v_strsql || '      t.product ';
        v_strsql := v_strsql || '   end,';
        v_strsql := v_strsql || '''' || '"' || ''',';
        v_strsql := v_strsql || '''' || ''') as new_AggNode,';
        v_strsql := v_strsql || ' t.*';
        v_strsql := v_strsql || ' from ' || P_TableName || ' t';
        v_strsql := v_strsql || ' where t.AggNode is null) t2';
        v_strsql := v_strsql || ' where t1.sel_bud = 71';
        v_strsql := v_strsql || ' and replace(t1.sel_cle, ''' || '"' ||
                    ''', ''' || ''') = t2.new_AggNode';
        execute immediate v_strsql;
        commit;
      end if;

      --init_three_key aggregate node procedure
      sp_init_three_key(v_strTableName, P_StrOption, P_FMUSER, p_nSqlCode);

      v_strsql := '';
      v_strsql := v_strsql ||
                  ' insert into bdg(bdg_em_addr,ID_bdg,b_cle)  ';
      v_strsql := v_strsql ||
                  ' select seq_bdg.nextval bdg_em_addr,71 ID_bdg,t.new_AggNode b_cle';
      v_strsql := v_strsql || ' from ' || v_strTableName ||
                  ' t left outer join bdg p';
      v_strsql := v_strsql || ' on p.ID_BDG=71';
      v_strsql := v_strsql || ' and t.new_AggNode=p.B_CLE';
      v_strsql := v_strsql || ' where p.B_CLE is null';
      execute immediate v_strsql;
      commit;

    end if;

    --insert aggregate node mod table
    v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr,num_mod) ';
    v_strsql := v_strsql || ' select seq_mod.nextval,t.bdg_em_addr, ' ||
                v_nNumMod;
    v_strsql := v_strsql || ' from bdg t ';
    v_strsql := v_strsql || ' where t.id_bdg=' || v_nIDFlag;
    v_strsql := v_strsql ||
                ' and not exists(select 1 from mod n where n.bdg30_em_addr=t.bdg_em_addr and n.num_mod=' ||
                v_nNumMod || ') ';

    execute immediate v_strsql;
    commit;

    --insert info forecast ;

    v_strDel := 'DELETE from mod_forecast t1 where exists (select 1 from (select n.BDG_EM_ADDR as BDG_EM_ADDR from ' ||
                v_strTableName || ' m,bdg n ';
    v_strDel := v_strDel || ' where n.ID_BDG=' || v_nIDFlag;
    if v_nObjectCode in (51, 2072, 2076, 2080) then
      v_strDel := v_strDel ||
                  ' and m.pvt_cle=n.b_cle) t2 where t1.BDG_EM_ADDR=t2.BDG_EM_ADDR and t1.NUM_MOD=' ||
                  v_nNumMod || ')';
    elsif v_nObjectCode in (52, 2074, 2078, 2082) then
      v_strDel := v_strDel ||
                  ' and m.NEW_AggNode=n.b_cle) t2 where t1.BDG_EM_ADDR=t2.BDG_EM_ADDR and t1.NUM_MOD=' ||
                  v_nNumMod || ')';
    end if;

    execute immediate v_strDel;
    commit;

    if instr(v_StrOption, 'DDP') > 0 or instr(v_StrOption, 'DFP') > 0 or
       instr(v_StrOption, 'OBJ') > 0 then

      if instr(v_StrOption, 'DDP') > 0 and instr(v_StrOption, 'DFP') > 0 then
        v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,DATE_FIN_PREV_ANNEE,Date_fin_prev_periode,DATE_DEB_PREV_ANNEE,DATE_DEB_PREV_PERIODE,NUM_MOD) ';
        --v_strSQL := v_strSQL || ' forecast_start_date varchar2(8),';
        --v_strSQL := v_strSQL || ' forecast_end_date   varchar2(8)';
        v_strsql := v_strsql ||
                    ' select j.MOD_EM_ADDR,n.BDG_EM_ADDR,to_number(substr(m.forecast_start_date,1,4)),to_number(substr(m.forecast_start_date,5,2)),to_number(substr(m.forecast_end_date,1,4)),to_number(substr(m.forecast_end_date,5,2)), ' ||
                    v_nNumMod;
      elsif instr(v_StrOption, 'DDP') > 0 and
            instr(v_StrOption, 'DFP') <= 0 then
        --v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,DATE_DEB_PREV_ANNEE,DATE_DEB_PREV_PERIODE,NUM_MOD) ';
        v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,DATE_FIN_PREV_ANNEE,Date_fin_prev_periode,NUM_MOD) ';
        --v_strSQL := v_strSQL || ' forecast_start_date varchar2(8)';
        v_strsql := v_strsql ||
                    ' select j.MOD_EM_ADDR,n.BDG_EM_ADDR,to_number(substr(m.forecast_start_date,1,4)),to_number(substr(m.forecast_start_date,5,2)), ' ||
                    v_nNumMod;
      elsif instr(v_StrOption, 'DDP') <= 0 and
            instr(v_StrOption, 'DFP') > 0 then
        --v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,DATE_FIN_PREV_ANNEE,Date_fin_prev_periode,NUM_MOD) ';
        v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,DATE_DEB_PREV_ANNEE,DATE_DEB_PREV_PERIODE,NUM_MOD) ';
        --v_strSQL := v_strSQL || ' forecast_end_date   varchar2(8)';
        v_strsql := v_strsql ||
                    ' select j.MOD_EM_ADDR,n.BDG_EM_ADDR,to_number(substr(m.forecast_end_date,1,4)),to_number(substr(m.forecast_end_date,5,2)),' ||
                    v_nNumMod;
      else
        null;
      end if;

      if instr(v_StrOption, 'OBJ') > 0 then
        v_strsql := ' insert into mod_forecast(MOD_EM_ADDR,BDG_EM_ADDR,OBJECTIF,VALOBJECTIF,NUM_MOD) ';
        --v_strSQL := v_strSQL || ' target_type  number,'; --0,1,2,...13
        --v_strSQL := v_strSQL || ' target_value varchar2(60)';
        v_strsql := v_strsql ||
                    ' select j.MOD_EM_ADDR,n.BDG_EM_ADDR,m.target_type,m.target_value,' ||
                    v_nNumMod;

      end if;
    end if;

    v_strsql := v_strsql || ' from ' || v_strTableName || ' m ,bdg n,mod j';
    v_strsql := v_strsql || ' where n.ID_BDG=' || v_nIDFlag;
    if v_nObjectCode in (51, 2072, 2076, 2080) then
      v_strsql := v_strsql || ' and m.pvt_cle=n.b_cle';
    elsif v_nObjectCode in (52, 2074, 2078, 2082) then
      v_strsql := v_strsql || ' and m.NEW_AggNode=n.b_cle';
    end if;

    v_strsql := v_strsql || ' and n.BDG_EM_ADDR=j.BDG30_EM_ADDR';

--add log
     -- Fmp_Log.logInfo(pIn_cSqlText =>'4_'||v_strSQL);
      
    execute immediate v_strsql;
    commit;

    execute immediate 'drop table ' || v_strTableName || ' purge';
    commit;

    execute immediate 'drop table ' || P_TableName || ' purge';
    commit;

  exception
    when others then
      p_nSqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(p_constant.e_oraerr, sqlcode || sqlerrm);
  end;

  procedure sp_initial_datas(p_nObjectCode in number,
                             P_TableName   in varchar2,
                             P_StrOption   in varchar2,
                             P_FMUSER      in varchar2,
                             --P_Desc        in varchar2,
                             p_nBestfitRuleFlag in number, --1: no best Fit Rule data in text file, 0: text file have best fit rule key and description
                             p_nSqlCode         out number) as
    v_strsql                 varchar2(32767);
    v_strsql_From            varchar2(32767);
    v_strdelsql              varchar2(32767) := '';
    p_strTableName           varchar2(30);
    v_strTempTableName       varchar2(30) := '';
    v_strMiddleTempTableName varchar2(30) := '';
    v_strtempsql             varchar2(4000) := '';
    v_strsqlheader           varchar2(32767) := '';
    v_strsqlhead             varchar2(100) := '';
    str_clob                 clob;
    v_nNumMod                integer := 0;
    v_StrOption              varchar2(500);
    v_strTableNamescl        varchar2(500);

  begin
    v_StrOption := upper(P_StrOption);
    --select seq_tb_pimport.Nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename(); -- 'TB_' || p_strTableName;

    if p_nObjectCode in (51, 52) then
      v_nNumMod := 49;
    elsif p_nObjectCode in (2074, 2072) then
      v_nNumMod := 53;
    elsif p_nObjectCode in (2078, 2076) then
      v_nNumMod := 54;
    elsif p_nObjectCode in (2082, 2080) then
      v_nNumMod := 55;
    end if;

    v_strMiddleTempTableName := fmf_gettmptablename();

    /*    v_strtempsql := 'create table ' || v_strMiddleTempTableName ||
                    ' nologging as ';
    v_strtempsql := v_strtempsql ||
                    ' select n.b_cle b_cle, n.id_bdg id_bdg, n.bdg_em_addr bdg_em_addr, m.mod_em_addr mod_em_addr,m.num_mod num_mod';
    v_strtempsql := v_strtempsql || ' from bdg n, mod m ';
    v_strtempsql := v_strtempsql ||
                    ' where n.bdg_em_addr = m.bdg30_em_addr ';
    v_strtempsql := v_strtempsql || ' and m.num_mod = ' || v_nNumMod;

    execute immediate v_strtempsql;*/

    if p_nObjectCode in (52, 2074, 2078, 2082) then
      --aggregate node
      /*v_strsql := 'CREATE TABLE TB_TEMP_FORE_AGGRENODE AS ';*/
      v_strsql := 'CREATE TABLE ' || p_strTableName || ' nologging AS ';
      v_strsql := v_strsql || ' select n.AggNode as new_AggNode,n.*';
      v_strsql := v_strsql || ' from sel t , ' || P_TableName;
      v_strsql := v_strsql || ' n ';
      v_strsql := v_strsql || ' where t.sel_bud=71';
      v_strsql := v_strsql || ' and t.sel_cle=n.AggNode ';
      execute immediate v_strsql;

      --switch p2r
      if instr(upper(P_StrOption), 'P2R') > 0 then
        /*v_strsql := 'insert into TB_TEMP_FORE_AGGRENODE ';*/
        v_strsql := 'insert into ' || p_strTableName;
        v_strsql := v_strsql || ' select t2.*';
        v_strsql := v_strsql || ' from sel t1,';

        /*        v_strsql := v_strsql || ' (select replace(case';
        v_strsql := v_strsql ||
                    ' when (t.sales is not null and';
        v_strsql := v_strsql ||
                    '       t.trade is not null) then';
        v_strsql := v_strsql || ' t.product ||''' || '-' ||
                    ''' || t.sales ||''' || '-' ||
                    '''|| t.trade';
        v_strsql := v_strsql ||
                    ' when (t.sales is not null and t.trade is null) then ';
        v_strsql := v_strsql || ' t.product ||''' || '-' ||
                    '''|| t.sales';
        v_strsql := v_strsql ||
                    ' when (t.sales is null and t.trade is not null) then ';
        v_strsql := v_strsql || '      t.product ||''' || '-' ||
                    '''|| t.trade';
        v_strsql := v_strsql ||
                    '     when (t.sales is null and t.trade is null) then ';
        v_strsql := v_strsql || '      t.product ';
        v_strsql := v_strsql || '   end,';*/

        if instr(upper(v_StrOption), 'NODIS') <= 0 or
           instr(upper(v_StrOption), 'NODIS') is null then
          v_strsql := v_strsql || ' (select replace(case';

          v_strsql := v_strsql ||
                      ' when (t.product is not null and t.sales is not null and';
          v_strsql := v_strsql || '       t.trade is not null) then';
          v_strsql := v_strsql || ' t.product ||''' || '-' ||
                      ''' || t.sales ||''' || '-' || '''|| t.trade';
          v_strsql := v_strsql ||
                      ' when (t.product is not null and t.sales is not null and t.trade is null) then ';
          v_strsql := v_strsql || ' t.product ||''' || '-' ||
                      '''|| t.sales';
          v_strsql := v_strsql ||
                      ' when (t.product is not null and t.sales is null and t.trade is not null) then ';
          v_strsql := v_strsql || '      t.product ||''' || '-' ||
                      '''|| t.trade';
          v_strsql := v_strsql ||
                      '     when (t.product is not null and t.sales is null and t.trade is null) then ';
          v_strsql := v_strsql || '      t.product ';

          --t.product is null
          v_strsql := v_strsql ||
                      '     when (t.product is null and t.sales is not null and t.trade is null) then ';
          v_strsql := v_strsql || '      t.sales ';
          v_strsql := v_strsql ||
                      '     when (t.product is null and t.sales is  null and t.trade is not null) then ';
          v_strsql := v_strsql || '      t.trade ';
          v_strsql := v_strsql ||
                      '     when (t.product is null and t.sales is not null and t.trade is not null) then ';
          v_strsql := v_strsql || 't.sales||''' || '-' || '''||t.trade ';
          v_strsql := v_strsql || '   end,';

        else
          --t.product is not null
          v_strsql := v_strsql ||
                      ' select replace(case when (t.product is not null and t.sales is not null';
          v_strsql := v_strsql || '     ) then';
          v_strsql := v_strsql || ' t.product ||''' || '-' ||
                      ''' || t.sales ';
          v_strsql := v_strsql ||
                      ' when (t.product is not null t.sales is  null ) then ';
          v_strsql := v_strsql || ' t.product ';
          --t.product is null
          v_strsql := v_strsql ||
                      ' when (t.product is  null t.sales is not null ) then ';
          v_strsql := v_strsql || ' t.sales ';
          v_strsql := v_strsql || '   end,';

        end if;

        v_strsql := v_strsql || '''' || '"' || ''',';
        v_strsql := v_strsql || '''' || ''') as new_AggNode,';
        v_strsql := v_strsql || ' t.*';
        /*v_strsql := v_strsql || ' from TB_TEMP_FORE_AGGRENODE t ';*/
        v_strsql := v_strsql || ' from ' || P_TableName || ' t';
        v_strsql := v_strsql || ' where t.AggNode is null) t2';
        v_strsql := v_strsql || ' where t1.sel_bud = 71';
        v_strsql := v_strsql || ' and replace(t1.sel_cle, ''' || '"' ||
                    ''', ''' || ''') = t2.new_AggNode';

        --dbms_output.put_line(v_strsql);
        execute immediate v_strsql;
        commit;
      end if;

      --init_three_key aggregate node procedure
      sp_init_three_key(p_strTableName, P_StrOption, P_FMUSER, p_nSqlCode);

      /*      --delete mod_forecast exists record
      v_strdelsql := 'DELETE FROM mod t1 WHERE exists(select 1 ';
      v_strdelsql := v_strdelsql ||
                     '   FROM bdg t2 where t1.bdg30_em_addr=t2.bdg_em_addr and t1.NUM_MOD=' ||
                     v_nNumMod || ' and t2.id_bdg=71 )';

      execute immediate v_strdelsql;
      commit;*/

      --insert aggregate node mod table
      v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr,num_mod) ';
      v_strsql := v_strsql || ' select seq_mod.nextval,t.bdg_em_addr, ' ||
                  v_nNumMod;
      v_strsql := v_strsql || ' from bdg t ';
      v_strsql := v_strsql || ' where t.id_bdg=71 ';
      v_strsql := v_strsql ||
                  ' and not exists(select 1 from mod n where n.bdg30_em_addr=t.bdg_em_addr and n.num_mod=' ||
                  v_nNumMod || ') ';

      execute immediate v_strsql;
      commit;

      v_strtempsql := 'create table ' || v_strMiddleTempTableName ||
                      ' nologging as ';
      v_strtempsql := v_strtempsql ||
                      ' select n.b_cle b_cle, n.id_bdg id_bdg, n.bdg_em_addr bdg_em_addr, m.mod_em_addr mod_em_addr,m.num_mod num_mod';
      v_strtempsql := v_strtempsql || ' from bdg n, mod m ';
      v_strtempsql := v_strtempsql ||
                      ' where n.bdg_em_addr = m.bdg30_em_addr ';
      v_strtempsql := v_strtempsql || ' and m.num_mod = ' || v_nNumMod;

      execute immediate v_strtempsql;

    end if;

    --detail node
    if p_nObjectCode in (51, 2072, 2076, 2080) then
      --init_three_key detail node procedure
      sp_init_three_key(P_TableName, P_StrOption, P_FMUSER, p_nSqlCode);

      --init detail node data for pvt bdg mod tables
      sp_DetailNode(p_nObjectCode,
                    P_TableName,
                    P_StrOption,
                    P_FMUSER,
                    p_nSqlCode);

      v_strtempsql := 'create table ' || v_strMiddleTempTableName ||
                      ' nologging as ';
      v_strtempsql := v_strtempsql ||
                      ' select n.b_cle b_cle, n.id_bdg id_bdg, n.bdg_em_addr bdg_em_addr, m.mod_em_addr mod_em_addr,m.num_mod num_mod';
      v_strtempsql := v_strtempsql || ' from bdg n, mod m ';
      v_strtempsql := v_strtempsql ||
                      ' where n.bdg_em_addr = m.bdg30_em_addr ';
      v_strtempsql := v_strtempsql || ' and m.num_mod = ' || v_nNumMod;

      execute immediate v_strtempsql;

    end if;

    --select seq_tb_pimport.Nextval into v_strTempTableName from dual;
    v_strTempTableName := fmf_gettmptablename(); -- 'TB_TEMP_' || v_strTempTableName;
    v_strsql           := 'CREATE TABLE ' || v_strTempTableName ||
                          ' nologging AS ';

    if instr(v_StrOption, 'MTOTAL') > 0 then

      v_strsql := v_strsql || ' SELECT  n.mod_em_addr MOD_EM_ADDR,';
      v_strsql := v_strsql || '         n.bdg_em_addr BDG_EM_ADDR,';
      v_strsql := v_strsql || '         t.F_Model       TYPE_PARAM,';
      v_strsql := v_strsql || '         t.Reference_node REFERENCE_NODE,'; --REFERENCE_NODE SUPPLIER.PERE_BDG,
      v_strsql := v_strsql || '         t.Max_periods NBPERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Start_History, 0)), 1, 4)) DEBUT_UTIL_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Start_History, 0)), 5, 2)) DEBUT_UTIL_PERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.End_History, 0)), 1, 4)) DATE_FIN_HISTO_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.End_History, 0)), 5, 2)) DATE_FIN_HISTO_PERIODE,';
      v_strsql := v_strsql || ' t.Horizon HORIZON,';
      v_strsql := v_strsql ||
                  ' to_number(substr(to_char(nvl(t.End_Forecast_date,0)),1,4)) DATE_FIN_PREV_ANNEE,';
      v_strsql := v_strsql ||
                  ' to_number(substr(to_char(nvl(t.End_Forecast_date,0)),5,2)) Date_fin_prev_periode,';
      v_strsql := v_strsql || '         t.trend_type            TYPE_ID,';
      v_strsql := v_strsql ||
                  '         t.Smoothing_coefficient ALPHA_INIT,';
      v_strsql := v_strsql ||
                  '         t.Autoadaptation        ADAPT_ALPHA,';
      --t.Trading_Day_table SCL.SCL_CLE/MOD SCL,
      v_strsql := v_strsql || '         t.Target_1 OBJECTIF,';
      v_strsql := v_strsql || '         t.Value_Target_1 VALOBJECTIF,';
      v_strsql := v_strsql || '         t.Target_profile TYPE_OBJECTIF,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Target_Start_date, 0)), 1, 4)) DATE_DEB_OBJ_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Target_Start_date, 0)), 5, 2)) DATE_DEB_OBJ_PERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Target_End_date, 0)), 1, 4)) DATE_FIN_OBJ_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Target_End_date, 0)), 5, 2)) DATE_FIN_OBJ_PERIODE,';
      v_strsql := v_strsql || '         t.Target_2 OBJECTIF2,';
      v_strsql := v_strsql || '         t.Value_Target_2 VALOBJECTIF2,';
      v_strsql := v_strsql || '         t.Corrected_Ext_Event AVEC_AS,';
      v_strsql := v_strsql || '         t.retained_split MAJ_BATCH,';
      v_strsql := v_strsql ||
                  '         t.Calculated_seasonal_correction FORCE_SAIS,';
      v_strsql := v_strsql || '         t.Seasonality_Month_1 COEF_SAIS1,';
      v_strsql := v_strsql || '         t.Seasonality_Month_2 COEF_SAIS2,';
      v_strsql := v_strsql || '         t.seasonality_month_3 COEF_SAIS3,';
      v_strsql := v_strsql || '         t.seasonality_month_4 COEF_SAIS4,';
      v_strsql := v_strsql || '         t.seasonality_month_5 COEF_SAIS5,';
      v_strsql := v_strsql || '         t.seasonality_month_6 COEF_SAIS6,';
      v_strsql := v_strsql || '         t.seasonality_month_7 COEF_SAIS7,';
      v_strsql := v_strsql || '         t.seasonality_month_8 COEF_SAIS8,';
      v_strsql := v_strsql || '         t.seasonality_month_9 COEF_SAIS9,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_10 COEF_SAIS10,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_11 COEF_SAIS11,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_12 COEF_SAIS12,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.history_end, 0)), 1, 4)) date_prev_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.history_end, 0)), 5, 2)) date_prev_periode,';
      v_strsql := v_strsql || '         t.F_Sales totmoins12,';
      v_strsql := v_strsql || '         t.Forecast totplus12,';
      v_strsql := v_strsql ||
                  '         t.Forecast_target_column totplus12_obj,';
      v_strsql := v_strsql || '         t.Percent_12M RATIO_12,';
      v_strsql := v_strsql ||
                  '         t.Percent_12M_target_column RATIO_12_OBJ,';
      v_strsql := v_strsql ||
                  '         t.Previous_financial_year TOTANPREC,';
      v_strsql := v_strsql || '         t.Current_year prevancours,';
      v_strsql := v_strsql ||
                  '         t.Current_year_target_column prevancours_obj,';
      v_strsql := v_strsql ||
                  '         t.Precent_Previous_Current_year RATIO_COUR_PREC,';
      v_strsql := v_strsql ||
                  '         t.Precent_Previ_Cur_year_target RATIO_COUR_PREC_OBJ,';
      v_strsql := v_strsql || '         t.Next_financial_year PREVANSUIV,';
      v_strsql := v_strsql ||
                  '         t.Next_financial_year_target PREVANSUIV_OBJ,';
      v_strsql := v_strsql ||
                  '         t.Percent_next_fin_year RATIO_SUIV_COUR,';
      v_strsql := v_strsql ||
                  '         t.Percent_next_fin_year_target RATIO_SUIV_COUR_OBJ,';
      v_strsql := v_strsql || '         t.Sales_date TOTANCOURS,';
      v_strsql := v_strsql || '         t.Percent_Sales_date RATIO_REAL,';
      v_strsql := v_strsql ||
                  '         t.Percent_Sales_date_target RATIO_REAL_OBJ,';
      v_strsql := v_strsql ||
                  '         t.Balance_sales_achieve reste_a_faire,';
      v_strsql := v_strsql ||
                  '         t.Balance_sales_achieve_target reste_a_faire_obj,';
      v_strsql := v_strsql ||
                  '         t.Percent_Balance_sales_achieve ratio_a_faire,';
      v_strsql := v_strsql ||
                  '         t.Percent_Balan_achieve_target ratio_a_faire_obj,';
      v_strsql := v_strsql || '         t.F_Mean MOYENNE,';
      v_strsql := v_strsql || '         t.Trend TENDANCE,';
      v_strsql := v_strsql || '         t.Alarm AWS,';
      v_strsql := v_strsql || '         t.Standard_deviation NF,';
      v_strsql := v_strsql || '         t.Absolute_mean_deviation MAD,';
      v_strsql := v_strsql || '         t.Percent_Deviation_Average ERR2,';
      v_strsql := v_strsql || '         t.Percent_Forecast_deviation ERR1,';
      v_strsql := v_strsql ||
                  '         t.Percent_Forecast_deviation_6M ERR_PRV_6M,';
      v_strsql := v_strsql ||
                  '         t.Percent_Continuation_Factor TAUX_SUITE_DE,';
      v_strsql := v_strsql || '         t.Offset_Periods DECALAGE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.forecast_Start_date, 0)),';
      v_strsql := v_strsql || '                          1,';
      v_strsql := v_strsql ||
                  '                          4)) DATE_DEB_PREV_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.forecast_Start_date, 0)),';
      v_strsql := v_strsql || '                          5,';
      v_strsql := v_strsql ||
                  '                          2)) DATE_DEB_PREV_PERIODE,';

      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_Start_date, 0)),';
      v_strsql := v_strsql || '                          1,';
      v_strsql := v_strsql ||
                  '                          4)) debut_util_saison_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_Start_date, 0)),';
      v_strsql := v_strsql || '                          5,';
      v_strsql := v_strsql ||
                  '                          2)) debut_util_saison_PERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_End_date, 0)), 1, 4)) fin_util_saison_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_End_date, 0)), 5, 2)) fin_util_saison_PERIODE,';
      v_strsql := v_strsql ||
                  '         t.Historical_smoothing_Filter FILTRAGE,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_Start_date,0)),1,4) SUPPLIER_startyear,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_Start_date,0)),5,2) SUPPLIER_startperiod,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_End_date,0)),1,4)   SUPPLIER_endyear,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_End_date,0)),5,2)   SUPPLIER_endperiod,';
      --t.External_Data_table  SCL';
      v_strsql := v_strsql ||
                  '         t.Display_horizon              HORIZON_AFFICHAGE,';
      v_strsql := v_strsql ||
                  '         t.Short_term_horizon           HORIZONFUTUR,';
      v_strsql := v_strsql ||
                  '         t.Short_term_horizon_Pds       HORIZONPASSE,';
      v_strsql := v_strsql ||
                  '         t.Short_term_Forecast_Trend    CHOIXFONCTION,';
      v_strsql := v_strsql ||
                  '         t.Short_term_Forecast_Seasonal SAISONNALITE,';
      v_strsql := v_strsql ||
                  '         t.Managing_Extremities         GESTIONDESBORDS,';
      v_strsql := v_strsql ||
                  '         t.Max_periods_seasonality      MAX_NBPERIODE_SAIS,';
      v_strsql := v_strsql ||
                  '         t.Best_Fit_Model               BESTFIT,';
      v_strsql := v_strsql ||
                  '         t.Smoothing_coefficient_trend  TAUX_EXPLIT,';
      v_strsql := v_strsql ||
                  '         t.Trend_Winters                HAUTEUR_REAPROVIS,';
      v_strsql := v_strsql || 't.R2                 COEF_CORREL_R2,';
      v_strsql := v_strsql || v_nNumMOD || '    as           NUM_MOD';

      if p_nBestfitRuleFlag = 0 then
        v_strsql := v_strsql || ',t.SZBESTFITRULENAME SZBESTFITRULENAME ';
        v_strsql := v_strsql || ',t.SZBESTFITRULEDESC SZBESTFITRULEDESC ';
      end if;

      v_strsql_From := '';
      --aggregate node
      if p_nObjectCode in (52, 2074, 2078, 2082) then

        /*        v_strsql_From := ' FROM ' || p_strTableName || '  t, bdg n, mod m';
        v_strsql_From := v_strsql_From ||
                         '   where t.new_AggNode = n.b_cle';
        v_strsql_From := v_strsql_From ||
                         '     and n.bdg_em_addr = m.bdg30_em_addr';
        v_strsql_From := v_strsql_From || '     and n.id_bdg = 71';*/

        v_strsql_From := ' FROM ' || p_strTableName || '  t, ' ||
                         v_strMiddleTempTableName || ' n ';
        v_strsql_From := v_strsql_From ||
                         '   where t.new_AggNode = n.b_cle';
        v_strsql_From := v_strsql_From || '     and n.id_bdg = 71';

      end if;

      --detail node
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        v_strsql_From := ' FROM ' || p_TableName || '  t, ' ||
                         v_strMiddleTempTableName || ' n ';

        /*        v_strsql_From := v_strsql_From || ' where replace(case';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is not null and';
        v_strsql_From := v_strsql_From ||
                         '       t.trade is not null) then';
        v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                         ''' || t.sales ||''' || '-' ||
                         '''|| t.trade';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is not null and t.trade is null) then ';
        v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                         '''|| t.sales';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is null and t.trade is not null) then ';
        v_strsql_From := v_strsql_From || '      t.product ||''' || '-' ||
                         '''|| t.trade';
        v_strsql_From := v_strsql_From ||
                         '     when (t.sales is null and t.trade is null) then ';
        v_strsql_From := v_strsql_From || '      t.product ';*/

        if instr(upper(v_StrOption), 'NODIS') <= 0 or
           instr(upper(v_StrOption), 'NODIS') is null then

          v_strsql_From := v_strsql_From || ' where replace(case';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is not null and';
          v_strsql_From := v_strsql_From ||
                           '       t.trade is not null) then';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           ''' || t.sales ||''' || '-' || '''|| t.trade';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is not null and t.trade is null) then ';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           '''|| t.sales';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is null and t.trade is not null) then ';
          v_strsql_From := v_strsql_From || '      t.product ||''' || '-' ||
                           '''|| t.trade';
          v_strsql_From := v_strsql_From ||
                           '     when (t.sales is null and t.trade is null) then ';
          v_strsql_From := v_strsql_From || '      t.product ';
        else
          v_strsql_From := v_strsql_From || ' where replace(case';
          v_strsql_From := v_strsql_From || ' when (t.sales is not null';
          v_strsql_From := v_strsql_From || '       ) then';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           ''' || t.sales ';
          v_strsql_From := v_strsql_From || ' when (t.sales is null) then ';
          v_strsql_From := v_strsql_From || ' t.product ';
        end if;

        v_strsql_From := v_strsql_From || '   end,';
        v_strsql_From := v_strsql_From || '''' || '"' || ''',';
        v_strsql_From := v_strsql_From || '''' || ''') =n.b_cle';
        v_strsql_From := v_strsql_From || '     and n.id_bdg = 80 ';
      end if;
      str_clob := v_strsqlheader || v_strsql || v_strsql_From;

      /* insert into t_test (f) values (str_clob);
      commit;*/
      execute immediate str_clob;
    end if;

    if instr(v_StrOption, 'MTOTAL') <= 0 then

      v_strsql := v_strsql || ' SELECT  n.mod_em_addr MOD_EM_ADDR,';
      v_strsql := v_strsql || '         n.bdg_em_addr BDG_EM_ADDR,';
      v_strsql := v_strsql || '         t.F_Model       TYPE_PARAM,';
      v_strsql := v_strsql || '         t.Reference_node REFERENCE_NODE,'; --REFERENCE_NODE SUPPLIER.PERE_BDG,
      v_strsql := v_strsql || '         t.Max_periods NBPERIODE,';
      /* v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Start_History, 0)), 1, 4)) DEBUT_UTIL_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.Start_History, 0)), 5, 2)) DEBUT_UTIL_PERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.End_History, 0)), 1, 4)) DATE_FIN_HISTO_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.End_History, 0)), 5, 2)) DATE_FIN_HISTO_PERIODE,';*/
      v_strsql := v_strsql || ' t.Horizon HORIZON,';

      /*      v_strsql := v_strsql ||
                  ' to_number(substr(to_char(nvl(t.End_Forecast_date,0)),1,4)) DATE_FIN_PREV_ANNEE,';
      v_strsql := v_strsql ||
                  ' to_number(substr(to_char(nvl(t.End_Forecast_date,0)),5,2)) Date_fin_prev_periode,';*/

      v_strsql := v_strsql ||
                  '         t.trend_type               TYPE_ID,';
      v_strsql := v_strsql ||
                  '         t.Smoothing_coefficient ALPHA_INIT,';
      v_strsql := v_strsql ||
                  '         t.Autoadaptation        ADAPT_ALPHA,';
      --t.Trading_Day_table SCL.SCL_CLE/MOD SCL,
      v_strsql := v_strsql || '         t.Target_1 OBJECTIF,';
      v_strsql := v_strsql || '         t.Value_Target_1 VALOBJECTIF,';
      v_strsql := v_strsql || '   t.Corrected_by_Ext_Event   AVEC_AS,';
      v_strsql := v_strsql ||
                  't.Calculated_seasonal_correction FORCE_SAIS,';
      v_strsql := v_strsql || '         t.Seasonality_Month_1 COEF_SAIS1,';
      v_strsql := v_strsql || '         t.Seasonality_Month_2 COEF_SAIS2,';
      v_strsql := v_strsql || '         t.seasonality_month_3 COEF_SAIS3,';
      v_strsql := v_strsql || '         t.seasonality_month_4 COEF_SAIS4,';
      v_strsql := v_strsql || '         t.seasonality_month_5 COEF_SAIS5,';
      v_strsql := v_strsql || '         t.seasonality_month_6 COEF_SAIS6,';
      v_strsql := v_strsql || '         t.seasonality_month_7 COEF_SAIS7,';
      v_strsql := v_strsql || '         t.seasonality_month_8 COEF_SAIS8,';
      v_strsql := v_strsql || '         t.seasonality_month_9 COEF_SAIS9,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_10 COEF_SAIS10,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_11 COEF_SAIS11,';
      v_strsql := v_strsql ||
                  '         t.seasonality_month_12 COEF_SAIS12,';

      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.history_end, 0)), 1, 4)) date_prev_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.history_end, 0)), 5, 2)) date_prev_periode,';

      --v_strsql := v_strsql ||
      --            '         to_number(substr(to_char(nvl(t.history_end, 0)), 1, 4)) Date_fin_histo_annee,';
      --v_strsql := v_strsql ||
      --            '         to_number(substr(to_char(nvl(t.history_end, 0)), 5, 2)) Date_fin_histo_periode,';

      v_strsql := v_strsql || '         t.F_Mean MOYENNE,';
      v_strsql := v_strsql || '         t.Trend TENDANCE,';
      v_strsql := v_strsql || '         t.F_Sales totmoins12,';
      v_strsql := v_strsql ||
                  '         t.Previous_financial_year TOTANPREC,';
      v_strsql := v_strsql || '         t.Forecast totplus12,';
      v_strsql := v_strsql || '         t.Current_year prevancours,';
      v_strsql := v_strsql || '         t.Sales_date TOTANCOURS,';
      /*      v_strsql := v_strsql ||
      '         t.Next_financial_year_target PREVANSUIV_OBJ,';*/
      /*v_strsql := v_strsql ||
      '         t.Next_financial_year PREVANSUIV_OBJ,';*/
      v_strsql := v_strsql || '         t.Next_financial_year PREVANSUIV,';
      v_strsql := v_strsql || '         t.Alarm AWS,';
      v_strsql := v_strsql || '         t.Absolute_mean_deviation MAD,';
      v_strsql := v_strsql || '         t.Percent_Forecast_deviation ERR1,';
      v_strsql := v_strsql || '         t.Percent_Deviation_Average ERR2,';
      v_strsql := v_strsql ||
                  '         t.Percent_Continuation_Factor TAUX_SUITE_DE,';
      v_strsql := v_strsql || '         t.Offset_Periods DECALAGE,';

      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.forecast_Start_date, 0)),';
      v_strsql := v_strsql || '                          1,';
      v_strsql := v_strsql ||
                  '                          4)) DATE_DEB_PREV_ANNEE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.forecast_Start_date, 0)),';
      v_strsql := v_strsql || '                          5,';
      v_strsql := v_strsql ||
                  '                          2)) DATE_DEB_PREV_PERIODE,';

      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_Start_date, 0)),';
      v_strsql := v_strsql || '                          1,';
      v_strsql := v_strsql ||
                  '                          4)) debut_util_saison_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_Start_date, 0)),';
      v_strsql := v_strsql || '                          5,';
      v_strsql := v_strsql ||
                  '                          2)) debut_util_saison_PERIODE,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_End_date, 0)), 1, 4)) fin_util_saison_annee,';
      v_strsql := v_strsql ||
                  '         to_number(substr(to_char(nvl(t.seasonality_End_date, 0)), 5, 2)) fin_util_saison_PERIODE,';
      v_strsql := v_strsql ||
                  '         t.Historical_smoothing_Filter FILTRAGE,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_Start_date,0)),1,4) SUPPLIER_startyear,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_Start_date,0)),5,2) SUPPLIER_startperiod,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_End_date,0)),1,4)   SUPPLIER_endyear,';
      v_strsql := v_strsql ||
                  ' substr(to_char(nvl(t.continued_history_End_date,0)),5,2)   SUPPLIER_endperiod,';
      --t.External_Data_table  SCL';
      v_strsql := v_strsql ||
                  '         t.Display_horizon              HORIZON_AFFICHAGE,';
      v_strsql := v_strsql ||
                  '         t.Short_term_horizon           HORIZONFUTUR,';
      v_strsql := v_strsql ||
                  '         t.Short_term_horizon_Pds       HORIZONPASSE,';
      v_strsql := v_strsql ||
                  '         t.Short_term_Forecast_Trend    CHOIXFONCTION,';
      v_strsql := v_strsql ||
                  '         t.Short_term_Forecast_Seasonal SAISONNALITE,';
      v_strsql := v_strsql ||
                  '         t.Managing_Extremities         GESTIONDESBORDS,';
      v_strsql := v_strsql ||
                  '         t.Max_periods_seasonality      MAX_NBPERIODE_SAIS,';
      v_strsql := v_strsql ||
                  '         t.Best_Fit_Model               BESTFIT,';
      v_strsql := v_strsql ||
                  '         t.Smoothing_coefficient_trend  TAUX_EXPLIT,';
      v_strsql := v_strsql ||
                  '         t.Trend_Winters                HAUTEUR_REAPROVIS,';
      v_strsql := v_strsql || 't.R2                 COEF_CORREL_R2,';
      v_strsql := v_strsql || v_nNumMOD || '    as           NUM_MOD';

      if p_nBestfitRuleFlag = 0 then
        v_strsql := v_strsql || ',t.SZBESTFITRULENAME SZBESTFITRULENAME ';
        v_strsql := v_strsql || ',t.SZBESTFITRULEDESC SZBESTFITRULEDESC ';
      end if;

      v_strsql_From := '';
      --aggregate node
      if p_nObjectCode in (52, 2074, 2078, 2082) then
        v_strsql_From := ' FROM ' || p_strTableName || '  t, ' ||
                         v_strMiddleTempTableName || ' n';
        v_strsql_From := v_strsql_From ||
                         '   where t.new_AggNode = n.b_cle ';
        v_strsql_From := v_strsql_From || ' and n.id_bdg = 71';
      end if;

      --detail node
      if p_nObjectCode in (51, 2072, 2076, 2080) then
        /*v_strsql_From := ' FROM ' || p_TableName || '  t, bdg n, mod m ';*/
        v_strsql_From := ' FROM ' || p_TableName || '  t, ' ||
                         v_strMiddleTempTableName || ' n ';

        /*        v_strsql_From := v_strsql_From || ' where replace(case';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is not null and';
        v_strsql_From := v_strsql_From ||
                         '       t.trade is not null) then';
        v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                         ''' || t.sales ||''' || '-' ||
                         '''|| t.trade';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is not null and t.trade is null) then ';
        v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                         '''|| t.sales';
        v_strsql_From := v_strsql_From ||
                         ' when (t.sales is null and t.trade is not null) then ';
        v_strsql_From := v_strsql_From || '      t.product ||''' || '-' ||
                         '''|| t.trade';
        v_strsql_From := v_strsql_From ||
                         '     when (t.sales is null and t.trade is null) then ';
        v_strsql_From := v_strsql_From || '      t.product ';
        v_strsql_From := v_strsql_From || '   end,';*/

        if instr(upper(v_StrOption), 'NODIS') <= 0 or
           instr(upper(v_StrOption), 'NODIS') is null then

          v_strsql_From := v_strsql_From || ' where replace(case';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is not null and';
          v_strsql_From := v_strsql_From ||
                           '       t.trade is not null) then';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           ''' || t.sales ||''' || '-' || '''|| t.trade';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is not null and t.trade is null) then ';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           '''|| t.sales';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is null and t.trade is not null) then ';
          v_strsql_From := v_strsql_From || '      t.product ||''' || '-' ||
                           '''|| t.trade';
          v_strsql_From := v_strsql_From ||
                           '     when (t.sales is null and t.trade is null) then ';
          v_strsql_From := v_strsql_From || '      t.product ';
        else
          v_strsql_From := v_strsql_From || ' where replace(case';
          v_strsql_From := v_strsql_From || ' when (t.sales is not null';
          v_strsql_From := v_strsql_From || '      ) then';
          v_strsql_From := v_strsql_From || ' t.product ||''' || '-' ||
                           ''' || t.sales ';
          v_strsql_From := v_strsql_From ||
                           ' when (t.sales is  null) then ';
          v_strsql_From := v_strsql_From || ' t.product ';
        end if;

        v_strsql_From := v_strsql_From || '   end,';
        v_strsql_From := v_strsql_From || '''' || '"' || ''',';
        v_strsql_From := v_strsql_From || '''' || ''') =n.b_cle ';
        v_strsql_From := v_strsql_From || ' and n.id_bdg = 80 ';
      end if;
      str_clob := v_strsqlheader || v_strsql || v_strsql_From;

      /*      insert into t_test_log (f) values (str_clob);
      commit;*/

      execute immediate str_clob;
    end if;
    --delete mod_forecast exists record
    v_strdelsql := 'DELETE FROM Mod_Forecast T1 WHERE exists(select 1 ';
    v_strdelsql := v_strdelsql || '   FROM ' || v_strTempTableName ||
                   ' T2 where t1.MOD_EM_ADDR=t2.MOD_EM_ADDR and t1.BDG_EM_ADDR=t2.BDG_EM_ADDR and t1.NUM_MOD=t2.NUM_MOD)';

    execute immediate v_strdelsql;
    commit;

    --insert new record
    v_strsqlheader := '';
    if instr(v_StrOption, 'MTOTAL') > 0 then

      v_strsqlhead   := 'insert into Mod_Forecast(';
      v_strsqlheader := v_strsqlheader || '   MOD_EM_ADDR,';
      v_strsqlheader := v_strsqlheader || '   BDG_EM_ADDR,';
      v_strsqlheader := v_strsqlheader || '   TYPE_PARAM,';
      --Reference_node SUPPLIER.PERE_BDG,';
      v_strsqlheader := v_strsqlheader || '   NBPERIODE,';
      v_strsqlheader := v_strsqlheader || '   DEBUT_UTIL_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DEBUT_UTIL_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_HISTO_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_HISTO_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   HORIZON,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_PREV_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   Date_fin_prev_periode,';
      v_strsqlheader := v_strsqlheader || '   TYPE_ID,';
      v_strsqlheader := v_strsqlheader || '   ALPHA_INIT,';
      v_strsqlheader := v_strsqlheader || '   ADAPT_ALPHA,';
      --t.Trading_Day_table SCL.SCL_CLE/MOD SCL,';
      v_strsqlheader := v_strsqlheader || '   OBJECTIF,';
      v_strsqlheader := v_strsqlheader || '   VALOBJECTIF,';
      v_strsqlheader := v_strsqlheader || '   TYPE_OBJECTIF,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_OBJ_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_OBJ_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_OBJ_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_OBJ_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   OBJECTIF2,';
      v_strsqlheader := v_strsqlheader || '   VALOBJECTIF2,';
      v_strsqlheader := v_strsqlheader || '   AVEC_AS,';
      v_strsqlheader := v_strsqlheader || '   MAJ_BATCH,';
      v_strsqlheader := v_strsqlheader || '   FORCE_SAIS,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS1,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS2,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS3,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS4,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS5,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS6,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS7,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS8,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS9,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS10,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS11,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS12,';
      v_strsqlheader := v_strsqlheader || '   date_prev_annee,';
      v_strsqlheader := v_strsqlheader || '   date_prev_periode,';
      v_strsqlheader := v_strsqlheader || '   totmoins12,';
      v_strsqlheader := v_strsqlheader || '   totplus12,';
      v_strsqlheader := v_strsqlheader || '   totplus12_obj,';
      v_strsqlheader := v_strsqlheader || '   RATIO_12,';
      v_strsqlheader := v_strsqlheader || '   RATIO_12_OBJ,';
      v_strsqlheader := v_strsqlheader || '   TOTANPREC,';
      v_strsqlheader := v_strsqlheader || '   prevancours,';
      v_strsqlheader := v_strsqlheader || '   prevancours_obj,';
      v_strsqlheader := v_strsqlheader || '   RATIO_COUR_PREC,';
      v_strsqlheader := v_strsqlheader || '   RATIO_COUR_PREC_OBJ,';
      v_strsqlheader := v_strsqlheader || '   PREVANSUIV,';
      v_strsqlheader := v_strsqlheader || '   PREVANSUIV_OBJ,';
      v_strsqlheader := v_strsqlheader || '   RATIO_SUIV_COUR,';
      v_strsqlheader := v_strsqlheader || '   RATIO_SUIV_COUR_OBJ,';
      v_strsqlheader := v_strsqlheader || '   TOTANCOURS,';
      v_strsqlheader := v_strsqlheader || '   RATIO_REAL,';
      v_strsqlheader := v_strsqlheader || '   RATIO_REAL_OBJ,';
      v_strsqlheader := v_strsqlheader || '   reste_a_faire,';
      v_strsqlheader := v_strsqlheader || '   reste_a_faire_obj,';
      v_strsqlheader := v_strsqlheader || '   ratio_a_faire,';
      v_strsqlheader := v_strsqlheader || '   ratio_a_faire_obj,';
      v_strsqlheader := v_strsqlheader || '   MOYENNE,';
      v_strsqlheader := v_strsqlheader || '   TENDANCE,';
      v_strsqlheader := v_strsqlheader || '   AWS,';
      v_strsqlheader := v_strsqlheader || '   NF,';
      v_strsqlheader := v_strsqlheader || '   MAD,';
      v_strsqlheader := v_strsqlheader || '   ERR2,';
      v_strsqlheader := v_strsqlheader || '   ERR1,';
      v_strsqlheader := v_strsqlheader || '   ERR_PRV_6M,';
      v_strsqlheader := v_strsqlheader || '   TAUX_SUITE_DE,';
      v_strsqlheader := v_strsqlheader || '   DECALAGE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_PREV_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_PREV_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   debut_util_saison_annee,';
      v_strsqlheader := v_strsqlheader || '   debut_util_saison_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   fin_util_saison_annee,';
      v_strsqlheader := v_strsqlheader || '   fin_util_saison_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   FILTRAGE,';
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_startyear,'; --SUPPLIER.startyear
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_startperiod,'; --SUPPLIER.startperiod
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_endyear,'; --SUPPLIER.endyear
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_endperiod,'; --SUPPLIER.endperiod
      --t.External_Data_table  SCL';
      v_strsqlheader := v_strsqlheader || '   HORIZON_AFFICHAGE,';
      v_strsqlheader := v_strsqlheader || '   HORIZONFUTUR,';
      v_strsqlheader := v_strsqlheader || '   HORIZONPASSE,';
      v_strsqlheader := v_strsqlheader || '   CHOIXFONCTION,';
      v_strsqlheader := v_strsqlheader || '   SAISONNALITE,';
      v_strsqlheader := v_strsqlheader || '   GESTIONDESBORDS,';
      v_strsqlheader := v_strsqlheader || '   MAX_NBPERIODE_SAIS,';
      v_strsqlheader := v_strsqlheader || '   BESTFIT,';
      v_strsqlheader := v_strsqlheader || '   TAUX_EXPLIT,';
      v_strsqlheader := v_strsqlheader || '   HAUTEUR_REAPROVIS, ';
      v_strsqlheader := v_strsqlheader || '   COEF_CORREL_R2, ';
      v_strsqlheader := v_strsqlheader || '   NUM_MOD  ';

      if p_nBestfitRuleFlag = 0 then
        v_strsqlheader := v_strsqlheader || ',SZBESTFITRULENAME ';
        v_strsqlheader := v_strsqlheader || ',SZBESTFITRULEDESC ';
      end if;
    end if;
    if instr(v_StrOption, 'MTOTAL') <= 0 then
      v_strsqlhead   := 'insert into Mod_Forecast(';
      v_strsqlheader := v_strsqlheader || '   MOD_EM_ADDR,';
      v_strsqlheader := v_strsqlheader || '   BDG_EM_ADDR,';
      v_strsqlheader := v_strsqlheader || '   TYPE_PARAM,';
      --Reference_node SUPPLIER.PERE_BDG,';
      v_strsqlheader := v_strsqlheader || '   NBPERIODE,';
      /*      v_strsqlheader := v_strsqlheader || '   DEBUT_UTIL_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DEBUT_UTIL_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_HISTO_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_HISTO_PERIODE,';*/
      v_strsqlheader := v_strsqlheader || '   HORIZON,';
      --v_strsqlheader := v_strsqlheader || '   DATE_FIN_PREV_ANNEE,';
      --v_strsqlheader := v_strsqlheader || '   Date_fin_prev_periode,';
      v_strsqlheader := v_strsqlheader || '   TYPE_ID,';
      v_strsqlheader := v_strsqlheader || '   ALPHA_INIT,';
      v_strsqlheader := v_strsqlheader || '   ADAPT_ALPHA,';
      --t.Trading_Day_table SCL.SCL_CLE/MOD SCL,';
      v_strsqlheader := v_strsqlheader || '   OBJECTIF,';
      v_strsqlheader := v_strsqlheader || '   VALOBJECTIF,';

      /*      v_strsqlheader := v_strsqlheader || '   TYPE_OBJECTIF,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_OBJ_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_OBJ_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_OBJ_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_FIN_OBJ_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   OBJECTIF2,';
      v_strsqlheader := v_strsqlheader || '   VALOBJECTIF2,';
      v_strsqlheader := v_strsqlheader || '   AVEC_AS,';
      v_strsqlheader := v_strsqlheader || '   MAJ_BATCH,';*/
      v_strsqlheader := v_strsqlheader || '   AVEC_AS,';
      v_strsqlheader := v_strsqlheader || '   FORCE_SAIS,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS1,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS2,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS3,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS4,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS5,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS6,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS7,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS8,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS9,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS10,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS11,';
      v_strsqlheader := v_strsqlheader || '   COEF_SAIS12,';
      v_strsqlheader := v_strsqlheader || '   date_prev_annee,';
      v_strsqlheader := v_strsqlheader || '   date_prev_periode,';
      v_strsqlheader := v_strsqlheader || '   MOYENNE,';
      v_strsqlheader := v_strsqlheader || '   TENDANCE,';
      v_strsqlheader := v_strsqlheader || '   totmoins12,';
      v_strsqlheader := v_strsqlheader || '   TOTANPREC,';
      v_strsqlheader := v_strsqlheader || '   totplus12,';
      v_strsqlheader := v_strsqlheader || '   prevancours,';
      v_strsqlheader := v_strsqlheader || '   TOTANCOURS,';
      v_strsqlheader := v_strsqlheader || '   PREVANSUIV_OBJ,';
      v_strsqlheader := v_strsqlheader || '   AWS,';
      v_strsqlheader := v_strsqlheader || '   MAD,';
      v_strsqlheader := v_strsqlheader || '   ERR1,';
      v_strsqlheader := v_strsqlheader || '   ERR2,';
      v_strsqlheader := v_strsqlheader || '   TAUX_SUITE_DE,';
      v_strsqlheader := v_strsqlheader || '   DECALAGE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_PREV_ANNEE,';
      v_strsqlheader := v_strsqlheader || '   DATE_DEB_PREV_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   debut_util_saison_annee,';
      v_strsqlheader := v_strsqlheader || '   debut_util_saison_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   fin_util_saison_annee,';
      v_strsqlheader := v_strsqlheader || '   fin_util_saison_PERIODE,';
      v_strsqlheader := v_strsqlheader || '   FILTRAGE,';
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_startyear,'; --SUPPLIER.startyear
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_startperiod,'; --SUPPLIER.startperiod
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_endyear,'; --SUPPLIER.endyear
      --v_strsqlheader := v_strsqlheader || '   SUPPLIER_endperiod,'; --SUPPLIER.endperiod
      --t.External_Data_table  SCL';
      v_strsqlheader := v_strsqlheader || '   HORIZON_AFFICHAGE,';
      v_strsqlheader := v_strsqlheader || '   HORIZONFUTUR,';
      v_strsqlheader := v_strsqlheader || '   HORIZONPASSE,';
      v_strsqlheader := v_strsqlheader || '   CHOIXFONCTION,';
      v_strsqlheader := v_strsqlheader || '   SAISONNALITE,';
      v_strsqlheader := v_strsqlheader || '   GESTIONDESBORDS,';
      v_strsqlheader := v_strsqlheader || '   MAX_NBPERIODE_SAIS,';
      v_strsqlheader := v_strsqlheader || '   BESTFIT,';
      v_strsqlheader := v_strsqlheader || '   TAUX_EXPLIT,';
      v_strsqlheader := v_strsqlheader || '   HAUTEUR_REAPROVIS, ';
      v_strsqlheader := v_strsqlheader || '   COEF_CORREL_R2, ';
      v_strsqlheader := v_strsqlheader || '   NUM_MOD  ';

      if p_nBestfitRuleFlag = 0 then
        v_strsqlheader := v_strsqlheader || ',SZBESTFITRULENAME ';
        v_strsqlheader := v_strsqlheader || ',SZBESTFITRULEDESC ';
      end if;

    end if;

    --v_strsqlheader := v_strsqlheader || '   NUM_MOD) ';
    --select
    v_strsql := ' SELECT ' || v_strsqlheader || ' FROM ' ||
                v_strTempTableName || ' t';

    /*    insert into t_test
    values
      (v_strsqlhead || v_strsqlheader || ')' || v_strsql);
    commit;*/

--add log
   --   Fmp_Log.logInfo(pIn_cSqlText => '5_'||v_strsqlhead || v_strsqlheader || ')' || v_strsql);
      
    execute immediate v_strsqlhead || v_strsqlheader || ')' || v_strsql;
    commit;

    --delete SUPPLIER exists record
    v_strdelsql := 'DELETE FROM SUPPLIER T1 WHERE exists(select 1 ';
    v_strdelsql := v_strdelsql || '   FROM ' || v_strTempTableName ||
                   ' T2 where t1.BDG51_EM_ADDR=t2.BDG_EM_ADDR and t2.reference_node is not null )';

    execute immediate v_strdelsql;
    commit;

    --insert SUPPLIER table three field.
    --supplier id_supplier not flag detail node and aggregate node
    --v_strsqlheader := 'insert into SUPPLIER(SUPPLIER_EM_ADDR,/*+PERE_BDG,*/ STARTYEAR,STARTPERIOD,ENDYEAR,ENDPERIOD)';
    --v_strsql       := 'select SEQ_SUPPLIER.nextval,/*+ t.Reference_node*/, substr(to_char(nvl(t.continued_history_Start_date,0)),1,4),';
    v_strsqlheader := 'insert into SUPPLIER(SUPPLIER_EM_ADDR,ID_SUPPLIER,PERE_BDG,FILS_BDG,STARTYEAR,STARTPERIOD,ENDYEAR,ENDPERIOD,coeff,BDG51_EM_ADDR)';
    v_strsql       := 'select       SEQ_SUPPLIER.nextval, ';
    v_strsql       := v_strsql || '  83,'; --ID_SUPPLIER
    --v_strsql       := v_strsql || '  decode(t.Reference_node,null,0),'; --Reference_node
    --v_strsql       := v_strsql || '  decode(t.Reference_node,null,0),'; --Reference_node
    v_strsql := v_strsql || '  nvl(t.BDG_EM_ADDR,0),'; --Reference_node =>PERE_BDG
    v_strsql := v_strsql || '  nvl(m.bdg_em_addr,0),'; --Reference_node =>FILS_BDG
    v_strsql := v_strsql || '  t.SUPPLIER_startyear,';
    v_strsql := v_strsql || '  t.SUPPLIER_startperiod,';
    v_strsql := v_strsql || '  t.SUPPLIER_endyear,';
    v_strsql := v_strsql || '  t.SUPPLIER_endperiod, ';
    v_strsql := v_strsql || '  t.taux_suite_de, ';
    v_strsql := v_strsql || '  t.BDG_EM_ADDR as BDG51_EM_ADDR ';
    v_strsql := v_strsql || '  FROM ' || v_strTempTableName ||
                ' t,bdg m where t.Reference_node=m.b_cle(+) and t.Reference_node is not null ';
     
    --add log
    --  Fmp_Log.logInfo(pIn_cSqlText => '6_'||v_strsql);
    execute immediate v_strsqlheader || v_strsql;
    commit;

    -- insert scl data(create table temp_scl)
    --Trading_Day_table is not null  id_scl is 80
    --v_strsqlheader := ' create table temp_scl(scl_em_addr NUMBER(19),id_scl INTEGER,SCL_CLE NVARCHAR2(60),MOD_EM_ADDR NUMBER(19)) nologging ';
    v_strsqlheader := ' create table temp_scl nologging ';
    v_strsqlheader := v_strsqlheader || '  as ';
    /*    v_strsql       := ' select  SEQ_SCL.NEXTVAL scl_em_addr,80 id_scl,t.Trading_Day_table SCL_CLE,m.MOD_EM_ADDR MOD_EM_ADDR';*/

    /*    insert into t_test_log
    values
      (v_strsqlheader || v_strsql || v_strsql_From ||
       ' and trim(t.Trading_Day_table) is not null ');
    commit;*/

    if (instr(v_StrOption, 'MTOTAL') <= 0) or
       (instr(v_StrOption, 'MTOTAL') is null) then
      v_strsql := ' select  SEQ_SCL.NEXTVAL scl_em_addr,80 id_scl,0 Num_MODSCL, t.Trading_Day_table1 SCL_CLE,n.MOD_EM_ADDR MOD_EM_ADDR';
      execute immediate v_strsqlheader || v_strsql || v_strsql_From ||
                        ' and trim(t.Trading_Day_table1) is not null ';
    else
      v_strsql := ' select  SEQ_SCL.NEXTVAL scl_em_addr,80 id_scl,0 Num_MODSCL,t.Trading_Day_table SCL_CLE,n.MOD_EM_ADDR MOD_EM_ADDR';
      execute immediate v_strsqlheader || v_strsql || v_strsql_From ||
                        ' and trim(t.Trading_Day_table) is not null ';
    end if;
    commit;

    --External_Data_table is not null id_scl  is 71
    v_strsqlheader := ' insert into temp_scl(scl_em_addr,id_scl,Num_MODSCL,scl_cle,MOD_EM_ADDR) ';
    v_strsql       := ' select  SEQ_SCL.NEXTVAL,71,2,t.EXTERNAL_DATA_TABLE,n.MOD_EM_ADDR ';
    execute immediate v_strsqlheader || v_strsql || v_strsql_From ||
                      ' and trim(t.External_Data_table) is not null ';
    commit;

    v_strTableNamescl := fmf_gettmptablename();

    -- create temp table of mod_forecast and SUPPLIER correlation of the field
    v_strsql := 'CREATE TABLE ' || v_strTableNamescl || ' AS ';
    if (instr(v_StrOption, 'MTOTAL') <= 0) or
       (instr(v_StrOption, 'MTOTAL') is null) then
      v_strsql := v_strsql ||
                  ' select distinct t.Trading_Day_table1 SCL_CLE ' ||
                  v_strsql_From;
      execute immediate v_strsql ||
                        ' and trim(t.Trading_Day_table1) is not null ';
    else
      v_strsql := v_strsql ||
                  ' select distinct t.Trading_Day_table SCL_CLE ' ||
                  v_strsql_From;
      execute immediate v_strsql ||
                        ' and trim(t.Trading_Day_table) is not null ';
    end if;

    v_strsql := 'insert into scl
      (scl_em_addr, id_scl, scl_cle)
       select SEQ_SCL.NEXTVAL, 80, SCL_CLE
        from ' || v_strTableNamescl || ' t
       where not exists (select 1 from scl m where m.scl_cle = t.scl_cle)';
    execute immediate v_strsql;
    commit;

    execute immediate 'drop table ' || v_strTableNamescl;

    v_strTableNamescl := fmf_gettmptablename();
    v_strsql          := ' create table  ' || v_strTableNamescl || ' as
                          select distinct EXTERNAL_DATA_TABLE  as scl_cle' ||
                         v_strsql_From ||
                         ' and trim(t.External_Data_table) is not null ';
    execute immediate v_strsql;
    commit;

    v_strsql := 'insert into scl
      (scl_em_addr, id_scl, scl_cle)
       select SEQ_SCL.NEXTVAL, 71, SCL_CLE
        from ' || v_strTableNamescl || ' t
       where not exists (select 1 from scl m where m.scl_cle = t.scl_cle)';
    execute immediate v_strsql;
    commit;
    execute immediate 'drop table ' || v_strTableNamescl;
    --insert into

    --
    v_strsql := 'insert into modscl(MODSCL_EM_ADDR,
                       NUM_MODSCL,
                       SCL41_EM_ADDR,
                       MOD42_EM_ADDR)
      select SEQ_MODSCL.NEXTVAL,
             m.Num_MODSCL,
             n.scl_em_addr,
             m.MOD_EM_ADDR
        from temp_scl m, scl n
       where m.scl_cle = n.scl_cle';

    execute immediate v_strsql;
    commit;

    execute immediate 'delete from modscl t1 where t1.rowid <(select max(t2.rowid) from modscl t2 where t2.mod42_em_addr=t1.mod42_em_addr and t2.scl41_em_addr=t1.scl41_em_addr)';
    commit;

    /*
    v_strsql := 'delete from temp_scl t1 where t1.rowid <(select max(t2.rowid) from temp_scl t2 where t1.scl_em_addr=t2.scl_em_addr)';
    execute immediate v_strsql;
    commit;

    v_strsql := 'delete from temp_scl t1 where t1.rowid <(select max(t2.rowid) from temp_scl t2 where t1.scl_cle=t2.scl_cle)';
    execute immediate v_strsql;
    commit;

    v_strsql := 'delete from scl t1 where exists(select 1 from temp_scl t2 where t1.scl_cle=t2.scl_cle)';
    execute immediate v_strsql;
    commit;

    --External_Data_table is not null id_scl  is 71
    v_strsqlheader := ' insert into scl(scl_em_addr, id_scl, scl_cle) ';
    v_strsql       := ' select t.scl_em_addr, t.id_scl,t.scl_cle from temp_scl t where not exists(select 1 from scl t1 where t1.scl_em_addr=t.scl_em_addr)';
    execute immediate v_strsqlheader || v_strsql;
    commit;

    --insert modscl
    v_strsqlheader := ' insert into modscl(modscl_em_addr,NUM_MODSCL, scl41_em_addr, mod42_em_addr) ';
    v_strsql       := ' select SEQ_MODSCL.NEXTVAL, t.NUM_MODSCL,t.scl_em_addr, t.MOD_EM_ADDR from temp_scl t ';
    execute immediate v_strsqlheader || v_strsql;
    commit;
    */

    v_strsql := 'drop table temp_scl';
    execute immediate v_strsql;
    commit;
    if p_nObjectCode in (52, 2074, 2078, 2082) then
      execute immediate 'drop table ' || p_strTableName;
      commit;
    end if;

    /*  execute immediate 'drop table ' || p_TableName;
    commit;

    execute immediate 'drop table ' || v_strMiddleTempTableName;
    commit;

    execute immediate 'drop table ' || v_strTempTableName;
    commit;*/
  exception
    when others then
      p_nSqlCode := sqlcode;

  end;
  procedure sp_CreateTmpTableModForecast(p_strTableName out varchar2,
                                         p_nSqlCode     out integer) is
    v_strSQL varchar2(30000) := '';
  begin

    --select seq_tb_pimport.Nextval into p_strTableName from dual;
    p_strTableName := fmf_gettmptablename();
    --p_strTableName := 'TB_' || p_strTableName;
    --add log
    --  Fmp_Log.logInfo(pIn_cSqlText => '2_'||p_strTableName);
    
    v_strSQL := 'Create Table ' || p_strTableName || ' (';

    /*v_strsql := v_strsql || ' MOD_EM_ADDR                      NUMBER(19),';*/
    v_strsql := v_strsql || ' BDG_EM_ADDR                      NUMBER,';
    v_strsql := v_strsql ||
                ' NUM_MOD                          INTEGER default 0,';
    v_strsql := v_strsql || ' TYPE_ID                          INTEGER,';
    v_strsql := v_strsql || ' ADAPT_ALPHA                      INTEGER,';
    v_strsql := v_strsql || ' ALPHA_INIT                       NUMBER,';
    v_strsql := v_strsql || ' FORCE_SAIS                       INTEGER,';
    v_strsql := v_strsql || ' FILTRAGE                         INTEGER,';
    v_strsql := v_strsql || ' NBPERIODE                        INTEGER,';
    v_strsql := v_strsql || ' OBJECTIF                         INTEGER,';
    v_strsql := v_strsql || ' VALOBJECTIF                      NUMBER,';
    v_strsql := v_strsql || ' FORCE_SAIS_J                     INTEGER,';
    v_strsql := v_strsql || ' MOYENNE                          NUMBER,';
    v_strsql := v_strsql || ' TENDANCE                         NUMBER,';
    v_strsql := v_strsql || ' AWS                              NUMBER,';
    v_strsql := v_strsql || ' TOTMOINS12                       NUMBER,';
    v_strsql := v_strsql || ' TOTPLUS12                        NUMBER,';
    v_strsql := v_strsql || ' TOTANPREC                        NUMBER,';
    v_strsql := v_strsql || ' TOTANCOURS                       NUMBER,';
    v_strsql := v_strsql || ' PREVANCOURS                      NUMBER,';
    v_strsql := v_strsql || ' ERR1                             NUMBER,';
    v_strsql := v_strsql || ' ERR2                             NUMBER,';
    v_strsql := v_strsql || ' MAD                              NUMBER,';
    v_strsql := v_strsql || ' PREVANSUIV                       NUMBER,';
    v_strsql := v_strsql || ' TYPE_PARAM                       INTEGER,';
    v_strsql := v_strsql || ' AVEC_AS                          INTEGER,';
    v_strsql := v_strsql || ' NF                               NUMBER,';
    v_strsql := v_strsql || ' RESTE_A_FAIRE                    NUMBER,';
    v_strsql := v_strsql || ' TOTPLUS12_OBJ                    NUMBER,';
    v_strsql := v_strsql || ' PREVANCOURS_OBJ                  NUMBER,';
    v_strsql := v_strsql || ' PREVANSUIV_OBJ                   NUMBER,';
    v_strsql := v_strsql || ' RESTE_A_FAIRE_OBJ                NUMBER,';
    v_strsql := v_strsql || ' RATIO_12                         NUMBER,';
    v_strsql := v_strsql || ' RATIO_12_OBJ                     NUMBER,';
    v_strsql := v_strsql || ' RATIO_COUR_PREC                  NUMBER,';
    v_strsql := v_strsql || ' RATIO_COUR_PREC_OBJ              NUMBER,';
    v_strsql := v_strsql || ' RATIO_SUIV_COUR                  NUMBER,';
    v_strsql := v_strsql || ' RATIO_SUIV_COUR_OBJ              NUMBER,';
    v_strsql := v_strsql || ' RATIO_REAL                       NUMBER,';
    v_strsql := v_strsql || ' RATIO_REAL_OBJ                   NUMBER,';
    v_strsql := v_strsql || ' RATIO_A_FAIRE                    NUMBER,';
    v_strsql := v_strsql || ' RATIO_A_FAIRE_OBJ                NUMBER,';
    v_strsql := v_strsql || ' AVEC_OBJ                         INTEGER,';
    v_strsql := v_strsql || ' MAJ_BATCH                        INTEGER,';
    v_strsql := v_strsql || ' ERR_PRV_6M                       NUMBER,';
    v_strsql := v_strsql || ' NB_HISTO                         INTEGER,';
    v_strsql := v_strsql || ' TAUX_SUITE_DE                    NUMBER,';
    v_strsql := v_strsql || ' DECALAGE                         INTEGER,';
    v_strsql := v_strsql || ' HAUTEUR_REAPROVIS                NUMBER,';
    v_strsql := v_strsql || ' TYPE_OBJECTIF                    INTEGER,';
    v_strsql := v_strsql || ' SERIE                            INTEGER,';
    v_strsql := v_strsql || ' HORIZON                          INTEGER,';
    v_strsql := v_strsql || ' TAUX_EXPLIT                      NUMBER,';
    v_strsql := v_strsql || ' ECART_PRV_DELAY                  NUMBER,';
    v_strsql := v_strsql || ' FORCE_SAIS_DECADE                INTEGER,';
    v_strsql := v_strsql || ' NBPERIODE_JOUR                   INTEGER,';
    v_strsql := v_strsql || ' OBJECTIF2                        INTEGER,';
    v_strsql := v_strsql || ' VALOBJECTIF2                     NUMBER,';
    v_strsql := v_strsql || ' UNITE_OBJECTIF2                  INTEGER,';
    v_strsql := v_strsql || ' HORIZON_AFFICHAGE                INTEGER,';
    v_strsql := v_strsql || ' RATIO_PRORATA                    INTEGER,';
    v_strsql := v_strsql || ' COEF_SAIS1                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS2                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS3                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS4                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS5                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS6                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS7                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS8                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS9                       NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS10                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS11                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS12                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS13                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS14                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS15                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS16                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS17                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS18                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS19                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS20                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS21                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS22                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS23                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS24                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS25                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS26                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS27                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS28                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS29                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS30                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS31                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS32                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS33                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS34                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS35                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS36                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS37                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS38                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS39                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS40                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS41                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS42                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS43                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS44                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS45                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS46                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS47                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS48                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS49                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS50                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS51                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS52                      NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR1                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR2                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR3                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR4                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR5                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR6                  NUMBER,';
    v_strsql := v_strsql || ' COEF_SAIS_JOUR7                  NUMBER,';
    v_strsql := v_strsql || ' UNUSED_ADDR                      NUMBER,';
    v_strsql := v_strsql || ' DATE_PREV_ANNEE                  INTEGER,';
    v_strsql := v_strsql || ' DATE_PREV_PERIODE                INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_SAISON_ANNEE          INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_SAISON_PERIODE        INTEGER,';
    v_strsql := v_strsql || ' FIN_UTIL_SAISON_ANNEE            INTEGER,';
    v_strsql := v_strsql || ' FIN_UTIL_SAISON_PERIODE          INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_PREV_ANNEE              INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_PREV_PERIODE            INTEGER,';
    v_strsql := v_strsql || ' DATE_DEB_PREV_ANNEE              INTEGER,';
    v_strsql := v_strsql || ' DATE_DEB_PREV_PERIODE            INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_ANNEE                 INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_PERIODE               INTEGER,';
    v_strsql := v_strsql || ' DEBUT_HISTO_ANNEE                INTEGER,';
    v_strsql := v_strsql || ' DEBUT_HISTO_PERIODE              INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_HISTO_ANNEE             INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_HISTO_PERIODE           INTEGER,';
    v_strsql := v_strsql || ' DATE_DEB_OBJ_ANNEE               INTEGER,';
    v_strsql := v_strsql || ' DATE_DEB_OBJ_PERIODE             INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_OBJ_ANNEE               INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_OBJ_PERIODE             INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_SAISON_J_ANNEE        INTEGER,';
    v_strsql := v_strsql || ' DEBUT_UTIL_SAISON_J_PERIODE      INTEGER,';
    v_strsql := v_strsql || ' FIN_UTIL_SAISON_J_ANNEE          INTEGER,';
    v_strsql := v_strsql || ' FIN_UTIL_SAISON_J_PERIODE        INTEGER,';
    v_strsql := v_strsql || ' DATE_DEBUT_SUITE_ANNEE           INTEGER,';
    v_strsql := v_strsql || ' DATE_DEBUT_SUITE_PERIODE         INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_SUITE_ANNEE             INTEGER,';
    v_strsql := v_strsql || ' DATE_FIN_SUITE_PERIODE           INTEGER,';
    v_strsql := v_strsql || ' DATE_CHOW_ANNEE                  INTEGER,';
    v_strsql := v_strsql || ' DATE_CHOW_PERIODE                INTEGER,';
    v_strsql := v_strsql || ' DATE_PREV_JOUR_ANNEE             INTEGER,';
    v_strsql := v_strsql || ' DATE_PREV_JOUR_PERIODE           INTEGER,';
    v_strsql := v_strsql || ' NIVEAU                           NUMBER,';
    v_strsql := v_strsql || ' PENTE                            NUMBER,';
    v_strsql := v_strsql || ' RESULTAT_CUSUM                   INTEGER,';
    v_strsql := v_strsql || ' HORIZONFUTUR                     INTEGER,';
    v_strsql := v_strsql || ' HORIZONPASSE                     INTEGER,';
    v_strsql := v_strsql || ' CHOIXFONCTION                    INTEGER,';
    v_strsql := v_strsql || ' SAISONNALITE                     INTEGER,';
    v_strsql := v_strsql || ' GESTIONDESBORDS                  INTEGER,';
    v_strsql := v_strsql || ' UNUSED_A                         INTEGER,';
    v_strsql := v_strsql || ' MAX_NBPERIODE_SAIS               INTEGER,';
    v_strsql := v_strsql || ' MAX_NBPERIODE_SAIS_JOUR          INTEGER,';
    v_strsql := v_strsql || ' CALCULAUTOJF                     INTEGER,';
    v_strsql := v_strsql || ' INDICE_PREV                      NUMBER,';
    v_strsql := v_strsql || ' BESTFIT                          INTEGER,';
    v_strsql := v_strsql || ' DUREEBESTFIT                     INTEGER,';
    v_strsql := v_strsql || ' JOUR_DATE_DEB_PREV               INTEGER,';
    v_strsql := v_strsql || ' JOUR_DATE_FIN_PREV               INTEGER,';
    v_strsql := v_strsql || ' JOUR_DATE_DEB_OBJ                INTEGER,';
    v_strsql := v_strsql || ' JOUR_DATE_FIN_OBJ                INTEGER,';
    v_strsql := v_strsql ||
                ' SZBESTFITRULENAME                VARCHAR2(60),';
    v_strsql := v_strsql ||
                ' SZBESTFITRULEDESC                VARCHAR2(120),';
    v_strsql := v_strsql || ' DLASTMODIFIEDTIME                NUMBER,';
    v_strsql := v_strsql || ' COEF_CORREL_R2                   NUMBER';
    v_strsql := v_strsql || ' )';

    execute immediate v_strsql;
    p_nSqlCode := 0;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;

  end sp_CreateTmpTableModForecast;

  procedure sp_SaveDatatoModForecast(p_strTableName in varchar2,
                                     p_nSqlCode     out integer) is
    v_strSQL varchar2(30000) := '';
  begin
    v_strsql := ' insert into mod(mod_em_addr,bdg30_em_addr,num_mod) ';
    v_strsql := v_strsql ||
                ' select seq_mod.nextval,m.BDG_EM_ADDR, m.num_mod';
    v_strsql := v_strsql || ' from ' || p_strTableName || ' m ';
    v_strsql := v_strsql || ' where  ';
    v_strsql := v_strsql ||
                '  not exists(select 1 from mod n where n.bdg30_em_addr=m.bdg_em_addr and m.num_mod=n.num_mod)';
    execute immediate v_strsql;
    commit;
    v_strsql := ' delete from mod_forecast t
               where t.MOD_EM_ADDR in (select n.MOD_EM_ADDR
               from ' || p_strTableName ||
                ' m, mod n
               where m.bdg_em_addr = n.bdg30_em_addr
               and m.num_mod = n.num_mod)';
    execute immediate v_strsql;
    commit;

    --insert mod_forecast
    v_strsql := 'insert into mod_forecast
      select n.MOD_EM_ADDR,
             m.BDG_EM_ADDR,
             m.TYPE_ID,
             m.ADAPT_ALPHA,
             m.ALPHA_INIT,
             m.FORCE_SAIS,
             m.FILTRAGE,
             m.NBPERIODE,
             m.OBJECTIF,
             m.VALOBJECTIF,
             m.FORCE_SAIS_J,
             m.MOYENNE,
             m.TENDANCE,
             m.AWS,
             m.TOTMOINS12,
             m.TOTPLUS12,
             m.TOTANPREC,
             m.TOTANCOURS,
             m.PREVANCOURS,
             m.ERR1,
             m.ERR2,
             m.MAD,
             m.PREVANSUIV,
             m.TYPE_PARAM,
             m.AVEC_AS,
             m.NF,
             m.RESTE_A_FAIRE,
             m.TOTPLUS12_OBJ,
             m.PREVANCOURS_OBJ,
             m.PREVANSUIV_OBJ,
             m.RESTE_A_FAIRE_OBJ,
             m.RATIO_12,
             m.RATIO_12_OBJ,
             m.RATIO_COUR_PREC,
             m.RATIO_COUR_PREC_OBJ,
             m.RATIO_SUIV_COUR,
             m.RATIO_SUIV_COUR_OBJ,
             m.RATIO_REAL,
             m.RATIO_REAL_OBJ,
             m.RATIO_A_FAIRE,
             m.RATIO_A_FAIRE_OBJ,
             m.AVEC_OBJ,
             m.MAJ_BATCH,
             m.ERR_PRV_6M,
             m.NB_HISTO,
             m.TAUX_SUITE_DE,
             m.DECALAGE,
             m.HAUTEUR_REAPROVIS,
             m.TYPE_OBJECTIF,
             m.SERIE,
             m.HORIZON,
             m.TAUX_EXPLIT,
             m.ECART_PRV_DELAY,
             m.FORCE_SAIS_DECADE,
             m.NBPERIODE_JOUR,
             m.OBJECTIF2,
             m.VALOBJECTIF2,
             m.UNITE_OBJECTIF2,
             m.HORIZON_AFFICHAGE,
             m.RATIO_PRORATA,
             m.COEF_SAIS1,
             m.COEF_SAIS2,
             m.COEF_SAIS3,
             m.COEF_SAIS4,
             m.COEF_SAIS5,
             m.COEF_SAIS6,
             m.COEF_SAIS7,
             m.COEF_SAIS8,
             m.COEF_SAIS9,
             m.COEF_SAIS10,
             m.COEF_SAIS11,
             m.COEF_SAIS12,
             m.COEF_SAIS13,
             m.COEF_SAIS14,
             m.COEF_SAIS15,
             m.COEF_SAIS16,
             m.COEF_SAIS17,
             m.COEF_SAIS18,
             m.COEF_SAIS19,
             m.COEF_SAIS20,
             m.COEF_SAIS21,
             m.COEF_SAIS22,
             m.COEF_SAIS23,
             m.COEF_SAIS24,
             m.COEF_SAIS25,
             m.COEF_SAIS26,
             m.COEF_SAIS27,
             m.COEF_SAIS28,
             m.COEF_SAIS29,
             m.COEF_SAIS30,
             m.COEF_SAIS31,
             m.COEF_SAIS32,
             m.COEF_SAIS33,
             m.COEF_SAIS34,
             m.COEF_SAIS35,
             m.COEF_SAIS36,
             m.COEF_SAIS37,
             m.COEF_SAIS38,
             m.COEF_SAIS39,
             m.COEF_SAIS40,
             m.COEF_SAIS41,
             m.COEF_SAIS42,
             m.COEF_SAIS43,
             m.COEF_SAIS44,
             m.COEF_SAIS45,
             m.COEF_SAIS46,
             m.COEF_SAIS47,
             m.COEF_SAIS48,
             m.COEF_SAIS49,
             m.COEF_SAIS50,
             m.COEF_SAIS51,
             m.COEF_SAIS52,
             m.COEF_SAIS_JOUR1,
             m.COEF_SAIS_JOUR2,
             m.COEF_SAIS_JOUR3,
             m.COEF_SAIS_JOUR4,
             m.COEF_SAIS_JOUR5,
             m.COEF_SAIS_JOUR6,
             m.COEF_SAIS_JOUR7,
             m.UNUSED_ADDR,
             m.DATE_PREV_ANNEE,
             m.DATE_PREV_PERIODE,
             m.DEBUT_UTIL_SAISON_ANNEE,
             m.DEBUT_UTIL_SAISON_PERIODE,
             m.FIN_UTIL_SAISON_ANNEE,
             m.FIN_UTIL_SAISON_PERIODE,
             m.DATE_FIN_PREV_ANNEE,
             m.DATE_FIN_PREV_PERIODE,
             m.DATE_DEB_PREV_ANNEE,
             m.DATE_DEB_PREV_PERIODE,
             m.DEBUT_UTIL_ANNEE,
             m.DEBUT_UTIL_PERIODE,
             m.DEBUT_HISTO_ANNEE,
             m.DEBUT_HISTO_PERIODE,
             m.DATE_FIN_HISTO_ANNEE,
             m.DATE_FIN_HISTO_PERIODE,
             m.DATE_DEB_OBJ_ANNEE,
             m.DATE_DEB_OBJ_PERIODE,
             m.DATE_FIN_OBJ_ANNEE,
             m.DATE_FIN_OBJ_PERIODE,
             m.DEBUT_UTIL_SAISON_J_ANNEE,
             m.DEBUT_UTIL_SAISON_J_PERIODE,
             m.FIN_UTIL_SAISON_J_ANNEE,
             m.FIN_UTIL_SAISON_J_PERIODE,
             m.DATE_DEBUT_SUITE_ANNEE,
             m.DATE_DEBUT_SUITE_PERIODE,
             m.DATE_FIN_SUITE_ANNEE,
             m.DATE_FIN_SUITE_PERIODE,
             m.DATE_CHOW_ANNEE,
             m.DATE_CHOW_PERIODE,
             m.DATE_PREV_JOUR_ANNEE,
             m.DATE_PREV_JOUR_PERIODE,
             m.NIVEAU,
             m.PENTE,
             m.RESULTAT_CUSUM,
             m.HORIZONFUTUR,
             m.HORIZONPASSE,
             m.CHOIXFONCTION,
             m.SAISONNALITE,
             m.GESTIONDESBORDS,
             m.UNUSED_A,
             m.MAX_NBPERIODE_SAIS,
             m.MAX_NBPERIODE_SAIS_JOUR,
             m.CALCULAUTOJF,
             m.INDICE_PREV,
             m.BESTFIT,
             m.DUREEBESTFIT,
             m.JOUR_DATE_DEB_PREV,
             m.JOUR_DATE_FIN_PREV,
             m.JOUR_DATE_DEB_OBJ,
             m.JOUR_DATE_FIN_OBJ,
             m.SZBESTFITRULENAME,
             m.SZBESTFITRULEDESC,
             m.DLASTMODIFIEDTIME,
             m.COEF_CORREL_R2,
             m.NUM_MOD
        from ' || p_strTableName ||
                ' m, mod n
       where m.bdg_em_addr = n.bdg30_em_addr
         and m.num_mod = n.num_mod';

    /*    insert into t_test values (v_strsql);
    commit;*/
--add log
    --  Fmp_Log.logInfo(pIn_cSqlText => '3_'||v_strsql);
    execute immediate v_strsql;
    commit;

    p_nSqlCode := 0;
    fmp_log.LOGEND;
  exception
    when others then
      p_nSqlCode := sqlcode;
      fmp_log.LOGERROR;

  end sp_SaveDatatoModForecast;

end P_PIMPORT_FORECAST;
/
