create or replace procedure SP_RuleIDtoDimentionLevel(P_AggregateRuleID in number,
                                                      f_level           out number,
                                                      g_level           out number,
                                                      d_level           out number,
                                                      p_SqlCode         out number) as

  v_strsql varchar2(1000);

begin
  p_SqlCode := 0;
  --retrieve the level of the aggregation node in P_AggregateRuleID
  v_Strsql := 'select regroup_pro, regroup_geo, regroup_dis from prv where prv_em_addr=' ||
              P_AggregateRuleID;
  execute immediate v_strsql
    into f_level, g_level, d_level;
  --if regoup_pro is zero, we need to get the level from product key condition
  if f_level = 0 then
    --product level
    v_Strsql := 'select nvl(F_IDtoLevel(''fam'',max(adr_cdt)),1) from cdt where  rcd_cdt=10000 and n0_val_cdt=0 and prv12_em_addr=' ||
                P_AggregateRuleID;
    execute immediate v_strsql
      into f_level;
  end if;
  if g_level = 0 then
    --Sales Territory level
    v_Strsql := 'select nvl(F_IDtoLevel(''geo'',max(adr_cdt)),1) from cdt where  rcd_cdt=10001 and n0_val_cdt=0 and prv12_em_addr=' ||
                P_AggregateRuleID;
    execute immediate v_strsql
      into g_level;
  end if;
  if d_level = 0 then
    --Trade Channel level
    v_Strsql := 'select nvl(F_IDtoLevel(''dis'',max(adr_cdt)),1) from cdt where  rcd_cdt=10002 and n0_val_cdt=0 and prv12_em_addr=' ||
                P_AggregateRuleID;
    execute immediate v_strsql
      into d_level;
  end if;

exception
  when others then
    p_SqlCode := sqlcode;
    raise_application_error(-20004, sqlerrm);
end;
/
