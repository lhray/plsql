create or replace package P_SortSequence Authid Current_User is

  --Sequence
  procedure sp_Sequence(P_Sequence  in varchar2,
                        P_AggruleID in number,
                        P_StrField  out clob,
                        P_Strwhere  out clob,
                        p_SqlCode   out number);

  --Sequence to Data
  procedure sp_SequencetoData(P_SeqID     in number,
                              P_level     in number,
                              P_Display   in varchar2,
                              P_Field0    in varchar2,
                              P_Field1    in varchar2,
                              P_Field2    in varchar2,
                              P_AggruleID in number,
                              T_level     in number,
                              P_i         in number,
                              P_Field     out varchar2,
                              P_wheresql  out varchar2,
                              p_SqlCode   out number);

  --Sequence to update aggregation
  procedure sp_SequencetoAgg(P_Sequence in varchar2,
                             P_StrField out varchar2,
                             P_Strwhere out varchar2,
                             p_SqlCode  out number);

  --Sequence to update aggregation
  procedure sp_SequencetoUpdate(P_SeqID    in number,
                                P_level    in number,
                                P_Display  in varchar2,
                                P_Field0   in varchar2,
                                P_Field1   in varchar2,
                                P_Field2   in varchar2,
                                T_level    in number,
                                P_i        in number,
                                P_Field    out varchar2,
                                P_wheresql out varchar2,
                                p_SqlCode  out number);
end P_SortSequence;
/
create or replace package body P_SortSequence is

  --Sort sequence
  procedure sp_Sequence(P_Sequence  in varchar2,
                        P_AggruleID in number, --if P_AggruleID=0 then DetailNode else AggNode
                        P_StrField  out clob,
                        P_Strwhere  out clob,
                        p_SqlCode   out number) as
    -- v_strsql   varchar2(2000);
    v_Sequence varchar2(5000);
    v_Option   varchar2(100);
    v_next     integer;
    v_length   int := 0;
  
    v_position int := 0;
    v_SeqID    varchar2(50);
    v_level    int;
    v_Display  int;
    v_Field0   varchar2(50);
    v_Field1   varchar2(50);
    v_Field2   varchar2(50);
  
    V_Field    clob;
    V_Strwhere clob;
  
    f_level int;
    g_level int;
    d_level int;
    T_level int;
    i       int;
  
    --FORECAST time series
    vforecast varchar2(100) := ' ';
    --Notes
    vnotes  varchar2(100) := ' ';
    iNopage int := 0;
    --Based on
    iBasedon int := 0;
    --DRP Parameters
    iDRP int := 0;
  
  begin
  
    p_SqlCode := 0;
    i         := 0;
    --add log
    Fmp_Log.FMP_SetValue(P_Sequence);
    Fmp_Log.FMP_SetValue(P_AggruleID);
    Fmp_Log.LOGBEGIN;
    if p_SqlCode <> 0 then
      return;
    end if;
    P_StrField := ',b.bdg_em_addr BDG_ID,b.B_cle BDG_K,b.BDg_desc BDG_D';
  
    if P_AggruleID = 0 then
      P_Strwhere := ' left join pvt p on t.pvt_em_addr=p.pvt_em_addr
                      left join bdg b on p.pvt_cle=b.b_cle and ID_BDG=80 '; --80:detail node
    
      f_level := 1;
      g_level := 1;
      d_level := 1;
    else
      --retrieve the level of the aggregation node in P_AggregateRuleID
      sp_RuleidtoDimentionLevel(p_aggregateruleid => P_AggruleID,
                                f_level           => f_level,
                                g_level           => g_level,
                                d_level           => d_level,
                                p_SqlCode         => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
    
      P_Strwhere := ' left join bdg b on t.sel_cle=b.b_cle and ID_BDG=71 '; --71:aggregate node
    
    end if;
  
    v_Sequence := trim(P_Sequence);
    v_length   := length(v_Sequence);
  
    while v_length > 0 LOOP
      --';' Separated values
      v_next := instr(v_Sequence, ';', 1, 1);
    
      IF v_next = 0 then
        v_Option := v_Sequence;
        v_length := 0;
      end if;
    
      if v_next > 1 then
        v_Option   := trim(substr(v_Sequence, 0, v_next - 1));
        v_Sequence := trim(substr(v_Sequence, v_next + 1));
        v_length   := length(v_Sequence);
      END IF;
    
      --',' Separated values
    
      v_position := INSTR(v_Option, ',', 1, 1);
      v_SeqID    := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_level    := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Display  := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Field0   := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Field1   := trim(substr(v_Option, 1, v_position - 1));
    
      v_Field2 := trim(substr(v_Option, v_position + 1));
    
      if v_SeqID = '9999' then
        --Product
        T_level := f_level;
      elsif v_SeqID = '10001' then
        --Sales Territory
        T_level := g_level;
      elsif v_SeqID = '10002' then
        --Trade Channel
        T_level := d_level;
      elsif v_SeqID = '1340' then
        --Based on
        IF iBasedon = 0 THEN
          P_Strwhere := P_Strwhere ||
                        ' left join SUPPLIER S on b.bdg_em_addr=S.pere_bdg and S.ID_supplier=83
                         left join BDG bd on bd.bdg_em_addr=S.Fils_bdg ';
        END IF;
        iBasedon := 1;
      
      elsif v_SeqID in ('1440', '2270') then
        --Notes
        if v_SeqID = '1440' then
          iNopage := 0;
        elsif v_SeqID = '2270' then
          iNopage := 1000;
        end if;
        IF instr(vnotes, v_level) = 0 THEN
          -- v_level IS NOT IN vnotes
        
          P_Strwhere := P_Strwhere || ' left join SERINOTE N' || v_level ||
                        ' on b.bdg_em_addr=N' || v_level ||
                        '.bdg3_em_addr and N' || v_level || '.Nopage=' ||
                        iNopage || ' and N' || v_level || '.num_mod=' ||
                        v_level;
        
          vnotes := vnotes || ',' || v_level;
        
        END IF;
      
      elsif v_SeqID in ('1300',
                        '1080',
                        '1010',
                        '1020',
                        '1030',
                        '1040',
                        '1050',
                        '1060',
                        '1070',
                        '1310',
                        '1551',
                        '1380',
                        '1552',
                        '1390',
                        '1400',
                        '1410',
                        '1350',
                        '1510',
                        '1450',
                        '1530',
                        '1470',
                        '1480',
                        '1520',
                        '1090',
                        '1180',
                        '1140',
                        '1100',
                        '1110',
                        '1190',
                        '1120',
                        '1130',
                        '1420',
                        '1554',
                        '1200',
                        '1210',
                        '1220',
                        '1230',
                        '1240',
                        '1250',
                        '1260',
                        '1270',
                        '1280',
                        '1320',
                        '1290',
                        '1330',
                        '1553') then
        --FORECAST
        IF instr(vforecast, v_level) = 0 THEN
          -- v_level IS NOT IN vforecast
        
          P_Strwhere := P_Strwhere || ' left join MOD_FORECAST M' ||
                        v_level || ' on b.bdg_em_addr=m' || v_level ||
                        '.bdg_em_addr and m' || v_level || '.num_mod=' ||
                        v_level;
        
          vforecast := vforecast || ',' || v_level;
        
        END IF;
      
      elsif v_SeqID in ('2030',
                        '2040',
                        '2326',
                        '2327',
                        '2328',
                        '2329',
                        '2050',
                        '2280',
                        '2060',
                        '2070',
                        '2090',
                        '2310',
                        '2325',
                        '2080',
                        '2081',
                        '2100',
                        '2110',
                        '2180',
                        '2324',
                        '2160',
                        '2170',
                        '2323',
                        '2210',
                        '2200',
                        '2150',
                        '2120',
                        '2130',
                        '2140',
                        '2334',
                        '2230',
                        '2240',
                        '2250',
                        '2290',
                        '2300',
                        '2320',
                        '2330',
                        '2331',
                        '2332',
                        '2321',
                        '2322',
                        '2190',
                        '2321',
                        '2333') then
        --DRP Parameters
        IF iDRP = 0 THEN
          P_Strwhere := P_Strwhere ||
                        ' left join MOD_DRP M on M.bdg_em_addr=b.bdg_em_addr and num_mod=' ||
                        v_level;
        END IF;
        iDRP := 1;
      
      else
        null;
      end if;
    
      i := i + 1;
      sp_SequencetoData(P_SeqID     => v_SeqID,
                        P_level     => v_level,
                        P_Display   => v_Display,
                        P_Field0    => v_Field0,
                        P_Field1    => v_Field1,
                        P_Field2    => v_Field2,
                        P_AggruleID => P_AggruleID, --0:DetailNode, other is AggNode
                        T_level     => T_level,
                        P_i         => i,
                        P_Field     => V_Field,
                        P_wheresql  => V_Strwhere,
                        p_SqlCode   => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
      P_StrField := P_StrField || V_Field;
      P_Strwhere := P_Strwhere || V_Strwhere;
    
    END LOOP;
    Fmp_log.LOGEND;
  exception
    when others then
      p_SqlCode := sqlcode;
      Fmp_Log.LOGERROR;
      raise_application_error(-20004, sqlerrm);
  end;

  --=======================================================================================================

  --Sequence dimension to sql
  procedure sp_SequencetoData(P_SeqID     in number,
                              P_level     in number,
                              P_Display   in varchar2,
                              P_Field0    in varchar2,
                              P_Field1    in varchar2,
                              P_Field2    in varchar2,
                              P_AggruleID in number, --0:DetailNode, other is AggNode
                              T_level     in number,
                              P_i         in number,
                              P_Field     out varchar2,
                              P_wheresql  out varchar2,
                              p_SqlCode   out number) as
  
    v_level      int;
    v_AttrNumber int;
    vTBName      varchar2(50);
  begin
  
    p_SqlCode := 0;
    v_level   := P_level + T_level - 1;
  
    case
      when P_SeqID = '9999' then
        --Product======================================================
      
        P_Field := P_Field || ',f' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',f' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
      
        case
          when P_Display in ('-1') then
            --Description and ShortDescription
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_desc ' || 'C_' || P_SeqID || '_' || P_level || '_D';
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || 'C_' || P_SeqID || '_' || P_level || '_S';
          
          when P_Display in ('0', '2') then
            --Key And Description
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when P_Display in ('5', '6') then
            --Key And ShortDescription
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
        if P_AggruleID = 0 then
          --DetailNode
        
          P_wheresql := P_wheresql || ' left join v_fam_level f' || P_i ||
                        ' on p.fam4_em_addr=f' || P_i || '.L_1_ID';
        else
          --AggNode
          if v_level = T_level then
            P_wheresql := P_wheresql || ' left join (select distinct L_' ||
                          v_level || '_ID,L_' || v_level || '_Key,L_' ||
                          v_level || '_Desc,L_' || v_level ||
                          '_shortdesc from v_fam_level) f' || P_i ||
                          ' on t.fam4_em_addr=f' || P_i || '.L_' || T_level ||
                          '_ID';
          else
            P_wheresql := P_wheresql || ' left join (select distinct  L_' ||
                          T_level || '_ID,L_' || v_level || '_ID,L_' ||
                          v_level || '_Key,L_' || v_level || '_Desc,L_' ||
                          v_level || '_shortdesc from v_fam_level) f' || P_i ||
                          ' on t.fam4_em_addr=f' || P_i || '.L_' || T_level ||
                          '_ID';
          end if;
        end if;
        --================================================================================================================================================================================
      when P_SeqID = '10001' then
        --Sales Territory
      
        P_Field := P_Field || ',g' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',g' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
        case
          when P_Display in ('-1') then
            --Description and ShortDescription
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_desc ' || 'C_' || P_SeqID || '_' || P_level || '_D';
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || 'C_' || P_SeqID || '_' || P_level || '_S';
          
          when P_Display in ('0', '2') then
            --Key And Description
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          when P_Display in ('5', '6') then
            --Key And ShortDescription
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
        if P_AggruleID = 0 then
          --DetailNode
          P_wheresql := P_wheresql || ' left join v_geo_level g' || P_i ||
                        ' on p.geo5_em_addr=g' || P_i || '.L_1_ID';
        else
          --AggNode
          if v_level = T_level then
            P_wheresql := P_wheresql || ' left join (select distinct L_' ||
                          v_level || '_ID,L_' || v_level || '_Key,L_' ||
                          v_level || '_Desc,L_' || v_level ||
                          '_shortdesc from v_geo_level) g' || P_i ||
                          ' on t.geo5_em_addr=g' || P_i || '.L_' || T_level ||
                          '_ID';
          else
            P_wheresql := P_wheresql || ' left join (select distinct  L_' ||
                          T_level || '_ID,L_' || v_level || '_ID,L_' ||
                          v_level || '_Key,L_' || v_level || '_Desc,L_' ||
                          v_level || '_shortdesc from v_geo_level) g' || P_i ||
                          ' on t.geo5_em_addr=g' || P_i || '.L_' || T_level ||
                          '_ID';
          end if;
        end if;
        --================================================================================================================================================================================
      when P_SeqID = '10002' then
        --Trade Channel
      
        P_Field := P_Field || ',d' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',d' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
        case
          when P_Display in ('-1') then
            --Description and ShortDescription
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_desc ' || 'C_' || P_SeqID || '_' || P_level || '_D';
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || 'C_' || P_SeqID || '_' || P_level || '_S';
          
          when P_Display in ('0', '2') then
            --Key And Description
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          when P_Display in ('5', '6') then
            --Key And ShortDescription
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
        if P_AggruleID = 0 then
          --DetailNode
          P_wheresql := P_wheresql || ' left join v_dis_level d' || P_i ||
                        ' on p.dis6_em_addr=d' || P_i || '.L_1_ID';
        
        else
          --AggNode
          if v_level = T_level then
            P_wheresql := P_wheresql || ' left join (select distinct L_' ||
                          v_level || '_ID,L_' || v_level || '_Key,L_' ||
                          v_level || '_Desc,L_' || v_level ||
                          '_shortdesc from v_dis_level) d' || P_i ||
                          ' on t.dis6_em_addr=d' || P_i || '.L_' || T_level ||
                          '_ID';
          else
            P_wheresql := P_wheresql || ' left join (select distinct  L_' ||
                          T_level || '_ID,L_' || v_level || '_ID,L_' ||
                          v_level || '_Key,L_' || v_level || '_Desc,L_' ||
                          v_level || '_shortdesc from v_dis_level) d' || P_i ||
                          ' on t.dis6_em_addr=d' || P_i || '.L_' || T_level ||
                          '_ID';
          end if;
        end if;
        --================================================================================================================================================================================
      when P_SeqID in ('30050',
                       '30051',
                       '30052',
                       '30053',
                       '30054',
                       '30055',
                       '30056',
                       '30057',
                       '30058',
                       '30059',
                       '30060',
                       '30061',
                       '30062',
                       '30063',
                       '30064',
                       '30065',
                       '30066',
                       '30067',
                       '30068') then
        --Name of Product Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) - 1;
        if P_AggruleID = 0 then
          P_Field := P_Field || ',f_A' || P_i || '.vct_em_addr ' ||
                     P_Field0;
          P_Field := P_Field || ',f_A' || P_i || '.val ' || P_Field1;
        else
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.vct_em_addr,f_A' || P_i ||
                     '.vct_em_addr) ' || P_Field0;
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.val,f_A' || P_i ||
                     '.val) ' || P_Field1;
        end if;
        case
          when P_Display in ('0', '2', '-1') then
          
            --Key And Description
            if P_AggruleID = 0 then
              P_Field := P_Field || ',f_A' || P_i || '.lib_crt ' ||
                         P_Field2;
            else
              P_Field := P_Field || ',nvl(f_SA' || P_i || '.lib_crt,f_A' || P_i ||
                         '.lib_crt) ' || P_Field2;
            end if;
          
          ELSE
            null;
        end case;
      
        --0:DetailNode
        P_wheresql := P_wheresql || ' left join v_AttrValue f_A' || P_i ||
                      ' on fam4_em_addr=f_A' || P_i ||
                      '.fam7_em_addr and f_A' || P_i ||
                      '.id_crt=80 and f_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
        if P_AggruleID <> 0 then
          --AggNode
          P_wheresql := P_wheresql || ' left join v_GetAttributeBySel f_SA' || P_i ||
                        ' on t.sel_em_addr=f_SA' || P_i ||
                        '.sel_em_addr and f_SA' || P_i ||
                        '.id_crt=80 and f_SA' || P_i ||
                        '.rcd_cdt=20007 and f_SA' || P_i || '.num_crt=' ||
                        v_AttrNumber;
        end if;
        --================================================================================================================================================================================
      when P_SeqID in ('30100',
                       '30101',
                       '30102',
                       '30103',
                       '30104',
                       '30105',
                       '30106',
                       '30107',
                       '30108',
                       '30109',
                       '30110',
                       '30111',
                       '30112',
                       '30113',
                       '30114',
                       '30115',
                       '30116',
                       '30117',
                       '30118') then
        --Name of Sales Territory Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) + 49;
      
        if P_AggruleID = 0 then
          P_Field := P_Field || ',g_A' || P_i || '.vct_em_addr ' ||
                     P_Field0;
          P_Field := P_Field || ',g_A' || P_i || '.val ' || P_Field1;
        else
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.vct_em_addr,g_A' || P_i ||
                     '.vct_em_addr) ' || P_Field0;
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.val,g_A' || P_i ||
                     '.val) ' || P_Field1;
        end if;
      
        case
          when P_Display in ('0', '2', '-1') then
            --Key And Description
            if P_AggruleID = 0 then
              P_Field := P_Field || ',g_A' || P_i || '.lib_crt ' ||
                         P_Field2;
            else
              P_Field := P_Field || ',nvl(f_SA' || P_i || '.lib_crt,g_A' || P_i ||
                         '.lib_crt) ' || P_Field2;
            end if;
          
          ELSE
            null;
        end case;
      
        P_wheresql := P_wheresql || ' left join v_AttrValue g_A' || P_i ||
					  ' on geo5_em_addr=g_A' || P_i ||
                      '.geo8_em_addr and g_A' || P_i ||
                      '.id_crt=71 and g_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
        if P_AggruleID <> 0 then
          --AggNode
          P_wheresql := P_wheresql || ' left join v_GetAttributeBySel f_SA' || P_i ||
                        ' on t.sel_em_addr=f_SA' || P_i ||
                        '.sel_em_addr and f_SA' || P_i ||
                        '.id_crt=80 and f_SA' || P_i ||
                        '.rcd_cdt=20008 and f_SA' || P_i || '.num_crt=' ||
                        v_AttrNumber;
        end if;
        --================================================================================================================================================================================
      when P_SeqID in ('30150',
                       '30151',
                       '30152',
                       '30153',
                       '30154',
                       '30155',
                       '30156',
                       '30157',
                       '30158',
                       '30159',
                       '30160',
                       '30161',
                       '30162',
                       '30163',
                       '30164',
                       '30165',
                       '30166',
                       '30167',
                       '30168') then
        --Name of Trade Channel Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) - 1;
      
        if P_AggruleID = 0 then
          P_Field := P_Field || ',d_A' || P_i || '.vct_em_addr ' ||
                     P_Field0;
          P_Field := P_Field || ',d_A' || P_i || '.val ' || P_Field1;
        else
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.vct_em_addr,d_A' || P_i ||
                     '.vct_em_addr) ' || P_Field0;
          P_Field := P_Field || ',nvl(f_SA' || P_i || '.val,d_A' || P_i ||
                     '.val) ' || P_Field1;
        end if;
        case
          when P_Display in ('0', '2', '-1') then
            --Key And Description
          
            if P_AggruleID = 0 then
              P_Field := P_Field || ',d_A' || P_i || '.lib_crt ' ||
                         P_Field2;
            else
              P_Field := P_Field || ',nvl(f_SA' || P_i || '.lib_crt,d_A' || P_i ||
                         '.lib_crt) ' || P_Field2;
            end if;
          
          ELSE
            null;
        end case;
      
        P_wheresql := P_wheresql || ' left join v_AttrValue d_A' || P_i ||
                      ' on dis6_em_addr=d_A' || P_i ||
                      '.dis9_em_addr and d_A' || P_i ||
                      '.id_crt=71 and d_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
        if P_AggruleID <> 0 then
          --AggNode
          P_wheresql := P_wheresql || ' left join v_GetAttributeBySel f_SA' || P_i ||
                        ' on t.sel_em_addr=f_SA' || P_i ||
                        '.sel_em_addr and f_SA' || P_i ||
                        '.id_crt=80 and f_SA' || P_i ||
                        '.rcd_cdt=20009 and f_SA' || P_i || '.num_crt=' ||
                        v_AttrNumber;
        end if;
        --================================================================================================================================================================================
      when P_SeqID in ('27000',
                       '27001',
                       '27002',
                       '27003',
                       '27004',
                       '27005',
                       '27006',
                       '27007',
                       '27008',
                       '27009',
                       '27010',
                       '27011',
                       '27012',
                       '27013',
                       '27014',
                       '27015',
                       '27016',
                       '27017',
                       '27018') then
        --Name of TIME SERIES  Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) + 49;
        P_Field      := P_Field || ',p_A' || P_i || '.crtserie_em_addr ' ||
                        P_Field0;
        P_Field      := P_Field || ',p_A' || P_i || '.val_crt_serie ' ||
                        P_Field1;
        case
          when P_Display in ('0', '2', '-1') then
            --Key And Description
          
            P_Field := P_Field || ',p_A' || P_i || '.lib_crt_serie ' ||
                       P_Field2;
          
          ELSE
            null;
        end case;
      
        if P_AggruleID = 0 then
          --0:DetailNode
        
          P_wheresql := P_wheresql || ' left join v_pvt_attrvalue p_A' || P_i ||
                        ' on p.pvt_em_addr=p_A' || P_i ||
                        '.pvt35_em_addr and p_A' || P_i ||
                        '.num_crt_serie=' || v_AttrNumber;
        else
          --AggNode
        
          P_wheresql := P_wheresql || ' left join v_sel_attrvalue p_A' || P_i ||
                        ' on t.sel_em_addr=p_A' || P_i ||
                        '.sel53_em_addr and p_A' || P_i ||
                        '.num_crt_serie=' || v_AttrNumber;
        end if;
        --================================================forecast================================================================================================================================
      when P_SeqID = '1300' then
        --Model
        P_Field := P_Field || ',m' || P_level || '.TYPE_PARAM ' || P_Field1;
      when P_SeqID = '1080' then
        --Trend Profile
        P_Field := P_Field || ',m' || P_level || '.TYPE_ID ' || P_Field1;
      when P_SeqID = '1340' then
      
        --Based on
        P_Field := P_Field || ',S.fils_BDG ' || P_Field0;
        P_Field := P_Field || ',bd.b_CLE ' || P_Field1;
      
      when P_SeqID = '1010' then
        --Periods of history
        P_Field := P_Field || ',m' || P_level || '.NBPERIODE ' || P_Field1;
      when P_SeqID = '1020' then
        --Start History
        P_Field := P_Field || ',m' || P_level || '.DEBUT_Util_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DEBUT_Util_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1030' then
        --End History
        P_Field := P_Field || ',m' || P_level || '.date_fin_histo_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.date_fin_histo_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1040' then
        --Period number
        if P_AggruleID = 0 then
          P_Field := P_Field || ',FMF_GetPeriod(' || P_Display ||
                     ',1,t.pvt_em_addr,49,substr(''' || P_Field0 ||
                     ''',1,4),substr(''' || P_Field0 || ''',6)) ' ||
                     P_Field1;
        else
          P_Field := P_Field || ',FMF_GetPeriod(' || P_Display ||
                     ',2,t.sel_em_addr,49,substr(''' || P_Field0 ||
                     ''',1,4),substr(''' || P_Field0 || ''',6)) ' ||
                     P_Field1;
        end if;
      when P_SeqID = '1050' then
        --Forecast Horizon
        P_Field := P_Field || ',m' || P_level || '.HORIZON ' || P_Field1;
      when P_SeqID = '1060' then
        --End Forecast
        P_Field := P_Field || ',m' || P_level || '.DATE_FIN_PREV_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DATE_FIN_PREV_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1070' then
        --Start Forecast
        P_Field := P_Field || ',m' || P_level || '.DATE_DEB_PREV_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DATE_DEB_PREV_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1310' then
        --1?? Target
        P_Field := P_Field || ',m' || P_level || '.OBJECTIF ' || P_Field1;
      when P_SeqID = '1551' then
        --Target Quantity 1
        P_Field := P_Field || ',m' || P_level || '.VALOBJECTIF ' ||
                   P_Field1;
      when P_SeqID = '1380' then
        --2?? Target
        P_Field := P_Field || ',m' || P_level || '.OBJECTIF2 ' || P_Field1;
      when P_SeqID = '1552' then
        --Target Quantity 2
        P_Field := P_Field || ',m' || P_level || '.VALOBJECTIF2 ' ||
                   P_Field1;
      when P_SeqID = '1390' then
        --Target Profile
        P_Field := P_Field || ',m' || P_level || '.TYPE_OBJECTIF ' ||
                   P_Field1;
      when P_SeqID = '1400' then
        --Start Target
        P_Field := P_Field || ',m' || P_level || '.DATE_DEB_OBJ_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DATE_DEB_OBJ_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1410' then
        --End Target
        P_Field := P_Field || ',m' || P_level || '.DATE_FIN_OBJ_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DATE_FIN_OBJ_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1350' then
        --As Forced when Splitting
        P_Field := P_Field || ',m' || P_level || '.MAJ_BATCH ' || P_Field1;
        /*when P_SeqID = '1360' then
          --Trading Day Table
          P_Field := P_Field || ',0 ' || P_Field1;
        when P_SeqID = '1370' then
          --External Data
          P_Field := P_Field || ',0 ' || P_Field1;*/
      when P_SeqID = '1510' then
        --Historical smoothing filter
        P_Field := P_Field || ',m' || P_level || '.FILTRAGE ' || P_Field1;
      when P_SeqID = '1450' then
        --Seasonality
        P_Field := P_Field || ',m' || P_level || '.FORCE_SAIS ' || P_Field1;
      when P_SeqID = '1530' then
        --Max No of Periods for Seas
        P_Field := P_Field || ',m' || P_level || '.MAX_NBPERIODE_SAIS  ' ||
                   P_Field1;
        --P_Field := P_Field || ',m' || P_level || '.NB_HISTO  ' || P_Field2;
      
        if P_AggruleID = 0 then
          P_Field := P_Field || ',nvl(m' || P_level ||
                     '.NB_HISTO,FMF_GetPeriod(' || P_Display ||
                     ',1,t.pvt_em_addr,49,substr(''' || P_Field0 ||
                     ''',1,4),substr(''' || P_Field0 || ''',6))) ' ||
                     P_Field2;
        else
          P_Field := P_Field || ',nvl(m' || P_level ||
                     '.NB_HISTO,FMF_GetPeriod(' || P_Display ||
                     ',2,t.sel_em_addr,49,substr(''' || P_Field0 ||
                     ''',1,4),substr(''' || P_Field0 || ''',6))) ' ||
                     P_Field2;
        end if;
      when P_SeqID = '1470' then
        --Start hist for Seas
        P_Field := P_Field || ',m' || P_level ||
                   '.debut_util_saison_annee ' || P_Field1;
        P_Field := P_Field || ',m' || P_level ||
                   '.debut_util_saison_periode ' || P_Field2;
      when P_SeqID = '1480' then
        --End Hist for Seas
        P_Field := P_Field || ',m' || P_level || '.FIN_UTIL_SAISON_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level ||
                   '.FIN_UTIL_SAISON_PERIODE ' || P_Field2;
      when P_SeqID = '1520' then
        --Managing extremities
        P_Field := P_Field || ',m' || P_level || '.GESTIONDESBORDS ' ||
                   P_Field1;
      when P_SeqID = '1090' then
        --Platform
        P_Field := P_Field || ',m' || P_level || '.MOYENNE ' || P_Field1;
        P_Field := P_Field || ',m' || P_level || '.Nf ' || P_Field2;
      when P_SeqID = '1180' then
        --Trend
        P_Field := P_Field || ',m' || P_level || '.TENDANCE ' || P_Field1;
      when P_SeqID = '1140' then
        --Warning Signal
        P_Field := P_Field || ',m' || P_level || '.AWS ' || P_Field1;
      when P_SeqID = '1100' then
        --Tracking Signal
        P_Field := P_Field || ',m' || P_level || '.NF ' || P_Field1;
        P_Field := P_Field || ',m' || P_level || '.Err2 ' || P_Field2;
      when P_SeqID = '1110' then
        --Mean Absolu Dev
        P_Field := P_Field || ',m' || P_level || '.MAD ' || P_Field1;
      when P_SeqID = '1190' then
        --% MAD./Platform
        P_Field := P_Field || ',m' || P_level || '.ERR2 ' || P_Field1;
      when P_SeqID = '1120' then
        --%Forecast Error
        P_Field := P_Field || ',m' || P_level || '.ERR1 ' || P_Field1;
      when P_SeqID = '1130' then
        --%Fore. Error(-6P)
        P_Field := P_Field || ',m' || P_level || '.ERR_PRV_6M ' || P_Field1;
      when P_SeqID = '1420' then
        --1st potential trend break
        P_Field := P_Field || ',m' || P_level || '.DATE_CHOW_ANNEE ' ||
                   P_Field1;
        P_Field := P_Field || ',m' || P_level || '.DATE_CHOW_PERIODE ' ||
                   P_Field2;
      when P_SeqID = '1554' then
        --R2
        P_Field := P_Field || ',m' || P_level || '.COEF_CORREL_R2 ' ||
                   P_Field1;
      when P_SeqID = '1200' then
        --Actual (-12 M)
        P_Field := P_Field || ',m' || P_level || '.TOTMOINS12 ' || P_Field1;
      when P_SeqID = '1210' then
        --Forecast (+12 M)
        P_Field := P_Field || ',m' || P_level || '.TOTPLUS12 ' || P_Field1;
      when P_SeqID = '1220' then
        --(+12 M)/(-12 M) (%)
        P_Field := P_Field || ',m' || P_level || '.RATIO_12 ' || P_Field1;
      when P_SeqID = '1230' then
        --Last Financial Year
        P_Field := P_Field || ',m' || P_level || '.TOTANPREC ' || P_Field1;
      when P_SeqID = '1240' then
        --Curr Financial Year
        P_Field := P_Field || ',m' || P_level || '.PREVANCOURS ' ||
                   P_Field1;
      when P_SeqID = '1250' then
        --Curr Fin Yr/Previous (%)
        P_Field := P_Field || ',m' || P_level || '.RATIO_COUR_PREC ' ||
                   P_Field1;
      when P_SeqID = '1260' then
        --Next Financial Year
        P_Field := P_Field || ',m' || P_level || '.PREVANSUIV ' || P_Field1;
      when P_SeqID = '1270' then
        --Next Fin Yr/Current (%)
        P_Field := P_Field || ',m' || P_level || '.RATIO_SUIV_COUR ' ||
                   P_Field1;
      when P_SeqID = '1280' then
        --Sales to Date
        P_Field := P_Field || ',m' || P_level || '.TOTANCOURS ' || P_Field1;
      when P_SeqID = '1320' then
        --Sales to Date (%)
        P_Field := P_Field || ',m' || P_level || '.RATIO_REAL ' || P_Field1;
      when P_SeqID = '1290' then
        --To be Sold Curr Fin Yr
        P_Field := P_Field || ',m' || P_level || '.RESTE_A_FAIRE ' ||
                   P_Field1;
      when P_SeqID = '1330' then
        --To be Sold Curr Fin Yr (%)
        P_Field := P_Field || ',m' || P_level || '.RATIO_A_FAIRE ' ||
                   P_Field1;
      when P_SeqID = '1553' then
        --Corrected by Ext Event
        P_Field := P_Field || ',m' || P_level || '.AVEC_AS ' || P_Field1;
      when P_SeqID in ('1440', '2270') then
        --Notes from Forecast Models
        P_Field := P_Field || ',N' || P_level || '.texte ' || P_Field1;
      
    --==================================DRP Parameters==============================================
      when P_SeqID = '2030' then
        --In Transit Lead Time
        P_Field := P_Field || ',M.delai_transit ' || P_Field1;
      when P_SeqID = '2040' then
        --Safety Stock
        P_Field := P_Field || ',M.alerte ' || P_Field1;
      when P_SeqID = '2326' then
        --Dynamic safety stock
        P_Field := P_Field || ',M.stock_securite_dynamique ' || P_Field1;
      when P_SeqID = '2327' then
        --Dynamic safety stock(in periode)
        P_Field := P_Field || ',M.ratio_couverture_stock_s_d ' || P_Field1;
      when P_SeqID in ('2328', '2329') then
        --Max dynamic stock
        P_Field := P_Field || ',M.ratio_couverture_stock_m_d ' || P_Field1;
      
      when P_SeqID = '2050' then
        --Safety Time
        P_Field := P_Field || ',M.time_param ' || P_Field1;
      when P_SeqID = '2280' then
        --Safety Time 2
        P_Field := P_Field || ',M.safety_time_2_param ' || P_Field1;
      when P_SeqID = '2060' then
        --Service Level
        P_Field := P_Field || ',M.service ' || P_Field1;
      when P_SeqID = '2070' then
        --Lead Time
        P_Field := P_Field || ',M.delai_appro ' || P_Field1;
      when P_SeqID = '2090' then
        --Lot Size
        P_Field := P_Field || ',M.multi_appro ' || P_Field1;
      when P_SeqID = '2310' then
        --Threshold /Lot Sup
        P_Field := P_Field || ',M.pourcent_lot_size ' || P_Field1;
      when P_SeqID = '2325' then
        --Lot size unit of measure
        P_Field := P_Field || ',M.um_lot_size ' || P_Field1;
      when P_SeqID = '2080' then
        --Min. Order Qty
        P_Field := P_Field || ',M.min_appro ' || P_Field1;
      when P_SeqID = '2081' then
        --Min/Max Order Qty unit of measure
        P_Field := P_Field || ',M.um_qte_min ' || P_Field1;
      when P_SeqID = '2100' then
        --Min.Ord.Qty (in Period)
        P_Field := P_Field || ',M.qt_min_time ' || P_Field1;
      when P_SeqID = '2110' then
        --Max. Order Qty
        P_Field := P_Field || ',M.max_appro ' || P_Field1;
      when P_SeqID = '2180' then
        --Max.Ord.Qty (in Period)
        P_Field := P_Field || ',M.qt_max_time ' || P_Field1;
      when P_SeqID = '2324' then
        --Max. Stock(% of Target Stock)
        P_Field := P_Field || ',M.max_stock_pourcent ' || P_Field1;
      when P_SeqID = '2160' then
        --Maximum Stock
        P_Field := P_Field || ',M.max_stock ' || P_Field1;
      when P_SeqID = '2170' then
        --Max. Stock(in Period)
        P_Field := P_Field || ',M.max_stock_time ' || P_Field1;
      when P_SeqID = '2323' then
        --Min. Stock(% of Target Stock)
        P_Field := P_Field || ',M.min_stock_pourcent ' || P_Field1;
      when P_SeqID = '2210' then
        --Minimum Stock
        P_Field := P_Field || ',M.min_stock ' || P_Field1;
      when P_SeqID = '2200' then
        --Min. Stock(in Period)
        P_Field := P_Field || ',M.min_stock_time ' || P_Field1;
      when P_SeqID = '2150' then
        --Periods to Release
        P_Field := P_Field || ',M.nb_periode_a_lancer ' || P_Field1;
      when P_SeqID = '2120' then
        --Available From
        P_Field := P_Field || ',M.date_deb_appro_annee ' || P_Field1;
        P_Field := P_Field || ',M.date_deb_appro_periode ' || P_Field2;
      when P_SeqID = '2130' then
        --Available Until
        P_Field := P_Field || ',M.date_fin_appro_annee ' || P_Field1;
        P_Field := P_Field || ',M.date_fin_appro_periode ' || P_Field2;
      when P_SeqID = '2140' then
        --Order Frequency
        P_Field := P_Field || ',M.nb_order_periode ' || P_Field1;
      when P_SeqID = '2334' then
        --From the period
        P_Field := P_Field || ',M.first_order_annee ' || P_Field1;
        P_Field := P_Field || ',M.first_order_periode ' || P_Field2;
      when P_SeqID = '2230' then
        --Stock on hand by Date Code
        P_Field := P_Field || ',M.mode_stock_dlc ' || P_Field1;
      when P_SeqID = '2240' then
        --Continuation of for Stock on hand
        P_Field := P_Field || ',M.suite_de ' || P_Field1;
      when P_SeqID = '2250' then
        --Min customer life
        P_Field := P_Field || ',M.dlc_customer ' || P_Field1;
      when P_SeqID = '2290' then
        --Consumer life
        P_Field := P_Field || ',M.dlc_consumer ' || P_Field1;
      when P_SeqID = '2300' then
        --Maturity periods
        P_Field := P_Field || ',M.delai_affinage ' || P_Field1;
      when P_SeqID = '2320' then
        --Number of Days by Week
        P_Field := P_Field || ',M.calendor_supply_nb_working_day ' ||
                   P_Field1;
      when P_SeqID = '2330' then
        --Release lead time
        P_Field := P_Field || ',M.ReleaseLeadTime ' || P_Field1;
      when P_SeqID = '2331' then
        --Release lead time activation
        P_Field := P_Field || ',M.ReleaseLeadTimeNotToApply ' || P_Field1;
      when P_SeqID = '2332' then
        --Level for specification of firm detailed orders
        P_Field := P_Field || ',M.LevelForDetailedF_P_O ' || P_Field1;
      when P_SeqID = '2322' then
        --Zero value for maximum stock allowed
        P_Field := P_Field || ',M.IsZeroOnStockMaxAccepted ' || P_Field1;
      when P_SeqID = '2010' then
        --Stock on hand
        P_Field := P_Field || ',TS.T' || substr(P_Field0, 6) || ' ' ||
                   P_Field1;
        if P_Display = 1 then
          --monthly
          vTBName := '_M';
        elsif P_Display = 2 then
          --2 Weekly
          vTBName := '_W';
        end if;
      
        if P_AggruleID = 0 then
          --1  Detail Node
          vTBName    := 'DON' || vTBName;
          P_wheresql := P_wheresql || ' left join ' || vTBName ||
                        ' TS ON t.pvt_em_addr=TS.PVTID and TSID=50 and VERSION=0 and YY=' ||
                        substr(P_Field0, 1, 4);
        else
          -- other is AggNode
          vTBName    := 'prb' || vTBName;
          P_wheresql := P_wheresql || ' left join ' || vTBName ||
                        ' TS ON t.sel_em_addr=TS.PVTID and TSID=50 and VERSION=0 and YY=' ||
                        substr(P_Field0, 1, 4);
        end if;
      
      when P_SeqID = '2190' then
        --Supplier's closing periods
        P_Field    := P_Field || ',v1.scl_cle ' || P_Field1;
        P_wheresql := P_wheresql ||
                      ' left join FMV_modscl v1 on v1.mod_em_addr=M.mod_em_addr and v1.id_scl=68';
      
      when P_SeqID = '2321' then
        --Frozen table
        P_Field    := P_Field || ',v2.scl_cle ' || P_Field1;
        P_wheresql := P_wheresql ||
                      ' left join FMV_modscl v2 on v2.mod_em_addr=M.mod_em_addr and v2.id_scl=72';
      
        --mode_gelage
        P_Field := P_Field || ',M.mode_gelage ' || P_Field2;
      when P_SeqID = '2333' then
        --Weekly weighting table
        P_Field    := P_Field || ',v3.scl_cle ' || P_Field1;
        P_wheresql := P_wheresql ||
                      ' left join FMV_modscl v3 on v3.mod_em_addr=M.mod_em_addr and v3.id_scl=74';
      
      else
        null;
    end case;
  
  exception
    when others then
      p_SqlCode := sqlcode;
      raise_application_error(-20004, sqlerrm);
  end;

  --Sort sequence to update aggregation
  procedure sp_SequencetoAgg(P_Sequence in varchar2,
                             P_StrField out varchar2,
                             P_Strwhere out varchar2,
                             p_SqlCode  out number) as
    -- v_strsql   varchar2(2000);
    v_Sequence varchar2(2000);
    v_Option   varchar2(100);
    v_next     integer;
    v_length   int := 0;
  
    v_position int := 0;
    v_SeqID    varchar2(50);
    v_level    int;
    v_Display  int;
    v_Field0   varchar2(50);
    v_Field1   varchar2(50);
    v_Field2   varchar2(50);
  
    V_Field    varchar2(5000);
    V_Strwhere varchar2(5000);
  
    f_level int;
    g_level int;
    d_level int;
    T_level int;
    i       int;
  begin
  
    p_SqlCode := 0;
    i         := 0;
  
    P_StrField := '';
  
    P_Strwhere := ' left join pvt p on t.pvt_em_addr=p.pvt_em_addr '; --80:detail node
  
    f_level := 1;
    g_level := 1;
    d_level := 1;
  
    v_Sequence := trim(P_Sequence);
    v_length   := length(v_Sequence);
  
    while v_length > 0 LOOP
      --';' Separated values
      v_next := instr(v_Sequence, ';', 1, 1);
    
      IF v_next = 0 then
        v_Option := v_Sequence;
        v_length := 0;
      end if;
    
      if v_next > 1 then
        v_Option   := trim(substr(v_Sequence, 0, v_next - 1));
        v_Sequence := trim(substr(v_Sequence, v_next + 1));
        v_length   := length(v_Sequence);
      END IF;
    
      --',' Separated values
    
      v_position := INSTR(v_Option, ',', 1, 1);
      v_SeqID    := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_level    := trim(substr(v_Option, 1, v_position - 1));
      IF v_level < 1 THEN
        v_level := 1;
      END IF;
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Display  := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Field0   := trim(substr(v_Option, 1, v_position - 1));
    
      v_Option   := trim(substr(v_Option, v_position + 1));
      v_position := INSTR(v_Option, ',', 1, 1);
      v_Field1   := trim(substr(v_Option, 1, v_position - 1));
    
      v_Field2 := trim(substr(v_Option, v_position + 1));
    
      if v_SeqID = '9999' then
        T_level := f_level;
      elsif v_SeqID = '10001' then
        T_level := g_level;
      elsif v_SeqID = '10002' then
        T_level := d_level;
      end if;
      i := i + 1;
    
      sp_SequencetoUpdate(P_SeqID    => v_SeqID,
                          P_level    => v_level,
                          P_Display  => v_Display,
                          P_Field0   => v_Field0,
                          P_Field1   => v_Field1,
                          P_Field2   => v_Field2,
                          T_level    => T_level,
                          P_i        => i,
                          P_Field    => V_Field,
                          P_wheresql => V_Strwhere,
                          p_SqlCode  => p_SqlCode);
      if p_SqlCode <> 0 then
        return;
      end if;
      P_StrField := P_StrField || V_Field;
      P_Strwhere := P_Strwhere || V_Strwhere;
    
    END LOOP;
  
  exception
    when others then
      p_SqlCode := sqlcode;
      raise_application_error(-20004, sqlerrm);
  end;

  --=======================================================================================================

  --Sequence  to update aggregation
  procedure sp_SequencetoUpdate(P_SeqID    in number,
                                P_level    in number,
                                P_Display  in varchar2,
                                P_Field0   in varchar2,
                                P_Field1   in varchar2,
                                P_Field2   in varchar2,
                                T_level    in number,
                                P_i        in number,
                                P_Field    out varchar2,
                                P_wheresql out varchar2,
                                p_SqlCode  out number) as
  
    v_level      int;
    v_AttrNumber int;
  begin
  
    p_SqlCode := 0;
    v_level   := P_level + T_level - 1;
    case
      when P_SeqID = '9999' then
        --Product======================================================
      
        P_Field := P_Field || ',f' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',f' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
      
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '2' then
            --Description Only
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '5' then
            --Key And ShortDescription
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          when '6' then
            --ShortDescription Only
          
            P_Field := P_Field || ',f' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
      
        --DetailNode
      
        P_wheresql := P_wheresql || ' left join v_fam_level f' || P_i ||
                      ' on p.fam4_em_addr=f' || P_i || '.L_1_ID';
      
    --================================================================================================================================================================================
      when P_SeqID = '10001' then
        --Sales Territory
      
        P_Field := P_Field || ',g' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',g' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '2' then
            --Description Only
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '5' then
            --Key And ShortDescription
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          when '6' then
            --ShortDescription Only
          
            P_Field := P_Field || ',g' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
      
        --DetailNode
        P_wheresql := P_wheresql || ' left join v_geo_level g' || P_i ||
                      ' on p.geo5_em_addr=g' || P_i || '.L_1_ID';
      
    --================================================================================================================================================================================
      when P_SeqID = '10002' then
        --Trade Channel
      
        P_Field := P_Field || ',d' || P_i || '.L_' || v_level || '_ID ' ||
                   P_Field0;
        P_Field := P_Field || ',d' || P_i || '.L_' || v_level || '_Key ' ||
                   P_Field1;
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '2' then
            --Description Only
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_desc ' || P_Field2;
          
          when '5' then
            --Key And ShortDescription
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          when '6' then
            --ShortDescription Only
          
            P_Field := P_Field || ',d' || P_i || '.L_' || v_level ||
                       '_shortdesc ' || P_Field2;
          
          ELSE
            null;
        end case;
      
        --DetailNode
        P_wheresql := P_wheresql || ' left join v_dis_level d' || P_i ||
                      ' on p.dis6_em_addr=d' || P_i || '.L_1_ID';
      
    --================================================================================================================================================================================
      when P_SeqID in ('30050',
                       '30051',
                       '30052',
                       '30053',
                       '30054',
                       '30055',
                       '30056',
                       '30057',
                       '30058',
                       '30059',
                       '30060',
                       '30061',
                       '30062',
                       '30063',
                       '30064',
                       '30065',
                       '30066',
                       '30067',
                       '30068') then
        --Name of Product Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) - 1;
        P_Field      := P_Field || ',f_A' || P_i || '.vct_em_addr ' ||
                        P_Field0;
        P_Field      := P_Field || ',f_A' || P_i || '.val ' || P_Field1;
      
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',f_A' || P_i || '.lib_crt ' || P_Field2;
          
          when '2' then
            --Description Only
            P_Field := P_Field || ',f_A' || P_i || '.lib_crt ' || P_Field2;
            /* when '5' then
              --Key And ShortDescription
            
              P_Field := P_Field || ','''' ' || P_Field2;
            when '6' then
              --ShortDescription Only
              P_Field := P_Field || ','''' ' || P_Field2;*/
          ELSE
            null;
        end case;
      
        P_wheresql := P_wheresql || ' left join v_AttrValue f_A' || P_i ||
                      ' on fam4_em_addr=f_A' || P_i ||
                      '.fam7_em_addr and f_A' || P_i ||
                      '.id_crt=80 and f_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
    --================================================================================================================================================================================
      when P_SeqID in ('30100',
                       '30101',
                       '30102',
                       '30103',
                       '30104',
                       '30105',
                       '30106',
                       '30107',
                       '30108',
                       '30109',
                       '30110',
                       '30111',
                       '30112',
                       '30113',
                       '30114',
                       '30115',
                       '30116',
                       '30117',
                       '30118') then
        --Name of Sales Territory Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) + 49;
        P_Field      := P_Field || ',g_A' || P_i || '.vct_em_addr ' ||
                        P_Field0;
        P_Field      := P_Field || ',g_A' || P_i || '.val ' || P_Field1;
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',g_A' || P_i || '.lib_crt ' || P_Field2;
          
          when '2' then
            --Description Only
            P_Field := P_Field || ',g_A' || P_i || '.lib_crt ' || P_Field2;
            /* when '5' then
              --Key And ShortDescription
            
              P_Field := P_Field || ','''' ' || P_Field2;
            when '6' then
              --ShortDescription Only
              P_Field := P_Field || ','''' ' || P_Field2;*/
          ELSE
            null;
        end case;
      
        P_wheresql := P_wheresql || ' left join v_AttrValue g_A' || P_i ||
                      ' on p.geo5_em_addr=g_A' || P_i ||
                      '.geo8_em_addr and g_A' || P_i ||
                      '.id_crt=71 and g_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
    --================================================================================================================================================================================
      when P_SeqID in ('30150',
                       '30151',
                       '30152',
                       '30153',
                       '30154',
                       '30155',
                       '30156',
                       '30157',
                       '30158',
                       '30159',
                       '30160',
                       '30161',
                       '30162',
                       '30163',
                       '30164',
                       '30165',
                       '30166',
                       '30167',
                       '30168') then
        --Name of Trade Channel Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) - 1;
        P_Field      := P_Field || ',d_A' || P_i || '.vct_em_addr ' ||
                        P_Field0;
        P_Field      := P_Field || ',d_A' || P_i || '.val ' || P_Field1;
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',d_A' || P_i || '.lib_crt ' || P_Field2;
          
          when '2' then
            --Description Only
            P_Field := P_Field || ',d_A' || P_i || '.lib_crt ' || P_Field2;
            /* when '5' then
              --Key And ShortDescription
            
              P_Field := P_Field || ','''' ' || P_Field2;
            when '6' then
              --ShortDescription Only
              P_Field := P_Field || ','''' ' || P_Field2;*/
          ELSE
            null;
        end case;
      
        P_wheresql := P_wheresql || ' left join v_AttrValue d_A' || P_i ||
                      ' on p.dis6_em_addr=d_A' || P_i ||
                      '.dis9_em_addr and d_A' || P_i ||
                      '.id_crt=71 and d_A' || P_i || '.num_crt=' ||
                      v_AttrNumber;
      
    --================================================================================================================================================================================
      when P_SeqID in ('27000',
                       '27001',
                       '27002',
                       '27003',
                       '27004',
                       '27005',
                       '27006',
                       '27007',
                       '27008',
                       '27009',
                       '27010',
                       '27011',
                       '27012',
                       '27013',
                       '27014',
                       '27015',
                       '27016',
                       '27017',
                       '27018') then
        --Name of TIME SERIES  Attribute 1-19
        v_AttrNumber := substr(p_seqid, -2) + 49;
        P_Field      := P_Field || ',p_A' || P_i || '.crtserie_em_addr ' ||
                        P_Field0;
        P_Field      := P_Field || ',p_A' || P_i || '.val_crt_serie ' ||
                        P_Field1;
        case P_Display
          when '0' then
            --Key And Description
          
            P_Field := P_Field || ',p_A' || P_i || '.lib_crt_serie ' ||
                       P_Field2;
          
          when '2' then
            --Description Only
            P_Field := P_Field || ',p_A' || P_i || '.lib_crt_serie ' ||
                       P_Field2;
            /* when '5' then
              --Key And ShortDescription
            
              P_Field := P_Field || ','''' ' || P_Field2;
            when '6' then
              --ShortDescription Only
              P_Field := P_Field || ','''' ' || P_Field2;*/
          ELSE
            null;
        end case;
      
        --1:DetailNode
      
        P_wheresql := P_wheresql || ' left join v_pvt_attrvalue p_A' || P_i ||
                      ' on p.pvt_em_addr=p_A' || P_i ||
                      '.pvt35_em_addr and p_A' || P_i || '.num_crt_serie=' ||
                      v_AttrNumber;
      
    --================================================================================================================================================================================
    
      else
        null;
    end case;
  
  exception
    when others then
      p_SqlCode := sqlcode;
      raise_application_error(-20004, sqlerrm);
  end;

end P_SortSequence;
/
