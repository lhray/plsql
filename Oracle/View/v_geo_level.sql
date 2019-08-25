create or replace view v_geo_level as
select g1.geo_em_addr   L_1_ID,
       g1.g_cle         L_1_key,
       g1.g_desc        L_1_desc,
       g1.g_desc_court  L_1_Shortdesc,
       g2.geo_em_addr   L_2_ID,
       g2.g_cle         L_2_key,
       g2.g_desc        L_2_desc,
       g2.g_desc_court  L_2_Shortdesc,
       g3.geo_em_addr   L_3_ID,
       g3.g_cle         L_3_key,
       g3.g_desc        L_3_desc,
       g3.g_desc_court  L_3_Shortdesc,
       g4.geo_em_addr   L_4_ID,
       g4.g_cle         L_4_key,
       g4.g_desc        L_4_desc,
       g4.g_desc_court  L_4_Shortdesc,
       g5.geo_em_addr   L_5_ID,
       g5.g_cle         L_5_key,
       g5.g_desc        L_5_desc,
       g5.g_desc_court  L_5_Shortdesc,
       g6.geo_em_addr   L_6_ID,
       g6.g_cle         L_6_key,
       g6.g_desc        L_6_desc,
       g6.g_desc_court  L_6_Shortdesc,
       g7.geo_em_addr   L_7_ID,
       g7.g_cle         L_7_key,
       g7.g_desc        L_7_desc,
       g7.g_desc_court  L_7_Shortdesc,
       g8.geo_em_addr   L_8_ID,
       g8.g_cle         L_8_key,
       g8.g_desc        L_8_desc,
       g8.g_desc_court  L_8_Shortdesc,
       g9.geo_em_addr   L_9_ID,
       g9.g_cle         L_9_key,
       g9.g_desc        L_9_desc,
       g9.g_desc_court  L_9_Shortdesc,
       g10.geo_em_addr  L_10_ID,
       g10.g_cle        L_10_key,
       g10.g_desc       L_10_desc,
       g10.g_desc_court L_10_Shortdesc,
       g11.geo_em_addr  L_11_ID,
       g11.g_cle        L_11_key,
       g11.g_desc       L_11_desc,
       g11.g_desc_court L_11_Shortdesc,
       g12.geo_em_addr  L_12_ID,
       g12.g_cle        L_12_key,
       g12.g_desc       L_12_desc,
       g12.g_desc_court L_12_Shortdesc,
       g13.geo_em_addr  L_13_ID,
       g13.g_cle        L_13_key,
       g13.g_desc       L_13_desc,
       g13.g_desc_court L_13_Shortdesc,
       g14.geo_em_addr  L_14_ID,
       g14.g_cle        L_14_key,
       g14.g_desc       L_14_desc,
       g14.g_desc_court L_14_Shortdesc,
       g15.geo_em_addr  L_15_ID,
       g15.g_cle        L_15_key,
       g15.g_desc       L_15_desc,
       g15.g_desc_court L_15_Shortdesc,
       g16.geo_em_addr  L_16_ID,
       g16.g_cle        L_16_key,
       g16.g_desc       L_16_desc,
       g16.g_desc_court L_16_Shortdesc,
       g17.geo_em_addr  L_17_ID,
       g17.g_cle        L_17_key,
       g17.g_desc       L_17_desc,
       g17.g_desc_court L_17_Shortdesc,
       g18.geo_em_addr  L_18_ID,
       g18.g_cle        L_18_key,
       g18.g_desc       L_18_desc,
       g18.g_desc_court L_18_Shortdesc,
       g19.geo_em_addr  L_19_ID,
       g19.g_cle        L_19_key,
       g19.g_desc       L_19_desc,
       g19.g_desc_court L_19_Shortdesc,
       g20.geo_em_addr  L_20_ID,
       g20.g_cle        L_20_key,
       g20.g_desc       L_20_desc,
       g20.g_desc_court L_20_Shortdesc
  from geo g1
  left join geo g2
    on g1.geo1_em_addr = g2.geo_em_addr
  left join geo g3
    on g2.geo1_em_addr = g3.geo_em_addr
  left join geo g4
    on g3.geo1_em_addr = g4.geo_em_addr
  left join geo g5
    on g4.geo1_em_addr = g5.geo_em_addr
  left join geo g6
    on g5.geo1_em_addr = g6.geo_em_addr
  left join geo g7
    on g6.geo1_em_addr = g7.geo_em_addr
  left join geo g8
    on g7.geo1_em_addr = g8.geo_em_addr
  left join geo g9
    on g8.geo1_em_addr = g9.geo_em_addr
  left join geo g10
    on g9.geo1_em_addr = g10.geo_em_addr
  left join geo g11
    on g10.geo1_em_addr = g11.geo_em_addr
  left join geo g12
    on g11.geo1_em_addr = g12.geo_em_addr
  left join geo g13
    on g12.geo1_em_addr = g13.geo_em_addr
  left join geo g14
    on g13.geo1_em_addr = g14.geo_em_addr
  left join geo g15
    on g14.geo1_em_addr = g15.geo_em_addr
  left join geo g16
    on g15.geo1_em_addr = g16.geo_em_addr
  left join geo g17
    on g16.geo1_em_addr = g17.geo_em_addr
  left join geo g18
    on g17.geo1_em_addr = g18.geo_em_addr
  left join geo g19
    on g18.geo1_em_addr = g19.geo_em_addr
  left join geo g20
    on g19.geo1_em_addr = g20.geo_em_addr
 where g1.geo_em_addr not in (select geo1_em_addr from geo);
