create or replace view v_dis_level as
select d1.dis_em_addr   L_1_ID,
       d1.d_cle         L_1_key,
       d1.d_desc        L_1_desc,
       d1.d_desc_court  L_1_Shortdesc,
       d2.dis_em_addr   L_2_ID,
       d2.d_cle         L_2_key,
       d2.d_desc        L_2_desc,
       d2.d_desc_court  L_2_Shortdesc,
       d3.dis_em_addr   L_3_ID,
       d3.d_cle         L_3_key,
       d3.d_desc        L_3_desc,
       d3.d_desc_court  L_3_Shortdesc,
       d4.dis_em_addr   L_4_ID,
       d4.d_cle         L_4_key,
       d4.d_desc        L_4_desc,
       d4.d_desc_court  L_4_Shortdesc,
       d5.dis_em_addr   L_5_ID,
       d5.d_cle         L_5_key,
       d5.d_desc        L_5_desc,
       d5.d_desc_court  L_5_Shortdesc,
       d6.dis_em_addr   L_6_ID,
       d6.d_cle         L_6_key,
       d6.d_desc        L_6_desc,
       d6.d_desc_court  L_6_Shortdesc,
       d7.dis_em_addr   L_7_ID,
       d7.d_cle         L_7_key,
       d7.d_desc        L_7_desc,
       d7.d_desc_court  L_7_Shortdesc,
       d8.dis_em_addr   L_8_ID,
       d8.d_cle         L_8_key,
       d8.d_desc        L_8_desc,
       d8.d_desc_court  L_8_Shortdesc,
       d9.dis_em_addr   L_9_ID,
       d9.d_cle         L_9_key,
       d9.d_desc        L_9_desc,
       d9.d_desc_court  L_9_Shortdesc,
       d10.dis_em_addr  L_10_ID,
       d10.d_cle        L_10_key,
       d10.d_desc       L_10_desc,
       d10.d_desc_court L_10_Shortdesc,
       d11.dis_em_addr  L_11_ID,
       d11.d_cle        L_11_key,
       d11.d_desc       L_11_desc,
       d11.d_desc_court L_11_Shortdesc,
       d12.dis_em_addr  L_12_ID,
       d12.d_cle        L_12_key,
       d12.d_desc       L_12_desc,
       d12.d_desc_court L_12_Shortdesc,
       d13.dis_em_addr  L_13_ID,
       d13.d_cle        L_13_key,
       d13.d_desc       L_13_desc,
       d13.d_desc_court L_13_Shortdesc,
       d14.dis_em_addr  L_14_ID,
       d14.d_cle        L_14_key,
       d14.d_desc       L_14_desc,
       d14.d_desc_court L_14_Shortdesc,
       d15.dis_em_addr  L_15_ID,
       d15.d_cle        L_15_key,
       d15.d_desc       L_15_desc,
       d15.d_desc_court L_15_Shortdesc,
       d16.dis_em_addr  L_16_ID,
       d16.d_cle        L_16_key,
       d16.d_desc       L_16_desc,
       d16.d_desc_court L_16_Shortdesc,
       d17.dis_em_addr  L_17_ID,
       d17.d_cle        L_17_key,
       d17.d_desc       L_17_desc,
       d17.d_desc_court L_17_Shortdesc,
       d18.dis_em_addr  L_18_ID,
       d18.d_cle        L_18_key,
       d18.d_desc       L_18_desc,
       d18.d_desc_court L_18_Shortdesc,
       d19.dis_em_addr  L_19_ID,
       d19.d_cle        L_19_key,
       d19.d_desc       L_19_desc,
       d19.d_desc_court L_19_Shortdesc,
       d20.dis_em_addr  L_20_ID,
       d20.d_cle        L_20_key,
       d20.d_desc       L_20_desc,
       d20.d_desc_court L_20_Shortdesc
  from dis d1
  left join dis d2
    on d1.dis2_em_addr = d2.dis_em_addr
  left join dis d3
    on d2.dis2_em_addr = d3.dis_em_addr
  left join dis d4
    on d3.dis2_em_addr = d4.dis_em_addr
  left join dis d5
    on d4.dis2_em_addr = d5.dis_em_addr
  left join dis d6
    on d5.dis2_em_addr = d6.dis_em_addr
  left join dis d7
    on d6.dis2_em_addr = d7.dis_em_addr
  left join dis d8
    on d7.dis2_em_addr = d8.dis_em_addr
  left join dis d9
    on d8.dis2_em_addr = d9.dis_em_addr
  left join dis d10
    on d9.dis2_em_addr = d10.dis_em_addr
  left join dis d11
    on d10.dis2_em_addr = d11.dis_em_addr
  left join dis d12
    on d11.dis2_em_addr = d12.dis_em_addr
  left join dis d13
    on d12.dis2_em_addr = d13.dis_em_addr
  left join dis d14
    on d13.dis2_em_addr = d14.dis_em_addr
  left join dis d15
    on d14.dis2_em_addr = d15.dis_em_addr
  left join dis d16
    on d15.dis2_em_addr = d16.dis_em_addr
  left join dis d17
    on d16.dis2_em_addr = d17.dis_em_addr
  left join dis d18
    on d17.dis2_em_addr = d18.dis_em_addr
  left join dis d19
    on d18.dis2_em_addr = d19.dis_em_addr
  left join dis d20
    on d19.dis2_em_addr = d20.dis_em_addr
 where d1.dis_em_addr not in (select dis2_em_addr from dis);
