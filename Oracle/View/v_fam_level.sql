create or replace view v_fam_level as
select f1.fam_em_addr   L_1_ID,
       f1.f_cle         L_1_key,
       f1.f_desc        L_1_desc,
       f1.f_desc_court  L_1_Shortdesc,
       f2.fam_em_addr   L_2_ID,
       f2.f_cle         L_2_key,
       f2.f_desc        L_2_desc,
       f2.f_desc_court  L_2_Shortdesc,
       f3.fam_em_addr   L_3_ID,
       f3.f_cle         L_3_key,
       f3.f_desc        L_3_desc,
       f3.f_desc_court  L_3_Shortdesc,
       f4.fam_em_addr   L_4_ID,
       f4.f_cle         L_4_key,
       f4.f_desc        L_4_desc,
       f4.f_desc_court  L_4_Shortdesc,
       f5.fam_em_addr   L_5_ID,
       f5.f_cle         L_5_key,
       f5.f_desc        L_5_desc,
       f5.f_desc_court  L_5_Shortdesc,
       f6.fam_em_addr   L_6_ID,
       f6.f_cle         L_6_key,
       f6.f_desc        L_6_desc,
       f6.f_desc_court  L_6_Shortdesc,
       f7.fam_em_addr   L_7_ID,
       f7.f_cle         L_7_key,
       f7.f_desc        L_7_desc,
       f7.f_desc_court  L_7_Shortdesc,
       f8.fam_em_addr   L_8_ID,
       f8.f_cle         L_8_key,
       f8.f_desc        L_8_desc,
       f8.f_desc_court  L_8_Shortdesc,
       f9.fam_em_addr   L_9_ID,
       f9.f_cle         L_9_key,
       f9.f_desc        L_9_desc,
       f9.f_desc_court  L_9_Shortdesc,
       f10.fam_em_addr  L_10_ID,
       f10.f_cle        L_10_key,
       f10.f_desc       L_10_desc,
       f10.f_desc_court L_10_Shortdesc,
       f11.fam_em_addr  L_11_ID,
       f11.f_cle        L_11_key,
       f11.f_desc       L_11_desc,
       f11.f_desc_court L_11_Shortdesc,
       f12.fam_em_addr  L_12_ID,
       f12.f_cle        L_12_key,
       f12.f_desc       L_12_desc,
       f12.f_desc_court L_12_Shortdesc,
       f13.fam_em_addr  L_13_ID,
       f13.f_cle        L_13_key,
       f13.f_desc       L_13_desc,
       f13.f_desc_court L_13_Shortdesc,
       f14.fam_em_addr  L_14_ID,
       f14.f_cle        L_14_key,
       f14.f_desc       L_14_desc,
       f14.f_desc_court L_14_Shortdesc,
       f15.fam_em_addr  L_15_ID,
       f15.f_cle        L_15_key,
       f15.f_desc       L_15_desc,
       f15.f_desc_court L_15_Shortdesc,
       f16.fam_em_addr  L_16_ID,
       f16.f_cle        L_16_key,
       f16.f_desc       L_16_desc,
       f16.f_desc_court L_16_Shortdesc,
       f17.fam_em_addr  L_17_ID,
       f17.f_cle        L_17_key,
       f17.f_desc       L_17_desc,
       f17.f_desc_court L_17_Shortdesc,
       f18.fam_em_addr  L_18_ID,
       f18.f_cle        L_18_key,
       f18.f_desc       L_18_desc,
       f18.f_desc_court L_18_Shortdesc,
       f19.fam_em_addr  L_19_ID,
       f19.f_cle        L_19_key,
       f19.f_desc       L_19_desc,
       f19.f_desc_court L_19_Shortdesc,
       f20.fam_em_addr  L_20_ID,
       f20.f_cle        L_20_key,
       f20.f_desc       L_20_desc,
       f20.f_desc_court L_20_Shortdesc
  from fam f1
  left join fam f2
    on f2.ID_fam <> 80 and f1.fam0_em_addr = f2.fam_em_addr
  left join fam f3
    on f3.ID_fam <> 80 and f2.fam0_em_addr = f3.fam_em_addr
  left join fam f4
    on f4.ID_fam <> 80 and f3.fam0_em_addr = f4.fam_em_addr
  left join fam f5
    on f5.ID_fam <> 80 and f4.fam0_em_addr = f5.fam_em_addr
  left join fam f6
    on f6.ID_fam <> 80 and f5.fam0_em_addr = f6.fam_em_addr
  left join fam f7
    on f7.ID_fam <> 80 and f6.fam0_em_addr = f7.fam_em_addr
  left join fam f8
    on f8.ID_fam <> 80 and f7.fam0_em_addr = f8.fam_em_addr
  left join fam f9
    on f9.ID_fam <> 80 and f8.fam0_em_addr = f9.fam_em_addr
  left join fam f10
    on f10.ID_fam <> 80 and f9.fam0_em_addr = f10.fam_em_addr
  left join fam f11
    on f11.ID_fam <> 80 and f10.fam0_em_addr = f11.fam_em_addr
  left join fam f12
    on f12.ID_fam <> 80 and f11.fam0_em_addr = f12.fam_em_addr
  left join fam f13
    on f13.ID_fam <> 80 and f12.fam0_em_addr = f13.fam_em_addr
  left join fam f14
    on f14.ID_fam <> 80 and f13.fam0_em_addr = f14.fam_em_addr
  left join fam f15
    on f15.ID_fam <> 80 and f14.fam0_em_addr = f15.fam_em_addr
  left join fam f16
    on f16.ID_fam <> 80 and f15.fam0_em_addr = f16.fam_em_addr
  left join fam f17
    on f17.ID_fam <> 80 and f16.fam0_em_addr = f17.fam_em_addr
  left join fam f18
    on f18.ID_fam <> 80 and f17.fam0_em_addr = f18.fam_em_addr
  left join fam f19
    on f19.ID_fam <> 80 and f18.fam0_em_addr = f19.fam_em_addr
  left join fam f20
    on f20.ID_fam <> 80 and f19.fam0_em_addr = f20.fam_em_addr
 where f1.ID_fam = 80;
