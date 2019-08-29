create or replace view v_detailnodewithcoeff as
select p.pvt_em_addr detailnodeid,
       year,
       value1,
       value2,
       value3,
       value4,
       value5,
       value6,
       value7,
       value8,
       value9,
       value10,
       value11,
       value12,
       value13,
       value14
  from (select d.geo39_em_addr,
               p.annee_prm year,
               f_Convert_1E20(p.m_prm_1) value1,
               f_Convert_1E20(p.m_prm_2) value2,
               f_Convert_1E20(p.m_prm_3) value3,
               f_Convert_1E20(p.m_prm_4) value4,
               f_Convert_1E20(p.m_prm_5) value5,
               f_Convert_1E20(p.m_prm_6) value6,
               f_Convert_1E20(p.m_prm_7) value7,
               f_Convert_1E20(p.m_prm_8) value8,
               f_Convert_1E20(p.m_prm_9) value9,
               f_Convert_1E20(p.m_prm_10) value10,
               f_Convert_1E20(p.m_prm_11) value11,
               f_Convert_1E20(p.m_prm_12) value12,
               f_Convert_1E20(p.m_prm_13) value13,
               f_Convert_1E20(p.m_prm_14) value14
          from geodvs d, prm p, rpe r
         where d.dvs38_em_addr <> 1
           and r.dvs26_em_addr = d.dvs38_em_addr
           and r.rpe_em_addr = p.rpe29_em_addr) t,
       pvt p
 where p.geo5_em_addr = t.geo39_em_addr(+);
