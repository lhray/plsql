create or replace view v_geo_coeff as
select g.geo39_em_addr,
       p.annee_prm,
       f_Convert_1E20(p.m_prm_1) m_prm_1,
       f_Convert_1E20(p.m_prm_2) m_prm_2,
       f_Convert_1E20(p.m_prm_3) m_prm_3,
       f_Convert_1E20(p.m_prm_4) m_prm_4,
       f_Convert_1E20(p.m_prm_5) m_prm_5,
       f_Convert_1E20(p.m_prm_6) m_prm_6,
       f_Convert_1E20(p.m_prm_7) m_prm_7,
       f_Convert_1E20(p.m_prm_8) m_prm_8,
       f_Convert_1E20(p.m_prm_9) m_prm_9,
       f_Convert_1E20(p.m_prm_10) m_prm_10,
       f_Convert_1E20(p.m_prm_11) m_prm_11,
       f_Convert_1E20(p.m_prm_12) m_prm_12,
       f_Convert_1E20(p.m_prm_13) m_prm_13,
       f_Convert_1E20(p.m_prm_14) m_prm_14
  from geodvs g, rpe r, prm p
 where g.geo39_em_addr = 3
   and g.n0_dvs = 0
   and g.dvs38_em_addr <> 1
   and r.dvs26_em_addr = g.dvs38_em_addr
   and p.rpe29_em_addr = r.rpe_em_addr;
