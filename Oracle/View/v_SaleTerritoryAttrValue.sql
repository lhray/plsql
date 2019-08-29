create or replace view v_saleterritoryattrvalue as
select g.geo_em_addr,g.g_cle,g.g_desc,g.geo1_em_addr,g.grp,g.isleaf,g.nlevel,p.C49,p.C50,p.C51,p.C52,p.C53,p.C54,p.C55,p.C56,p.C57,p.C58,p.C59,p.C60,p.C61,p.C62,p.C63,p.C64,p.C65,p.C66,p.C67
from v_geo_tree g ,(
select geo8_em_addr,"C49","C50","C51","C52","C53","C54","C55","C56","C57","C58","C59","C60","C61","C62","C63","C64","C65","C66","C67"
  from (select distinct geo8_em_addr,num_crt,vct_em_addr from v_AttrValue where id_crt=71)
 pivot(max(vct_em_addr) for num_crt in(49 as c49,
                                                                             50 as c50,
                                                                             51 as c51,
                                                                             52 as c52,
                                                                             53 as c53,
                                                                             54 as c54,
                                                                             55 as c55,
                                                                             56 as c56,
                                                                             57 as c57,
                                                                             58 as c58,
                                                                             59 as c59,
                                                                             60 as c60,
                                                                             61 as c61,
                                                                             62 as c62,
                                                                             63 as c63,
                                                                             64 as c64,
                                                                             65 as c65,
                                                                             66 as c66,
                                                                             67 as c67))) p
where g.geo_em_addr=p.geo8_em_addr(+);
