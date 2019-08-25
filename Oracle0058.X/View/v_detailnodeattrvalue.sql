create or replace view v_detailnodeattrvalue as
select p.pvt_em_addr,p.fam4_em_addr,p.geo5_em_addr,p.dis6_em_addr,"C49","C50","C51","C52","C53","C54","C55","C56","C57","C58","C59","C60","C61","C62","C63","C64","C65","C66","C67"
from pvt p ,(select pvt35_em_addr pvt_em_addr  ,"C49","C50","C51","C52","C53","C54","C55","C56","C57","C58","C59","C60","C61","C62","C63","C64","C65","C66","C67"
  from (select r.pvt35_em_addr,r.numero_crt_pvt,r.crtserie36_em_addr from pvtcrt r )  pivot(max(crtserie36_em_addr) for numero_crt_pvt in(49 as c49,50 as c50,  51 as c51, 52 as c52,  53 as c53, 54 as c54, 55 as c55, 56 as c56, 57 as c57, 58 as c58, 59 as c59, 60 as c60, 61 as c61, 62 as c62, 63 as c63, 64 as c64, 65 as c65,  66 as c66,67 as c67)) ) t
  where p.pvt_em_addr=t.pvt_em_addr(+);
