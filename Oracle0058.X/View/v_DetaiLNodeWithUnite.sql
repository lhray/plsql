create or replace view v_detailnodewithunite as
select p.pvt_em_addr detailnodeid,
       nvl(f_Convert_1E20(f.unite_1),1)  unite1,
       nvl(f_Convert_1E20(f.unite_2),1)  unite2,
       nvl(f_Convert_1E20(f.unite_3),1)  unite3,
       nvl(f_Convert_1E20(f.unite_4),1)  unite4,
       nvl(f_Convert_1E20(f.unite_5),1)  unite5,
       nvl(f_Convert_1E20(f.unite_6),1)  unite6,
       nvl(f_Convert_1E20(f.unite_7),1)  unite7,
       nvl(f_Convert_1E20(f.unite_8),1)  unite8,
       nvl(f_Convert_1E20(f.unite_9),1)  unite9,
       nvl(f_Convert_1E20(f.unite_10),1) unite10
  from pvt p, fam f
 where p.fam4_em_addr = f.fam_em_addr;
