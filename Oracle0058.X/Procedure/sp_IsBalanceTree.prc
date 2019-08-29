create or replace procedure sp_IsBalanceTree(p_Demension in number, -- 1 product ,2 sale territory ,3 trade channel
                                             p_Rootid    in number,
                                             P_IsBalance out number,
                                             p_sqlcode   out number) authid current_user as
  /********
  Created by JYliu on 15/10/2012 to check whether the tree under the specified key is balance
  Modified by JYLiu on 6/12/2012 check the balance tree by the level of the leaf node.if the leaf's level is the same ,the tree is balance
  ********/
begin
  p_sqlcode := 0;
  case p_Demension
    when 1 then
      --product tree
      select count(distinct nlevel)
        into P_IsBalance
        from (select connect_by_root fam_em_addr grp, level nlevel
                from fam f
               where f.id_fam = 80
                 and connect_by_isleaf = 1
               start with f.fam0_em_addr = p_Rootid
              connect by prior f.fam_em_addr = f.fam0_em_addr);
    when 2 then
      --sale territory tree
      select count(distinct nlevel)
        into P_IsBalance
        from (select connect_by_root geo_em_addr grp, level nlevel
                from geo g
               where connect_by_isleaf = 1
               start with g.geo1_em_addr = p_Rootid
              connect by prior g.geo_em_addr = g.geo1_em_addr);
    when 3 then
      --trade channel tree
      select count(distinct nlevel)
        into P_IsBalance
        from (select connect_by_root dis_em_addr grp, level nlevel
                from dis d
               where connect_by_isleaf = 1
               start with d.dis2_em_addr = p_Rootid
              connect by prior d.dis_em_addr = d.dis2_em_addr);
    else
      p_sqlcode := -1;
  end case;

exception
  when others then
    p_sqlcode := -20004;
    raise_application_error(p_sqlcode, sqlcode || sqlerrm);
end;
/
