create or replace function f_GetUoMNoForGroup(f_aggnodeid number)
  return number as
  /********
  Created by JYliu on 2012/10/23 the function return which UoM the Aggregate node will use for calculate the Unit of Measure
  Rule 1:the aggregate is made by a product group ,return the address(VCT) of 1st UoM of this product group
  Rule 2:the aggregate is made by a product,return the address(VCT) of 1st UoM of the product 's parent (its group)
  Rule 3:no product is made in this aggregate node,return 0.special value
  ********/
  v_result      number := 0;
  v_productID   number;
  v_productType number;
begin
  --find the aggregete node 's product
  select nvl(max(c.adr_cdt), -1)
    into v_productID
    from cdt c
   where c.sel11_em_addr = f_aggnodeid
     and c.rcd_cdt = 10000
     and c.operant = 1;
  if v_productID = -1 then
    -- rule 3
    v_result := 0;
  else
    select f.id_fam
      into v_productType
      from fam f
     where f.fam_em_addr = v_productID;

    case v_productType
      when 71 then
        --rule 1
        null;
      when 80 then
        --rule 2   find its parant
        select f.fam0_em_addr
          into v_productID
          from fam f
         where f.fam_em_addr = v_productID;
    end case;
    select r.vct10_em_addr
      into v_result
      from rfc r
     where r.fam7_em_addr = v_productID
       and r.ident_crt = 70
       and r.numero_crt = 69;
  end if;
  return v_result;
exception
  when others then
    raise_application_error(-20004, sqlcode || sqlerrm);
end;
/
