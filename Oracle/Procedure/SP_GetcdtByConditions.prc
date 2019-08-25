create or replace procedure SP_GetcdtByConditions(P_Conditions in varchar2,
                                                  p_SqlCode    out number) as

  /********
  Created by FQWang on 13/7/2012 parse the condition string and insert it into tmp_cdt. string '0,10000,1,0,3' is the order of ATTRORDNO,TABID,OPE,VAL_IDX,ADDR
  ********/
  v_sourcestr_next int := 1;
  v_regstr_length  int := 0;
  v_strsql         varchar2(32767);
  v_Conditions     varchar2(32767);
  v_position       int := 1; --Find the beginning of the location
  v_value1         int;
  v_value2         int;
  v_value3         int;
  v_value4         int;
  v_value5         int;
begin
  delete from tmp_cdt;
  v_Conditions    := P_Conditions;
  v_regstr_length := length(v_Conditions);

  while v_regstr_length > 0 LOOP
    --';' Separated values
    v_sourcestr_next := instr(v_Conditions, ';', 1, 1);
    IF v_sourcestr_next = 0 then
      v_strsql        := v_Conditions;
      v_regstr_length := 0;
    end if;
    if v_sourcestr_next > 1 then
      v_strsql := substr(v_Conditions, 0, v_sourcestr_next - 1);
    
      v_Conditions    := substr(v_Conditions, v_sourcestr_next + 1);
      v_regstr_length := length(v_Conditions);
    END IF;
  
    --',' Separated values
    v_position := INSTR(v_strsql, ',', 1, 1);
    v_value1   := substr(v_strsql, 1, v_position - 1);
    v_strsql   := substr(v_strsql, v_position + 1);
  
    v_position := INSTR(v_strsql, ',', 1, 1);
    v_value2   := substr(v_strsql, 1, v_position - 1);
    v_strsql   := substr(v_strsql, v_position + 1);
  
    v_position := INSTR(v_strsql, ',', 1, 1);
    v_value3   := substr(v_strsql, 1, v_position - 1);
    v_strsql   := substr(v_strsql, v_position + 1);
  
    v_position := INSTR(v_strsql, ',', 1, 1);
    v_value4   := substr(v_strsql, 1, v_position - 1);
    v_strsql   := substr(v_strsql, v_position + 1);
  
    v_value5 := v_strsql;
    insert into tmp_cdt
      (tabid, attrordno, ope, val_idx, addr)
    values
      (v_value2, v_value1, v_value3, v_value4, v_value5);
    commit;
  
  END LOOP;
  p_SqlCode := 0;
exception
  when others then
    p_SqlCode := sqlcode;
    raise_application_error(-20004, sqlerrm);
  
end;
/
