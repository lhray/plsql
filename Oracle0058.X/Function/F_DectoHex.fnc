create or replace function F_DectoHex(f_table in number, f_addr in number)
  RETURN raw IS
  v_addr   varchar2(32);
  v_result varchar2(64);
  v_table  number;
begin
  v_result := '0000';
  v_table  := to_char(f_table, 'xxxx');
  v_result := v_result || substr(v_table, -2) ||
              substr(v_table, 1, 2);
  v_result := v_result || '00000000';
  v_addr   := replace(UPPER(to_char(f_addr, 'xxxxxxxxxxxxxxxx')), ' ', '0');

  for i in 1 .. 8 loop
    v_result := v_result || substr(v_addr, -2);
    v_addr   := substr(v_addr, 1, length(v_addr) - 2);
  end loop;
  return v_result;
end;
/
