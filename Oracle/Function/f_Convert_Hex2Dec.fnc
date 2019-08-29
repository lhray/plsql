create or replace function f_convert_hex2dec(f_adr_cdt raw) return number is
  /********
  Created by JyLIu on 3/7/2012 For: calculate the last 16 bit of adr_cdt of cdt, (convert HEX to DEC)
  ********/
  v_result number := 0; --result 
  v_raw    raw(16); --the last 16bit HEX data
  v_tmp    raw(16); --every 2 byte data
begin
  v_raw := substr(f_adr_cdt, -16);
  --loop 8 times,every time calculate 2 bit. 
  for i in 1 .. 8 loop
    v_tmp    := substr(v_raw, 2 * i - 1, 2);
    v_result := v_result +
                to_number(substr(v_tmp, -1, 1), 'x') * power(16, 2 * i - 2) +
                to_number(substr(v_tmp, 1, 1), 'x') * power(16, 2 * i - 1);
  end loop;
  return v_result;
exception
  when others then
    raise_application_error(sqlcode, sqlerrm);
end;
/
