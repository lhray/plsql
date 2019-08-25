declare
   v_njobno   number;
   v_nsqlcode number; 
   v_esqlmsg  varchar2(1024); 
begin 
   -- Call the procedure
  fmisp_gather_stats(pin_isjobrun  => 1,
                     pout_njobno   => v_njobno,
                     pout_nsqlcode => v_nsqlcode,
                     pout_esqlmsg  => v_esqlmsg);
end;
/ 
exit;