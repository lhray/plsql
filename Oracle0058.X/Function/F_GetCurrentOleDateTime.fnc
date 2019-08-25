create or replace function F_GetCurrentOleDateTime return number is
  Result number;
begin

  /* 
   * Get the current Date which is a number of OleDateTime formate. 
   *
   * Created by Frank on 2012-09-07.
   */

  Result := F_ConvertDateToOleDateTime(sysdate);

  return(Result);

end F_GetCurrentOleDateTime;
/
