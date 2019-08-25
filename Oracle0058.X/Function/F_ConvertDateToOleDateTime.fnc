create or replace function F_ConvertDateToOleDateTime(dtDateTime in Date)
  return number is
  Result number;
begin
  
  /* 
  * Convert Date to COleDateTime. 
  *
  * Created by Frank on 2012-09-07.
  */
  
  Result := dtDateTime - F_GetOleDateTimeBegin();
  
  return(Result);
  
end F_ConvertDateToOleDateTime;
/
