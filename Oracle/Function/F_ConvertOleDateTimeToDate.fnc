create or replace function F_ConvertOleDateTimeToDate(fDateTime in number)
  return date is
  Result date;
begin

  /* 
   * Convert COleDateTime to Date. 
   *
   * Created by Frank on 2012-09-07.
   */

  Result := F_GetOleDateTimeBegin() + fDateTime;

  return(Result);

end F_ConvertOleDateTimeToDate;
/
