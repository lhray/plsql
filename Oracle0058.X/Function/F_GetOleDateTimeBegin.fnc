create or replace function F_GetOleDateTimeBegin return date is
  Result date;
begin

  /* 
  * Get the beginning Date of the COleDateTime.    
  *
  * Created by Frank on 2012-09-07.
  */

  /* 
  * About COleDateTime:
  * 
  * COleDateTime does not have a base class.
  * 
  * It is one of the possible types for the VARIANT data type of OLE automation. 
  * A COleDateTime value represents an absolute date and time value.
  * 
  * The DATE type is implemented as a floating-point value, measuring days 
  * from midnight, 30 December 1899. So, midnight, 31 December 1899 is 
  * represented by 1.0. Similarly, 6 AM, 1 January 1900 is represented by 2.25, 
  * and midnight, 29 December 1899 is ??¨¬C 1.0. However, 6 AM, 29 December 1899 is ??¨¬C 1.25.
  * 
  * For detail, please refer to: http://msdn.microsoft.com/en-us/library/38wh24td(VS.80).aspx
  */

  Result := to_date('1899-12-30 00:00:00', 'yyyy/mm/dd hh24:mi:ss');

  return(Result);

end F_GetOleDateTimeBegin;
/
