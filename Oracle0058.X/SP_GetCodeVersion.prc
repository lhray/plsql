create or replace procedure SP_GetCodeVersion(p_Version out varchar2, p_SqlCode out number)
as
begin
       --Get the database code version number
       p_Version:='7.0.0058.28';
       
       p_SqlCode := 0;
end;
/
