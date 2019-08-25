create or replace package FMP_Dimesion is

  function FMF_GetRoot(pIn_nDimesion in number) return number;

end FMP_Dimesion;
/
create or replace package body FMP_Dimesion is

  --*****************************************************************
  -- Description: public procedure to process 3D
  --
  --
  -- Author:      JY.Liu
  -- Revision History
  -- Version      Date            Author       Reason for Change
  -- --------------------------------------------------------------
  --  V7.0        14-JAN-2013     JY.Liu     Created.
  -- **************************************************************

  function FMF_GetRoot(pIn_nDimesion in number) return number
  --*****************************************************************
    -- Description: get the root id of 3D
    --
    -- Parameters:
    --       pIn_nDimesion: 1 fam;2 geo;3:dis
  
    -- Error Conditions Raised:
    --
    -- Author:      JY.Liu
    -- Revision History
    -- Version      Date            Author       Reason for Change
    -- --------------------------------------------------------------
    --  V7.0        14-JAN-2013     JY.Liu     Created.
    -- **************************************************************
   is
    result number;
  begin
    if pIn_nDimesion = 1 then
      select fam_em_addr into result from fam where f_cle is null;
    elsif pIn_nDimesion = 2 then
      select geo_em_addr into result from geo where ascii(g_cle) = 1;    
    elsif pIn_nDimesion = 3 then
      select dis_em_addr into result from dis where ascii(d_cle) = 1;
    end if;
    return result;
  exception
    when others then
      raise_application_error(-20004, sqlcode);
  end;
end FMP_Dimesion;
/
