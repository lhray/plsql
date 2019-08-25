create or replace procedure SP_Old_Tab_Is_Exists(p_new_tab_name in varchar2,
                                                 E_flag         out boolean)
/******************************************************************************/
  /* author zuojunhua                                                           */
  /* date   2012-10-31                                                          */
  /* info: if table is exists ,it's diffrent,else it's created!                 */
  /******************************************************************************/
 is
  v_num         number;
  v_new_tabname varchar2(50);
begin
  v_new_tabname := p_new_tab_name;
  E_flag        := False;
  v_num         := 0;

  select count(1)
    into v_num
    from user_tables t
   where t.TABLE_NAME =
         substr(trim(v_new_tabname), 6, length(trim(v_new_tabname)) - 5);

  if v_num = 1 then
    E_flag := TRUE;
  else
    E_flag := FALSE;
  end if;

Exception
  when others then
    null;
end SP_Old_Tab_Is_Exists;
/
