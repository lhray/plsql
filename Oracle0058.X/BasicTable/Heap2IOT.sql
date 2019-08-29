--procedure
@@p_reCreateIOT.prc
@@p_ImpData2IOT.prc
--
exec p_reCreateIOT;
@@IOT_Tables.sql
exec p_ImpData2IOT;
@@CreateTrigger.sql
