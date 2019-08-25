-------------------------------------------------------------------------
-- Install.sql (sqlplus script)
-- The script is use to create objects in the current schema
-- The objects will be installed includes:table, index, sequence,temporary tables,function,view,materialized view,procedure,package

-- History 
--          JYLiu   11/14/2012   Create

-- Usage:
--      exec the script in SQL*PLUS on WIN.
--      locate your curent location to current directory then exec the script.(use cd command)
--      check the install.log in the current directory 
-------------------------------------------------------------------------

spool instll.log
--basictables
@ .\BasicTable\FM610.sql


--functions
@ .\Function\functions.sql

--views 
@ .\View\views.sql

--mviews
@ .\Mview\MViews.sql

--procedures
@ .\Procedure\procedures.sql

--packages
@ .\Package\packages.sql

--create the GetCodeVersion stored procedure

@@SP_GetCodeVersion.prc

spool off