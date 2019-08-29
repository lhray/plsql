-------------------------------------------------------------------
--
--  File : install.sql (SQLPlus script)
--
--  Description : installation of the LOG4PLSQL framework
-------------------------------------------------------------------
--
-- history : who                 created     comment
--     v1    Bertrand Caradec   15-MAY-08    creation
--                                     
--
-------------------------------------------------------------------
/*
 * Copyright (C) LOG4PLSQL project team. All rights reserved.
 *
 * This software is published under the terms of the The LOG4PLSQL 
 * Software License, a copy of which has been included with this
 * distribution in the LICENSE.txt file.  
 * see: <http://log4plsql.sourceforge.net>  */


spool install.txt

PROMPT LOG4PLSQL Installation
PROMPT **********************
PROMPT 

SET VERIFY OFF


PROMPT Create table TLOGLEVEL ...

@@create_table_tloglevel

PROMPT Insert rows into TLOGLEVEL ...

@@insert_into_tloglevel

PROMPT Create table TLOG ...

@@create_table_tlog

PROMPT Create sequence SQ_STG ...

@@create_sequence_sq_stg

PROMPT Create package PLOGPARAM ...

@@ps_plogparam
@@pb_plogparam

PROMPT Create package PLOG_OUT_TLOG ...

@@ps_plog_out_tlog
@@pb_plog_out_tlog


PROMPT Create dynamically the package PLOG_INTERFACE ...

@@ps_plog_interface
@@pb_plog_interface

PROMPT Create the main package PLOG ...

@@ps_plog
@@pb_plog

PROMPT Create the view VLOG

@@create_view_vlog

spool off

