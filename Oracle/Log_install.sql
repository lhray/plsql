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


spool install.log

PROMPT LOG4PLSQL Installation
PROMPT **********************
PROMPT 

SET VERIFY OFF



@.\BasicTable\FM_DropSeq.sql
@.\BasicTable\FM_DropTable.sql

PROMPT Create table TLOGLEVEL ...

@@.\log\create_table_tloglevel



PROMPT Create table TLOG ...

@@.\log\create_table_tlog

--new log table
@@.\log\create_table_log_operation_level.sql
@@.\log\create_table_log_operation.sql
--new log table

-- modify table
@@.\BasicTable\FM_ModifyTable.sql

PROMPT Create sequence SQ_STG ...

@@.\log\create_sequence_sq_stg

--new log sequence
@@.\log\create_sequence_seq_fmlog_id.sql
--new log sequence

-- modify seq
@@.\BasicTable\FM_ModifySeq.sql




PROMPT Insert rows into TLOGLEVEL ...

@@.\log\insert_into_tloglevel

PROMPT Insert rows into log_operation_level...
@@.\log\insert_into_log_operation_level.sql

PROMPT Create package PLOGPARAM ...

@@.\log\ps_plogparam
@@.\log\pb_plogparam

PROMPT Create package PLOG_OUT_TLOG ...

@@.\log\ps_plog_out_tlog
@@.\log\pb_plog_out_tlog


PROMPT Create dynamically the package PLOG_INTERFACE ...

@@.\log\ps_plog_interface
@@.\log\pb_plog_interface

PROMPT Create the main package PLOG ...

@@.\log\ps_plog
@@.\log\pb_plog

PROMPT Create the view VLOG

@@.\log\create_view_vlog

PROMPT  sp_Log_v1 log
@@.\log\sp_Log_v1.sql
