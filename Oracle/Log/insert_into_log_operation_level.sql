delete from LOG_OPERATION_LEVEL;
commit;

insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (10, 99999, null, 'OFF', 'The OFF has the highest possible rank and is intended to turn off logging.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (20, 60000, null, 'FATAL', 'The FATAL level designates very severe error events that will presumably lead the application to abort.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (30, 50000, null, 'CrucInfo', 'The Crucial INFO shows the progress of the application which  may make an very important  modification to the main datas.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (40, 40000, null, 'ERROR', 'the ERROR level designates error events that might still allow the application  to continue running.', '1');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (50, 30000, null, 'WARN', 'The WARN level designates potentially harmful situations.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (60, 20000, null, 'INFO', 'The INFO level designates informational messages that highlight the progress of the application at coarse-grained level.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (70, 10000, null, 'DEBUG', 'The DEBUG Level designates fine-grained informational events that are most useful to debug an application.', '0');
insert into LOG_OPERATION_LEVEL (LLEVEL, LJLEVEL, LSYSLOGEQUIV, LCODE, LDESC, LTYPE)
values (80, 0, null, 'ALL', 'The ALL has the lowest possible rank and is intended to turn on all logging.', '0');
commit;