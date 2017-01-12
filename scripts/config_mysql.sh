#!/bin/bash

db_root_user=root
db_root_passwd=root123

mysql -u$db_root_user -p$db_root_passwd < mysql/create_tstat_db.sql

tstat_db='tstat_db'
tstat_db_user='tstat_user'
tstat_db_passwd='tstatpwd'

mysql -u$tstat_db_user -p$tstat_db_passwd $tstat_db < mysql/create_tstat_table.sql