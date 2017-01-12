CREATE DATABASE tstat_db;

GRANT USAGE ON *.* TO tstat_user@localhost IDENTIFIED BY 'tstatpwd';

GRANT ALL PRIVILEGES ON tstat_db.* TO tstat_user@localhost;

FLUSH PRIVILEGES;