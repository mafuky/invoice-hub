#!/usr/bin/env bash
set -euo pipefail
[[ "$MYSQL_APP_PASSWORD" =~ ^[a-fA-F0-9]{32,}$ ]]
MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot <<SQL
CREATE DATABASE IF NOT EXISTS invoice;
CREATE DATABASE IF NOT EXISTS provider;
CREATE DATABASE IF NOT EXISTS settlement;
CREATE DATABASE IF NOT EXISTS invoice_test;
CREATE DATABASE IF NOT EXISTS provider_test;
CREATE DATABASE IF NOT EXISTS settlement_test;
CREATE USER IF NOT EXISTS 'app'@'%' IDENTIFIED BY '${MYSQL_APP_PASSWORD}';
GRANT ALL PRIVILEGES ON invoice.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON provider.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON settlement.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON \`invoice\_test\`.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON \`provider\_test\`.* TO 'app'@'%';
GRANT ALL PRIVILEGES ON \`settlement\_test\`.* TO 'app'@'%';
SQL
