-- Create airflowdb database for Airflow metadata
CREATE DATABASE IF NOT EXISTS airflowdb CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grant privileges to appuser on airflowdb
GRANT ALL PRIVILEGES ON airflowdb.* TO 'appuser'@'%';
FLUSH PRIVILEGES;
