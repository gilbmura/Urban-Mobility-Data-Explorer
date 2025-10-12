-- create DB & user used by your app
CREATE DATABASE IF NOT EXISTS `nyc_mobility` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE USER IF NOT EXISTS 'nyc_user'@'%' IDENTIFIED BY 'nyc_pass';
GRANT ALL PRIVILEGES ON `nyc_mobility`.* TO 'nyc_user'@'%';
FLUSH PRIVILEGES;
