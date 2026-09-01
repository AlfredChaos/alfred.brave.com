-- mysql 容器首次启动时由 docker-entrypoint-initdb.d 自动执行（仅空 volume 时跑一次）。
-- 幂等：IF NOT EXISTS，重复执行无害。
CREATE DATABASE IF NOT EXISTS brave CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE brave;

-- 与 database/migration/20221222181408_init.sql 保持一致
CREATE TABLE IF NOT EXISTS `users` (
    `uid` varchar(36) NOT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    `login_at` datetime DEFAULT NULL,
    `user_name` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `profile` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `avatar` varbinary(255) DEFAULT NULL,
    `password` varbinary(255) DEFAULT NULL,
    PRIMARY KEY (`uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `friends` (
    `uid` varchar(36) NOT NULL,
    `owner_uid` varchar(36) NOT NULL,
    `friend_uid` varchar(36) NOT NULL,
    PRIMARY KEY (`uid`),
    KEY `idx_friends_owner_uid` (`owner_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
