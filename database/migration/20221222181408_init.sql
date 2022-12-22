-- +goose Up

--
-- Table structure for table `users`
--
CREATE TABLE `users` (
    `uid` varchar(36) NOT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    `deleted_at` datetime DEFAULT NULL,
    `login_at` datetime DEFAULT NULL,
    `user_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    PRIMARY KEY (`uid`),
    KEY `idx_users_deleted_at` (`deleted_at`)
);

--
-- Table structure for table `passwords`
--
CREATE TABLE `passwords` (
    `uid` varchar(36) NOT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    `deleted_at` datetime DEFAULT NULL,
    `hash` varbinary(255) DEFAULT NULL,
    `user_id` varchar(36) NOT NULL,
    PRIMARY KEY (`uid`),
    KEY `idx_passwords_deleted_at` (`deleted_at`)
);


-- +goose Down
DROP TABLE IF EXISTS `passwords`;
DROP TABLE IF EXISTS `users`;
