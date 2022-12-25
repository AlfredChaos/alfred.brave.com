-- +goose Up

--
-- Table structure for table `users`
--
CREATE TABLE `users` (
    `uid` varchar(36) NOT NULL,
    `created_at` datetime DEFAULT NULL,
    `updated_at` datetime DEFAULT NULL,
    `login_at` datetime DEFAULT NULL,
    `user_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `email` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `profile` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `avatar` varbinary(255) DEFAULT NULL,
    `password` varbinary(255) DEFAULT NULL,
    PRIMARY KEY (`uid`)
);

--
-- Table structure for table `passwords`
--
CREATE TABLE `friends` (
    `uid` varchar(36) NOT NULL,
    `owner_uid` varchar(36) NOT NULL,
    `friend_uid` varchar(36) NOT NULL, 
    PRIMARY KEY (`uid`),
    KEY `idx_friends_owner_uid` (`owner_uid`)
);


-- +goose Down
DROP TABLE IF EXISTS `users`;
DROP TABLE IF EXISTS `friends`;
