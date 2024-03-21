CREATE TABLE `test_kvp`
(
    `key` VARCHAR(191) NOT NULL,
    `val` VARCHAR(191) NOT NULL,
    PRIMARY KEY (`key`)
) ENGINE innoDB
  CHARACTER SET `utf8mb4`
  COLLATE `utf8mb4_unicode_ci`;

CREATE TABLE `test_table`
(
    id BIGINT AUTO_INCREMENT,
    PRIMARY KEY (id)
) ENGINE innoDB
  CHARACTER SET `utf8mb4`
  COLLATE `utf8mb4_unicode_ci`;