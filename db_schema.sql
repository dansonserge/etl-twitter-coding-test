lear
-- -----------------------------------------------------
-- Schema twitter_db
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `twitter_db`;

CREATE SCHEMA IF NOT EXISTS `twitter_db` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci ;
USE `twitter_db` ;

-- -----------------------------------------------------
-- Table `twitter_db`.`users`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `twitter_db`.`users` (
  `user_id` BIGINT NOT NULL,
  `screen_name` TEXT NULL DEFAULT NULL,
  `description` TEXT NULL DEFAULT NULL,
  `created_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`user_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter_db`.`tweets`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `twitter_db`.`tweets` (
  `tweet_id` BIGINT NOT NULL,
  `user_id` BIGINT NULL DEFAULT NULL,
  `text` TEXT NULL DEFAULT NULL,
  `type` TEXT NULL DEFAULT NULL,
  `hashtags` TEXT NULL DEFAULT NULL,
  `created_at` TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (`tweet_id`),
  INDEX `user_id_idx` (`user_id` ASC) VISIBLE,
  CONSTRAINT `user_id`
    FOREIGN KEY (`user_id`)
    REFERENCES `twitter_db`.`users` (`user_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `twitter_db`.`contact_tweets`
-- -----------------------------------------------------
CREATE TABLE `twitter_db`.`contact_tweets` (
    `contact_tweets_id` BIGINT NOT NULL,
    `user_id` BIGINT NULL DEFAULT NULL,
    `contact_user` BIGINT NULL DEFAULT NULL,
    `tweet_id` BIGINT NULL DEFAULT NULL,
    PRIMARY KEY (`contact_tweets_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;

ALTER TABLE `twitter_db`.`contact_tweets`
ADD CONSTRAINT `FK_ContactTweetsId` 
FOREIGN KEY (`tweet_id`) REFERENCES `twitter_db`.`tweets`(`tweet_id`);

ALTER TABLE `twitter_db`.`contact_tweets`
ADD CONSTRAINT `FK_UserId`
FOREIGN KEY (`user_id`) REFERENCES `twitter_db`.`users`(`user_id`);