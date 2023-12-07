-- -----------------------------------------------------
-- Schema advertising_agency
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `advertising_agency`;
DROP SCHEMA IF EXISTS `agency`;
CREATE SCHEMA IF NOT EXISTS `advertising_agency` DEFAULT CHARACTER SET utf8;
USE `advertising_agency`;

-- -----------------------------------------------------
-- Table `advertising_agency`.`category`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `advertising_agency`.`category`;

CREATE TABLE IF NOT EXISTS `advertising_agency`.`category`
(
    `id`   INT          NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`id`)
);

create index category_name_index
    on category (name);

INSERT INTO `category` (`name`)
VALUES ('Media Advertising'),
       ('Outdoor Advertising'),
       ('Advertising on Transport'),
       ('Advertising at Points of Sale'),
       ('Souvenir Advertising'),
       ('Printed Advertising'),
       ('Direct Advertising'),
       ('Internet Advertising');

-- -----------------------------------------------------
-- Table `advertising_agency`.`client`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `advertising_agency`.`client`;

CREATE TABLE IF NOT EXISTS `advertising_agency`.`client`
(
    `id`           INT          NOT NULL AUTO_INCREMENT,
    `username`     VARCHAR(45)  NOT NULL,
    `firstname`    VARCHAR(32)  NOT NULL,
    `lastname`     VARCHAR(150) NOT NULL,
    `phone_number` VARCHAR(10)  NOT NULL,
    `email`        VARCHAR(255) NOT NULL,
    `password`     VARCHAR(255) NOT NULL,
    PRIMARY KEY (`id`)
);
create index client_email_index
    on client (email);

create index client_password_index
    on client (password);

INSERT INTO `client` (`username`, `firstname`, `lastname`, `phone_number`, `email`, `password`)
VALUES ('client1', 'John', 'Doe', '1234567890', 'john.doe@example.com', 'password1'),
       ('client2', 'Jane', 'Smith', '9876543210', 'jane.smith@example.com', 'password2'),
       ('client3', 'Alice', 'Johnson', '5551112233', 'alice.johnson@example.com', 'password3'),
       ('client4', 'Bob', 'Williams', '1112223344', 'bob.williams@example.com', 'password4'),
       ('client5', 'Bob', 'Jones', '1133567890', 'bob.jones@gmail.com', 'password5'),
       ('client6', 'Jane', 'Miller', '9996543210', 'miller@example.com', 'password6'),
       ('client7', 'David', 'Brown', '2223334455', 'david.brown@example.com', 'password7'),
       ('client8', 'Emily', 'Davis', '3334445566', 'emily.davis@example.com', 'password8'),
       ('client9', 'George', 'Smith', '4445556677', 'george.smith@example.com', 'password9'),
       ('client10', 'Hannah', 'Taylor', '5556667788', 'hannah.taylor@example.com', 'password10'),
       ('client11', 'Isaac', 'Lee', '6667778899', 'isaac.lee@example.com', 'password11'),
       ('client12', 'Julia', 'Clark', '7778889900', 'julia.clark@example.com', 'password12'),
       ('client13', 'Kevin', 'Wong', '8889990011', 'kevin.wong@example.com', 'password13'),
       ('client14', 'Linda', 'Nguyen', '9990001122', 'linda.nguyen@example.com', 'password14'),
       ('client15', 'Michael', 'Johnson', '1112223334', 'michael.johnson@example.com', 'password15'),
       ('client16', 'Nancy', 'Chen', '2223334445', 'nancy.chen@example.com', 'password16'),
       ('client17', 'Oscar', 'Garcia', '3334445556', 'oscar.garcia@example.com', 'password17'),
       ('client18', 'Paul', 'Kim', '4445556667', 'paul.kim@example.com', 'password18'),
       ('client19', 'Quincy', 'Lee', '5556667778', 'quincy.lee@example.com', 'password19'),
       ('client20', 'Rachel', 'Smith', '6667778889', 'rachel.smith@example.com', 'password20'),
       ('client21', 'Samuel', 'Williams', '7778889990', 'samuel.williams@example.com', 'password21'),
       ('client22', 'Tina', 'Miller', '8889990001', 'tina.miller@example.com', 'password22'),
       ('client23', 'Ulysses', 'Jones', '9990001112', 'ulysses.jones@example.com', 'password23'),
       ('client24', 'Victoria', 'Brown', '1112223333', 'victoria.brown@example.com', 'password24'),
       ('client25', 'Walter', 'Davis', '2223334444', 'walter.davis@example.com', 'password25');


-- -----------------------------------------------------
-- Table `advertising_agency`.`advertising`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `advertising_agency`.`advertising`;

CREATE TABLE IF NOT EXISTS `advertising_agency`.`advertising`
(
    `id`          INT           NOT NULL AUTO_INCREMENT,
    `category_id` INT           NOT NULL,
    `name`        VARCHAR(255)  NOT NULL,
    `measurement` VARCHAR(255)  NOT NULL,
    `unit_price`  DECIMAL(7, 2) NOT NULL,
    `description` VARCHAR(255)  NULL     DEFAULT NULL,
    `updated_at`  TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `fk_advertising_category_idx` (`category_id` ASC) VISIBLE,
    CONSTRAINT `fk_advertising_category`
        FOREIGN KEY (`category_id`)
            REFERENCES `advertising_agency`.`category` (`id`)
            ON DELETE CASCADE
            ON UPDATE RESTRICT
);

create index advertising_measurement_index
    on advertising (measurement);

create index advertising_name_index
    on advertising (name);

create index advertising_unit_price_index
    on advertising (unit_price);


INSERT INTO `advertising` (`category_id`, `name`, `measurement`, `unit_price`, `description`)
VALUES (1, 'TV Advertising', 'Seconds', 100.00, 'Prime time TV slot'),
       (1, 'Radio Advertising', 'Seconds', 50.00, 'Peak hour radio slot'),
       (2, 'Billboard Advertising', 'Per Billboard', 200.00, 'City-wide billboard placement'),
       (3, 'Vehicle Advertising', 'Per Vehicle', 75.00, 'Exterior vehicle advertising'),
       (4, 'Store Window Advertising', 'Per Store', 50.00, 'Internal and external store window ads'),
       (5, 'Branded Notebooks', 'Per Unit', 10.00, 'Custom branded notebooks'),
       (6, 'Brochure Printing', 'Per Page', 5.00, 'Custom brochures for advertising'),
       (7, 'Direct Mail Campaign', 'Per Mailer', 2.00, 'Targeted direct mail advertising'),
       (8, 'Internet Banner Ads', 'Per Click', 1.00, 'Online banner ads on popular websites'),
       (1, 'Morning Talk Show Sponsorship', 'Seconds', 120.00, 'Sponsorship of a popular morning talk show'),
       (2, 'Business Magazine Ad', 'Full Page', 80.00, 'Full-page ad in a business magazine'),
       (3, 'Bus Wrap Advertising', 'Per Bus', 150.00, 'Full bus wrap with advertising graphics'),
       (4, 'Point-of-Purchase Displays', 'Per Display', 30.00, 'Custom in-store promotional displays'),
       (5, 'Custom Logo Pens', 'Per Pen', 5.00, 'Promotional pens with company logo'),
       (6, 'Handout Distribution', 'Per 1000', 20.00, 'Distribution of advertising handouts'),
       (7, 'Email Newsletter Campaign', 'Per Newsletter', 1.50, 'Targeted email marketing campaign'),
       (8, 'Facebook Ads', 'Per Impression', 0.50, 'Advertising on Facebook platform'),
       (1, 'Evening News Sponsorship', 'Seconds', 70.00, 'Sponsorship of a popular evening news program'),
       (2, 'Newspaper Ad', 'Full Page', 50.00, 'Full-page ad in a newspaper'),
       (3, 'Bus Stop Advertising', 'Per Bus Stop', 100.00, 'Advertising at a bus stop. High foot traffic'),
       (5, 'Custom T-Shirts', 'Per Shirt', 10.00, 'Custom t-shirts with company logo'),
       (6, 'Flyer Distribution', 'Per 1000', 15.00, 'Distribution of advertising flyers'),
       (7, 'SMS Marketing Campaign', 'Per Message', 0.50, 'Targeted SMS marketing campaign'),
       (8, 'Google Ads', 'Per Click', 0.50, 'Advertising on Google platform');


-- -----------------------------------------------------
-- Table `advertising_agency`.`program`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `advertising_agency`.`program`;

CREATE TABLE IF NOT EXISTS `advertising_agency`.`program`
(
    `id`             INT          NOT NULL AUTO_INCREMENT,
    `client_id`      INT          NOT NULL,
    `campaign_title` VARCHAR(100) NOT NULL,
    `description`    VARCHAR(255) NULL     DEFAULT NULL,
    `created_at`     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    INDEX `fk_program_client_idx` (`client_id` ASC) VISIBLE,
    CONSTRAINT `fk_program_client`
        FOREIGN KEY (`client_id`)
            REFERENCES `advertising_agency`.`client` (`id`)
            ON DELETE CASCADE
            ON UPDATE RESTRICT
);

INSERT INTO `program` (`client_id`, `campaign_title`, `description`)
VALUES (1, 'Summer Campaign', 'Summer advertising campaign'),
       (2, 'Winter Campaign', 'Winter advertising campaign'),
       (3, 'Spring Campaign', 'Spring advertising campaign'),
       (5, 'Christmas Campaign', 'Christmas advertising campaign'),
       (6, 'New Year Campaign', 'New Year advertising campaign'),
       (7, 'Easter Campaign', 'Easter advertising campaign'),
       (8, 'Valentine''s Day Campaign', 'Valentine''s Day advertising campaign'),
       (9, 'Halloween Campaign', 'Halloween advertising campaign'),
       (14, 'Black Friday Campaign', 'Black Friday advertising campaign'),
       (15, 'Cyber Monday Campaign', 'Cyber Monday advertising campaign'),
       (16, 'Back to School Campaign', 'Back to School advertising campaign'),
       (23, 'Earth Day Campaign', 'Earth Day advertising campaign');

-- -----------------------------------------------------
-- Table `advertising_agency`.`program_has_advertising`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `advertising_agency`.`program_advertising`;

CREATE TABLE IF NOT EXISTS `advertising_agency`.`program_advertising`
(
    `program_id`     INT NOT NULL,
    `advertising_id` INT NOT NULL,
    `quantity`       INT NOT NULL,
    PRIMARY KEY (`program_id`, `advertising_id`),
    INDEX `fk_program_advertising_advertising_idx` (`advertising_id` ASC) VISIBLE,
    INDEX `fk_program_advertising_program_idx` (`program_id` ASC) VISIBLE,
    CONSTRAINT `fk_program_advertising_program`
        FOREIGN KEY (`program_id`)
            REFERENCES `advertising_agency`.`program` (`id`)
            ON DELETE CASCADE
            ON UPDATE RESTRICT,
    CONSTRAINT `fk_program_advertising_advertising`
        FOREIGN KEY (`advertising_id`)
            REFERENCES `advertising_agency`.`advertising` (`id`)
            ON DELETE CASCADE
            ON UPDATE RESTRICT
);

INSERT INTO `program_advertising` (`program_id`, `advertising_id`, `quantity`)
VALUES (1, 1, 3),
       (1, 3, 2),
       (2, 2, 1),
       (2, 5, 4),
       (3, 4, 2),
       (3, 6, 1),
       (4, 7, 3),
       (4, 8, 2),
       (5, 9, 1),
       (5, 10, 2),
       (6, 11, 1),
       (6, 12, 2),
       (2, 16, 4);


/*SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE `client`;
TRUNCATE TABLE `program`;
TRUNCATE TABLE `program_advertising`;
TRUNCATE TABLE `advertising`;
TRUNCATE TABLE `category`;
SET FOREIGN_KEY_CHECKS = 1;*/