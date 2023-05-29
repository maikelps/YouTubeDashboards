DROP database IF EXISTS youtubeproject;

/*Create database*/
CREATE DATABASE IF NOT EXISTS youtubeproject DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';

/*tell which database you will use*/
USE youtubeproject;


CREATE TABLE IF NOT EXISTS `channel_details` (
  `ChannelName` VARCHAR(50) DEFAULT NULL,
  `CreatedOn` DATE NOT NULL,
  `UploadPlaylist` VARCHAR(50) DEFAULT NULL,
  `ChannelID` VARCHAR(50) NOT NULL,
CONSTRAINT IterID PRIMARY KEY (`ChannelID`)
);

CREATE TABLE IF NOT EXISTS `channel_evolution` (
  `ChannelID` VARCHAR(50) NOT NULL,
  `ExtractionDay` DATE NOT NULL,
  `views` BIGINT NOT NULL,
  `subscribers` BIGINT NOT NULL,
  `videoNr` BIGINT NOT NULL,
CONSTRAINT IterID PRIMARY KEY (`ChannelID`, `ExtractionDay`)
);

CREATE TABLE IF NOT EXISTS `video_statics` (
  `ChannelID` VARCHAR(50) NOT NULL,
  `video_id` VARCHAR(50) NOT NULL,
  `CreatedOn` DATE NOT NULL,
  `title` VARCHAR(100) NOT NULL,
  `descr` VARCHAR(5000) NOT NULL,
  `duration` BIGINT NOT NULL,
CONSTRAINT IterID PRIMARY KEY (`ChannelID`, `video_id`)
);

CREATE TABLE IF NOT EXISTS `video_variables` (
  `video_id` VARCHAR(50) NOT NULL,
  `ExtractionDay` DATE NOT NULL,
  `viewCount` BIGINT NOT NULL,
  `likeCount` BIGINT NOT NULL,
  `commentCount` BIGINT NOT NULL,
CONSTRAINT IterID PRIMARY KEY (`ExtractionDay`, `video_id`)
);

USE youtubeproject;

-- limited queries
select * from channel_details limit 5;
select * from video_statics limit 5;

select * from video_variables where video_id = "ACpnhkvc8M4";


select distinct(ExtractionDay) from video_variables;

-- delete from video_variables where ExtractionDay in ('2022-12-30', '2022-12-31', '2023-01-01', '2023-01-02', '2023-01-03');
-- video_statics video_variables

-- truncate table channel_evolution;