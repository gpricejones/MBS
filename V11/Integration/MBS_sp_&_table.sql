DROP TABLE IF EXISTS `t_links`;

CREATE TABLE `t_links` (
  `itemid` varchar(50) NOT NULL DEFAULT '',
  `isbn` varchar(50) NOT NULL DEFAULT '',
  `dept` varchar(50) NOT NULL DEFAULT '',
  `course` varchar(50) NOT NULL DEFAULT '',
  `section` varchar(50) NOT NULL DEFAULT '',
  `term` varchar(50) NOT NULL DEFAULT '',
  PRIMARY KEY (`itemid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `t_secondary_file`;

CREATE TABLE `t_secondary_file` (
  `isbn` varchar(25) NOT NULL DEFAULT '',
  `term` varchar(25) NOT NULL DEFAULT '',
  `term_description` varchar(50) NOT NULL DEFAULT '',
  `dept` varchar(25) NOT NULL DEFAULT '',
  `course` varchar(25) NOT NULL DEFAULT '',
  `section` varchar(25) NOT NULL DEFAULT '',
  `original_section` varchar(25) NOT NULL DEFAULT '',
  `loc_code` varchar(25) NOT NULL DEFAULT '',
  `bookxofy` varchar(25) NOT NULL DEFAULT '',
  `course_id` varchar(25) NOT NULL DEFAULT '',
  `instructor` varchar(100) NOT NULL DEFAULT '',
  `course_code` varchar(25) NOT NULL DEFAULT '',
  `delete_flag` varchar(5) NOT NULL DEFAULT '',
  `ebook_adopted` varchar(5) NOT NULL DEFAULT '',
  `class_cap` varchar(5) NOT NULL DEFAULT '',
  `prof_requested` varchar(5) NOT NULL DEFAULT '',
  `estimated_sales` varchar(5) NOT NULL DEFAULT '',
  `ebook1_vendor` varchar(25) NOT NULL DEFAULT '',
  `ebook1_period_1` varchar(10) NOT NULL DEFAULT '',
  `ebook1_price_1` varchar(10) NOT NULL DEFAULT '0',
  `ebook1_period_2` varchar(10) NOT NULL DEFAULT '',
  `ebook1_price_2` varchar(10) NOT NULL DEFAULT '0',
  `ebook1_period_3` varchar(10) NOT NULL DEFAULT '',
  `ebook1_price_3` varchar(10) NOT NULL DEFAULT '0',
  `ebook1_period_4` varchar(10) NOT NULL DEFAULT '',
  `ebook1_price_4` varchar(10) NOT NULL DEFAULT '0',
  `ebook1_period_5` varchar(10) NOT NULL DEFAULT '',
  `ebook1_price_5` varchar(10) NOT NULL DEFAULT '0',
  `ebook2_vendor` varchar(25) NOT NULL DEFAULT '',
  `ebook2_period_1` varchar(10) NOT NULL DEFAULT '',
  `ebook2_price_1` varchar(10) NOT NULL DEFAULT '0',
  `ebook2_period_2` varchar(10) NOT NULL DEFAULT '',
  `ebook2_price_2` varchar(10) NOT NULL DEFAULT '0',
  `ebook2_period_3` varchar(10) NOT NULL DEFAULT '',
  `ebook2_price_3` varchar(10) NOT NULL DEFAULT '0',
  `ebook2_period_4` varchar(10) NOT NULL DEFAULT '',
  `ebook2_price_4` varchar(10) NOT NULL DEFAULT '0',
  `ebook2_period_5` varchar(10) NOT NULL DEFAULT '',
  `ebook2_price_5` varchar(10) NOT NULL DEFAULT '0',
  FULLTEXT KEY `term` (`term`),
  FULLTEXT KEY `isbn` (`isbn`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS `t_license_check`;

CREATE TABLE `t_license_check` (
  `l_interval` int(10) NOT NULL DEFAULT '0',
  `license_type` int(1) NOT NULL DEFAULT '0',
  `last_check` date NOT NULL DEFAULT '1900-01-01',
  `last_status` int(1) NOT NULL DEFAULT '0',
  `ex_date` date NOT NULL DEFAULT '1900-01-01'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create procedure sp_lookup_mbs_itemid()
SELECT itemid FROM t_links;



DELIMITER $$

USE `pricer`$$

DROP PROCEDURE IF EXISTS `sp_lookup_isbn`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `sp_lookup_isbn`(
    IN i_isbn VARCHAR(50))
BEGIN
    
	SELECT
		itemid,
		dept,
		course,
		section,
		term
	FROM
		t_links
	WHERE
		isbn = i_isbn
	ORDER BY
		dept,
		course,
		section,
		term;
    END$$

DELIMITER ;



create procedure sp_insert_mbs_itemid(IN i_itemid varchar(50), IN i_isbn varchar(50), IN i_dept varchar(50), IN i_course varchar(50), IN i_section varchar(50), IN i_term varchar(50))
insert into pricer.t_links
        set
            pricer.t_links.itemid = i_itemid,
            pricer.t_links.isbn = i_isbn,
            pricer.t_links.dept = i_dept,
            pricer.t_links.course = i_course,
            pricer.t_links.section = i_section,
            pricer.t_links.term = i_term;
