create table if not exists t_links
(
	itemid varchar(50) default '' not null
		primary key,
	isbn varchar(50) default '' not null,
	dept varchar(50) default '' not null,
	course varchar(50) default '' not null,
	section varchar(50) default '' not null,
	term varchar(50) default '' not null
)
charset=utf8;

