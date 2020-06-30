create database if not exists findmypast_test
default character set = utf8;

drop table if exists peoples;
drop table if exists places;

create table peoples (
	given_name varchar(45) default null,
    family_name varchar(45) default null,
    date_of_birth date default null,
    place_of_birth varchar(45) default null,
    primary key (given_name)
);

create table places (
	city varchar(45) default null,
	county varchar(45) default null,
    country varchar(45) default null
);

create table normalized (
	id int auto_increment not null primary key, -- Surrogate key
    place_of_birth varchar(45) default null
);