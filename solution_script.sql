drop table if exists peoples;
drop table if exists country, county, city;


create table country (
	country_id integer auto_increment not null,
	country_name varchar(255) unique,
	primary key(country_id)
); 

create table county (
	county_id integer auto_increment not null,
	county_name varchar(255), 
	country_id int ,
    primary key(county_id),
    constraint uc_county unique (county_name, country_id),
    constraint fk_country foreign key(country_id) references country(country_id)
);
    
    
create table city (
	city_id integer auto_increment not null,
	city_name varchar(255), 
    county_id integer ,
	primary key(city_id),
	constraint uc_county unique (city_name, county_id),
    constraint fk_county foreign key(county_id) references county(county_id)
) ;

create table peoples (
	peoples_id integer auto_increment not null,
	given_name varchar(45) default null,
	family_name varchar(45) default null,
	date_of_birth DATE default null,
	place_of_birth varchar(45) default null,
    city_id int,
    primary key(peoples_id),
    constraint unc_person unique (given_name, family_name, date_of_birth, place_of_birth),
    constraint fk_city foreign key(city_id) references city(city_id)
) ;


