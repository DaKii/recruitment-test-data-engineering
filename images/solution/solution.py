#!/usr/bin/env python

import csv
import json
import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'codetest'

config = {
  'user': 'codetest',
  'password': 'swordfish',
  'database': DB_NAME,
  'host': "database",
}

# Connecting to Database
try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
    print("Successfully connected to:", DB_NAME )
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something wrong with password/username")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)
    exit()

# Creating tables
print("Do you want to drop and re-create tables? y if yes, any other character if no")
if input("Enter input:") == 'y':
        
    TABLES = {}
    TABLES['country'] = (
        "create table country ( \
        country_id integer auto_increment not null, \
        country_name varchar(255) unique, \
        primary key(country_id) \
        ) ENGINE=InnoDB" 
    )

    TABLES['county'] = (
        "create table county ( \
        county_id integer auto_increment not null, \
        county_name varchar(255), \
        country_id int , \
        primary key(county_id), \
        constraint uc_county unique (county_name, country_id), \
        constraint fk_country foreign key(country_id) references country(country_id) \
        ) ENGINE=InnoDB "
    )
        
    TABLES['city'] = (
        "create table city ( \
        city_id integer auto_increment not null,\
        city_name varchar(255),  \
        county_id integer , \
        primary key(city_id), \
        constraint uc_county unique (city_name, county_id), \
        constraint fk_county foreign key(county_id) references county(county_id) \
        ) ENGINE=InnoDB "
    )

    TABLES['peoples'] = (
        "create table peoples ( \
        peoples_id integer auto_increment not null, \
        given_name varchar(45) default null, \
        family_name varchar(45) default null, \
        date_of_birth DATE default null, \
        place_of_birth varchar(45) default null, \
        city_id int, \
        primary key(peoples_id), \
        constraint unc_person unique (given_name, family_name, date_of_birth, place_of_birth), \
        constraint fk_city foreign key(city_id) references city(city_id) \
        ) ENGINE=InnoDB "
    )

    cursor.execute('SET FOREIGN_KEY_CHECKS=0')
    for table in TABLES:
        try:
            
            cursor.execute("drop table if exists {}".format(table))
            print("Creating table {} :".format(table), end='')
            cursor.execute(TABLES[table])
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print("OK")
    cursor.execute('SET FOREIGN_KEY_CHECKS=1')
#Roundabout way to disable FK constraints to drop



###
#Adding data queries
add_person = (
    "insert ignore into peoples"
    "(peoples_id, given_name, family_name, date_of_birth, place_of_birth)"
    "values (%(peoples_id)s, %(given_name)s, %(family_name)s, %(date_of_birth)s, %(place_of_birth)s)"
)

add_countries = (
    "insert ignore into country"
    "(country_id, country_name)"
    "values(%s, %s)"
)

add_counties = (
    "insert ignore into county"
    "(county_id, county_name, country_id)"
    "values(%s, %s, %s)"
)

add_cities = (
    "insert ignore into city"
    "(city_id, city_name, county_id)"
    "values(%s, %s, %s)"
)


# read people CSV file and insert into tables
try:
    with open('/data/people.csv') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        next(reader)
        id_count = cursor.lastrowid
        for row in reader:
            data_people = {
                'peoples_id': id_count,
                'given_name': row[0],
                'family_name': row[1],
                'date_of_birth': row[2],
                'place_of_birth': row[3],
            }
            cursor.execute(add_person, data_people)
            id_count = cursor.lastrowid + 1
        print("Successfully imported peoples csv")
except IOError:
    print("Can't read peoples csv file")

# read places CSV file and insert into tables
try:
    with open('/data/places.csv') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        next(reader)
        
        #holds regions and their id
        country_dictionary = {}
        county_dictionary = {}

        for row in reader:

            #checks and stores country and their id
            if row[2] in country_dictionary:
                country_id = country_dictionary[row[2]]
            else:
                country_dictionary[row[2]] = cursor.lastrowid
                country_id = country_dictionary[row[2]]

            #checks and stores county and their id
            if row[1] in county_dictionary:
                county_id = county_dictionary[row[1]]
            else:
                county_dictionary[row[1]] = cursor.lastrowid
                county_id = county_dictionary[row[1]]

            data_country = (country_id, row[2])
            data_county = (county_id, row[1], country_id)
            data_city = (cursor.lastrowid + 1, row[0], county_id)
            cursor.execute(add_countries, data_country)
            cursor.execute(add_counties, data_county)
            cursor.execute(add_cities, data_city)
        print("Successfully imported places csv")
except IOError:
    print("Could not read places csv file")


##Update people foreign key
update_people_fk = (
    "update peoples as a\
    inner join city as b on a.place_of_birth = b.city_name \
    set a.city_id = b.city_id  \
    where a.city_id is NULL "
)
cursor.execute(update_people_fk)



## Query to get the intended outcome
## Using foreign key to inner join with normalized database
## use left-join incase no match???? 
country_people_count = (
    " select a.country_name, count(*)\
    from country as a \
    inner join county as b on a.country_id = b.country_id \
    inner join city as c on b.county_id = c.county_id \
    inner join peoples as d on c.city_id = d.city_id \
    group by a.country_name \
    order by count(*)  desc\
    "
)
cursor.execute(country_people_count)
print("Getting intended output")

##Getting query and outputting in a json
result = cursor.fetchall()
try:
    with open('/data/solution.json', 'w') as json_file:
        rows = {}
        for x in result:
            row = {x[0] : x[1]}
            rows.update(row)
        print("Output:", rows)
        json.dump(rows, json_file, separators=(',', ':'))
    print("Successfully wrote output in a json file in /data/solution")
except IOError:
    print("Unable to write output in a file")


## Close connection
cnx.commit()
cursor.close()
cnx.close()
