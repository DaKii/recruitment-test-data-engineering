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


try:
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something wrong with password/username")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)


#Adding data queries
add_person = (
    "insert into peoples"
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

# read places CSV file and insert into tables
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







cnx.commit()
cursor.close()
cnx.close()
