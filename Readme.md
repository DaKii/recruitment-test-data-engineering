# REEADME file for Justine Quiapos

# Files used for solution

## /images/solution/
- Dockerfile # Simple docker file to run python 
- solution.py # imports csv data and prints intended output
- requirements # stores required modules for python

## docker-compose.yml
- Uses same docker-compose file as given in the test 

## solution-script
- sql script that creates the table, the python program already has an option to drop and re-create the tables in order to reapply the schemas easier.

## To run python program
- docker-compose build
- docker-compose run solution
