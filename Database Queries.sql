-- Create Database
create database incubyte;

-- select database
use incubyte;

-- create table
create table customers(
	customerName varchar(255) not null,
    customerId varchar(18) not null,
    openDate date not null,
    lastCunsultedDate date,
    vaccinationId char(5),
    drName CHAR(255),
    state CHAR(5),
    country CHAR(5),
    postCode INT(5),
    DOB date,
    isActive CHAR(1),
    primary key (customerName)
);

-- create table for saperate country
create table customers_USA(
	customerName varchar(255) not null,
    customerId varchar(18) not null,
    openDate date not null,
    lastCunsultedDate date,
    vaccinationId char(5),
    drName CHAR(255),
    state CHAR(5),
    country CHAR(5),
    postCode INT(5),
    DOB date,
    isActive CHAR(1),
    primary key (customerName)
);

select * from customers;

-- Add age column 
alter table customers add column age int;

-- update values for age column
UPDATE customers
  SET age = TIMESTAMPDIFF(YEAR, DOB, CURDATE());

-- create event to update age column every day
CREATE EVENT refresh_age_event
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_DATE + INTERVAL 1 DAY
DO
  UPDATE customers
  SET age = TIMESTAMPDIFF(YEAR, DOB, CURDATE());

-- Check created events
show events;

-- display derived column number of days since last consulted date
SELECT 
    *,
    case when TIMESTAMPDIFF(DAY,lastCunsultedDate,CURDATE()) < 30 then TIMESTAMPDIFF(DAY,lastCunsultedDate,CURDATE())
    else '> 30' 
    end lastCunsultedSince
FROM
    customers;
