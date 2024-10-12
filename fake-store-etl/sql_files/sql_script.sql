create table products(
id_product int not null primary key,
price float,
category varchar (200),
image varchar (500),
total_reviews int,
rating float (50));

create table users (
id_user int not null primary key,
firstname varchar(100),
lastname varchar (100),
city varchar (200),
address varchar (200),
zipcode varchar (200),
latitude varchar (200),
longitude varchar (200),
password varchar (200),
phone varchar(200),
username varchar(200));

create table carts (
id_cart int not null primary key,
id_user int not null,
date timestamp,
products jsonb);



