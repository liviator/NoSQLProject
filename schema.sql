#drop database nosql;

create database nosql;
use nosql;

CREATE TABLE nosql.`jsonfile` (
Id VARCHAR(64), 
Event_type VARCHAR(64) NOT NULL,
Date_creation VARCHAR(64) NOT NULL,
Date_ajout VARCHAR(64) NOT NULL,
Version_file VARCHAR(10) NOT NULL,
Graph_id varchar(20) not null,
nature varchar(20) not null,
Object_name varchar(20) not null,
object_path varchar (264) not null,
primary key (Id));

