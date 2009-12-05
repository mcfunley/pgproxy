drop database if exists test;
create database test;

\c test

create table foo(x integer);

insert into foo (x) values(1);
insert into foo (x) values(2);
