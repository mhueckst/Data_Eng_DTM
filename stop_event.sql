drop table if exists StopEvent;

create table StopEvent(
        id int,
        trip_id int, 
        route_id integer,
        service_key varchar,
        direction int, 
        PRIMARY KEY (id)
);
