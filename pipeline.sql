-- Drop existing tables and types if they exist to avoid conflicts when creating new ones
drop table if exists BreadCrumb;
drop table if exists Trip;
drop type if exists service_type;
drop type if exists tripdir_type;

-- Create enums for service types and trip directions for use in table definitions
create type service_type as enum ('Weekday', 'Saturday', 'Sunday');
create type tripdir_type as enum ('Out', 'Back');

-- Create the Trip table with relevant fields and primary key
-- The trip_id serves as a unique identifier for each trip
create table Trip (
        trip_id integer,         -- Unique identifier for the trip
        route_id integer,        -- Identifier for the route
        vehicle_id integer,      -- Identifier for the vehicle used
        service_key service_type,-- Type of service under which the trip falls
        direction tripdir_type,  -- Direction of the trip (Out or Back)
        PRIMARY KEY (trip_id)    -- Sets trip_id as the primary key
);

-- Create the BreadCrumb table to store timestamped location data for trips
-- Each record links to a trip in the Trip table
create table BreadCrumb (
        tstamp timestamp,        -- Timestamp of the record
        latitude float,          -- Latitude part of the location
        longitude float,         -- Longitude part of the location
        speed float,             -- Speed of the vehicle at the time of recording
        trip_id integer,         -- Corresponding trip identifier
        FOREIGN KEY (trip_id) REFERENCES Trip  -- Foreign key linking to the Trip table
);

