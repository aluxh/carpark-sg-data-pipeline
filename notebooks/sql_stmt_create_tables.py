# DROP TABLES
staging_temperature_drop = "DROP TABLE IF EXISTS staging_temperature CASCADE"
staging_rainfall_drop = "DROP TABLE IF EXISTS staging_rainfall CASCADE"
staging_carpark_availability_drop = "DROP TABLE IF EXISTS staging_carpark_availability CASCADE"
staging_carpark_info_drop = "DROP TABLE IF EXISTS staging_carpark_info CASCADE"
staging_weather_stations_info_drop = "DROP TABLE IF EXISTS staging_weather_station_info CASCADE"
temperature_events_drop = "DROP TABLE IF EXISTS temperature_events CASCADE"
rainfall_events_drop = "DROP TABLE IF EXISTS rainfall_events CASCADE"
carpark_availability_drop = "DROP TABLE IF EXISTS carpark_availability CASCADE"
weather_station_drop = "DROP TABLE IF EXISTS weather_stations CASCADE"
carpark_drop = "DROP TABLE IF EXISTS carpark CASCADE"
time_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES
staging_temperature_create = ("""CREATE TABLE public.staging_temperature (
        date_time timestamptz NOT NULL,
        station_id varchar,
        temperature double precision
);""")

staging_rainfall_create = ("""CREATE TABLE public.staging_rainfall (
        date_time timestamptz NOT NULL,
        station_id varchar,
        rainfall double precision
);""")

staging_carpark_availability_create = ("""CREATE TABLE public.staging_carpark_availability (
        date_time timestamptz NOT NULL,
        carpark_id varchar,
        lot_type varchar,
        lots_available integer,
        total_lots integer        
);""")

staging_carpark_info_create = ("""CREATE TABLE public.staging_carpark_info (
        carpark_id varchar,
        carpark_location varchar,
        carpark_latitude double precision,
        carpark_longitude double precision        
);""")

staging_weather_stations_info_create = ("""CREATE TABLE public.staging_weather_station_info (
        station_id varchar,
        station_location varchar,
        station_latitude double precision,
        station_longitude double precision
);""")

temperature_events_create = ("""CREATE TABLE public.temperature_events (
        date_time timestamptz NOT NULL,
        station_id varchar,
        temperature double precision,
        FOREIGN KEY (station_id) references weather_stations (station_id)
) diststyle KEY distkey(date_time) compound sortkey (date_time, station_id);
""")

rainfall_events_create = ("""CREATE TABLE public.rainfall_events (
        date_time timestamptz NOT NULL,
        station_id varchar,
        rainfall double precision,
        FOREIGN KEY (station_id) references weather_stations (station_id)
) diststyle KEY distkey(date_time) compound sortkey (date_time, station_id);
""")

carpark_availability_create = ("""CREATE TABLE public.carpark_availability (
        date_time timestamptz NOT NULL,
        carpark_id varchar,
        lots_available integer,
        FOREIGN KEY (carpark_id) references carpark (carpark_id)
) diststyle KEY distkey (date_time) compound sortkey (date_time, carpark_id);
""")

weather_station_create = ("""CREATE TABLE public.weather_stations (
        station_id varchar NOT NULL,
        station_location varchar,
        station_latitude double precision,
        station_longitude double precision,
        CONSTRAINT weather_stations_pkey PRIMARY KEY (station_id)
);""")

carpark_create = ("""CREATE TABLE public.carpark (
        carpark_id varchar,
        carpark_location varchar,
        carpark_latitude double precision,
        carpark_longitude double precision,
        total_lots integer,
        CONSTRAINT carpark_pkey PRIMARY KEY (carpark_id)
);""")

time_create = ("""CREATE TABLE public.time(
        date_time timestamptz PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
);""")

create_table_queries = [staging_temperature_create, staging_rainfall_create, staging_carpark_availability_create,
staging_carpark_info_create, staging_weather_stations_info_create, weather_station_create, carpark_create, time_create,
temperature_events_create, rainfall_events_create, carpark_availability_create]

drop_table_queries = [staging_temperature_drop, staging_rainfall_drop, staging_carpark_availability_drop, staging_carpark_info_drop, staging_weather_stations_info_drop, 
temperature_events_drop, rainfall_events_drop, carpark_availability_drop, weather_station_drop, carpark_drop, time_drop]