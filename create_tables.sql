CREATE TABLE public.staging_temperature (
        date_time timestamptz NOT NULL,
        station_id varchar,
        temperature double precision
);

CREATE TABLE public.staging_rainfall (
        date_time timestamptz NOT NULL,
        station_id varchar,
        rainfall double precision
);

CREATE TABLE public.staging_carpark_availability (
        date_time timestamptz NOT NULL,
        carpark_id varchar,
        lot_type varchar,
        lots_available integer,
        total_lots integer        
);

CREATE TABLE public.staging_weather_station_info (
        station_id varchar,
        station_location varchar,
        station_latitude double precision,
        station_longitude double precision
)

CREATE TABLE public.staging_carpark_info (
        carpark_id varchar,
        carpark_location varchar,
        carpark_latitude double precision,
        carpark_longitude double precision        
);

CREATE TABLE public.temperature_events (
        date_time timestamptz NOT NULL,
        station_id varchar,
        temperature double precision,
        FOREIGN KEY (station_id) references weather_stations (station_id)
) diststyle KEY distkey(date_time) compound sortkey (date_time, station_id);

CREATE TABLE public.rainfall_events (
        date_time timestamptz NOT NULL,
        station_id varchar,
        rainfall double precision,
        FOREIGN KEY (station_id) references weather_stations (station_id)
) diststyle KEY distkey(date_time) compound sortkey (date_time, station_id);

CREATE TABLE public.carpark_availability (
        date_time timestamptz NOT NULL,
        carpark_id varchar,
        lots_available integer,
        FOREIGN KEY (carpark_id) references carpark (carpark_id)
) diststyle KEY distkey (date_time) compound sortkey (date_time, carpark_id);

CREATE TABLE public.weather_stations (
        station_id varchar NOT NULL,
        station_location varchar,
        station_latitude double precision,
        station_longitude double precision,
        CONSTRAINT weather_stations_pkey PRIMARY KEY (station_id)
);

CREATE TABLE public.carpark (
        carpark_id varchar,
        carpark_location varchar,
        carpark_latitude double precision,
        carpark_longitude double precision,
        total_lots integer,
        CONSTRAINT carpark_pkey PRIMARY KEY (carpark_id)
);

CREATE TABLE public.time(
        date_time timestamptz PRIMARY KEY,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday integer
);