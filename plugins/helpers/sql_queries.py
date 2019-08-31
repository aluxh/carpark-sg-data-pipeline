class SqlQueries:
        """
        Contains all the SQL statements to insert the data from staging to fact or dimension tables.
        These statements will be ran in Redshift to execute the instructions.
        """
        
        temperature_events_insert = ("""
                INSERT INTO temperature_events (date_time, station_id, temperature)
                SELECT date_time, station_id, temperature
                FROM staging_temperature
        """)

        rainfall_events_insert = ("""
                INSERT INTO rainfall_events (date_time, station_id, rainfall)
                SELECT date_time, station_id, rainfall
                FROM staging_rainfall
        """)      

        carpark_availability_insert = ("""
                INSERT INTO carpark_availability (date_time, carpark_id, lots_available)
                SELECT date_time, carpark_id, lots_available
                FROM staging_carpark_availability
        """)

        carpark_insert = ("""
                INSERT INTO carpark (carpark_id, carpark_location, carpark_latitude, carpark_longitude, total_lots)
                SELECT ci.carpark_id, ci.carpark_location, ci.carpark_latitude, ci.carpark_longitude, ca.total_lots
                FROM staging_carpark_info ci
                LEFT JOIN staging_carpark_availability ca
                ON ci.carpark_id = ca.carpark_id               
        """)

        weather_stations_insert = ("""
                INSERT INTO weather_stations (station_id, station_location, station_latitude, station_longitude)
                SELECT station_id, station_location, station_latitude, station_longitude
                FROM staging_weather_station_info               
        """)

        time_table_insert = ("""
                INSERT INTO time (date_time, hour, day, week, month, year, weekday)
                SELECT date_time, extract(hour from date_time), extract(day from date_time), extract(week from date_time), 
                extract(month from date_time), extract(year from date_time), extract(dayofweek from date_time)
                FROM staging_carpark_availability
        """)

        ## Old implementation for PostgreSQL (Dev Environment)
        staging_temperature_copy = ("""
        COPY staging_temperature (date_time, station_id, temperature) 
        FROM '{}' 
        WITH (FORMAT CSV, HEADER TRUE);
        """)

        staging_rainfall_copy = ("""
        COPY staging_rainfall (date_time, station_id, rainfall) 
        FROM '{}' 
        WITH (FORMAT CSV, HEADER TRUE);
        """)

        staging_carpark_availability_copy = ("""
        COPY staging_carpark_availability (date_time, carpark_id, lot_type, lots_available, total_lots)
        FROM '{}'
        WITH (FORMAT CSV, HEADER TRUE)
        """)

