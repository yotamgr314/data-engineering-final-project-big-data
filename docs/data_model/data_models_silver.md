## Silver Layer – Cleansed and Enriched Data Tables

The silver layer contains cleansed, normalized, and enriched data derived from the raw bronze layer. It enables analytical queries and serves as input for the gold layer.

---

### `SILVER_FLIGHTS`  
Normalized flight schedule and metadata with references to route and enriched attributes.

### `SILVER_ROUTES`  
Cleaned route information with coordinates and geospatial fields.

### `SILVER_WAYPOINT_SAMPLING_WEATHER_POINTS`  
Waypoints along each route used to associate weather samples with specific flight segments.

### `SILVER_TICKET_PRICES`  
Validated and time-bound ticket pricing information associated with specific flights.

### `SILVER_CUSTOMERS`  
Structured customer data including passport and loyalty attributes.

### `SILVER_FLIGHT_WEATHER`  
Flight-level weather readings linked to sampling waypoints and flight IDs.

### `SILVER_BOOKED_TICKETS`  
Structured information about individual booked tickets including customer linkage.

### `SILVER_FLIGHTS_EVENTS`  
Recorded events and delays linked to individual flights, including delay categorization.

### `SILVER_BOARDING_SUMMARY`  
Aggregated summary of boarding activity per flight including luggage metrics.

### `SILVER_AGG_FREQUENT_DELAY_REASONS`  
Aggregated counts of delay reasons derived from flight event data.

### `SILVER_WEATHER_SUMMARY`  
Per-flight aggregated weather statistics across all sampling points.

### `SILVER_TICKET_EVENTS`  
Event log of ticket-related actions such as bookings, changes, and cancellations.

### `SILVER_AGG_CANCELATION_RATE`  
Aggregated cancelation rate metric computed from ticket events.