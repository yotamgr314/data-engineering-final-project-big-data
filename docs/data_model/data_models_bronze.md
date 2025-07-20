## Bronze Layer – Raw Data Tables

The bronze layer contains raw ingested data with minimal transformation. It represents the first landing zone for data from external systems (APIs, etc.).

---

### `BRONZE_ROUTES_RAW`  
Contains information about available flight routes, including geolocation of origin and destination airports.

### `BRONZE_FLIGHTS`  
Stores scheduled flights linked to specific routes, including departure and arrival times.

### `BRONZE_TICKET_PRICES`  
Captures pricing information per flight and ticket class, including validity periods.

### `BRONZE_BOOKED_TICKETS_RAW`  
Raw records of ticket bookings along with passenger personal data and booking metadata.

### `BRONZE_FLIGHT_WEATHER_RAW_API`  
Raw weather data collected from external APIs at specific coordinates and altitudes.

### `BRONZE_REGISTERED_CUSTOMERS`  
List of customers registered in the system with demographic and loyalty information.

### `BRONZE_ROUTE_WEATHER_POINTS`  
Waypoint coordinates along a route used to correlate with weather data.

### `BRONZE_BOARDING_EVENTS_RAW`  
Raw events capturing passenger boarding actions with time and weight details.

### `BRONZE_FLIGHT_EVENTS_RAW`  
Logs operational events related to flights, such as delays or status updates.

### `BRONZE_TICKET_EVENTS_RAW`  
Tracks lifecycle events for tickets, such as creation, modification, or cancellation.