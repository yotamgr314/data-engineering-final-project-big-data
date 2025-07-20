## ✅ Data Quality Checks

The following data quality checks are implemented across various layers of the pipeline to ensure reliability, consistency, and correctness of the data.

---

### Bronze Layer

- **Null Checks**: Ensured key fields (e.g., IDs, timestamps) are not null.
- **Type Validation**: Verified schema during ingestion using Spark DataFrame schemas.
- **Deduplication**: Removed duplicate raw events using unique IDs.
- **Basic Range Checks**: Validated values like latitude, longitude, and ticket price fall within expected ranges.

---

### Silver Layer

- **Referential Integrity**: Checked foreign key relationships (e.g., customer IDs in booked tickets).
- **Enum Validation**: Validated allowed values for fields like `ticket_class`, `delay_reason`, `weather_condition`.
- **Date Validation**: Ensured proper chronological order for fields like `booking_date`, `flight_date`, `event_time`.
- **Data Conformance**: Applied strict type enforcement using Spark schema definitions.

---

### Gold Layer

- **Aggregate Consistency**: Verified that total values (e.g., total revenue, total flights) match corresponding detailed metrics.
- **No Nulls in KPIs**: All key performance indicators in fact tables are guaranteed to be complete.
- **Dimensional Completeness**: Ensured all fact records correctly join with dimension tables.
- **Duplicate Prevention**: Unique constraints on dimensional keys are enforced during write.

---

"""