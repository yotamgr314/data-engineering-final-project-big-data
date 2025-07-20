import os

# נתיב לקבצים שלך
script_path = "/opt/bitnami/spark/jobs/"

# רשימה של כל הקבצים להרצה
scripts = [
    "silver_etl_routes.py",
    "silver_etl_flights.py",
    "silver_etl_ticket_prices.py",
    "silver_etl_customers.py",
    "silver_etl_flight_events.py",
    "silver_etl_boarding_summary.py"
]

# הרץ כל קובץ
for script in scripts:
    os.system(f"spark-submit --master spark://spark-master:7077 {script_path}{script}")
