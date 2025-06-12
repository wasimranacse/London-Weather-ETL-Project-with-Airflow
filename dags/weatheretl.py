from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# Coordinates for London
LATITUDE = 51.5072
LONGITUDE = -0.1276

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}


@dag(
    dag_id='london_weather_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['weather', 'london', 'etl']
)
def london_weather_etl_pipeline():

    @task()
    def extract_london_weather_data():
        """Extract current weather data from Open-Meteo API"""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f'Failed to fetch weather data: {response.status_code}')

    @task()
    def transform_weather_data(weather_data: dict):
        """Extract and transform fields from the raw API response"""
        current = weather_data['current_weather']
        return {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current['temperature'],
            'windspeed': current['windspeed'],
            'winddirection': current['winddirection'],
            'weathercode': current['weathercode']
        }

    @task()
    def load_weather_data(data: dict):
        """Load transformed data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            data['latitude'],
            data['longitude'],
            data['temperature'],
            data['windspeed'],
            data['winddirection'],
            data['weathercode']
        ))

        conn.commit()
        cursor.close()
        conn.close()

    raw_data = extract_london_weather_data()
    transformed = transform_weather_data(raw_data)
    load_weather_data(transformed)


london_weather_etl_pipeline()
