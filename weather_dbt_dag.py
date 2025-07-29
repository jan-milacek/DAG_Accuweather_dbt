import os
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowFailException

# Load environment variables
load_dotenv()

# Email configuration
def get_email_config():
    return {
        'email': os.environ.get('NOTIFICATION_EMAIL', 'your_email@example.com'),
        'email_on_failure': True,
        'email_on_retry': True,
        'email_on_success': False,
    }

# Custom error handler
def task_failure_callback(context):
    """Send detailed email notification on task failure"""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    execution_date = context['execution_date']
    exception = context.get('exception')
    
    subject = f"dbt Weather Analytics Failed: {task_id}"
    
    html_content = f"""
    <h3>dbt Analytics Pipeline Failure</h3>
    <p><strong>DAG</strong>: {dag_id}</p>
    <p><strong>Task</strong>: {task_id}</p>
    <p><strong>Execution Date</strong>: {execution_date}</p>
    <p><strong>Status</strong>: Failed</p>
    
    <h4>Error Details</h4>
    <pre>{exception}</pre>
    
    <p>Please check the logs and verify data quality.</p>
    """
    
    send_email_smtp(
        to=get_email_config()['email'], 
        subject=subject, 
        html_content=html_content
    )

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    **get_email_config(),
    'on_failure_callback': task_failure_callback
}

# Create the DAG
weather_analytics_dag = DAG(
    'weather_analytics_dbt',
    default_args=default_args,
    description='Run dbt transformations on weather data',
    schedule_interval='0 3 * * *',  # Run at 3 AM, after data ingestion
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'weather', 'analytics'],
)

# Sensor to wait for data ingestion DAG completion
wait_for_data_ingestion = ExternalTaskSensor(
    task_id='wait_for_data_ingestion',
    external_dag_id='accuweather_daily',
    external_task_id='load_to_postgres',
    timeout=300,  # 5 minutes timeout
    poke_interval=60,  # Check every minute
    dag=weather_analytics_dag,
)

# Check if fresh data is available
def check_fresh_data(**context):
    """Verify that fresh weather data is available for processing"""
    load_dotenv()
    
    # Get database connection details
    pg_host = os.environ.get('POSTGRES_HOST', 'localhost')
    pg_port = os.environ.get('POSTGRES_PORT', '5432')
    pg_db = os.environ.get('POSTGRES_DB', 'weather_db')
    pg_user = os.environ.get('POSTGRES_USER', 'postgres')
    pg_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    
    # Get yesterday's date (what we expect to have data for)
    execution_date = context['execution_date']
    expected_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password
        )
        
        with conn.cursor() as cursor:
            # Check if we have data for expected date
            cursor.execute("""
                SELECT COUNT(*), MIN(observation_date), MAX(observation_date)
                FROM weather_db_schema.historical_weather_data 
                WHERE fetch_date = %s
            """, (expected_date,))
            
            count, min_obs, max_obs = cursor.fetchone()
            
            print(f"Found {count} records for {expected_date}")
            print(f"Observation range: {min_obs} to {max_obs}")
            
            if count == 0:
                raise AirflowFailException(f"No weather data found for {expected_date}")
            
            if count < 12:  # Expect at least 12 hours of data
                print(f"WARNING: Only {count} records found, proceeding with limited data")
            
            # Store metrics for success email
            context['ti'].xcom_push(key='data_count', value=count)
            context['ti'].xcom_push(key='data_date', value=expected_date)
            
            return f"Data validation passed: {count} records for {expected_date}"
            
    except psycopg2.Error as e:
        raise AirflowFailException(f"Database error during data check: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

# Run dbt models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='''
    cd {{ params.dbt_project_dir }} && 
    dbt run --profiles-dir {{ params.profiles_dir }}
    ''',
    params={
        'dbt_project_dir': os.environ.get('DBT_PROJECT_DIR', '/opt/airflow/dbt'),
        'profiles_dir': os.environ.get('DBT_PROFILES_DIR', '/opt/airflow/dbt')
    },
    dag=weather_analytics_dag,
)

# Run dbt tests
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='''
    cd {{ params.dbt_project_dir }} && 
    dbt test --profiles-dir {{ params.profiles_dir }}
    ''',
    params={
        'dbt_project_dir': os.environ.get('DBT_PROJECT_DIR', '/opt/airflow/dbt'),
        'profiles_dir': os.environ.get('DBT_PROFILES_DIR', '/opt/airflow/dbt')
    },
    dag=weather_analytics_dag,
)

# Generate business summary
def generate_business_summary(**context):
    """Create business-friendly summary of the analytics results"""
    load_dotenv()
    
    # Get database connection details
    pg_host = os.environ.get('POSTGRES_HOST', 'localhost')
    pg_port = os.environ.get('POSTGRES_PORT', '5432')
    pg_db = os.environ.get('POSTGRES_DB', 'weather_db')
    pg_user = os.environ.get('POSTGRES_USER', 'postgres')
    pg_password = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    
    try:
        conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password
        )
        
        with conn.cursor() as cursor:
            # Get latest daily summary
            cursor.execute("""
                SELECT 
                    data_date,
                    avg_temperature,
                    min_temperature,
                    max_temperature,
                    predominant_condition,
                    daily_temperature_assessment,
                    data_quality_flag
                FROM weather_analytics.daily_weather_summary 
                ORDER BY data_date DESC 
                LIMIT 1
            """)
            
            result = cursor.fetchone()
            if result:
                date, avg_temp, min_temp, max_temp, condition, assessment, quality = result
                
                # Get trend information
                cursor.execute("""
                    SELECT 
                        temp_change_daily,
                        daily_trend,
                        rolling_avg_7day
                    FROM weather_analytics.weather_trends 
                    ORDER BY data_date DESC 
                    LIMIT 1
                """)
                
                trend_result = cursor.fetchone()
                temp_change, trend, rolling_avg = trend_result if trend_result else (None, 'unknown', None)
                
                # Create business summary
                summary = {
                    'date': str(date),
                    'avg_temperature': float(avg_temp),
                    'temperature_range': f"{min_temp}°C to {max_temp}°C",
                    'condition': condition,
                    'assessment': assessment,
                    'trend': trend,
                    'temp_change': float(temp_change) if temp_change else 0,
                    'rolling_avg': float(rolling_avg) if rolling_avg else None,
                    'data_quality': quality
                }
                
                context['ti'].xcom_push(key='business_summary', value=summary)
                
                return f"Business summary generated for {date}"
            else:
                raise AirflowFailException("No analytics data found")
                
    except psycopg2.Error as e:
        raise AirflowFailException(f"Database error during summary generation: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()

# Send success notification with business insights
def send_success_notification(**context):
    """Send business-friendly success notification"""
    ti = context['ti']
    
    # Get business summary
    summary = ti.xcom_pull(key='business_summary')
    data_count = ti.xcom_pull(key='data_count')
    
    if not summary:
        summary = {'date': 'unknown', 'avg_temperature': 'N/A'}
    
    # Create business-focused email
    subject = f"Weather Analytics Updated - {summary['avg_temperature']}°C avg"
    
    html_content = f"""
    <h3>Daily Weather Analytics Complete</h3>
    
    <h4>Today's Weather Summary ({summary.get('date', 'N/A')})</h4>
    <ul>
        <li><strong>Average Temperature:</strong> {summary.get('avg_temperature', 'N/A')}°C</li>
        <li><strong>Temperature Range:</strong> {summary.get('temperature_range', 'N/A')}</li>
        <li><strong>Conditions:</strong> {summary.get('condition', 'N/A')}</li>
        <li><strong>Assessment:</strong> {summary.get('assessment', 'N/A')}</li>
        <li><strong>Trend:</strong> {summary.get('trend', 'N/A')}</li>
    </ul>
    
    <h4>Data Quality</h4>
    <ul>
        <li><strong>Records Processed:</strong> {data_count or 'N/A'}</li>
        <li><strong>Data Quality:</strong> {summary.get('data_quality', 'N/A')}</li>
    </ul>
    
    <p>Analytics tables updated and ready for business intelligence dashboards.</p>
    """
    
    send_email_smtp(
        to=get_email_config()['email'],
        subject=subject,
        html_content=html_content
    )

# Define Python operators
check_data_task = PythonOperator(
    task_id='check_fresh_data',
    python_callable=check_fresh_data,
    provide_context=True,
    dag=weather_analytics_dag,
)

generate_summary_task = PythonOperator(
    task_id='generate_business_summary',
    python_callable=generate_business_summary,
    provide_context=True,
    dag=weather_analytics_dag,
)

send_notification_task = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=weather_analytics_dag,
)

# Set task dependencies
wait_for_data_ingestion >> check_data_task >> dbt_run >> dbt_test >> generate_summary_task >> send_notification_task
