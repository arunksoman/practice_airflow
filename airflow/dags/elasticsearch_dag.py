from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchSQLHook
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'elasticsearch_document_pipeline',
    default_args=default_args,
    description='Create index, insert data, and verify in Elasticsearch',
    schedule=None,
    catchup=False,
    tags=['elasticsearch'],
)


def create_index_if_not_exists(**context):
    """Create documents index if it doesn't exist"""
    es_hook = ElasticsearchSQLHook(elasticsearch_conn_id='test-elasticsearch')
    es_client = es_hook.get_conn()
    
    index_name = 'documents'
    # Check if index exists
    if es_client.es.indices.exists(index=index_name):
        # Create index with mapping
        index_body = {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'title': {'type': 'text'},
                    'content': {'type': 'text'},
                    'author': {'type': 'keyword'},
                    'created_at': {'type': 'date'}
                }
            }
        }
        es_client.es.indices.create(index=index_name, body=index_body)
        print(f"Index '{index_name}' created successfully")
    else:
        print(f"Index '{index_name}' already exists")


def insert_document(**context):
    """Insert a document into the documents index"""
    es_hook = ElasticsearchSQLHook(elasticsearch_conn_id='test-elasticsearch')
    es_client = es_hook.get_conn()
    
    index_name = 'documents'
    
    # Sample document to insert
    document = {
        'title': 'Sample Document',
        'content': 'This is a test document inserted by Airflow',
        'author': 'Airflow DAG',
        'created_at': datetime.now().isoformat()
    }
    
    # Insert document with a specific ID
    doc_id = 'airflow_test_doc_1'
    response = es_client.es.index(index=index_name, id=doc_id, body=document)
    
    print(f"Document inserted: {response}")
    
    # Push document ID to XCom for next task
    context['task_instance'].xcom_push(key='doc_id', value=doc_id)
    
    return doc_id


def verify_document_exists(**context):
    """Verify that the document exists in Elasticsearch"""
    es_hook = ElasticsearchSQLHook(elasticsearch_conn_id='test-elasticsearch')
    es_client = es_hook.get_conn()
    
    index_name = 'documents'
    
    # Pull document ID from previous task
    doc_id = context['task_instance'].xcom_pull(task_ids='insert_document', key='doc_id')
    
    try:
        # Try to get the document
        response = es_client.es.get(index=index_name, id=doc_id)
        
        if response['found']:
            print(f"Document verified successfully: {response['_source']}")
            return True
        else:
            raise Exception(f"Document with ID '{doc_id}' not found in index '{index_name}'")
    
    except NotFoundError:
        raise Exception(f"Document with ID '{doc_id}' does not exist in index '{index_name}'")
    except Exception as e:
        raise Exception(f"Error verifying document: {str(e)}")


# Define tasks
create_index_task = PythonOperator(
    task_id='create_index',
    python_callable=create_index_if_not_exists,
    dag=dag,
)

insert_document_task = PythonOperator(
    task_id='insert_document',
    python_callable=insert_document,
    dag=dag,
)

verify_document_task = PythonOperator(
    task_id='verify_document',
    python_callable=verify_document_exists,
    dag=dag,
)

# Set task dependencies
create_index_task >> insert_document_task >> verify_document_task
