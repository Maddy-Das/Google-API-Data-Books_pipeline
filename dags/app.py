# DAG - directed acyclic graph
# Tasks: 1) Fetch books using Google Books API 2) Clean data (transform) 3) Create and store data in table on postgres (load)
# Operators: Python Operator and SQLExecuteQueryOperator
# Hooks - allows connection to postgres
# Dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def fetch_books_from_google_api(num_books, ti):
    """
    Fetch book data using Google Books API (free, reliable, no API key required)
    """
    books = []
    query = "data+engineering"
    
    try:
        logging.info(f"Fetching {num_books} books from Google Books API")
        
        # Google Books API endpoint
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={min(num_books, 40)}&printType=books"
        
        response = requests.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            logging.info(f"Google Books API returned {len(items)} items")
            
            for item in items[:num_books]:
                volume_info = item.get('volumeInfo', {})
                sale_info = item.get('saleInfo', {})
                
                title = volume_info.get('title', 'Unknown Title')
                authors = ', '.join(volume_info.get('authors', ['Unknown Author']))
                
                # Get price info
                price = "N/A"
                if sale_info.get('saleability') == 'FOR_SALE':
                    price_info = sale_info.get('retailPrice', {})
                    if price_info:
                        amount = price_info.get('amount', 'N/A')
                        currency = price_info.get('currencyCode', '$')
                        price = f"{amount} {currency}" if amount != 'N/A' else 'N/A'
                elif sale_info.get('saleability') == 'FREE':
                    price = "Free"
                
                # Get rating info
                rating = volume_info.get('averageRating', 'N/A')
                rating_count = volume_info.get('ratingsCount', 0)
                if rating != 'N/A':
                    rating = f"{rating}/5 ({rating_count} reviews)"
                
                books.append({
                    'title': title,
                    'author': authors,
                    'price': price,
                    'rating': str(rating)
                })
            
            logging.info(f"Successfully processed {len(books)} books from Google Books API")
            
        else:
            logging.error(f"Google Books API request failed with status code: {response.status_code}")
            # Fallback to sample data
            books = generate_sample_book_data(num_books)
            logging.info(f"Using {len(books)} sample books as fallback")
            
    except requests.RequestException as e:
        logging.error(f"Network error with Google Books API: {str(e)}")
        books = generate_sample_book_data(num_books)
        logging.info(f"Using {len(books)} sample books as fallback")
    except Exception as e:
        logging.error(f"Unexpected error with Google Books API: {str(e)}")
        books = generate_sample_book_data(num_books)
        logging.info(f"Using {len(books)} sample books as fallback")
    
    if not books:
        # Final safety net
        books = generate_sample_book_data(num_books)
        logging.info(f"No books found, using {len(books)} sample books")
    
    # Convert to DataFrame and clean data
    df = pd.DataFrame(books)
    
    # Clean and deduplicate
    if not df.empty:
        df.drop_duplicates(subset='title', inplace=True)
        df = df.head(num_books)  # Ensure we don't exceed requested number
        
        # Clean data - remove very long titles/authors for database compatibility
        df['title'] = df['title'].str[:200]
        df['author'] = df['author'].str[:200]
        df['price'] = df['price'].str[:50]
        df['rating'] = df['rating'].str[:50]
    
    final_books = df.to_dict('records')
    logging.info(f"Final dataset: {len(final_books)} books ready for database insertion")
    
    # Push to XCom
    ti.xcom_push(key='book_data', value=final_books)



def insert_book_data_into_postgres(ti):
    """
    Insert book data into PostgreSQL with proper error handling.
    """
    try:
        book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
        
        if not book_data:
            raise ValueError("No book data found in XCom")
        
        logging.info(f"Attempting to insert {len(book_data)} book into database")
        
        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        
        # Fixed INSERT query without created_at reference
        insert_query = """
        INSERT INTO book (title, authors, price, rating)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (title) DO UPDATE SET 
            authors = EXCLUDED.authors,
            price = EXCLUDED.price,
            rating = EXCLUDED.rating;
        """
        
        inserted_count = 0
        
        for book in book_data:
            try:
                postgres_hook.run(
                    insert_query, 
                    parameters=(book['title'], book['author'], book['price'], book['rating'])
                )
                inserted_count += 1
                
            except Exception as e:
                logging.error(f"Error inserting book '{book['title']}': {str(e)}")
                continue
        
        logging.info(f"Successfully processed {inserted_count} books in database")
        
        # Log some statistics
        count_query = "SELECT COUNT(*) FROM book;"
        total_books = postgres_hook.get_first(count_query)[0]
        logging.info(f"Total books in database: {total_books}")
        
    except Exception as e:
        error_msg = f"Error in database operation: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)


# DAG configuration
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'fetch_and_store_books_with_google_api',
    default_args=default_args,
    description='Fetch book data using Google Books API and store in PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=['books', 'data_engineering', 'api', 'postgresql'],
)

# Tasks
create_table_task = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='books_connection',
    sql="""
    -- Drop and recreate table to ensure correct schema
    DROP TABLE IF EXISTS book;
    
    CREATE TABLE book (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL UNIQUE,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    
    -- Create indexes for better performance
    CREATE INDEX IF NOT EXISTS idx_books_title ON book(title);
    CREATE INDEX IF NOT EXISTS idx_books_authors ON book(authors);
    """,
    dag=dag,
)


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=fetch_books_from_google_api,
    op_args=[25],  # Number of books to fetch
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

create_table_task >> fetch_book_data_task >> insert_book_data_task
