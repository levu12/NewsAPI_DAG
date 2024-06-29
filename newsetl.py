import requests
import pandas as pd
import sqlalchemy
from sqlalchemy import inspect
import sqlite3
from airflow.models import Variable

API_KEY = Variable.get("NEWSAPI")
DATABASE = '/mnt/d/newsdb.db'
DATABASE_ENGINE = f'sqlite:///{DATABASE}'
COUNTRY = 'us'
def main():
    r = requests.get(f'https://newsapi.org/v2/top-headlines?country={COUNTRY}&apiKey={API_KEY}')

    if r.status_code == 200:
        data = r.json()
        authors, titles, descriptions, urls, urls_to_images, published_ats, ids = [], [], [], [], [], [], []

        if data['status'] == 'ok':
            for article in data['articles']:
                authors.append(article.get('author', ''))  # Replace None with an empty string
                titles.append(article.get('title', 'N/A'))
                descriptions.append(article.get('description', 'N/A'))
                urls.append(article.get('url', 'N/A'))
                urls_to_images.append(article.get('urlToImage', 'N/A'))
                published_ats.append(article.get('publishedAt', 'N/A'))
                ids.append(article['source'].get('id', ''))  # Replace None with an empty string

            news_dict = {
                'authors': authors,
                'titles': titles,
                'descriptions': descriptions,
                'urls': urls,
                'urls_to_images': urls_to_images,
                'published_ats': published_ats,
                'ids': ids
            }

            news_df = pd.DataFrame(news_dict, columns=['authors', 'titles', 'descriptions', 'urls', 'urls_to_images', 'published_ats', 'ids'])

            def transform(news_df: pd.DataFrame) -> pd.DataFrame:
                news_df['published_ats'] = pd.to_datetime(news_df['published_ats'], errors='coerce')
                news_df['published_ats'] = news_df['published_ats'].dt.strftime('%Y-%m-%d %H:%M:%S')
                return news_df

            news_df = transform(news_df)
            print(news_df)  # Print the DataFrame to verify data

            sqlite = sqlite3.connect(DATABASE)
            cursor = sqlite.cursor()

            engine = sqlalchemy.create_engine(DATABASE_ENGINE)

            # Check if the table exists
            inspector = inspect(engine)
            table_exists = inspector.has_table('news')

            # Create the table if it doesn't exist
            if not table_exists:
                cursor.execute("""
                    CREATE TABLE news (
                        id VARCHAR(32),
                        author TEXT,
                        title TEXT,
                        description TEXT,
                        url TEXT,
                        url_to_image TEXT,
                        published_at TEXT,
                        source_id TEXT
                    );
                """)
                sqlite.commit()
                print("Table created.")

            # Insert data into the table
            for index, row in news_df.iterrows():
                cursor.execute("SELECT COUNT(*) FROM news WHERE id = ? AND author = ? AND title = ? AND description = ? AND url = ? AND url_to_image = ? AND published_at = ? AND source_id = ?", (row['ids'], row['authors'], row['titles'], row['descriptions'], row['urls'], row['urls_to_images'], row['published_ats'], row['ids']))
                if cursor.fetchone()[0] == 0:  # Check if the row already exists
                    cursor.execute("""
                        INSERT INTO news (id, author, title, description, url, url_to_image, published_at, source_id)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (row['ids'], row['authors'], row['titles'], row['descriptions'], row['urls'], row['urls_to_images'], row['published_ats'], row['ids']))
            sqlite.commit()
            print("Data inserted into the table.")

            # Fetch the data from the table
            cursor.execute("SELECT * FROM news")
            rows = cursor.fetchall()
            fetched_df = pd.DataFrame(rows, columns=['id', 'author', 'title', 'description', 'url', 'url_to_image', 'published_at', 'source_id'])
            print(fetched_df)

            sqlite.close()

    else:
        print(f"Failed to fetch data. Status code: {r.status_code}")


if __name__ == '__main__':
    main()