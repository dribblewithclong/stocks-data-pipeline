import json
import psycopg2


class TableCreation:

    def __init__(self):
        self.stock_info = """
        CREATE TABLE IF NOT EXISTS stock_info (
            symbol VARCHAR PRIMARY KEY,
            country VARCHAR NOT NULL,
            currency VARCHAR NOT NULL,
            estimate_currency VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL,
            industry VARCHAR NOT NULL,
            ipo_date DATE NOT NULL,
            logo TEXT NOT NULL,
            market_capitalization FLOAT NOT NULL,
            name VARCHAR NOT NULL,
            phone FLOAT NOT NULL,
            share_outstanding FLOAT NOT NULL,
            web_url TEXT  NOT NULL
        );
        """

        self.stock_price = """
        CREATE TABLE IF NOT EXISTS stock_price (
            id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            price FLOAT NOT NULL,
            volume INTEGER NOT NULL
        );
        """

        self.stock_reddit_sentiment = """
        CREATE TABLE IF NOT EXISTS stock_reddit_sentiment (
            id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            mention INTEGER NOT NULL,
            positive_score FLOAT NOT NULL,
            negative_score FLOAT NOT NULL,
            positive_mention INTEGER NOT NULL,
            negative_mention INTEGER NOT NULL,
            sentiment_score FLOAT NOT NULL
        );
        """

        self.stock_twitter_sentiment = """
        CREATE TABLE IF NOT EXISTS stock_twitter_sentiment (
            id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            mention INTEGER NOT NULL,
            positive_score FLOAT NOT NULL,
            negative_score FLOAT NOT NULL,
            positive_mention INTEGER NOT NULL,
            negative_mention INTEGER NOT NULL,
            sentiment_score FLOAT NOT NULL
        );
        """

        self.stock_articles = """
        CREATE TABLE IF NOT EXISTS stock_articles (
            id VARCHAR PRIMARY KEY,
            symbol VARCHAR NOT NULL,
            date DATE NOT NULL,
            time TIME NOT NULL,
            category VARCHAR NOT NULL,
            headline VARCHAR NOT NULL,
            image_url TEXT NOT NULL,
            source VARCHAR NOT NULL,
            summary TEXT NOT NULL,
            article_url TEXT NOT NULL
        );
        """

    def create_table(self):
        with open('postgresql.json', 'r') as psql:
            config = json.load(psql)
        host = config['host']
        db = config['database']

        conn = psycopg2.connect(
            host=host,
            database=db,
        )
        cur = conn.cursor()

        cur.execute(self.stock_info)
        cur.execute(self.stock_price)
        cur.execute(self.stock_reddit_sentiment)
        cur.execute(self.stock_twitter_sentiment)
        cur.execute(self.stock_articles)

        cur.close()

        conn.commit()


if __name__ == '__main__':
    TableCreation().create_table()
