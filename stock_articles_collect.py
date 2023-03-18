"""
daily: to_time = from_time
"""

import json
import finnhub


class ArticleCollector:

    def __init__(self, stock_symbol, from_time, to_time):
        self.from_time = from_time
        self.to_time = to_time
        self.stock_symbol = stock_symbol

        with open('finhub_api.json', 'r') as api:
            self.api_key = json.load(api)['api_key']
        self.client = finnhub.Client(
            self.api_key
        )

    def get_articles(self):
        articles = self.client.company_news(
            symbol=self.stock_symbol,
            _from=self.from_time,
            to=self.to_time,
        )

        return articles

    def get_category(self, article):

        return article['category']

    def get_headline(self, article):

        return article['headline']

    def get_source(self, article):

        return article['source']

    def get_summary(self, article):

        return article['summary']

    def get_article_url(self, article):

        return article['url']
