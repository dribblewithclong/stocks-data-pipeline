import finnhub


class StockInfoCollector:

    def __init__(self, stock_symbol):
        self.stock_symbol = stock_symbol

        self.api_key = 'cgbgpj1r01qispgnbh8gcgbgpj1r01qispgnbh90'
        self.client = finnhub.Client(
            self.api_key
        )

    def get_stock_info(self):
        info = self.client.company_profile2(
            symbol=self.stock_symbol,
        )

        return info

    def get_name(self):
        info = self.get_stock_info()

        return info['name']

    def get_web_url(self):
        info = self.get_stock_info()

        return info['weburl']

    def get_country(self):
        info = self.get_stock_info()

        return info['country']

    def get_industry(self):
        info = self.get_stock_info()

        return info['finnhubIndustry']

    def get_exchange(self):
        info = self.get_stock_info()

        return info['exchange']

    def get_ipo_date(self):
        info = self.get_stock_info()

        return info['ipo']

    def get_market_capitalization(self):
        info = self.get_stock_info()

        return info['marketCapitalization']

    def get_share_outstanding(self):
        info = self.get_stock_info()

        return info['shareOutstanding']
