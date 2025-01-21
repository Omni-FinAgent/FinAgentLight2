import os
from typing import List, Dict, Any, Optional, Tuple
import json
from dotenv import load_dotenv
load_dotenv(verbose=True)
from datetime import datetime, timedelta
import pandas as pd
from pandas_market_calendars import get_calendar
from urllib.request import urlopen
import certifi
import signal
import asyncio
from tqdm.auto import tqdm

from dataset.logger import logger

class TimeoutException(Exception):
    pass
def timeout_handler(signum, frame):
    raise TimeoutException('Time out')

def get_jsonparsed_data(url):
    response = urlopen(url, cafile=certifi.where())
    data = response.read().decode('utf-8')
    return json.loads(data)

NYSE = get_calendar('XNYS')

def generate_intervals(start_date, end_date, interval_level='year'):

    intervals = []

    if interval_level == 'year':
        current_date = start_date
        while current_date < end_date:
            next_year = current_date.replace(year=current_date.year + 1)
            if next_year > end_date:
                next_year = end_date
            interval = (current_date, next_year)
            intervals.append(interval)
            current_date = next_year
    elif interval_level == 'day':
        current_date = start_date
        while current_date < end_date:
            next_day = current_date + timedelta(days=1)
            interval = (current_date, next_day)
            intervals.append(interval)
            current_date = next_day
    elif interval_level == 'month':
        current_date = start_date

        while current_date < end_date:
            year, month = current_date.year, current_date.month
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            if next_month > end_date:
                next_month = end_date
            interval = (current_date, next_month)
            intervals.append(interval)
            current_date = next_month
    else:
        return None

    return intervals

class FMPPriceDownloader():
    def __init__(self,
                 api_key: Optional[str] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 symbol: Optional[str] = None,
                 workdir: Optional[str] = None,
                 **kwargs):
        self.api_key = api_key

        if self.api_key is None:
            self.api_key = os.getenv('FMP_API_KEY')

        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date

        self.request_url = 'https://financialmodelingprep.com/api/v3/historical-price-full/{}?from={}&to={}&apikey={}'

        self.workdir = workdir
        os.makedirs(self.workdir, exist_ok=True)

    async def _check_download(self,
                              symbol: Optional[str] = None,
                              intervals: Optional[List[Tuple[datetime, datetime]]] = None):

        download_infos = []

        for (start, end) in intervals:
            name = '{}'.format(start.strftime('%Y-%m-%d'))
            if os.path.exists(os.path.join(self.workdir, symbol, name)):
                item = {
                    'name': name,
                    'downloaded': True,
                    'start': start,
                    'end': end
                }
            else:
                item = {
                    'name': name,
                    'downloaded': False,
                    'start': start,
                    'end': end
                }
            download_infos.append(item)

        logger.info(f'Downloaded / Total: [{len([info for info in download_infos if info['downloaded']])} / {len(download_infos)}]')

        return download_infos

    async def run(self,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  symbol: Optional[str] = None):

        start_date = datetime.strptime(start_date if start_date else self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date if end_date else self.end_date, '%Y-%m-%d')
        symbol = symbol if symbol else self.symbol

        intervals = generate_intervals(start_date, end_date, 'year')

        download_infos = await self._check_download(
            symbol=symbol,
            intervals=intervals,
        )

        slice_dir = os.path.join(self.workdir, symbol)
        os.makedirs(slice_dir, exist_ok=True)

        df = pd.DataFrame()

        bar_format = f'Download {symbol} Prices:' + '{bar:50}{percentage:3.0f}%|{elapsed}/{remaining}{postfix}'
        for info in tqdm(download_infos, bar_format=bar_format):

            name = info['name']
            downloaded = info['downloaded']
            start = info['start']
            end = info['end']

            is_trading_day = NYSE.valid_days(start_date=start, end_date=end).size > 0
            if is_trading_day:
                if downloaded:
                    chunk_df = pd.read_csv(os.path.join(slice_dir, '{}.csv'.format(name)))
                else:
                    chunk_df = {
                        'open': [],
                        'high': [],
                        'low': [],
                        'close': [],
                        'volume': [],
                        'timestamp': [],
                        'adjClose': [],
                        'unadjustedVolume': [],
                        'change': [],
                        'changePercent': [],
                        'vwap': [],
                        'label': [],
                        'changeOverTime': []
                    }

                    request_url = self.request_url.format(
                        symbol,
                        start.strftime('%Y-%m-%d'),
                        end.strftime('%Y-%m-%d'),
                        self.api_key)

                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(60)

                    try:
                        await asyncio.sleep(1)
                        aggs = get_jsonparsed_data(request_url)
                        aggs = aggs['historical'] if 'historical' in aggs else []
                        signal.alarm(0)
                    except TimeoutException:
                        print('Time out')
                        aggs = []

                    if len(aggs) == 0:
                        continue

                    for a in aggs:
                        chunk_df['open'].append(a['open'])
                        chunk_df['high'].append(a['high'])
                        chunk_df['low'].append(a['low'])
                        chunk_df['close'].append(a['close'])
                        chunk_df['volume'].append(a['volume'])
                        chunk_df['timestamp'].append(a['date'])
                        chunk_df['adjClose'].append(a['adjClose'])
                        chunk_df['unadjustedVolume'].append(a['unadjustedVolume'])
                        chunk_df['change'].append(a['change'])
                        chunk_df['changePercent'].append(a['changePercent'])
                        chunk_df['vwap'].append(a['vwap'])
                        chunk_df['label'].append(a['label'])
                        chunk_df['changeOverTime'].append(a['changeOverTime'])

                    chunk_df = pd.DataFrame(chunk_df, index=range(len(chunk_df['timestamp'])))
                    chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp']).apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

                    chunk_df.to_csv(os.path.join(slice_dir, name), index=False)

                df = pd.concat([df, chunk_df], axis=0)

        df = df.sort_values(by='timestamp', ascending=True)
        df = df[['timestamp'] + [col for col in df.columns if col != 'timestamp']]
        df.to_csv(os.path.join(self.workdir, '{}.csv'.format(symbol)), index=False)

class FMPNewsDownloader():
    def __init__(self,
                 api_key: Optional[str] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 symbol: Optional[str] = None,
                 workdir: Optional[str] = None,
                 max_pages: int = 10,
                 **kwargs):
        self.api_key = api_key

        if self.api_key is None:
            self.api_key = os.getenv('FMP_API_KEY')

        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        self.max_pages = max_pages

        self.request_url = 'https://financialmodelingprep.com/api/v3/stock_news?tickers={}&page={}&limit=1000&from={}&to={}&apikey={}'

        self.workdir = workdir
        os.makedirs(self.workdir, exist_ok=True)

    async def _check_download(self,
                              symbol: Optional[str] = None,
                              intervals: Optional[List[Tuple[datetime, datetime]]] = None):

        download_infos = []

        for (start, end) in intervals:
            for page in range(1, self.max_pages + 1):
                name = '{}_page_{:04d}'.format(start.strftime('%Y-%m-%d'), page)
                if os.path.exists(os.path.join(self.workdir, symbol, name)):
                    item = {
                        'name': name,
                        'downloaded': True,
                        'start': start,
                        'end': end,
                        'page': page
                    }
                else:
                    item = {
                        'name': name,
                        'downloaded': False,
                        'start': start,
                        'end': end,
                        'page': page
                    }
                download_infos.append(item)

        logger.info(f'Downloaded / Total: [{len([info for info in download_infos if info['downloaded']])} / {len(download_infos)}]')

        return download_infos

    async def run(self,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  symbol: Optional[str] = None):

        start_date = datetime.strptime(start_date if start_date else self.start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date if end_date else self.end_date, '%Y-%m-%d')
        symbol = symbol if symbol else self.symbol

        intervals = generate_intervals(start_date, end_date, 'year')

        download_infos = await self._check_download(
            symbol=symbol,
            intervals=intervals,
        )

        slice_dir = os.path.join(self.workdir, symbol)
        os.makedirs(slice_dir, exist_ok=True)

        df = pd.DataFrame()

        bar_format = f'Download {symbol} News:' + '{bar:50}{percentage:3.0f}%|{elapsed}/{remaining}{postfix}'

        for info in tqdm(download_infos, bar_format=bar_format):

            name = info['name']
            downloaded = info['downloaded']
            start = info['start']
            end = info['end']
            page = info['page']

            is_trading_day = NYSE.valid_days(start_date=start, end_date=end).size > 0
            if is_trading_day:

                if downloaded:
                    chunk_df = pd.read_csv(os.path.join(slice_dir, '{}.csv'.format(name)))
                else:
                    chunk_df = {
                        'symbol': [],
                        'publishedDate': [],
                        'title': [],
                        'image': [],
                        'site': [],
                        'text': [],
                        'url': []
                    }

                    request_url = self.request_url.format(
                        symbol,
                        page,
                        start.strftime('%Y-%m-%d'),
                        end.strftime('%Y-%m-%d'),
                        self.api_key)

                    signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(60)

                    try:
                        await asyncio.sleep(1)
                        aggs = get_jsonparsed_data(request_url)
                        signal.alarm(0)
                    except TimeoutException:
                        print('Time out')
                        aggs = []

                    if len(aggs) == 0:
                        continue

                    for a in aggs:
                        chunk_df['symbol'].append(a['symbol'])
                        chunk_df['publishedDate'].append(a['publishedDate'])
                        chunk_df['title'].append(a['title'])
                        chunk_df['image'].append(a['image'])
                        chunk_df['site'].append(a['site'])
                        chunk_df['text'].append(a['text'])
                        chunk_df['url'].append(a['url'])

                    chunk_df = pd.DataFrame(chunk_df, index=range(len(chunk_df['publishedDate'])))
                    chunk_df['timestamp'] = pd.to_datetime(chunk_df['publishedDate']).apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))

                    chunk_df.to_csv(os.path.join(slice_dir, '{}.csv'.format(name)), index=False)

                df = pd.concat([df, chunk_df], axis=0)

        df = df.sort_values(by='timestamp', ascending=True)
        df = df.drop_duplicates(subset=['publishedDate', 'title'], keep='first')
        df = df[['timestamp'] + [col for col in df.columns if col != 'timestamp']]
        df.to_csv(os.path.join(self.workdir, '{}.csv'.format(symbol)), index=False)

class Downloader():
    def __init__(self,
                 assets_path: Optional[str] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None,
                 workdir: Optional[str] = None,
                 batch_size: int = 10,
                 ):

        self.assets_path = assets_path
        self.start_date = start_date
        self.end_date = end_date

        self.assets_info, self.symbols = self._load_assets()

        assert len(self.symbols) > 0, 'No symbols to download'
        if len(self.symbols) <= batch_size:
            batch_size = len(self.symbols)
        self.batch_size = batch_size

        self.workdir = workdir
        os.makedirs(self.workdir, exist_ok=True)

    async def _run_batch(self, tasks):
        '''
        Run a batch of tasks and handle any potential errors.
        '''
        return await asyncio.gather(*tasks)

    def _load_assets(self):
        '''
        Load assets from the assets file.
        :return:
        '''
        with open(self.assets_path) as f:
            assets_info = json.load(f)
        symbols = [asset for asset in assets_info]
        logger.info(f'Loaded {len(symbols)} assets from {self.assets_path}')
        return assets_info, symbols

    async def _download_fmp_price(self):
        '''
        Download price data from FMP API.
        :return:
        '''

        price_workdir = os.path.join(self.workdir, 'price')
        os.makedirs(price_workdir, exist_ok=True)

        tasks = []
        for symbol in self.symbols:
            downloader = FMPPriceDownloader(
                api_key=os.getenv('FMP_API_KEY'),
                start_date=self.start_date,
                end_date=self.end_date,
                symbol=symbol,
                workdir=price_workdir
            )
            tasks.append(downloader.run())

            if len(tasks) == self.batch_size:
                await self._run_batch(tasks)
                tasks = []

        if tasks:
            await self._run_batch(tasks)

    async def _download_fmp_news(self):
        '''
        Download news data from FMP API.
        :return:
        '''
        news_workdir = os.path.join(self.workdir, 'news')
        os.makedirs(news_workdir, exist_ok=True)

        tasks = []
        for symbol in self.symbols:
            downloader = FMPNewsDownloader(
                api_key=os.getenv('FMP_API_KEY'),
                start_date=self.start_date,
                end_date=self.end_date,
                symbol=symbol,
                workdir=news_workdir
            )
            tasks.append(downloader.run())

            if len(tasks) == self.batch_size:
                await self._run_batch(tasks)
                tasks = []

        if tasks:
            await self._run_batch(tasks)

    async def run(self):
        await self._download_fmp_price()

        await self._download_fmp_news()

