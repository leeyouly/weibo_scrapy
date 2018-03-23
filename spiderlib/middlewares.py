# -*- coding: utf-8 -*-
import logging
from scrapy.exceptions import NotConfigured
from spiderlib.data import PageStorage, Page
from scrapy.http import Request
from scrapy import Item
import datetime

class SpiderLogMiddleware(object):
    def process_spider_output(self, response, result, spider):
        logging.info(str(response) + str(result) + str(spider))
        return (r for r in result or ())
        
class IndexPageSaveMiddleware(object):
    def __init__(self, database_setting):
        self.page_storage = PageStorage(database_setting)
        
    @classmethod
    def from_settings(cls, settings):
        database_setting = settings.get('DATABASE')
        if not database_setting:
            raise NotConfigured
        return cls(database_setting)
        
    def process_spider_output(self, response, result, spider):
        if 'redirect_urls' in response.meta:
            page_url = response.meta['redirect_urls'][0]
            logging.debug('redirect_urls: %s' % response.meta['redirect_urls'])
        else:
            page_url = response.url
        result_has_item = False
        for ret_item in result:
            if isinstance(ret_item, Item):
                result_has_item = True
            # 如果是结果中包含request，检查是否在pagestorage中已存在，如果存在则跳过，不进行抓取
            if isinstance(ret_item, Request):
                if self.page_storage.find_page(ret_item.url):
                    continue
            yield ret_item
        ignore_page_incremental = False
        if hasattr(spider, 'ignore_page_incremental'):
            ignore_page_incremental = getattr(spider, 'ignore_page_incremental')
        # 当前页面parse的结果包含item，则保存当前页面的url到pagestorage
        if result_has_item and not ignore_page_incremental:
            page = Page(page_url)
            page.status = 3
            page.update_time = datetime.datetime.now()
            page.create_time = datetime.datetime.now()
            self.page_storage.save_page(page)
