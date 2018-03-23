# -*- coding:utf8 -*-
from spiderlib.data import EtlLog, EtlLogStorage
import logging
from scrapy import signals
from scrapy.exceptions import NotConfigured
import datetime

logger = logging.getLogger(__name__)

class WriteEtlLog(object):
    def __init__(self, crawler, database_setting):
        
        self.storage = EtlLogStorage(database_setting)
        self.bot_name = crawler.settings.get('BOT_NAME')
        self.stats = crawler.stats
        self.start_dt = None
        self.end_dt = None

    @classmethod
    def from_crawler(cls, crawler):
        # first check if the extension should be enabled and raise
        # NotConfigured otherwise
        if not crawler.settings.get('DATABASE'):
            raise NotConfigured

        # get the number of items from settings
        database_setting = crawler.settings.get('DATABASE')

        # instantiate the extension object
        ext = cls(crawler, database_setting)

        # connect the extension object to signals
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)

        # return the extension object
        return ext

    def spider_opened(self, spider):
        logger.info("opened spider %s", spider.name)
        self.start_dt = datetime.datetime.now()

    def spider_closed(self, spider):
        self.end_dt = datetime.datetime.now()
        items = self.stats.get_value('spiderlog/save_count', 0)
        if items > 0:
            for target_table in self.stats.get_value('spiderlog/target_tables'):
                etllog = EtlLog()
                etllog['source_name'] = self.stats.get_value('spiderlog/source_name')
                etllog['target_table'] = target_table
                etllog['start_dt'] = self.start_dt
                etllog['end_dt'] = self.end_dt
                etllog['data_row'] = items
                etllog['is_successful'] = 'Y'
                etllog['log_desc'] = ''
                etllog['datetime_stamp'] = datetime.datetime.now()
                
                self.storage.save(etllog)
                
    def item_scraped(self, item, spider):
        # self.items_scraped += 1
        # if self.items_scraped % self.item_count == 0:
            # logger.info("scraped %d items", self.items_scraped)
        pass