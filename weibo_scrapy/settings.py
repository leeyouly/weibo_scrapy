# -*- coding: utf-8 -*-

# Scrapy settings for weibo_scrapy project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://doc.scrapy.org/en/latest/topics/settings.html
#     https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://doc.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'weibo_scrapy'

SPIDER_MODULES = ['weibo_scrapy.spiders']
NEWSPIDER_MODULE = 'weibo_scrapy.spiders'


USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'
DOWNLOAD_DELAY=3
DOWNLOAD_TIMEOUT = 900

ITEM_PIPELINES = {
   # 'conab_gov_br.pipelines.SendMailPipeline': 300,
}

SPIDER_MIDDLEWARES = {
   'spiderlib.middlewares.IndexPageSaveMiddleware': 300,
}

MAIL_HOST = 'smtp.kffund.cn'
MAIL_USER = 'bi_system@kffund.cn'
MAIL_PASS = 'bi1234'
MAIL_FROM = 'bi_system@kffund.cn'
MAIL_TO = 'ly@kffund.cn'

DATABASE = 'oracle://stg:stg123@10.6.0.94:1521/?service_name=db'
# DATABASE = 'oracle://stg:devstg123@10.10.50.17:1534/?service_name=db'

LOG_LEVEL = 'INFO'

FTP_HOST = '192.168.8.234'
FTP_USER = 'ftp4dce'
FTP_PASSWORD = 'DCEftp123'
FTP_PATH = '/SYSTEM/data/AGs/CONAB-Crop Estimate'