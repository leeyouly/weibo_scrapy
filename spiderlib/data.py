import md5
import PyDB
import datetime
import urlparse
import scrapy

class Page(object):
    def __init__(self, url):
        self.url = url
        self.url_hash = md5.new(url.encode('utf8')).hexdigest()
        self.status = None
        self.update_time = None
        self.create_time = None
    
class EtlLog(scrapy.Item):
    source_name         = scrapy.Field()
    target_table        = scrapy.Field()
    start_dt            = scrapy.Field()
    end_dt              = scrapy.Field()
    data_row            = scrapy.Field()
    is_successful       = scrapy.Field()
    log_desc            = scrapy.Field()
    datetime_stamp      = scrapy.Field()

class DataStorage(object):
    __pool = None
    def build_connection(self, db_url):
        if DataStorage.__pool is not None:
            return DataStorage.__pool
            
        urlparts = urlparse.urlparse(db_url)

        params = dict([(k,v[0]) for k,v in urlparse.parse_qs(urlparts.query).items()])
        if urlparts.scheme == 'mysql':
            port = 3306
            if urlparts.port:
                port = int(urlparts.port)
            db = PyDB.MySQLContext(
                {'user':urlparts.username, 
                'host':urlparts.hostname, 
                'db':urlparts.path.lstrip('/'), 
                'port':port, 
                'passwd': urlparts.password, 
                'charset':'utf8'}
            )
        elif urlparts.scheme == 'oracle':
            port = 1521
            if urlparts.port:
                port = int(urlparts.port)
            db = PyDB.OracleContext(
                user = urlparts.username,
                password = urlparts.password,
                host = urlparts.hostname,
                port = port, 
                **params
            )
        DataStorage.__pool = db
        return db
    def save(self, item):
        self.db.save(self.table_name, item)
        self.db.commit()
        
    def get(self, keys):
        return self.db.get(self.table_name, keys)
    
    def exist(self, keys):
        return self.db.exists_key(self.table_name, keys)
        
class PageStorage(DataStorage):
    def __init__(self, db_url):
        self.db = self.build_connection(db_url)
        self.table_name = 't_bm_pages'
        self.db.set_metadata(self.table_name, [
                                       PyDB.StringField("url_hash", is_key=True),
                                       PyDB.StringField("url", is_key=True),
                                       PyDB.IntField('status'),
                                       PyDB.DatetimeField("create_time"),
                                       PyDB.DatetimeField('update_time'),
                                       ])
                                       
    
    def find_page(self, url):
        url_hash = md5.new(url.encode('utf8')).hexdigest()
        return self.db.get(self.table_name, {
                'url_hash': url_hash, 
                'url' : url}
            )
        

    def save_page(self, page):
        page_obj = {
            'url_hash': page.url_hash,
            'url': page.url,
            'status': page.status,
            'update_time' : datetime.datetime.now(),
            'create_time' : datetime.datetime.now(),
        }
        self.db.save(self.table_name, page_obj)
        self.db.commit()
        
class EtlLogStorage(DataStorage):
    def __init__(self, db_url):
        self.db = self.build_connection(db_url)
        self.table_name = 'etl_log.Etl_Log_Stg'
        self.db.set_metadata(self.table_name, [
                                       PyDB.StringField("source_name", is_key=True),
                                       PyDB.StringField("target_table", is_key=True),
                                       PyDB.DatetimeField("start_dt"),
                                       PyDB.DatetimeField('end_dt'),
                                       PyDB.IntField('data_row'),
                                       PyDB.StringField('is_successful'),
                                       PyDB.StringField('log_desc'),
                                       PyDB.DatetimeField('datetime_stamp'),
                                       ])