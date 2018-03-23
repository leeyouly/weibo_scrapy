'''

'''
from PyDB.fields import IntField, StringField, DatetimeField, DateField
import datetime


class DbContext(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        pass
        
    def _generate_insert_sql(self, tablename, table_metadata, data):
        data = data.copy()
        fields = ""
        values = ""
        for field in data.keys():
            if field not in table_metadata.keys():
                break
            
        
        fields = ','.join(data.keys())
        values = ','.join([self._generate_insert_value(x) for x in data.keys()])
        sql = 'insert into ' + tablename + ' ' + fields + ' values (' + values + ')'
        
    def _generate_insert_value(self, field, value):
        if field is StringField:
            return '\'' + value.replace('\'', '\'\'') + '\'' 
        if field is DatetimeField:
            return '\'' + value + '\''
        return value
    
class Dialect:
    def format_value_string(self, field, value):
        if isinstance(field, StringField):
            return '\'' + value.replace('\'', '\'\'') + '\'' 
        if isinstance(field, DatetimeField):
            if isinstance(value, datetime.datetime):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            if isinstance(value, datetime.date):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            return '\'' + str(value) + '\''
        if isinstance(field, DateField):
            if isinstance(value, datetime.datetime):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            if isinstance(value, datetime.date):
                return "'" + value.strftime('%Y-%m-%d %H:%M:%S') + "'"
            return '\'' + str(value) + '\''
        return str(value)