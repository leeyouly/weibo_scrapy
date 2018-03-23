'''
'''
from PyDB import DbContext
from fields import IntField, StringField, DatetimeField, DateField, DecimalField, BinaryField
from DbContext import Dialect
import logging
import datetime

import os 
os.environ["NLS_LANG"] = ".UTF8"

logger = logging.getLogger('PyDB')


class OracleContext(DbContext):
    '''
    classdocs
    '''
    

    def __init__(self, user, password, host, port=1521, sid=None, service_name=None, **kwargs):
        '''
        Constructor
        '''
        import cx_Oracle
        self._metadata = {}
        self.dialect = OracleDialect()
        if sid:
            dsn = cx_Oracle.makedsn(host, port, sid=sid)
        else:
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
        self._context = cx_Oracle.connect(user = user, password = password, dsn=dsn)
        self._cursor = self._context.cursor()
        
    def execute_sql(self, sql, params=None):
        cursor = self._cursor
        params = params or {}
        cursor.execute(sql, params)
        return cursor
    
    def save(self, tablename, data):
        table_metadata = self._metadata[tablename]
        
        data = data.copy()
        for field in data.keys():
            if field not in table_metadata.keys():
                del data[field]
            
        
        fields = ','.join(data.keys())
        values = ','.join([':%s'%key for key in data.keys()])
        sql = 'insert into ' + tablename + ' (' + fields + ') values (' + values + ')'
        params = dict(data)
        logger.debug(sql)
        logger.debug(params)
        return self.execute_sql(sql, params)
    
    def load_metadata(self, tablename):
        sql = """
        select user_tab_cols.COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, 
            NULLABLE, CONSTRAINT_TYPE from user_tab_cols
        left join( 
            select user_cons_columns.TABLE_NAME, user_cons_columns.COLUMN_NAME, 
                user_constraints.CONSTRAINT_TYPE,user_cons_columns.POSITION 
            from user_cons_columns
            left join user_constraints 
                on user_constraints.CONSTRAINT_TYPE = 'P' 
                and user_constraints.TABLE_NAME = user_cons_columns.TABLE_NAME 
                and user_constraints.CONSTRAINT_NAME = user_cons_columns.CONSTRAINT_NAME
            where user_cons_columns.TABLE_NAME = '%(TableName)s'
            ) key_columns 
            on user_tab_cols.TABLE_NAME = key_columns.TABLE_NAME 
            and user_tab_cols.COLUMN_NAME = key_columns.COLUMN_NAME 
            and key_columns.CONSTRAINT_TYPE = 'P'
        where user_tab_cols.TABLE_NAME = '%(TableName)s'
        """
        sql = sql % {"TableName" : tablename}
        logger.debug(sql)
        
        cursor = self.execute_sql(sql, None)
        fields = []
        for row in cursor:
            field_info = {
                     'Field' : row[0],
                     'Type' : row[1],
                     'Length' : row[2],
                     'Precision' : row[3],
                     'Nullable' : row[4],
                     'Key' : row[5]
                     }
            fields.append(self.load_field_info(field_info))
        return fields

    def load_field_info(self, field_info):
        field_datatype = field_info["Type"]
        is_key = field_info['Key'] == 'P'
        field_name = field_info['Field']
        field_length = 0
        if field_datatype == "NUMBER" and (field_info['Precision'] == 0 or field_info['Precision'] == None):
            return IntField(field_name, is_key=is_key)
        elif field_datatype == 'NUMBER' and field_info['Precision'] > 0:
            return DecimalField(field_name, is_key=is_key )
        elif field_datatype == 'NVARCHAR2':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'VARCHAR2':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'FLOAT':
            return DecimalField(field_name, is_key=is_key)
        elif field_datatype == 'CHAR':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'TIMESTAMP(6)':
            return DatetimeField(field_name, is_key=is_key)
        elif field_datatype == 'CLOB':
            return StringField(field_name, is_key=is_key)
        elif field_datatype == 'DATE':
            return DateField(field_name, is_key=is_key)
        else:
            raise Exception('Unsupportted type ' + field_datatype)
                
    def set_metadata(self, tablename, fields):
        field_dict = {}
        for field in fields:
            field_dict[field.name] = field
        self._metadata[tablename] = field_dict
        return field_dict
        
    def save_or_update(self, tablename, data):
        table_metadata = self._metadata[tablename]
        key_fields = []
        for field in table_metadata.values():
            if field.is_key:
                key_fields.append(field)
               
        key_signed = False  # find whether key field is specified in data 
        for key_field in key_fields:
            if key_field.name in data.keys():
                key_signed = True
                
        if key_signed:
            if self.exists_key(tablename, data):
                return self.update(tablename, data)
        
        return self.save(tablename, data)
    
    def get(self, tablename, keys = None):
        sql = 'select '
        table_metadata = self._metadata[tablename]
        sql_field = ','.join([field for field in table_metadata])
        sql += sql_field + ' from ' + tablename
        key_fields = []
        for field in table_metadata.values():
            if field.is_key:
                key_fields.append(field)
        
        if keys is not None:
            key_values = ((key.name, self.dialect.format_value_string(key, keys[key.name]) ) for key in key_fields)
            key_condition = 'and'.join([' %s = :%s ' % (key.name, key.name) for key in key_fields])
            sql += ' where ' + key_condition
        
        logger.debug(sql)
        ret = []
        for row in self.execute_sql(sql, keys):
            data_row = {}
            for i in range(len(table_metadata)):
                data_row[table_metadata.keys()[i]] = row[i]
            ret.append(data_row)
        if len(ret) == 0:
            return None
        if len(ret) > 1:
            logger.warn('get fetched more than one records, returning the first one')
        return ret[0]
    
    def exists_key(self, tablename, keys):
        sql = 'select count(*)'
        table_metadata = self._metadata[tablename]
        sql += ' from ' + tablename + ''
        key_fields = filter(lambda x: x.is_key, table_metadata.values())
        
        if keys is not None:
            key_condition = 'and'.join([' %s = :%s ' % (key.name, key.name) for key in key_fields])
            sql += ' where ' + key_condition
        
        logger.debug(sql)
        params = dict([(key.name, keys[key.name]) for key in key_fields])
        logger.debug(params)
        cursor = self.execute_sql(sql, params)
        ret = cursor.fetchone()
        if ret[0] > 0:
            return True
        return False
    
    def update(self, tablename, data):
        table_metadata = self._metadata[tablename]
        key_fields = []
        for field in table_metadata.values():
            if field.is_key:
                key_fields.append(field)
                
        data = data.copy()
        for key in data.keys():
            if key not in table_metadata:
                del data[key]
               
        sql = """update """ + tablename + " set "
        
        sql += ','.join(['%s=:%s' % (field_name,field_name) for field_name in data])
        key_condition = ' and '.join('%s=:%s' % (key.name, key.name) for key in key_fields)
        sql += " where " + key_condition
        params = dict(data)
        logger.debug(sql)
        logger.debug(params)
        return self.execute_sql(sql, params)
        
    def commit(self):
        self._context.commit()
    
class OracleDialect(Dialect):
    def format_value_string(self, field, value):
        if isinstance(field, StringField):
            return '\'' + value.replace('\'', '\'\'') + '\'' 
        if isinstance(field, DatetimeField):
            if isinstance(value, datetime.datetime):
                return "TO_DATE('" + value.strftime('%Y-%m-%d %H:%M:%S') + "','yyyy-mm-dd hh24:mi:ss')"
            if isinstance(value, datetime.date):
                return "TO_DATE('" + value.strftime('%Y-%m-%d %H:%M:%S') + "','yyyy-mm-dd hh24:mi:ss')"
            return "TO_DATE(\'" + str(value) + "\', 'yyyy/mm/dd hh24:mi:ss')"
        if isinstance(field, DateField):
            if isinstance(value, datetime.datetime):
                return "TO_DATE('" + value.strftime('%Y-%m-%d') + "','yyyy-mm-dd')"
            if isinstance(value, datetime.date):
                return "TO_DATE('" + value.strftime('%Y-%m-%d') + "','yyyy-mm-dd')"
            return "TO_DATE(\'" + str(value) + "\', 'yyyy/mm/dd hh24:mi:ss')"
        return str(value)
        