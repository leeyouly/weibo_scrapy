'''
'''
from PyDB.MySQLContext import MySQLContext

class ContextFactory(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
    @staticmethod
    def build_context(url):
        return MySQLContext(url)