'''
'''

class Field(object):
    '''
    classdocs
    '''
    is_key = False
    not_null = False

    def __init__(self, name, **kwargs):
        '''
        Constructor
        '''
        self.name = name
        if 'is_key' in kwargs:
            self.is_key = kwargs['is_key']
        

class DatetimeField(Field):
    '''
    classdocs
    '''
    pass

class IntField(Field):
    '''
    classdocs
    '''
    pass
    
class StringField(Field):
    pass
        
class DateField(Field):
    pass

class DecimalField(Field):
    pass

class BinaryField(Field):
    pass