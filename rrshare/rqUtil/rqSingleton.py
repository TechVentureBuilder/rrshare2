from functools import wraps

class ConSqlDb(object):
    db = []  # 设置连接池
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:#判断单例是否存在,不存在就创建否则直接返回
            cls._instance = object.__new__(cls)
        return cls._instance

class Singleton(object):
    _instance = None
    _is_init = False
    def __new__(cls, *args,**kwargs):
        if not cls._instance:
            cls._instance = super(Singleton,cls).__new__(cls,*args,**kwargs)
        return cls._instance
    """
    def __init__(self,name):
        if not self._is_init:
            self.name = name
            self._is_init = True

    def __str__(self):
        return self.name
    """


def Singleton_wraps(cls):
    _instance = {}
    @wraps(cls)
    def wrapper(*args, **kwargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kwargs)
        return _instance[cls]
    return wrapper

if __name__ == '__main__':
    @Singleton_wraps
    class Setting(object):
        def __init__(self):
            pass
    print(id(Setting()),id(Setting()))








