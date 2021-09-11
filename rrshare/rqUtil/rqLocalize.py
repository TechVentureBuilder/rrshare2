# coding:utf-8
import os
"""创建本地文件夹
1. setting_path ==> 用于存放配置文件 setting.cfg
2. cache_path ==> 用于存放临时文件
3. log_path ==> 用于存放储存的log
"""

path = os.path.expanduser('~')
rq_path_name = 'rrsdk'
rq_path = f'{path}/.{rq_path_name}'
#print(rq_path)


def generate_path(name):
    return '{}{}{}'.format(rq_path, os.sep, name)


def make_dir(path, exist_ok=True):
    os.makedirs(path, exist_ok=exist_ok)
    print(f"makedir path: {path}")


setting_path = generate_path('setting')
cache_path = generate_path('cache')
log_path = generate_path('log')


def make_dir_path():
    make_dir(rq_path, exist_ok=True)
    make_dir(setting_path, exist_ok=True)
    make_dir(cache_path, exist_ok=True)
    make_dir(log_path, exist_ok=True)
  
   
if __name__ == '__main__':
    make_dir_path()
