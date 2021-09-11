import json
import os , sys, platform,shutil, cmd

from rrshare.rqUtil.rqSingleton import Singleton, Singleton_wraps

# diffrence OS path
path = os.path.expanduser('~')
rq_path = '{}{}{}'.format(path, os.sep, '.rrshare')
#print(platform.system(),path, rq_path)


def get_path_file_name(path_name, file_name):
    # diffrence OS path
    if platform.system() == 'Linux':
        path_name_chg = ''.join([rq_path,'/' ,path_name, '/', file_name])
    if platform.system() == 'Windows':
        path_name_chg =  ''.join([rq_path,'\\', path_name, '\\',file_name])
    print(path_name_chg)
    return path_name_chg

path_setting = get_path_file_name('setting','config.json')


@Singleton_wraps
class Setting(object):
    def __init__(self, path_config=path_setting):
        self.path_config = path_config
        
    def setting(self):
        config = open(self.path_config)
        return json.load(config)

setting = Setting().setting()
#print(setting)

if __name__ == '__main__':
    s = Setting()
    #print(id(s), id(Setting()))
    print(s.setting()['TSPRO_TOKEN'])

    


