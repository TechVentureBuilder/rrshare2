import csv


def rq_util_save_csv(data, name, column=None, location=None):
    # 重写了一下保存的模式
    # 增加了对于可迭代对象的判断 2017/8/10
    """
    explanation:
        将数据保存为csv
    
    params:
        * data->
            含义: 需要被保存的数据
            类型: List
            参数支持: []
        * name:
            含义: 要保存的名字
            类型: str
            参数支持: []
        * column:
            含义: 行的名称(可选)
            类型: str
            参数支持: [None]
        * location:
            含义: 保存位置(可选)
            类型: str
            参数支持: []
    """
    assert isinstance(data, list)
    if location is None:
        path = './' + str(name) + '.csv'
    else:
        path = location + str(name) + '.csv'
    with open(path, 'w', newline='') as f:
        csvwriter = csv.writer(f)
        if column is None:
            pass
        else:
            csvwriter.writerow(column)

        for item in data:

            if isinstance(item, list):
                csvwriter.writerow(item)
            else:
                csvwriter.writerow([item])


if __name__ == '__main__':
    rq_util_save_csv(['a', 'v', 2, 3], 'test')
    rq_util_save_csv([['a', 'v', 2, 3]], 'test2')
