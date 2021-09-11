"""输入参数
名称	类型	必选	描述
ts_code	str	Y	股票代码
period	str	N	报告期(每个季度最后一天的日期,比如20171231表示年报)
type	str	N	类型：P按产品 D按地区（请输入大写字母P或者D）
start_date	str	N	报告期开始日期
end_date	str	N	报告期结束日期

输出参数
名称	类型	描述
ts_code	str	TS代码
end_date	str	报告期
bz_item	str	主营业务来源
bz_sales	float	主营业务收入(元)
bz_profit	float	主营业务利润(元)
bz_cost	float	主营业务成本(元)
curr_type	str	货币代码
update_flag	str	是否更新

"""
from rrshare.rqFetch import pro

df = pro.fina_mainbz_vip(ts_code='000627.SZ', type='P')
print(df)

all = pro.fina_mainbz_vip(ts_code='002236.SZ',period='20201231', type= 'D')
print(all)