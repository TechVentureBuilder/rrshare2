"""
输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码(二选一)
ann_date	str	N	公告日期 (二选一)
start_date	str	N	公告开始日期
end_date	str	N	公告结束日期
period	str	N	报告期(每个季度最后一天的日期，比如20171231表示年报)
type	str	N	预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
输出参数

名称	类型	描述
ts_code	str	TS股票代码
ann_date	str	公告日期
end_date	str	报告期
type	str	业绩预告类型(预增/预减/扭亏/首亏/续亏/续盈/略增/略减)
p_change_min	float	预告净利润变动幅度下限（%）
p_change_max	float	预告净利润变动幅度上限（%）
net_profit_min	float	预告净利润下限（万元）
net_profit_max	float	预告净利润上限（万元）
last_parent_net	float	上年同期归属母公司净利润
first_ann_date	str	首次公告日
summary	str	业绩预告摘要
change_reason	str	业绩变动原因

"""

from rrshare.rqFetch import pro

df = pro.forecast(ann_date='20190131', fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min')

df2 = pro.forecast_vip(period='20210630',fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min')

df2 = df2.sort_values(by='p_change_max', ascending=False)
df2 = df2[df2['net_profit_min'] > 5000]
print(df2)