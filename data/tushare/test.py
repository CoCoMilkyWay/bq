import tushare as ts
from pathlib import Path

ts.set_token('042b931e57b6baa4f9a1e1fc2fcd20ce6e3976c0b2af0fb4ae1e1ea6')

pro = ts.pro_api()

ann_date = '20260409'
ts_code = "300233.SZ"
# result = pro.forecast(ann_date=ann_date, fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason')
result = pro.income_vip(start_date='20190324', end_date='20190327')
# result = pro.income(ts_code='600000.SH', start_date='20190324', end_date='20190327')
print(result)

output_dir = Path('data') / ann_date
output_dir.mkdir(parents=True, exist_ok=True)
result.to_json(output_dir / 'a.json', orient='records', force_ascii=False, indent=2)
