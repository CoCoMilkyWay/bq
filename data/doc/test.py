import tushare as ts
from pathlib import Path

ts.set_token('042b931e57b6baa4f9a1e1fc2fcd20ce6e3976c0b2af0fb4ae1e1ea6')

pro = ts.pro_api()

ann_date = '20260409'
result = pro.forecast(ts_code="300233.SZ", fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min,net_profit_max,last_parent_net,first_ann_date,summary,change_reason')

output_dir = Path('data') / ann_date
output_dir.mkdir(parents=True, exist_ok=True)
result.to_json(output_dir / 'forecast.json', orient='records', force_ascii=False, indent=2)
print(f'已保存到 {output_dir / "forecast.json"}')