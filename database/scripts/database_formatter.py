import json
from datetime import datetime, timedelta
from pathlib import Path


CATEGORIES = {
    "通用数据": [],
    "股票数据": ["股票行情", "分钟行情", "股票信息"],
    "财务数据": ["原始数据", "衍生数据", "财务分析", "一致预期"],
    "指数数据": ["指数行情", "指数信息"],
    "行业板块": ["行业行情", "行业信息"],
    "量化因子": ["股票因子", "研报因子"],
    "另类数据": ["新闻舆情"],
    "风险因子": ["BQ"],
}

DATA_FREQ_OVERRIDES = {
    "分钟频": [
        "cn_stock_bar1m", "cn_stock_bar1m_c", "cn_stock_bar1m_derived", "cn_stock_bar1m_derived_c",
        "cn_stock_bar5m", "cn_stock_bar5m_c", "cn_stock_bar5m_derived",
        "cn_stock_bar15m", "cn_stock_bar15m_c", "cn_stock_bar15m_derived",
        "cn_stock_bar30m", "cn_stock_bar30m_c", "cn_stock_bar30m_derived",
        "cn_stock_bar60m", "cn_stock_bar60m_c", "cn_stock_bar60m_derived",
        "cn_stock_moneyflow_ns",
        "cn_stock_index_bar1m",
    ]
}

def load_json(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def extract_all_tables(data: dict, categories: dict[str, list[str]] | None) -> list[dict]:
    tables = []
    category_order = list(categories.keys()) if categories else list(data.keys())
    
    for category_name, category_data in data.items():
        if categories is not None and category_name not in categories:
            continue
        if isinstance(category_data, dict):
            allowed_subs = categories.get(category_name, []) if categories else []
            for sub_category_name, items in category_data.items():
                if categories is not None and allowed_subs and sub_category_name not in allowed_subs:
                    continue
                if isinstance(items, list):
                    for item in items:
                        if sub_category_name == '_root':
                            category_label = category_name
                        else:
                            category_label = f'{category_name}-{sub_category_name}'
                        item['_category'] = category_label
                        item['_category_order'] = category_order.index(category_name)
                        tables.append(item)
    return tables


def get_last_friday(date: datetime.date) -> datetime.date:
    days_since_friday = (date.weekday() - 4) % 7
    if days_since_friday == 0:
        days_since_friday = 7
    return date - timedelta(days=days_since_friday)


def calc_update_freq(update_time: str, latest_date: datetime.date) -> str:
    if not update_time:
        return '废弃'
    dt = datetime.strptime(update_time, '%Y-%m-%d %H:%M:%S')
    update_date = dt.date()
    delta_days = (latest_date - update_date).days
    last_friday = get_last_friday(latest_date)
    
    if delta_days == 0:
        return '日频'
    elif update_date == last_friday:
        return '周频'
    elif delta_days <= 7:
        return '日频(延时)'
    else:
        return '废弃'


def format_markdown(tables: list[dict]) -> str:
    sorted_tables = sorted(tables, key=lambda x: (x['_category_order'], x['_category'], x['name']))

    update_times = [t.get('update_time', '') for t in tables if t.get('update_time')]
    latest_dt = max(datetime.strptime(ut, '%Y-%m-%d %H:%M:%S') for ut in update_times)
    latest_date = latest_dt.date()

    lines = [
        '| 类别 | 中文名 | 英文名(dai) | 收费 | 稳定 | 数据频率 | 更新频率 | 更新时间 | 描述 |',
        '| --- | --- | --- | --- | --- | --- | --- | --- | --- |',
    ]

    for t in sorted_tables:
        category = t.get('_category', '')
        cn_name = t.get('cn_name', '')
        name = t.get('name', '')
        tag = t.get('tag', '')
        badge = t.get('badge', '')
        update_time = t.get('update_time', '')
        desc = t.get('desc', '').replace('\n', ' ').replace('|', '\\|')
        
        fee = '免费' if '免费' in tag else '旗舰版'
        stable = '稳定' if badge == '' else badge
        data_freq = '日频'
        for freq, names in DATA_FREQ_OVERRIDES.items():
            if name in names:
                data_freq = freq
                break
        update_freq = calc_update_freq(update_time, latest_date)
        
        lines.append(f'| {category} | {cn_name} | {name} | {fee} | {stable} | {data_freq} | {update_freq} | {update_time} | {desc} |')

    return '\n'.join(lines)


def main():
    script_dir = Path(__file__).parent
    json_path = script_dir / 'database_crawler.json'
    output_path = script_dir / 'database_tables.md'

    data = load_json(json_path)
    tables = extract_all_tables(data, CATEGORIES)
    md_content = format_markdown(tables)

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(md_content)

    print(f'写入 {len(tables)} 条记录到 {output_path}')


if __name__ == '__main__':
    main()
