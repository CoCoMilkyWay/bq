from playwright.sync_api import sync_playwright
from urllib.parse import quote
from pathlib import Path
import json
import re
import time

SCRIPT_DIR = Path(__file__).parent

BASE_URL = "https://bigquant.com/data/categories/"

CATEGORIES = {
    # "通用数据": [],
    # "股票数据": ["股票行情", "分钟行情", "股票信息"],
    # "财务数据": ["原始数据", "衍生数据", "财务分析", "一致预期"],
    # "指数数据": ["指数行情", "指数信息"],
    # "行业板块": ["行业行情", "行业信息"],
    # "期货数据": ["期货行情", "期货信息"],
    # "期权数据": ["期权行情", "期权信息"],
    # "基金数据": ["基金行情", "基金信息"],
    # "可转债数据": ["可转债行情", "可转债信息"],
    # "债券数据": ["债券行情"],
    # "宏观行业": ["生猪", "综合", "人口统计", "就业统计", "对外经贸", "白糖", "价格指数", "国民经济", "汽车业", "人民生活", "黄金", "工农业"],
    # "量化因子": ["股票因子", "研报因子", "期货因子"],
    # "另类数据": ["新闻舆情", "专利数据", "股吧数据"],
    # "风险因子": ["CERM", "BQ", "CNE"],
    # "海外数据": ["行情数据", "信息数据", "基本信息"],
    # "大模型": [],
    # "宏观数据": ["国民经济", "对外经贸"],

    "量化因子": ["股票因子"],

}


def build_url(cat1, cat2=None):
    if cat2:
        return BASE_URL + "-" + quote(cat1) + "-" + quote(cat2)
    return BASE_URL + "-" + quote(cat1)


SAVED_HTML = True


def crawl_page(page, url, label):
    global SAVED_HTML
    print(f"  爬取: {label}")
    page.goto(url)
    try:
        page.wait_for_selector(".dataSource-list", timeout=10000)
    except:
        print(f"    无数据卡片")
        return []
    time.sleep(1)

    if not SAVED_HTML:
        with open(SCRIPT_DIR / "page.html", "w") as f:
            f.write(page.content())
        SAVED_HTML = True
        print("    [已保存 page.html，请先分析结构]")
        raise SystemExit(0)

    tables = []
    cards = page.query_selector_all(".dataSource-list")
    for card in cards:
        title_el = card.query_selector("h2 span")
        desc_el = card.query_selector(".text-description .value")

        if not title_el:
            continue

        title = title_el.inner_text().strip()
        match = re.match(r"(.+?)\(([^)]+)\)", title)
        if match:
            cn_name, en_name = match.groups()
        else:
            cn_name, en_name = title, title

        desc = desc_el.inner_text().strip() if desc_el else ""

        # 更新时间: .label.mb-10px 下的 .value
        update_el = card.query_selector(".label.mb-10px .value")
        update_time = update_el.inner_text().strip() if update_el else ""

        # 底部 tag (免费/续订等): .flex.justify-end 下的 div
        tag_el = card.query_selector(".flex.justify-end > div")
        tag = tag_el.inner_text().strip() if tag_el else ""

        # 右上角 badge (Alpha/Beta): .alpha.tag 或 .beta.tag
        badge_el = card.query_selector(".alpha.tag, .beta.tag")
        badge = badge_el.inner_text().strip() if badge_el else ""

        tables.append({
            "name": en_name,
            "cn_name": cn_name,
            "desc": desc,
            "update_time": update_time,
            "tag": tag,
            "badge": badge,
        })

    print(f"    找到 {len(tables)} 个表")
    return tables


output_file = SCRIPT_DIR / "database_crawler.json"


def save(data):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True, executable_path="/usr/bin/chromium-browser")
    page = browser.new_page()

    all_data = {}
    for cat1, sub_cats in CATEGORIES.items():
        print(f"\n=== {cat1} ===")
        all_data[cat1] = {}

        if not sub_cats:
            url = build_url(cat1)
            all_data[cat1]["_root"] = crawl_page(page, url, cat1)
        else:
            for cat2 in sub_cats:
                url = build_url(cat1, cat2)
                all_data[cat1][cat2] = crawl_page(page, url, cat2)

        save(all_data)

    browser.close()

print(f"\n\n保存到: {output_file}")
