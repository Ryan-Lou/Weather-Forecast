import csv
import re
from bs4 import BeautifulSoup
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
import sqlite3

f = open('nanjing_weather_last_3_months.csv', mode='a', encoding='utf-8', newline='')
csv_writer = csv.writer(f)

csv_writer.writerow([
    '最高气温',
    '最低气温',
    '天气',
    '风向',
    '日期',
    '星期',
    '风力',
    '平均气温'
])

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (HTML, like Gecko) Chrome/105.0.0.0 '
                  'Safari/537.36'
}

today = datetime.today()
months_to_scrape = []

for i in range(3):
    month_to_scrape = today - timedelta(days=i * 30)
    year_str = month_to_scrape.strftime("%Y")
    month_str = month_to_scrape.strftime("%m")
    months_to_scrape.append((year_str, month_str))

conn = sqlite3.connect('weather_data.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Weather (
        最高气温 TEXT,
        最低气温 TEXT,
        天气 TEXT,
        风向 TEXT,
        日期 TEXT,
        星期 TEXT,
        风力 TEXT,
        平均气温 TEXT
    )
''')


def get_weekday(date_str):
    date_obj = datetime.strptime(date_str, '%Y年%m月%d日')
    return date_obj.strftime('%A')


for year_str, month_str in tqdm(months_to_scrape, desc="Processing months", unit="month"):
    url = f'http://www.tianqihoubao.com/lishi/nanjing/month/{year_str}{month_str}.html'
    response = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    tr_list = soup.find_all('tr')

    for tr in tqdm(tr_list[1:], desc=f"Processing days in month {month_str}", unit="day"):
        td_list = tr.find_all('td')
        if len(td_list) < 4:
            continue

        try:
            date = td_list[0].text.strip()
            weather = td_list[1].text.strip()
            temperature = td_list[2].text.strip()
            temp_high, temp_low = re.findall(r'(-?\d+)℃', temperature)
            avg_temp = (int(temp_high) + int(temp_low)) // 2

            wind_info = td_list[3].text.strip().split('/')
            first_wind = wind_info[0].split(' ')[0]
            wind_powers = []

            for wind in wind_info:
                wind_match = re.findall(r'(\d+)-(\d+)级', wind)
                if wind_match:
                    wind_powers.append(max(int(wind_match[0][0]), int(wind_match[0][1])))

            max_wind_power = max(wind_powers) if wind_powers else ''
            weekday = get_weekday(date)

            csv_writer.writerow([temp_high, temp_low, weather, first_wind, date, weekday, max_wind_power, avg_temp])

            cursor.execute('''
                INSERT INTO Weather (最高气温, 最低气温, 天气, 风向, 日期, 星期, 风力, 平均气温)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (temp_high, temp_low, weather, first_wind, date, weekday, max_wind_power, avg_temp))

        except Exception as e:
            print(f"Error parsing data for {year_str}-{month_str}: {e}")
            continue

conn.commit()
conn.close()
f.close()
