import pandas as pd
import numpy as np
from datetime import datetime

ad = pd.read_csv("E:\\.ML\\[Анатолий Карпов] [Stepic] Data Analytic Часть 2\\4 AirFlow\\Задания\\ads_data_121288.csv", date_parser=['date'])

view_click_count = piv = ad.pivot_table(columns='event', index='date', values='time', aggfunc='count')
view_click_count['ctr'] = round((view_click_count.click / view_click_count.view) * 100, 2)

sum_div_1000 = ad.query('event == "view"').ad_cost.sum() / 1000
view_click_count['spent'] = view_click_count.view * sum_div_1000
change_perc = round((view_click_count / view_click_count.iloc[:1].values) * 100, 2).tail(1)
change_perc = change_perc.apply(lambda x: x - 100)

max_date = pd.to_datetime(ad.date).max()
last_day_stats = view_click_count.tail(1)


report_text = f'''
Отчет по объявлению {ad.ad_id.loc[0]} за {max_date.day} {max_date.month_name()}
Траты: {round(last_day_stats.spent.values[0])} рублей ({round(change_perc.spent.values[0], 2)}%)
Показы: {last_day_stats.view.values[0]} ({round(change_perc.view.values[0], 2)}%)
Клики: {last_day_stats.click.values[0]} ({round(change_perc.click.values[0], 2)}%)
CTR: {last_day_stats.ctr.values[0]} ({round(change_perc.ctr.values[0], 2)}%)
'''

