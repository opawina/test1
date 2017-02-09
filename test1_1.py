import psycopg2
import pandas as pd
import numpy as np

# подключаюсь к БД
conn = psycopg2.connect('''host = 'localhost'
                           dbname = 'kolxoz'
                           user = 'postgres'
                           password = 'deloper' ''')
# осоновной запрос к БД
df_dirty = pd.read_sql('''SELECT calendar.year,
                           dim_terr.territory_name,
                           dim_terr.id,
                           fact.value_additive
                    FROM public.calendar, public.dim_terr, public.fact
                    WHERE calendar.date = fact.id_date AND
                           dim_terr.id = fact.id_terr AND
                           fact.id_pokazatel = 106 AND
                           (calendar.year between 2005 and 2015) ''', conn)

# пишлось делать ещё один запрос к БД т.к. названия регионов без показаелей по продукту ->
# не выгрузились в основном запросе к БД
df_region = pd.read_sql('''SELECT dim_terr.territory_name, dim_terr.id FROM public.dim_terr;''', conn)
df_region.set_index('id', inplace=True)

# создаю ДФ
df_res = pd.DataFrame(columns=('Территория', '2005-2010', '2011-2015'), index=np.arange(1, 18))
# вписываю в ДФ регионы и определяю тип ячеек для арифмеических операций
num_reg = np.arange(1, 18)
for i in num_reg:
    q = df_region['territory_name'][i]
    df_res['Территория'][i] = q
    df_res['2005-2010'][i] = 0
    df_res['2011-2015'][i] = 0

# закидываю данные о количестве продукта в ДФ, с сортировкой по периуду времени
num_time = np.arange(2005, 2011)
for row in df_dirty.index:
    q = df_dirty.loc[row]
    if q['year'] in num_time:
        df_res['2005-2010'][q['id']] += q['value_additive']
    else:
        df_res['2011-2015'][q['id']] += q['value_additive']

# заменяю нули на сообщение
df_res = df_res.replace(to_replace=[0], value=['Нет данных'])
# print(df_res)

# выгружаю ДФ в csv
df_res.to_csv('ethanol_stat.csv', index=False)