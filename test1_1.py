import psycopg2
import pandas as pd
import numpy as np

# подключаюсь к БД
conn = psycopg2.connect('''host = 'localhost'
                           dbname = 'kolxoz'
                           user = 'postgres'
                           password = 'deloper' ''')
# запрос к БД
df_dirty = pd.read_sql('''SELECT  dt.id, dt.territory_name, f.value_additive, c.year
                          FROM calendar c
                          JOIN fact f ON c.date = f.id_date
                          RIGHT OUTER JOIN dim_terr dt ON dt.id = f.id_terr
                          AND f.id_pokazatel = 106
                          AND c.year > 2004 ''', conn)

conn.close()

# создаю ДФ
df_res = pd.DataFrame(data=[(0, 0, 0)], columns=('Территория', '2005-2010', '2011-2015'), index=np.arange(1, 18))

# закидываю названия регионов и количество продукта в ДФ, с сортировкой по периуду времени
num_time = np.arange(2005, 2011)
for row in df_dirty.index:
    cur_row = df_dirty.loc[row]
    # закидываю названия регионов
    name_reg = df_dirty['territory_name'][row]
    df_res.loc[cur_row['id'], 'Территория'] = name_reg
    # закидываю данные
    if cur_row['year'] in num_time:
        df_res.loc[cur_row['id'], '2005-2010'] += cur_row['value_additive']
    else:
        df_res.loc[cur_row['id'], '2011-2015'] += cur_row['value_additive']

# заменяю нули на сообщение
df_res = df_res.replace([0, None], ['Нет данных', 'Нет данных'])

# выгружаю ДФ в csv
df_res.to_csv('ethanol_stat.csv', float_format='%g', index=False)
