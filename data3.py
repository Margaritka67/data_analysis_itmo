import seaborn as sns
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # Устанавливаем backend Agg для рендеринга в файл

import matplotlib.pyplot as plt

"""
1. Анализ распределения сумм транзакций

Цель:
	Построить гистограммы для сумм транзакций по двум группам — мошеннические и немошеннические, чтобы визуально оценить распределение и выявить выбросы.
Что делать:
	Разделить данные на две подвыборки по признаку is_fraud.
	Построить гистограммы сумм (amount или amount в USD, если есть конвертация).
	Посмотреть, есть ли экстремальные значения (очень большие суммы), которые могут искажать среднее.
	При необходимости построить гистограммы с логарифмической шкалой по оси суммы, чтобы лучше видеть распределение.
"""

# Загрузка транзакций
df_tx = pd.read_parquet('transaction_fraud_data.parquet')

# 1. Загрузка транзакций
df_tx = pd.read_parquet('transaction_fraud_data.parquet')

# 2. Извлечение даты из timestamp
df_tx['date'] = pd.to_datetime(df_tx['timestamp']).dt.date

# 3. Загрузка курсов
df_fx = pd.read_parquet('historical_currency_exchange.parquet')
df_fx['date'] = pd.to_datetime(df_fx['date']).dt.date

# 4. Перевод курсов в "длинный" формат
df_fx_long = df_fx.melt(id_vars=['date'], var_name='currency', value_name='exchange_rate')
df_fx_long = df_fx_long.dropna(subset=['exchange_rate'])

# 5. Объединение транзакций с курсами
df_merged = df_tx.merge(df_fx_long, on=['date', 'currency'], how='inner')

# Проверка: если есть пропущенные курсы
if len(df_merged) < len(df_tx):
    missing = df_tx.merge(df_fx_long, on=['date', 'currency'], how='left', indicator=True)
    missing = missing[missing['_merge'] == 'left_only']
    print(f"⚠️ Не найдены курсы для {len(missing)} транзакций")
    print("Валюты без курса:", missing['currency'].unique())
    print("Даты без курса:", missing['date'].unique())

# 6. Перевод суммы в USD: amount / exchange_rate
# exchange_rate — это сколько единиц валюты за 1 USD → 1 EUR = 0.93 → 1 EUR = 1 / 0.93 USD
df_merged['amount_usd'] = df_merged['amount'] / df_merged['exchange_rate']

plt.figure(figsize=(14,6))

# Гистограмма для немошеннических операций
sns.histplot(df_merged[df_merged['is_fraud'] == False]['amount_usd'], bins=100, color='green', label='Немошеннические', log_scale=(False, True), alpha=0.6)

# Гистограмма для мошеннических операций
sns.histplot(df_merged[df_merged['is_fraud'] == True]['amount_usd'], bins=100, color='red', label='Мошеннические', log_scale=(False, True), alpha=0.6)

plt.xlabel('Сумма транзакции (USD, логарифмическая шкала)')
plt.ylabel('Количество транзакций')
plt.title('Распределение сумм транзакций по мошенничеству')
plt.legend()
plt.savefig('plot.png')
print("График сохранён в plot.png")
