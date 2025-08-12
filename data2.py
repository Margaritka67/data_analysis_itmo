import pandas as pd


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

# 7. Разделение на мошеннические и честные
fraud = df_merged[df_merged['is_fraud'] == True]
not_fraud = df_merged[df_merged['is_fraud'] == False]

# 8. Вычисление метрик
results = {
    "non_fraud": {
        "mean_usd": not_fraud['amount_usd'].mean(),
        "std_usd": not_fraud['amount_usd'].std()
    },
    "fraud": {
        "mean_usd": fraud['amount_usd'].mean() if len(fraud) > 0 else 0,
        "std_usd": fraud['amount_usd'].std() if len(fraud) > 0 else 0
    }
}

# 9. Вывод результатов
print("📊 Статистика по операциям в USD:")
print("-" * 50)

print(f"Немошеннические операции:")
print(f"  • Среднее:      {results['non_fraud']['mean_usd']:8.2f} USD")
print(f"  • Стандартное отклонение: {results['non_fraud']['std_usd']:8.2f} USD")

print(f"Мошеннические операции:")
print(f"  • Среднее:      {results['fraud']['mean_usd']:8.2f} USD")
print(f"  • Стандартное отклонение: {results['fraud']['std_usd']:8.2f} USD")
