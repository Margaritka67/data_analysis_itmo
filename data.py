import pandas as pd

# -------------------------------------------------
# Читаем файл
# -------------------------------------------------
# Путь к файлу
parquet_path = "transaction_fraud_data.parquet"

# read_parquet автоматически определит типы колонок, в том числе struct‑поле
df = pd.read_parquet(parquet_path)

# -------------------------------------------------
# Быстро посмотрим на структуру данных
# -------------------------------------------------
print("Первые 5 строк:")
print(df.head())
print("\nИнфа о колонках:")
print(df.dtypes)

# -------------------------------------------------
# Подсчитаем долю мошеннических транзакций
# -------------------------------------------------
# Столбец is_fraud – Boolean, где True = мошенничество
total_transactions   = len(df)
fraud_transactions   = df["is_fraud"].sum()           # sum() считает True как 1
fraud_ratio          = fraud_transactions / total_transactions

print("\n=== Статистика мошенничества ===")
print(f"Всего транзакций            : {total_transactions:,}")
print(f"Мошеннических транзакций    : {fraud_transactions:,}")
print(f"Доля мошенничества          : {fraud_ratio:.4%}")   # формат в процентах


#___________________________________________________________________________________________________

fraud_df = df[df["is_fraud"]]

# -------------------------------------------------
# Считаем их количество по стране
# -------------------------------------------------
country_counts = (
    fraud_df["country"]
    .value_counts()          # уже сортирует по убыванию
    .rename_axis("country")  # название индекса для красоты
    .reset_index(name="fraud_transactions")
)

# -------------------------------------------------
# Топ‑5 стран
# -------------------------------------------------
top5 = country_counts.head(5)

print("\nТоп‑5 стран с наибольшим числом мошеннических транзакций:")
print(top5.to_string(index=False))

#_________________________________________________________________________________________________________-


# -------------------------------------------------
# Приводим timestamp к datetime (если уже не datetime – безопасно)
# -------------------------------------------------
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

# -------------------------------------------------
# Округляем до начала часа (floor)
# -------------------------------------------------
df["hour_ts"] = df["timestamp"].dt.floor('h')   # например 2023‑03‑12 14:00:00+00:00

# -------------------------------------------------
# Считаем количество транзакций в каждой паре (customer, hour)
# -------------------------------------------------
customer_hour_counts = (
    df
    .groupby(["customer_id", "hour_ts"])
    .size()
    .reset_index(name="tx_per_hour")
)

# -------------------------------------------------
# Считаем среднее
# -------------------------------------------------
total_transactions   = customer_hour_counts["tx_per_hour"].sum()
unique_customer_hours = len(customer_hour_counts)

avg_tx_per_customer_hour = total_transactions / unique_customer_hours

print("\n=== Результат ===")
print(f"Всего транзакций                     : {total_transactions:,}")
print(f"Уникальных (customer, hour) пар       : {unique_customer_hours:,}")
print(f"Среднее число транзакций за час: {avg_tx_per_customer_hour:.3f}")



#____________________________________________________________________________________

# -------------------------------------------------
# 2. Проверяем, что столбец действительно Boolean
# -------------------------------------------------
if df["is_high_risk_vendor"].dtype != bool:
    # Иногда bool хранится как int (0/1) – приводим к bool явно
    df["is_high_risk_vendor"] = df["is_high_risk_vendor"].astype(bool)

# -------------------------------------------------
# 3. Фильтруем только транзакции у «high‑risk» продавцов
# -------------------------------------------------
high_risk_df = df[df["is_high_risk_vendor"]]

# -------------------------------------------------
# 4. Считаем количество всех и только мошеннических транзакций
# -------------------------------------------------
total_high_risk      = len(high_risk_df)                 # всех транзакций у high‑risk
fraud_high_risk      = high_risk_df["is_fraud"].sum()    # True считается как 1
fraud_ratio_high_risk = fraud_high_risk / total_high_risk if total_high_risk else 0.0

# -------------------------------------------------
# 5. Выводим результат
# -------------------------------------------------
print("\n=== Мошенничество у продавцов с высоким риском ===")
print(f"Всего транзакций (high‑risk)            : {total_high_risk:,}")
print(f"Мошеннических транзакций (high‑risk)    : {fraud_high_risk:,}")
print(f"Доля мошенничества                      : {fraud_ratio_high_risk:.4%}")



#____________________________________________________________________________________
# -------------------------------------------------
# 5. Проверяем, что поле amount – числовое
# -------------------------------------------------
if not pd.api.types.is_numeric_dtype(df["amount"]):
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

# -------------------------------------------------
# 6. Группируем по city и считаем среднее amount
# -------------------------------------------------
city_avg = (
    df
    .groupby("city", dropna=False)          # сохраняем также NaN‑города, если такие есть
    ["amount"]
    .mean()
    .reset_index(name="avg_amount")
)

# -------------------------------------------------
# 7. Находим город с максимальным средним
# -------------------------------------------------
max_row = city_avg.loc[city_avg["avg_amount"].idxmax()]

city_with_max_avg = max_row["city"]
max_average_amount = max_row["avg_amount"]

# -------------------------------------------------
# 8. Выводим результат
# -------------------------------------------------
print("\n=== Город с наибольшей средней суммой транзакций ===")
print(f"Город               : {city_with_max_avg!r}")
print(f"Средняя сумма (USD) : {max_average_amount:,.2f}")

# Если хотите увидеть топ‑5 городов, добавьте:
print("\n=== Топ‑5 городов по средней сумме: === ")
print(city_avg.sort_values("avg_amount", ascending=False).head(5)
      .rename(columns={"avg_amount":"avg_amount_USD"})
      .to_string(index=False))




#_______________________________________________________________
# Фильтрация по 'fast_food' (в vendor_type, регистронезависимо)
fast_food_mask = df['vendor_type'].str.contains('fast_food', case=False, na=False)

df_fast_food = df[fast_food_mask]

if df_fast_food.empty:
    print("Транзакции с fast_food не найдены.")
else:
    # Группируем по городу и считаем средний чек
    avg_amount_by_city = df_fast_food.groupby('city')['amount'].mean().reset_index()
    
    # Сортируем по убыванию и берём топ-3
    top_3_cities = avg_amount_by_city.sort_values(by='amount', ascending=False).head(3)

    print("\n=== Топ-3 города по среднему чеку в fast_food: ===")
    for _, row in top_3_cities.iterrows():
        print(f"{row['city']}: {row['amount']:.2f} {df_fast_food['currency'].iloc[0] if not df_fast_food.empty else ''}")


#__________________________________________________________________________________________

# 1. Загрузка транзакций
df_tx = df

# 2. Оставить только немошеннические
df_tx = df_tx[df_tx['is_fraud'] == False].copy()

# 3. Извлечь дату из timestamp
df_tx['date'] = pd.to_datetime(df_tx['timestamp']).dt.date

# 4. Загрузка курсов
df_fx = pd.read_parquet('historical_currency_exchange.parquet')
df_fx['date'] = pd.to_datetime(df_fx['date']).dt.date

# 5. Привести к "длинному" формату: currency и rate
df_fx_long = df_fx.melt(id_vars=['date'], var_name='currency', value_name='exchange_rate')

# 6. Убедиться, что exchange_rate не NaN
df_fx_long = df_fx_long.dropna(subset=['exchange_rate'])

# 7. Объединить с транзакциями по дате и валюте
df_merged = df_tx.merge(df_fx_long, on=['date', 'currency'], how='inner')

# Если какие-то транзакции не попали (нет курса) — можно посмотреть:
if len(df_merged) < len(df_tx):
    missing = df_tx.merge(df_fx_long, on=['date', 'currency'], how='left', indicator=True)
    missing = missing[missing['_merge'] == 'left_only']
    print(f"Не удалось найти курс для {len(missing)} транзакций")
    print("Валюты без данных:", missing['currency'].unique())
    print("Даты без данных:", missing['date'].unique())

# 8. Перевести сумму в USD: amount / exchange_rate
# Например: 100 EUR, exchange_rate = 0.93 → 100 / 0.93 ≈ 107.53 USD
df_merged['amount_usd'] = df_merged['amount'] / df_merged['exchange_rate']

# 9. Посчитать среднее
avg_amount_usd = df_merged['amount_usd'].mean()

print(f"\n=== Средняя сумма немошеннической операции в USD: === \n {avg_amount_usd:.2f} USD")



#__________________________________________________________________________

# 1. Создадим колонку с округлением времени до часа
df['hour'] = df['timestamp'].dt.floor('h')

# 2. Для каждой пары (customer_id, hour) посчитаем число уникальных продавцов
unique_vendors_per_hour = (
    df.groupby(['customer_id', 'hour'])['vendor']
    .nunique()
    .reset_index(name='unique_vendors_count')
)

# 3. Присоединим это значение обратно к каждой транзакции по customer_id и hour
df = df.merge(unique_vendors_per_hour, on=['customer_id', 'hour'], how='left')

# 4. Для каждого клиента вычислим медиану показателя unique_vendors_count
median_unique_vendors = (
    df.groupby('customer_id')['unique_vendors_count']
    .median()
    .reset_index(name='median_unique_vendors')
)

# 5. Найдём 95-й перцентиль медианных значений
quantile_95 = median_unique_vendors['median_unique_vendors'].quantile(0.95)

# 6. Посчитаем, сколько клиентов имеют медиану строго выше 95-го перцентиля
count_clients = (median_unique_vendors['median_unique_vendors'] > quantile_95).sum()

print("\n=== Клиентов имеют медиану строго выше 95-го перцентиля: ===")
print(count_clients)
