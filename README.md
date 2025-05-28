
#### Курс OTUS Data Engineer
## Курсовой проект
# Сбор и анализ данных по газу с EPIAS Transparency Platform 

### Цель проекта
Реализация регулярного ELT-процесса сбора данных с сайта **EPIAS Transparency Platform** (https://seffaflik.epias.com.tr/home
) для последующего анализа и визуализации.

### Описание реализации
Сбор данных осуществляется по обращению к API платформы из python.
Полученные исходные данные сохраняются в **Postgres**. 
Затем на основе этих данных строится витрина в **Clickhouse**.
И на основе нее производится визуализация в **Metabase/Superset**.
Управляет запуском задач **Airflow**

### Схема взаимодействия сервисов и компонентов 
![Схема взаимодействия сервисов](docs/etl_schema.png)


### Описание исходных данных
1. Данные по ценам на газ (раздел Daily Reference Price https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/daily-reference-price)
2. Данные по объему торгов (раздел GRP Trade Volume https://seffaflik.epias.com.tr/natural-gas-service/v1/markets/sgp/data/total-trade-volume)

### Описание выходных форматов 
#### Витрина в clickhouse
Таблица с объединенными данными по ценам и объемам торгов   

#### Дашборд с небольшой аналитикой:
1. Текущая цена и отклонение с предыдущим днем
2. Ежемесячный объем торгов
3. Ежедневное сравнение цен и объема торгов 

