# HSE DE ETL. Задание 2

Исходные данные сохранил в таблицу
```sql
    create table if not exists temperature_readings (
        id serial primary key,
        noted_date date not null,
        temp numeric not null,
        out_in varchar(10) not null,
        device_id varchar(255)
    );
```
Отфильтрованные и агрегированные в в другую таблицу 
```sql
    create table if not exists temperature_metrics (
        id serial primary key,
        metric_type varchar(50) not null,
        noted_date date not null,
        avg_temp numeric not null,
        min_temp numeric,
        max_temp numeric,
        created_at timestamp default current_timestamp
    );
```
Выпонение дага и вывод результатов
![](./temperature_data_select.png)

Сам даг
[даг](./airflow/dags/temperature_etl_dag.py)
