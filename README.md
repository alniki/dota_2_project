# Dota2 - сбор и подготовка данных об открытых матчах

Репозиторий включает проект по сбору, обработке, хранению, визуализации открытых матчей Dota2.


## Содержание

- [Dota2 - сбор и подготовка данных об открытых матчах](#dota2---сбор-и-подготовка-данных-об-открытых-матчах)
  - [Содержание](#содержание)
  - [Обзор проекта](#обзор-проекта)
  - [Этап 1: Сбор и первичная обработка данных](#этап-1-сбор-и-первичная-обработка-данных)
    - [Этап 1.1. Сбор данных](#этап-11-сбор-данных)
    - [Этап 1.2. Знакомство с данными и первичная обработка](#этап-12-знакомство-с-данными-и-первичная-обработка)
  - [Этап 2: Создание базы данных](#этап-2-создание-базы-данных)
  - [Этап 3. Поиск закономерностей и EDA](#этап-3-поиск-закономерностей-и-eda)
  - [Этап 3: Создание материализованных представлений (MV)](#этап-3-создание-материализованных-представлений-mv)
  - [Этап 4: Создание дашборда](#этап-4-создание-дашборда)
  - [Этап 4: Подготовка ETL-пайплайна](#этап-4-подготовка-etl-пайплайна)
  - [Использование](#использование)
  - [Зависимости](#зависимости)


## Обзор проекта

Проект включает этапы:
* Получение данных об открытых матчах с OpenDota API, загрузка ~ 20 тыс матчей (python, requests)
* Знакомство и первичная обработка данных в Jupyter Notebook (python, pandas)
* Создание базы данных PostgreSQL, база развернута на сервере supabase.com
* Подключение к базе данных с использованием SQLAlchemy, поиск закономерностей в данных
* Создание материализованных представлений для ускорения работы BI-системы (SQLAlchemy, SQL)
* Построение аналитической панели для визуализации найденных закономерностей в Yandex DataLens 
* Создание ETL пайплана (скачивание данных, формирование датафреймов, преобразование данных, загрузка в базу данных, обновление материализованных представлений)

**Стек**: 
* Python: requests, pandas, sqlalchemy, matplotlib, seaborn, numpy
* SQL
* BI система: Yandex DataLens

## Этап 1: Сбор и первичная обработка данных

### Этап 1.1. Сбор данных


Jupyter Notebook файлы:  
Парсинг данных с OpenDota API [parsing_api_dota_initial.ipynb](parsing_api_dota_initial.ipynb)  
Парсинг данных с GitHub [parce_items_git.ipynb](parce_items_git.ipynb)  


* Подключение к OpenDota API:
  * Ограничение: не более 2000 запросов в сутки, не более 60 запросов в минуту
  * Загрузка перечня id открытых матчей (`match_id`):
    * Эндпоинт OpenDota: `GET /publicMatches`:
    * Получение списка открытых матчей (`match_id`) с пагинацией
    * Сохранение прогресса в `match_ids.json`
  * Сбор информации о матчах:
    * Эндпоинт OpenDota: `GET /matches/{match_id}`
    * Скачивается полная карточка каждого матча
    * Загрузка в `matches_raw.jsonl`
  * Загрузка информации о героях:
    * Эндпоинт OpenDota: `GET /heroStats`
    * Получение справочника всех героев с характеристиками (атрибуты, роли, базовые параметры и т.д.)
  * Создание CSV-файлов:
    * df_matches.csv — информация о матчах
    * df_players.csv — информация об игроках
    * df_heroes.csv - информация о героях
* Сбор информации о предметах из GitHub
  * Источник: https://raw.githubusercontent.com/odota/dotaconstants/master/build/items.json
  * Получение справочника всех предметов с характеристиками
  * items.csv - информация о предметах

Итого: Скачано ~ 20 тыс матчей

### Этап 1.2. Знакомство с данными и первичная обработка


Jupyter Notebook файл: [csv_preparation_initial.ipynb](csv_preparation_initial.ipynb)  


* Подготовка данных к загрузке в базу данных:
  * Импорт в Jupyter Notebook ранее сохранённых CSV: matches.csv, players.csv, heroes.csv, items.csv
  * Нормализация строковых данных, преобразование типов (Int, bool)
  * Разделение `players.csv` на три таблицы:
    * `players_matches` – основная информация об игроках в матчах
    * `player_stat` – статистика игроков
    * `player_items` – предметы игроков, преобразованные из широкого формата в плоский
  * Проверка совпадения внешних ключей между таблицами (`match_id`, `player_slot`, `hero_id`, `item_id`)
  * Удаление дубликатов и лишних строк
  * Сохранение подготовленных датафреймов в CSV: matches.csv, players.csv, player_stat.csv, player_items.csv, heroes.csv, items.csv
  
## Этап 2: Создание базы данных

База данных PostgreSQL развернута на сервисе Supabase.com 

- Создание таблиц PostgreSQL:
  - `matches` (PK: `match_id`)
  - `players` (PK: `match_id, player_slot)`  
  - `players_stat` (PK: `match_id, player_slot`) 
  - `players_items` (PK: `match_id, player_slot, item_slot`)
  - `heroes` (PK: `hero_id`)
  - `items`  (PK: `item_id`)
- Настройка внешних ключей для целостности данных:
  - `PLAYERS.match_id → MATCHES.id`  
  - `PLAYERS.hero_id → HEROES.id`  
  - `PLAYERS_STAT.match_id, PLAYERS_STAT.player_slot → PLAYERS.match_id, PLAYERS.player_slot` 
  - `PLAYERS_ITEMS.match_id, PLAYERS_ITEMS.player_slot → PLAYERS.match_id, PLAYERS.player_slot`
  - `PLAYERS_ITEMS.item_id → ITEMS.id`  

Выполнена загрузка CSV файлов, подготовленных на ранних этапах в базу данных

Диаграмма Базы Данных:  

```mermaid
flowchart TD
  subgraph DB ["Postgres База данных"]
    MATCHES["matches<br/>PK: match_id"]:::dbtable
    PLAYERS["players<br/>PK: match_id, player_slot"]:::dbtable
    PLAYERS_STAT["players_stat<br/>PK: match_id, player_slot"]:::dbtable
    PLAYERS_ITEMS["players_items<br/>PK: match_id, player_slot, item_slot"]:::dbtable
    HEROES["heroes<br/>PK: hero_id"]:::dbtable
    ITEMS["items<br/>PK: item_id"]:::dbtable
  end

  PLAYERS -->|match_id FK → MATCHES.match_id| MATCHES
  PLAYERS -->|hero_id FK → HEROES.hero_id| HEROES
  PLAYERS_STAT -->|match_id, player_slot FK → PLAYERS.match_id, player_slot| PLAYERS
  PLAYERS_ITEMS -->|match_id, player_slot FK → PLAYERS.match_id, player_slot| PLAYERS
  PLAYERS_ITEMS -->|item_id FK → ITEMS.item_id| ITEMS

  classDef dbtable fill:#dcedc8,stroke:#558b2f,stroke-width:1px,color:#000;
    ...
```
## Этап 3. Поиск закономерностей и EDA

* Подключение к базе данных
  * Использовалась SQLAlchemy (`create_engine`) для подключения к PostgreSQL (Supabase).
  * Настройка параметров через `.env` файл
  * Проверка соединения через `engine.connect()`.
* Выполнено знакомство с данными, проведен исследовательский анализ данных EDA, поиск закономерностей и инсайтов с использованием `Python(pandas, matplotlib, seaborn)`.   
  * *Внимание: Данный код не вошел в настоящий репозиторий*
 
## Этап 3: Создание материализованных представлений (MV)


Jupyter Notebook: [mv_creation.ipynb](mv_creation.ipynb)

* Подключение к базе данных
  * Использовалась SQLAlchemy (`create_engine`) для подключения к PostgreSQL (Supabase).
  * Настройка параметров через `.env` файл. 
    * ссылка на пример .env файла [example.env](example.env)
  * Проверка соединения через `engine.connect()`.
* Создание MV для аналитики и дашбордов с использованием SQLAlchemy и SQL
  * Статистика побед Radiant по длительности матчей `radiant_win_by_duration`
    * Рассчитывает суммарное количество побед Radiant и процент побед в зависимости от временного интервала матча.
  * Статистика по регионам `regions_stat`
    * Считает количество матчей, среднюю и медианную длительность, среднее время первого убийства, количество побед Radiant и процент побед по каждому региону.
  * Факторы, влияющие на победу `win_factors`
    * Рассчитывает средние и медианные показатели по Tower Damage, Hero Damage, GPM, XP, Last Hits, Assists, Kills, Deaths и First Blood в зависимости от победы.
  * Статистика героев `heroes_stat`
    * Анализирует winrate героев в нормальной и поздней фазе игры, популярность (pickup rate) и флаги лучших/худших героев.
  * Winrate по золоту на команду (GPM) `gmp_winrate`
    * Рассчитывает winrate команд в зависимости от GPM и длительности матчей, с разбивкой по децилям.
  * Статистика по неизвестным игрокам (анонимам) `unknown_players`
    * Рассчитывает средние и медианные показатели игроков без Steam ID и Personaname: Kills, Deaths, Assists, Denies, GPM, XP, Hero Damage, Tower Damage.

- Создание MV для аналитики и отчетности:
  - `public.radiant_win_by_duration`  
  - `public.win_factors`  
  - `public.gmp_winrate`  
  - `public.regions_stat`  
  - `public.unknown_players`  
  - `public.heroes_stat`  


## Этап 4: Создание дашборда


Ссылка на дашборд: https://datalens.ru/kv8lvzzuoft65


- Цель: создание у новичка общего представления об игре Дота2
- Визуализация данных через Yandex DataLens
- Подключение к PostgreSQL и материализованным представлениям
- Вкладки дашборда:
  - Общая информация: общая информация об игре и собранных данных
  - Факторы, влияющие на победу
  - Статистика по серверам
  - Герои: поиск инсайтов о героях, более детальная информация
  - Анонимы: поиск различий в игре анонимов и публичных игроков

## Этап 4: Подготовка ETL-пайплайна


Jupyter Notebook: [etl_pipeline.ipynb](etl_pipeline.ipynb)

Полный пайплайн скачивания данных по матчам и игрокам Dota2 и загрузки их в базу данных:
* Получение ID матчей и их данных через API
* Дедупликация и нормализация данных
* Создание DataFrame: df_matches, df_players, df_players_stat, df_players_items
* Валидация данных
* Вставка данных в PostgreSQL (транзакции)
* Обновление материализованных представлений

*Используемые библиотеки*: pandas, requests, SQLAlchemy, dotenv, pathlib, json

Диаграмма детализированного пайплайна:

```mermaid
flowchart TD
  %% Внешний источник
  API["OpenDota API"]:::external

  %% Хранилище сырых данных
  JSON_IDS["match_ids_parced_{today}.json"]:::storage
  JSONL["matches_raw_{today}.jsonl"]:::storage
  DOWNLOADED_IDS["downloaded_ids_{today}.json"]:::storage

  %% Этапы ETL
  subgraph ETL ["Обработка ETL"]
    ENV["Загрузка env & создание DB engine"]:::process
    FETCH_IDS["Получение id открытых матчей /publicMatches (пагинация)"]:::process
    FILTER_IDS["Фильтрация новых match_id (БД & seen(скачанные сегодня))"]:::process
    SAVE_IDS["Сохранение match_ids_parced_{today}.json"]:::process
    SELECT_TO_FETCH["Выбор подмножества матчей для загрузки"]:::process
    DOWNLOAD_MATCHES["Загрузка /matches/{id}"]:::process
    APPEND_JSONL["Добавление матчей → matches_raw_{today}.jsonl"]:::process
    LOAD_JSONL["Загрузка JSONL → raw_matches (дедупликация (БД & seen(скачанные сегодня)))"]:::process
    BUILD_DFS["Создание DataFrames: df_matches, df_players"]:::process
    NORMALIZE["Нормализация строк, конвертация типов, удаление дубликатов"]:::process
    CREATE_TABLES["Создание df_players_matches, df_players_stat, df_players_items"]:::process
    FK_CHECK["Проверка внешних ключей & удаление лишних строк"]:::process
    CONVERT_TYPES["Конвертация типов для вставки в БД"]:::process
    INSERT_DB["Загрузка DataFrame в таблицы PostgreSQL"]:::process
    REFRESH_MV["Обновление материализованных представлений"]:::process
  end

  %% Таблицы БД
  subgraph DB ["Postgres База данных"]
    MATCHES["matches"]:::dbtable
    PLAYERS["players"]:::dbtable
    PLAYERS_STAT["players_stat"]:::dbtable
    PLAYERS_ITEMS["players_items"]:::dbtable
    HEROES["heroes"]:::dbtable
    ITEMS["items"]:::dbtable
  end

  %% Materialized views / Dashboard
  MV["Материализованные представления / Дашборд"]:::dashboard

  %% Flow connections
  API --> FETCH_IDS
  FETCH_IDS --> FILTER_IDS --> SAVE_IDS --> JSON_IDS
  JSON_IDS --> SELECT_TO_FETCH --> DOWNLOAD_MATCHES --> APPEND_JSONL --> JSONL
  DOWNLOADED_IDS -->|дедупликация| SELECT_TO_FETCH
  JSONL --> LOAD_JSONL --> BUILD_DFS --> NORMALIZE --> CREATE_TABLES --> FK_CHECK --> CONVERT_TYPES --> INSERT_DB
  INSERT_DB --> MATCHES
  INSERT_DB --> PLAYERS
  INSERT_DB --> PLAYERS_STAT
  INSERT_DB --> PLAYERS_ITEMS
  MATCHES --> MV
  PLAYERS --> MV
  PLAYERS_STAT --> MV
  PLAYERS_ITEMS --> MV

  %% Foreign key relationships
PLAYERS -->|match_id FK| MATCHES
PLAYERS_STAT -->|match_id FK| PLAYERS
PLAYERS_STAT -->|player_slot FK| PLAYERS
PLAYERS_ITEMS -->|match_id FK| PLAYERS_STAT
PLAYERS_ITEMS -->|player_slot FK| PLAYERS_STAT
PLAYERS_ITEMS -->|item_slot FK| PLAYERS_STAT
PLAYERS -->|hero_id FK| HEROES
PLAYERS_ITEMS -->|item_id FK| ITEMS

  %% Styling
  classDef external fill:#ffebc6,stroke:#cc9c00,stroke-width:1px,color:#000;
  classDef storage fill:#e0f7fa,stroke:#00acc1,stroke-width:1px,color:#000;
  classDef process fill:#f8f9fa,stroke:#333,stroke-width:1px,color:#000;
  classDef dbtable fill:#dcedc8,stroke:#558b2f,stroke-width:1px,color:#000;
  classDef dashboard fill:#f0f4c3,stroke:#9e9d24,stroke-width:1px,color:#000;
  ...
```

## Использование

* Создание .env или supabase.env с данными для подключения к БД.
* Запуск ETL-пайплайна (Этап 5)
* Открытие дашборда для анализа.

