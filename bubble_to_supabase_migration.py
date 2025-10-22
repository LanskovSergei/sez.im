#!/usr/bin/env python3
"""
Скрипт для миграции данных из Bubble в Supabase
Таблица: Rating Merch
"""

import requests
import psycopg2
from psycopg2.extras import execute_batch
import sys
from datetime import datetime
import json

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Настройки Bubble API
BUBBLE_CONFIG = {
    'app_name': 'sez.im',
    'api_token': '56721b98053d142416bfa051ab4feeba',
    'table_name': 'rating_merch',
    'api_version': '1.1'
}

# Настройки Supabase PostgreSQL
SUPABASE_CONFIG = {
    'host': 'localhost',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'chA1IH5S3v2UWUn3j1YCE8cCXVDMtgCu',
    'port': '5432'
}

# Маппинг полей: Bubble -> Supabase
FIELD_MAPPING = {
    'average_revenue_per_session': None,
    'average_revenue_per_session_percentage': None,
    'rating_number': None,
    'retention': None,
    'sessions_count': None,
    'sessions_first_count': None,
    'sessions_per_user': None,
    'sessions_second_count': None,
    'target_average_revenue_per_session': None,
    'total_revenue': None,
    'users_count': None,
    'merch': 'merch',
    'period': 'period',
}

# ============================================================================
# ФУНКЦИИ МИГРАЦИИ
# ============================================================================

def log(message, level="INFO"):
    """Логирование с временной меткой"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def fetch_bubble_data(table_name, api_token, app_name, api_version="1.1"):
    """
    Получение всех данных из таблицы Bubble через API
    
    Returns:
        list: Список записей из Bubble
    """
    url = f"https://{app_name}/version-test/api/{api_version}/obj/{table_name}"
    headers = {
        "Authorization": f"Bearer {api_token}"
    }
    
    all_records = []
    cursor = 0
    limit = 100
    
    log(f"Начинаю загрузку данных из Bubble таблицы '{table_name}'...")
    log(f"URL: {url}")
    
    while True:
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"cursor": cursor, "limit": limit},
                timeout=30
            )
            
            if response.status_code != 200:
                log(f"Ошибка API Bubble: {response.status_code}", "ERROR")
                log(f"Ответ: {response.text}", "ERROR")
                break
            
            data = response.json()
            results = data.get("response", {}).get("results", [])
            
            if not results:
                log(f"Больше нет данных. Всего загружено: {len(all_records)} записей")
                break
            
            all_records.extend(results)
            log(f"Загружено {len(all_records)} записей...")
            
            cursor += limit
            
            if len(results) < limit:
                break
                
        except requests.exceptions.RequestException as e:
            log(f"Ошибка при запросе к Bubble API: {e}", "ERROR")
            break
    
    return all_records


def transform_record(bubble_record, field_mapping):
    """
    Трансформация записи из формата Bubble в формат Supabase
    
    Args:
        bubble_record: Запись из Bubble
        field_mapping: Словарь маппинга полей
    
    Returns:
        dict: Трансформированная запись для Supabase
    """
    supabase_record = {}
    
    for bubble_field, supabase_field in field_mapping.items():
        target_field = supabase_field if supabase_field else bubble_field
        value = bubble_record.get(bubble_field)
        
        if value is not None:
            if isinstance(value, dict) and '_id' in value:
                value = value.get('_id')
                log(f"Найдена связь в поле '{bubble_field}': {value}", "WARNING")
            
            supabase_record[target_field] = value
    
    return supabase_record


def insert_to_supabase(records, connection_config):
    """
    Вставка записей в Supabase PostgreSQL
    
    Args:
        records: Список записей для вставки
        connection_config: Конфигурация подключения к БД
    
    Returns:
        tuple: (успешных вставок, ошибок)
    """
    if not records:
        log("Нет записей для вставки", "WARNING")
        return 0, 0
    
    log(f"Подключаюсь к Supabase PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host=connection_config['host'],
            database=connection_config['database'],
            user=connection_config['user'],
            password=connection_config['password'],
            port=connection_config['port']
        )
        
        cursor = conn.cursor()
        log("Подключение установлено успешно")
        
        all_fields = set()
        for record in records:
            all_fields.update(record.keys())
        
        fields = sorted(list(all_fields))
        
        if not fields:
            log("Нет полей для вставки", "ERROR")
            return 0, 0
        
        placeholders = ', '.join(['%s'] * len(fields))
        fields_str = ', '.join(fields)
        
        update_str = ', '.join([f"{field} = EXCLUDED.{field}" for field in fields if field != 'id'])
        
        insert_query = f"""
            INSERT INTO rating_merch ({fields_str})
            VALUES ({placeholders})
        """
        
        if update_str:
            insert_query += f"\nON CONFLICT (id) DO UPDATE SET\n    {update_str}"
        
        records_data = []
        for record in records:
            row = []
            for field in fields:
                value = record.get(field)
                row.append(value)
            records_data.append(tuple(row))
        
        log(f"Начинаю вставку {len(records_data)} записей...")
        
        success_count = 0
        error_count = 0
        
        for i, row_data in enumerate(records_data, 1):
            try:
                cursor.execute(insert_query, row_data)
                conn.commit()
                success_count += 1
                
                if i % 10 == 0:
                    log(f"Обработано {i}/{len(records_data)} записей...")
                    
            except Exception as e:
                error_count += 1
                log(f"Ошибка при вставке записи {i}: {e}", "ERROR")
                log(f"Данные записи: {dict(zip(fields, row_data))}", "ERROR")
                conn.rollback()
                continue
        
        log(f"Транзакция успешно завершена")
        
        cursor.close()
        conn.close()
        
        log(f"Вставка завершена: {success_count} успешно, {error_count} ошибок")
        return success_count, error_count
        
    except psycopg2.Error as e:
        log(f"Ошибка PostgreSQL: {e}", "ERROR")
        return 0, len(records)
    except Exception as e:
        log(f"Неожиданная ошибка: {e}", "ERROR")
        return 0, len(records)


def migrate_table():
    """
    Основная функция миграции
    """
    log("=" * 80)
    log("МИГРАЦИЯ ДАННЫХ: Bubble -> Supabase")
    log("Таблица: rating_merch")
    log("=" * 80)
    
    bubble_records = fetch_bubble_data(
        table_name=BUBBLE_CONFIG['table_name'],
        api_token=BUBBLE_CONFIG['api_token'],
        app_name=BUBBLE_CONFIG['app_name'],
        api_version=BUBBLE_CONFIG['api_version']
    )
    
    if not bubble_records:
        log("Не удалось загрузить данные из Bubble", "ERROR")
        return False
    
    log(f"Успешно загружено {len(bubble_records)} записей из Bubble")
    
    log("Сохраняю исходные данные в bubble_raw_data.json...")
    with open('bubble_raw_data.json', 'w', encoding='utf-8') as f:
        json.dump(bubble_records, f, indent=2, ensure_ascii=False)
    
    log("Начинаю трансформацию данных...")
    transformed_records = []
    
    for i, record in enumerate(bubble_records, 1):
        try:
            transformed = transform_record(record, FIELD_MAPPING)
            transformed_records.append(transformed)
        except Exception as e:
            log(f"Ошибка при трансформации записи {i}: {e}", "ERROR")
            continue
    
    log(f"Трансформировано {len(transformed_records)} записей")
    
    log("Сохраняю трансформированные данные в supabase_transformed_data.json...")
    with open('supabase_transformed_data.json', 'w', encoding='utf-8') as f:
        json.dump(transformed_records, f, indent=2, ensure_ascii=False)
    
    success, errors = insert_to_supabase(transformed_records, SUPABASE_CONFIG)
    
    log("=" * 80)
    log("МИГРАЦИЯ ЗАВЕРШЕНА")
    log(f"Всего записей в Bubble: {len(bubble_records)}")
    log(f"Трансформировано: {len(transformed_records)}")
    log(f"Успешно вставлено в Supabase: {success}")
    log(f"Ошибок: {errors}")
    log("=" * 80)
    
    return errors == 0


if __name__ == "__main__":
    try:
        success = migrate_table()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        log("\nМиграция прервана пользователем", "WARNING")
        sys.exit(1)
    except Exception as e:
        log(f"Критическая ошибка: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        sys.exit(1)
