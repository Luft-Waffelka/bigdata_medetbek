#!/usr/bin/env python
"""
Полная эмуляция процесса обработки как в views.py
"""

import os
import sys
import django
import json
import time
import traceback

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bigdata_config.settings')
django.setup()

import pandas as pd
from dataprocessor.utils.file_loader import load_file_to_dataframe
from dataprocessor.utils.data_cleaner import (
    get_shape,
    get_null_info,
    get_numeric_stats,
    get_top_values,
    clean_data,
)

def normalize_shape(shape_data):
    """Нормализация shape данных"""
    if isinstance(shape_data, dict):
        return shape_data
    elif isinstance(shape_data, tuple) and len(shape_data) >= 2:
        return {
            'row_count': int(shape_data[0]),
            'column_count': int(shape_data[1]),
            'column_names': []
        }
    else:
        return {
            'row_count': 0,
            'column_count': 0,
            'column_names': []
        }

def safe_get_int(value):
    """Безопасное преобразование в int"""
    try:
        if isinstance(value, (tuple, list)):
            return int(value[0]) if value else 0
        return int(value) if value is not None else 0
    except (ValueError, TypeError, IndexError):
        return 0

print("=" * 80)
print("ПОЛНАЯ ЭМУЛЯЦИЯ ПРОЦЕССА ОБРАБОТКИ")
print("=" * 80)

test_file = 'media/uploads/students.csv'

try:
    print(f"\n1️⃣ Загрузка файла: {test_file}")
    raw_df = load_file_to_dataframe(test_file, 'csv')
    print(f"   ✓ DataFrame загружен: shape={raw_df.shape}")
    
    print(f"\n2️⃣ Сбор статистики ДО обработки")
    cleaning_before = {
        'shape': get_shape(raw_df),
        'null_info': get_null_info(raw_df),
    }
    print(f"   ✓ Данные собраны")
    print(f"   - shape type: {type(cleaning_before['shape'])}")
    print(f"   - null_info type: {type(cleaning_before['null_info'])}")
    print(f"   - shape: {cleaning_before['shape']}")
    
    print(f"\n3️⃣ Обработка данных (clean_data)")
    cleaned_df = clean_data(raw_df)
    print(f"   ✓ Данные очищены: shape={cleaned_df.shape}")
    
    print(f"\n4️⃣ Сбор статистики ПОСЛЕ обработки")
    cleaning_after = {
        'shape': get_shape(cleaned_df),
        'null_info': get_null_info(cleaned_df),
    }
    print(f"   ✓ Данные собраны")
    print(f"   - shape type: {type(cleaning_after['shape'])}")
    print(f"   - null_info type: {type(cleaning_after['null_info'])}")
    print(f"   - shape: {cleaning_after['shape']}")
    
    print(f"\n5️⃣ Сохранение в JSON (как сохраняется в БД)")
    json_before = json.dumps(cleaning_before, default=str)
    json_after = json.dumps(cleaning_after, default=str)
    print(f"   ✓ JSON сохранено")
    
    print(f"\n6️⃣ Восстановление из JSON (как читается из БД)")
    restored_before = json.loads(json_before)
    restored_after = json.loads(json_after)
    print(f"   ✓ JSON восстановлено")
    
    print(f"\n7️⃣ КРИТИЧЕСКИЙ МОМЕНТ - вычисление разницы (results_view)")
    print(f"   restored_before: {restored_before}")
    print(f"   restored_after: {restored_after}")
    
    if restored_before and restored_after:
        # Нормализация (защита от старого формата)
        before_shape = normalize_shape(restored_before.get('shape', {}))
        after_shape = normalize_shape(restored_after.get('shape', {}))
        
        print(f"\n   after normalization:")
        print(f"   - before_shape: {before_shape}")
        print(f"   - after_shape: {after_shape}")
        
        # Получение null_info
        before_nulls = safe_get_int(restored_before.get('null_info', {}).get('total_nulls', 0))
        after_nulls = safe_get_int(restored_after.get('null_info', {}).get('total_nulls', 0))
        
        print(f"\n   null values:")
        print(f"   - before_nulls: {before_nulls} (type: {type(before_nulls)})")
        print(f"   - after_nulls: {after_nulls} (type: {type(after_nulls)})")
        
        # ВЫЧИСЛЕНИЕ РАЗНИЦЫ - ТУТ МОЖЕТ БЫТЬ ОШИБКА
        print(f"\n   ⚠️ КРИТИЧЕСКИЙ МОМЕНТ - вычисление разницы:")
        
        before_rows = before_shape['row_count']
        after_rows = after_shape['row_count']
        print(f"   - before_rows: {before_rows} (type: {type(before_rows)})")
        print(f"   - after_rows: {after_rows} (type: {type(after_rows)})")
        
        try:
            rows_diff = after_rows - before_rows
            print(f"   ✓ rows_diff = {rows_diff}")
        except Exception as e:
            print(f"   ❌ ОШИБКА при rows_diff: {e}")
            print(f"   Traceback:")
            traceback.print_exc()
        
        try:
            cols_diff = after_shape['column_count'] - before_shape['column_count']
            print(f"   ✓ cols_diff = {cols_diff}")
        except Exception as e:
            print(f"   ❌ ОШИБКА при cols_diff: {e}")
            print(f"   Traceback:")
            traceback.print_exc()
        
        try:
            nulls_diff = after_nulls - before_nulls
            print(f"   ✓ nulls_diff = {nulls_diff}")
        except Exception as e:
            print(f"   ❌ ОШИБКА при nulls_diff: {e}")
            print(f"   Traceback:")
            traceback.print_exc()
        
        cleaning_changes = {
            'rows': rows_diff,
            'columns': cols_diff,
            'nulls': nulls_diff,
        }
        print(f"\n   ✓ cleaning_changes создан: {cleaning_changes}")
    
    print(f"\n8️⃣ Сбор остальных результатов")
    result_data = {}
    
    # Shape
    result_data['shape'] = get_shape(cleaned_df)
    print(f"   ✓ shape: {result_data['shape']}")
    
    # Null info
    result_data['null_info'] = get_null_info(cleaned_df)
    print(f"   ✓ null_info total_nulls: {result_data['null_info']['total_nulls']}")
    
    # Numeric stats
    result_data['numeric_stats'] = get_numeric_stats(cleaned_df)
    print(f"   ✓ numeric_stats: {list(result_data['numeric_stats'].keys())}")
    
    # Top values
    result_data['top_values'] = get_top_values(cleaned_df, top_n=5)
    print(f"   ✓ top_values: {list(result_data['top_values'].keys())}")
    
    print(f"\n✅ ПОЛНОЕ ТЕСТИРОВАНИЕ ЗАВЕРШИЛОСЬ УСПЕШНО!")
    print(f"   Ошибок не обнаружено!")
    
except Exception as e:
    print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
    print(f"\nПолный traceback:")
    traceback.print_exc()

print(f"\n" + "=" * 80)
