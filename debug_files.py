#!/usr/bin/env python
"""
Debug скрипт для тестирования сохранения/восстановления данных в JSON.
"""

import os
import sys
import django
import json

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bigdata_config.settings')
django.setup()

import pandas as pd
import numpy as np
from dataprocessor.utils.data_cleaner import (
    get_shape,
    get_null_info,
    get_numeric_stats,
    get_top_values,
    clean_data,
)
from dataprocessor.utils.file_loader import load_file_to_dataframe

print("=" * 80)
print("ТЕСТИРОВАНИЕ РЕАЛЬНЫХ ФАЙЛОВ")
print("=" * 80)

# Тестируем реальные файлы из media
test_files = [
    'media/uploads/students.csv',
    'media/uploads/employees.json',
    'media/uploads/sales.txt',
]

for file_path in test_files:
    if not os.path.exists(file_path):
        print(f"\n⚠️  Файл не найден: {file_path}")
        continue
    
    print(f"\n{'=' * 80}")
    print(f"ФАЙЛ: {file_path}")
    print(f"{'=' * 80}")
    
    try:
        # Определяем тип файла
        ext = os.path.splitext(file_path)[1].lower().lstrip('.')
        if ext == 'txt':
            ext = 'txt'
        
        print(f"✓ Тип файла: {ext}")
        
        # Загружаем файл
        df = load_file_to_dataframe(file_path, ext)
        print(f"✓ DataFrame shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        
        # Тестируем функции
        print(f"\nТестирование функций:")
        
        # get_shape
        shape = get_shape(df)
        print(f"  get_shape(): {shape}")
        
        # get_null_info
        null_info = get_null_info(df)
        print(f"  get_null_info() total_nulls: {null_info['total_nulls']} (type: {type(null_info['total_nulls'])})")
        
        # get_numeric_stats
        stats = get_numeric_stats(df)
        print(f"  get_numeric_stats() keys: {list(stats.keys())}")
        
        # get_top_values
        top_vals = get_top_values(df, top_n=3)
        print(f"  get_top_values() keys: {list(top_vals.keys())}")
        
        # clean_data
        cleaned_df = clean_data(df)
        print(f"  clean_data() shape: {cleaned_df.shape}")
        
        # ТЕСТИРУЕМ JSON СОХРАНЕНИЕ И ВОССТАНОВЛЕНИЕ
        print(f"\n📦 Тестирование JSON сохранения:")
        
        cleaning_before = {
            'shape': get_shape(df),
            'null_info': get_null_info(df),
        }
        
        cleaning_after = {
            'shape': get_shape(cleaned_df),
            'null_info': get_null_info(cleaned_df),
        }
        
        # Сохраняем в JSON (как Django делает)
        json_before = json.dumps(cleaning_before, default=str)
        json_after = json.dumps(cleaning_after, default=str)
        
        print(f"  ✓ JSON сохранено успешно")
        
        # Восстанавливаем из JSON
        restored_before = json.loads(json_before)
        restored_after = json.loads(json_after)
        
        print(f"  ✓ JSON восстановлено успешно")
        
        # ТЕСТИРУЕМ ВЫЧИСЛЕНИЯ КАК В results_view
        print(f"\n🔢 Тестирование вычисленийё:")
        
        before_shape = restored_before.get('shape', {})
        after_shape = restored_after.get('shape', {})
        
        print(f"  before_shape type: {type(before_shape)}, value: {before_shape}")
        print(f"  after_shape type: {type(after_shape)}, value: {after_shape}")
        
        if isinstance(before_shape, dict) and isinstance(after_shape, dict):
            before_rows = before_shape.get('row_count')
            after_rows = after_shape.get('row_count')
            
            print(f"  before_rows type: {type(before_rows)}, value: {before_rows}")
            print(f"  after_rows type: {type(after_rows)}, value: {after_rows}")
            
            if isinstance(before_rows, int) and isinstance(after_rows, int):
                diff = after_rows - before_rows
                print(f"  ✓ Разница rows: {diff}")
            else:
                print(f"  ❌ ОШИБКА: before/after rows имеют неправильный тип!")
        
        print(f"  ✅ Файл {file_path} обработан успешно!")
        
    except Exception as e:
        print(f"  ❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()

print(f"\n{'=' * 80}")
print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
print(f"{'=' * 80}")
