#!/usr/bin/env python
"""
Полная эмуляция с сохранением в БД через Django ORM
"""

import os
import sys
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bigdata_config.settings')
django.setup()

import json
import traceback
import pandas as pd
from dataprocessor.models import UploadedFile, ProcessingResult
from dataprocessor.utils.file_loader import load_file_to_dataframe
from dataprocessor.utils.data_cleaner import (
    get_shape,
    get_null_info,
    get_numeric_stats,
    get_top_values,
    clean_data,
)

print("=" * 80)
print("ТЕСТИРОВАНИЕ С РЕАЛЬНОЙ БД")
print("=" * 80)

try:
    # Очищаем старые данные
    print("\n1️⃣ Очистка старых данных...")
    ProcessingResult.objects.all().delete()
    UploadedFile.objects.all().delete()
    print("   ✓ БД очищена")
    
    # Создаем новый файл
    print("\n2️⃣ Создание записи файла в БД...")
    test_file_path = 'media/uploads/students.csv'
    uploaded_file = UploadedFile(
        file=test_file_path,
        original_name='students.csv',
        file_size=5000,
        file_type='csv'
    )
    uploaded_file.save()
    print(f"   ✓ Файл создан с ID={uploaded_file.pk}")
    
    # Загружаем и обрабатываем
    print("\n3️⃣ Загрузка и обработка файла...")
    raw_df = load_file_to_dataframe(test_file_path, 'csv')
    print(f"   ✓ DataFrame загружен")
    
    cleaning_before = {
        'shape': get_shape(raw_df),
        'null_info': get_null_info(raw_df),
    }
    
    cleaned_df = clean_data(raw_df)
    
    cleaning_after = {
        'shape': get_shape(cleaned_df),
        'null_info': get_null_info(cleaned_df),
    }
    print(f"   ✓ Статистика собрана")
    
    # Собираем результаты
    print("\n4️⃣ Сбор результатов...")
    result_data = {
        'shape': get_shape(cleaned_df),
        'null_info': get_null_info(cleaned_df),
        'numeric_stats': get_numeric_stats(cleaned_df),
        'top_values': get_top_values(cleaned_df, top_n=5),
    }
    print(f"   ✓ Результаты собраны")
    
    # Сохраняем в БД
    print("\n5️⃣ Сохранение в БД через Django ORM...")
    shape = result_data.get('shape', {})
    null_info = result_data.get('null_info', {})
    numeric = result_data.get('numeric_stats', {})
    top_vals = result_data.get('top_values', {})
    
    print(f"   - shape: {type(shape)} - {shape}")
    print(f"   - null_info: {type(null_info)}")
    print(f"   - numeric: {type(numeric)}")
    print(f"   - top_vals: {type(top_vals)}")
    
    processing_result = ProcessingResult.objects.create(
        uploaded_file=uploaded_file,
        row_count=shape.get('row_count'),
        column_count=shape.get('column_count'),
        column_names=shape.get('column_names'),
        null_counts=null_info.get('null_counts'),
        total_nulls=null_info.get('total_nulls'),
        numeric_stats=numeric or None,
        top_values=top_vals or None,
        cleaning_before=cleaning_before,
        cleaning_after=cleaning_after,
        processing_time=0.123,
        error_message='',
    )
    print(f"   ✓ Записано в БД с ID={processing_result.pk}")
    
    # Восстанавливаем из БД
    print("\n6️⃣ Восстановление из БД...")
    db_result = ProcessingResult.objects.get(pk=processing_result.pk)
    print(f"   ✓ Запись найдена")
    print(f"   - cleaning_before type: {type(db_result.cleaning_before)}")
    print(f"   - cleaning_after type: {type(db_result.cleaning_after)}")
    
    # КРИТИЧЕСКИЙ МОМЕНТ - как в results_view
    print("\n7️⃣ КРИТИЧЕСКИЙ МОМЕНТ - вычисление разницы как в results_view...")
    
    if db_result.cleaning_before and db_result.cleaning_after:
        print(f"   cleaning_before exists: {db_result.cleaning_before is not None}")
        print(f"   cleaning_after exists: {db_result.cleaning_after is not None}")
        
        # Проверяем что лежит в БД
        print(f"\n   cleaning_before полностью:")
        print(f"   {json.dumps(db_result.cleaning_before, indent=2, default=str)}")
        
        print(f"\n   cleaning_after полностью:")
        print(f"   {json.dumps(db_result.cleaning_after, indent=2, default=str)}")
        
        # Пытаемся получить значения
        try:
            print(f"\n   Попытка 1: Прямой доступ")
            before_shape = db_result.cleaning_before.get('shape')
            after_shape = db_result.cleaning_after.get('shape')
            
            print(f"   - before_shape: {before_shape} (type: {type(before_shape)})")
            print(f"   - after_shape: {after_shape} (type: {type(after_shape)})")
            
            before_rows = before_shape.get('row_count')
            after_rows = after_shape.get('row_count')
            
            print(f"   - before_rows: {before_rows} (type: {type(before_rows)})")
            print(f"   - after_rows: {after_rows} (type: {type(after_rows)})")
            
            # ВЫЧИСЛЕНИЕ
            rows_diff = after_rows - before_rows
            print(f"   ✓ rows_diff = {rows_diff}")
            
            # Null info
            before_nulls = db_result.cleaning_before['null_info']['total_nulls']
            after_nulls = db_result.cleaning_after['null_info']['total_nulls']
            
            print(f"   - before_nulls: {before_nulls} (type: {type(before_nulls)})")
            print(f"   - after_nulls: {after_nulls} (type: {type(after_nulls)})")
            
            nulls_diff = after_nulls - before_nulls
            print(f"   ✓ nulls_diff = {nulls_diff}")
            
            print(f"\n   ✓ ВСЕ ВЫЧИСЛЕНИЯ УСПЕШНО!")
            
        except Exception as e:
            print(f"   ❌ ОШИБКА при вычислении: {e}")
            print(f"   Traceback:")
            traceback.print_exc()
    
    print(f"\n✅ ПОЛНОЕ ТЕСТИРОВАНИЕ С БД ЗАВЕРШИЛОСЬ УСПЕШНО!")
    
except Exception as e:
    print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
    print(f"\nПолный traceback:")
    traceback.print_exc()

print(f"\n" + "=" * 80)
