#!/usr/bin/env python
"""
Debug скрипт для тестирования функций обработки данных.
Запускайте: python debug_cleaner.py
"""

import os
import sys
import django

# Установка Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'bigdata_config.settings')
django.setup()

import pandas as pd
from dataprocessor.utils.data_cleaner import (
    get_shape,
    get_null_info,
    get_numeric_stats,
    get_top_values,
    clean_data,
)

# Тестовые данные
test_data = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [25, 30, None, 35, 28],
    'salary': [50000, 60000, 55000, 70000, 52000],
}

df = pd.DataFrame(test_data)

print("=" * 80)
print("ТЕСТИРОВАНИЕ ФУНКЦИЙ ОБРАБОТКИ ДАННЫХ")
print("=" * 80)

print("\n1️⃣  БАСТАПҚЫ DATAFRAME:")
print(df)
print(f"Shape: {df.shape}")
print(f"Type: {type(df.shape)}")

try:
    print("\n2️⃣  get_shape():")
    shape = get_shape(df)
    print(f"✅ Нәтиже: {shape}")
    print(f"   Type: {type(shape)}")
except Exception as e:
    print(f"❌ ҚАТЕ: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n3️⃣  get_null_info():")
    null_info = get_null_info(df)
    print(f"✅ Нәтиже: {null_info}")
    print(f"   Type: {type(null_info['total_nulls'])}")
except Exception as e:
    print(f"❌ ҚАТЕ: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n4️⃣  get_numeric_stats():")
    stats = get_numeric_stats(df)
    print(f"✅ Нәтиже: {stats}")
except Exception as e:
    print(f"❌ ҚАТЕ: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n5️⃣  get_top_values():")
    top_vals = get_top_values(df, top_n=3)
    print(f"✅ Нәтиже: {top_vals}")
except Exception as e:
    print(f"❌ ҚАТЕ: {e}")
    import traceback
    traceback.print_exc()

try:
    print("\n6️⃣  clean_data():")
    cleaned = clean_data(df)
    print(f"✅ Нәтиже DataFrame shape: {cleaned.shape}")
    print(f"   Columns: {list(cleaned.columns)}")
    print(cleaned)
except Exception as e:
    print(f"❌ ҚАТЕ: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
print("ТЕСТИРОВАНИЕ АЯҚТАЛДЫ")
print("=" * 80)
