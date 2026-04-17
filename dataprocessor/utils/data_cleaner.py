"""
████████████████████████████████████████████████████████████████████████
  СТУДЕНТКЕ АРНАЛҒАН ТАПСЫРМА — осы файлдың барлық TODO бөлімдерін толтыр
████████████████████████████████████████████████████████████████████████

Бұл модуль ТЗ 2.2 талаптарын іске асырады:
  ✅  get_shape()        — жол/баған саны
  ✅  get_null_info()    — бос мәндер статистикасы
  ✅  get_numeric_stats()— min / max / mean
  ✅  get_top_values()   — ең жиі кездесетін мәндер
  ➕  clean_data()       — деректерді тазалау (Қосымша)

Барлық функция pandas DataFrame қабылдайды және dict қайтарады.
"""

import pandas as pd
from typing import Any


# ═══════════════════════════════════════════════════════════════════════
# 1. ЖОЛ / БАҒАН САНЫ
# ═══════════════════════════════════════════════════════════════════════

def get_shape(df: pd.DataFrame) -> dict:
    """
    DataFrame-дің жол және баған санын қайтарады.

    Мысал нәтиже:
        {
            'row_count': 1500,
            'column_count': 8,
            'column_names': ['id', 'name', 'age', ...]
        }

    TODO (СТУДЕНТ):
        1. df.shape арқылы жол және баған санын алыңыз.
        2. df.columns.tolist() арқылы баған атауларын алыңыз.
        3. Нәтижені dict ретінде қайтарыңыз.
    """
    # ▼▼▼ КОДТЫ ОСЫДАН БАСТАҢЫЗ ▼▼▼
    try:
        row_count, column_count = df.shape
        return {
            'row_count': int(row_count),
            'column_count': int(column_count),
            'column_names': list(df.columns),  # Гарантирует список, не tuple
        }
    except Exception as e:
        print(f"⚠️ get_shape() қатесі: {e}")
        return {
            'row_count': 0,
            'column_count': 0,
            'column_names': [],
        }

    # ▲▲▲ КОДТЫ ОСЫМЕН АЯҚТАҢЫЗ ▲▲▲


# ═══════════════════════════════════════════════════════════════════════
# 2. БОС МӘНДЕР (NULL) СТАТИСТИКАСЫ
# ═══════════════════════════════════════════════════════════════════════

def get_null_info(df: pd.DataFrame) -> dict:
    """
    Әр бағандағы бос мәндер (NaN/None) санын есептейді.

    Мысал нәтиже:
        {
            'null_counts': {'name': 0, 'age': 12, 'salary': 3},
            'total_nulls': 15,
            'null_percent': {'name': 0.0, 'age': 8.0, 'salary': 2.0}
        }

    TODO (СТУДЕНТ):
        1. df.isnull().sum() арқылы әр бағандың null санын алыңыз.
        2. Жалпы null санын есептеңіз.
        3. Пайыздық үлесін есептеңіз: (null / жол_саны) * 100
        4. Нәтижені dict ретінде қайтарыңыз.

    Кеңес:
        null_series = df.isnull().sum()
        null_dict = null_series.to_dict()
    """
    # ▼▼▼ КОДТЫ ОСЫДАН БАСТАҢЫЗ ▼▼▼

    null_series = df.isnull().sum()
    null_counts = {col: int(count) for col, count in null_series.to_dict().items()}
    total_nulls = int(null_series.sum())
    row_count = len(df)
    
    null_percent = {}
    for col, count in null_counts.items():
        if row_count > 0:
            null_percent[col] = round((count / row_count) * 100, 2)
        else:
            null_percent[col] = 0.0
    
    return {
        'null_counts': null_counts,
        'total_nulls': total_nulls,
        'null_percent': null_percent,
    }

    # ▲▲▲ КОДТЫ ОСЫМЕН АЯҚТАҢЫЗ ▲▲▲


# ═══════════════════════════════════════════════════════════════════════
# 3. САНДЫҚ БАҒАНДАР СТАТИСТИКАСЫ
# ═══════════════════════════════════════════════════════════════

def get_numeric_stats(df: pd.DataFrame) -> dict:
    """
    Сандық (numeric) бағандар бойынша min, max, орташа мән есептейді.

    Мысал нәтиже:
        {
            'stats': {
                'age':    {'min': 18,  'max': 65,    'mean': 34.7},
                'salary': {'min': 500, 'max': 15000, 'mean': 4200.5},
            },
            'numeric_columns': ['age', 'salary']
        }

    TODO (СТУДЕНТ):
        1. df.select_dtypes(include='number') арқылы сандық бағандарды алыңыз.
        2. Әр баған үшін min(), max(), mean() есептеңіз.
        3. round() арқылы мәндерді 2 дұрыс цифрға дейін дөңгелектеңіз.
        4. Нәтижені dict ретінде қайтарыңыз.

    Кеңес:
        numeric_df = df.select_dtypes(include='number')
        for col in numeric_df.columns:
            ...
    """
    # ▼▼▼ КОДТЫ ОСЫДАН БАСТАҢЫЗ ▼▼▼
    try:
        numeric_df = df.select_dtypes(include='number')
        stats = {}
        for col in numeric_df.columns:
            try:
                col_min = numeric_df[col].min()
                col_max = numeric_df[col].max()
                col_mean = numeric_df[col].mean()
                stats[col] = {
                    'min': round(float(col_min), 2) if pd.notna(col_min) else None,
                    'max': round(float(col_max), 2) if pd.notna(col_max) else None,
                    'mean': round(float(col_mean), 2) if pd.notna(col_mean) else None,
                }
            except Exception as e:
                print(f"⚠️ {col} статистикасы есептелмеді: {e}")
                stats[col] = {'min': None, 'max': None, 'mean': None}
        
        return {
            'stats': stats,
            'numeric_columns': list(numeric_df.columns),  # Гарантирует список
        }
    except Exception as e:
        print(f"⚠️ get_numeric_stats() қатесі: {e}")
        return {
            'stats': {},
            'numeric_columns': [],
        }

    # ▲▲▲ КОДТЫ ОСЫМЕН АЯҚТАҢЫЗ ▲▲▲


# ═══════════════════════════════════════════════════════════════════════
# 4. ЕҢ ЖИІІ КЕЗДЕСЕТІН МӘНДЕР (TOP-N)
# ═══════════════════════════════════════════════════════════════════════

def get_top_values(df: pd.DataFrame, top_n: int = 5) -> dict:
    """
    Әр бағандағы ең жиі кездесетін top_n мәнді қайтарады.

    Мысал нәтиже (top_n=3):
        {
            'top_values': {
                'city': [
                    {'value': 'Алматы',   'count': 320},
                    {'value': 'Астана',   'count': 210},
                    {'value': 'Шымкент',  'count': 95},
                ],
                'status': [
                    {'value': 'active',   'count': 800},
                    {'value': 'inactive', 'count': 200},
                ],
            }
        }

    TODO (СТУДЕНТ):
        1. df.columns арқылы барлық бағандарды аралаңыз.
        2. Әр баған үшін df[col].value_counts().head(top_n) қолданыңыз.
        3. Нәтижені жоғарыдағы форматқа сай dict-ке айналдырыңыз.

    Параметрлер:
        df    — pandas DataFrame
        top_n — ең жиі мәндер саны (default: 5)
    """
    # ▼▼▼ КОДТЫ ОСЫДАН БАСТАҢЫЗ ▼▼▼
    try:
        top_n = int(top_n) if top_n else 5
        top_n = max(1, min(top_n, 100))  # Ограничиваем 1-100
        
        top_values = {}
        for col in df.columns:
            try:
                counts = df[col].value_counts(dropna=False).head(top_n)
                top_values[col] = []
                for idx, count in counts.items():
                    try:
                        top_values[col].append({
                            'value': str(idx) if pd.isna(idx) else idx,  # Конвертируем NaN в строку
                            'count': int(count)
                        })
                    except Exception as e:
                        print(f"⚠️ {col} бағанының мәні өңделмеді: {e}")
            except Exception as e:
                print(f"⚠️ {col} top_values есептелмеді: {e}")
                top_values[col] = []
        
        return {
            'top_values': top_values,
        }
    except Exception as e:
        print(f"⚠️ get_top_values() қатесі: {e}")
        return {
            'top_values': {},
        }

    # ▲▲▲ КОДТЫ ОСЫМЕН АЯҚТАҢЫЗ ▲▲▲


# ═══════════════════════════════════════════════════════════════════════
# 5. ДЕРЕКТЕРДІ ТАЗАЛАУ — ҚОСЫМША ТАПСЫРМА (+5 балл)
# ═══════════════════════════════════════════════════════════════════════

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Деректерді тазалайды және тазаланған DataFrame қайтарады.

    TODO (СТУДЕНТ — қосымша):
        Кем дегенде 2 тазалау операциясын іске асырыңыз:

        Мысалдар:
          - Толықтай қайталанатын жолдарды жою: df.drop_duplicates()
          - Бос мәндерді орташамен толтыру: df[col].fillna(df[col].mean())
          - Жол басы/соңы бос орындарды жою: df[col].str.strip()
          - Баған атауларын кіші әріпке келтіру: df.columns.str.lower()

    Параметрлер:
        df — бастапқы DataFrame

    Қайтарады:
        pd.DataFrame — тазаланған DataFrame
    """
    # ▼▼▼ КОДТЫ ОСЫДАН БАСТАҢЫЗ ▼▼▼
    try:
        cleaned_df = df.copy()

        # 1) Баған атауларын кіші әріпке келтіру
        try:
            cleaned_df.columns = cleaned_df.columns.str.lower()
        except (AttributeError, TypeError):
            # Если columns не имеет метода str.lower(), просто конвертируем в список и обратно
            cleaned_df.columns = [str(col).lower() for col in cleaned_df.columns]

        # 2) Қайталанатын жолдарды жою
        cleaned_df = cleaned_df.drop_duplicates().reset_index(drop=True)

        # 3) Мәтіндік бағандардағы айналасындағы бос орындарды жою
        text_columns = list(cleaned_df.select_dtypes(include=['object', 'string']).columns)
        for col in text_columns:
            try:
                cleaned_df[col] = cleaned_df[col].astype('string').str.strip()
            except (AttributeError, TypeError):
                # Если функция не работает, просто пропускаем
                pass

        # 4) Сандық бағандардағы бос мәндерді бағандық орташа мәнмен толтыру
        numeric_columns = list(cleaned_df.select_dtypes(include='number').columns)
        for col in numeric_columns:
            if cleaned_df[col].isna().any():
                try:
                    mean_value = float(cleaned_df[col].mean())
                    if pd.notna(mean_value):
                        cleaned_df[col] = cleaned_df[col].fillna(mean_value)
                except (TypeError, ValueError):
                    # Если не можем вычислить среднее, пропускаем
                    pass

        return cleaned_df
    
    except Exception as e:
        # Если что-то совсем не сработает, просто вернём оригинальный DataFrame
        print(f"⚠️ ДИАГНОСТИКА: clean_data() ішінде қате: {e}")
        return df.copy()

    # ▲▲▲ КОДТЫ ОСЫМЕН АЯҚТАҢЫЗ ▲▲▲
