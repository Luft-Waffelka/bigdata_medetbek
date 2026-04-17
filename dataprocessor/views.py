"""
Views — HTTP сұраулар өңдеушілері.

Маршруттар:
    /                    → index_view      — бас бет (жүктеу формасы)
    /process/<id>/       → process_view    — файлды өңдеу
    /results/<id>/       → results_view    — нәтижені браузерде көрсету
    /download/<id>/      → download_view   — нәтижені жүктеп алу
    /history/            → history_view    — жүктелген файлдар тарихы
"""

import time
import os
import json
import pandas as pd
from django.shortcuts import render, redirect, get_object_or_404
from django.http import HttpResponse, JsonResponse
from django.contrib import messages

from .models import UploadedFile, ProcessingResult
from .forms import FileUploadForm, ProcessingOptionsForm
from .utils.file_loader import load_file_to_dataframe
from .utils.result_exporter import export_result_as_csv, export_result_as_json

# Студент іске асыратын функциялар:
from .utils.data_cleaner import (
    get_shape,
    get_null_info,
    get_numeric_stats,
    get_top_values,
    clean_data,
)


def normalize_shape(shape_data):
    """
    Shape деректерін нормализациялайды.
    Егер tuple болса, dict түрге айналдырады.
    
    Args:
        shape_data: dict немесе tuple
        
    Returns:
        dict: {'row_count': int, 'column_count': int, 'column_names': list}
    """
    if isinstance(shape_data, dict):
        return shape_data
    elif isinstance(shape_data, tuple) and len(shape_data) >= 2:
        # Ескі формат: (row_count, column_count)
        return {
            'row_count': int(shape_data[0]),
            'column_count': int(shape_data[1]),
            'column_names': []
        }
    else:
        # Қауіпсіз әдепкі
        return {
            'row_count': 0,
            'column_count': 0,
            'column_names': []
        }


def safe_get_int(value):
    """
    Кез келген мәнді қауіпсіз түрде int-ке айналдырады.
    Tuple, str немесе басқа типтан int ала алады.
    """
    try:
        if isinstance(value, (tuple, list)):
            return int(value[0]) if value else 0
        return int(value) if value is not None else 0
    except (ValueError, TypeError, IndexError):
        return 0


def build_cleaning_diff(original_df, cleaned_df):
    diff = []
    before = original_df.copy()
    before.columns = before.columns.str.lower()
    after = cleaned_df.copy()

    # Обрабатываем переименование колонок
    for orig_col, clean_col in zip(original_df.columns, after.columns):
        if orig_col.lower() == clean_col and orig_col != clean_col:
            diff.append({
                'object': 'column name',
                'field': orig_col,
                'row': None,
                'before': orig_col,
                'after': clean_col,
                'note': 'Column name lowercased',
            })

    # Сравнение строк до и после
    min_rows = min(len(before), len(after))

    def normalize_value(value):
        if pd.isna(value):
            return None
        if hasattr(value, 'item'):
            try:
                return value.item()
            except Exception:
                pass
        if isinstance(value, (pd.Timestamp, pd.Timedelta)):
            return str(value)
        return value

    def values_equal(a, b):
        if pd.isna(a) and pd.isna(b):
            return True
        return a == b

    for row_idx in range(min_rows):
        for col in after.columns:
            if col not in before.columns:
                continue
            before_value = before.at[before.index[row_idx], col]
            after_value = after.at[after.index[row_idx], col]
            if values_equal(before_value, after_value):
                continue

            note = 'Значение изменено'
            if isinstance(before_value, str) and isinstance(after_value, str) and before_value.strip() == after_value:
                note = 'Удалены лишние пробелы'
            elif pd.isna(before_value) and not pd.isna(after_value):
                note = 'Пустое значение заполнено'
            elif not pd.isna(before_value) and pd.isna(after_value):
                note = 'Значение удалено'

            diff.append({
                'object': f'row {before.index[row_idx] + 1}',
                'field': col,
                'row': before.index[row_idx] + 1,
                'before': normalize_value(before_value),
                'after': normalize_value(after_value),
                'note': note,
            })
            if len(diff) >= 30:
                break
        if len(diff) >= 30:
            break

    removed_rows = len(before) - len(after)
    if removed_rows > 0:
        diff.insert(0, {
            'object': 'rows',
            'field': None,
            'row': None,
            'before': len(before),
            'after': len(after),
            'note': f'{removed_rows} duplicate row(s) removed',
        })

    return diff


# ═══════════════════════════════════════════════════════════════════════
# БАС БЕТ — файл жүктеу
# ═══════════════════════════════════════════════════════════════════════

def index_view(request):
    """
    GET  → файл жүктеу формасын көрсетеді.
    POST → файлды қабылдап, дерекқорға сақтайды, process_view-ке бағыттайды.
    """
    if request.method == 'POST':
        form = FileUploadForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file_obj = request.FILES['file']

            # UploadedFile моделіне сақтау
            record = UploadedFile(
                file=uploaded_file_obj,
                original_name=uploaded_file_obj.name,
                file_size=uploaded_file_obj.size,
            )
            record.save()

            messages.success(request, f'Файл сәтті жүктелді: {record.original_name}')
            # Өңдеу бетіне бағыттау
            return redirect('process', file_id=record.pk)
        else:
            messages.error(request, 'Файл жүктеу қатесі. Форманы тексеріңіз.')
    else:
        form = FileUploadForm()

    context = {
        'form': form,
        'recent_files': UploadedFile.objects.all()[:5],
    }
    return render(request, 'dataprocessor/index.html', context)


# ═══════════════════════════════════════════════════════════════════════
# ӨҢДЕУ — data_cleaner функцияларын шақыру
# ═══════════════════════════════════════════════════════════════════════

def process_view(request, file_id: int):
    """
    GET  → өңдеу параметрлерін таңдау формасын көрсетеді.
    POST → файлды оқып, data_cleaner функцияларын шақырып, нәтижені сақтайды.
    """
    file_record = get_object_or_404(UploadedFile, pk=file_id)

    if request.method == 'POST':
        options_form = ProcessingOptionsForm(request.POST)
        if options_form.is_valid():
            opts = options_form.cleaned_data
            start_time = time.time()

            try:
                # 1. Файлды жүктеп DataFrame-ге айналдыру
                file_path = file_record.file.path
                raw_df = load_file_to_dataframe(file_path, file_record.file_type)

                # Деректерді тазалау алдындағы қысқаша статистика
                cleaning_before = {
                    'shape': get_shape(raw_df),
                    'null_info': get_null_info(raw_df),
                }

                # Деректерді тазалау (қосымша)
                cleaned_df = clean_data(raw_df)

                # Деректерді тазалағаннан кейінгі статистика
                cleaning_after = {
                    'shape': get_shape(cleaned_df),
                    'null_info': get_null_info(cleaned_df),
                }

                # 2. Нәтиже объектін дайындау
                result_data = {}

                # ТЗ 2.2 — Жол/баған саны
                if opts.get('show_shape'):
                    result_data['shape'] = get_shape(cleaned_df)

                # ТЗ 2.2 — Null мәндер
                if opts.get('show_nulls'):
                    result_data['null_info'] = get_null_info(cleaned_df)

                # ТЗ 2.2 — Сандық статистика
                if opts.get('show_stats'):
                    try:
                        result_data['numeric_stats'] = get_numeric_stats(cleaned_df)
                    except Exception as stats_error:
                        print(f"⚠️ Numeric stats қатесі: {stats_error}")
                        result_data['numeric_stats'] = {'stats': {}, 'numeric_columns': []}

                # ТЗ 2.2 — Top-N мәндер
                if opts.get('show_top_values'):
                    try:
                        top_n = opts.get('top_n') or 5
                        result_data['top_values'] = get_top_values(cleaned_df, top_n=top_n)
                    except Exception as top_error:
                        print(f"⚠️ Top values қатесі: {top_error}")
                        result_data['top_values'] = {'top_values': {}}

                elapsed = time.time() - start_time

                # 3. ProcessingResult дерекқорға сақтау
                shape = result_data.get('shape', {})
                null_info = result_data.get('null_info', {})
                numeric = result_data.get('numeric_stats', {})
                top_vals = result_data.get('top_values', {})
                cleaning_diff = build_cleaning_diff(raw_df, cleaned_df)

                ProcessingResult.objects.update_or_create(
                    uploaded_file=file_record,
                    defaults={
                        'row_count':      shape.get('row_count'),
                        'column_count':   shape.get('column_count'),
                        'column_names':   shape.get('column_names'),
                        'null_counts':    null_info.get('null_counts'),
                        'total_nulls':    null_info.get('total_nulls'),
                        'numeric_stats':  numeric or None,
                        'top_values':     top_vals or None,
                        'cleaning_before': cleaning_before,
                        'cleaning_after': cleaning_after,
                        'cleaning_diff':   cleaning_diff,
                        'processing_time': round(elapsed, 3),
                        'error_message':  '',
                    }
                )

                file_record.is_processed = True
                file_record.save()

                messages.success(request, f'Өңдеу аяқталды! ({elapsed:.2f} сек)')
                return redirect('results', file_id=file_id)

            except NotImplementedError as e:
                messages.error(request, f'Функция әлі іске асырылмаған: {e}')
            except Exception as e:
                # Қатені дерекқорға жазу
                ProcessingResult.objects.update_or_create(
                    uploaded_file=file_record,
                    defaults={'error_message': str(e)}
                )
                messages.error(request, f'Өңдеу кезінде қате: {e}')

    else:
        options_form = ProcessingOptionsForm()

    context = {
        'file_record': file_record,
        'options_form': options_form,
    }
    return render(request, 'dataprocessor/process.html', context)


# ═══════════════════════════════════════════════════════════════════════
# НӘТИЖЕЛЕР — браузерде көрсету
# ═══════════════════════════════════════════════════════════════════════

def results_view(request, file_id: int):
    """
    Өңдеу нәтижесін браузерде HTML кестелер / диаграммалар түрінде көрсетеді.
    ТЗ 2.3 — Өңдеу нәтижесі браузерде көрсетіледі.
    """
    file_record = get_object_or_404(UploadedFile, pk=file_id)
    result = get_object_or_404(ProcessingResult, uploaded_file=file_record)

    cleaning_changes = None
    if result.cleaning_before and result.cleaning_after:
        # Shape деректерін нормализациялау (ескі форматтан қорғану)
        before_shape = normalize_shape(result.cleaning_before.get('shape', {}))
        after_shape = normalize_shape(result.cleaning_after.get('shape', {}))
        
        # Null мәндерін қауіпсіз түрде өңдеу
        before_nulls = safe_get_int(result.cleaning_before.get('null_info', {}).get('total_nulls', 0))
        after_nulls = safe_get_int(result.cleaning_after.get('null_info', {}).get('total_nulls', 0))
        
        cleaning_changes = {
            'rows': after_shape['row_count'] - before_shape['row_count'],
            'columns': after_shape['column_count'] - before_shape['column_count'],
            'nulls': after_nulls - before_nulls,
        }

    stats_json = json.dumps(result.numeric_stats or {}) if result.numeric_stats is not None else '{}'

    context = {
        'file_record': file_record,
        'result': result,
        'download_form': ProcessingOptionsForm(),
        'cleaning_changes': cleaning_changes,
        'stats_json': stats_json,
    }
    return render(request, 'dataprocessor/results.html', context)


# ═══════════════════════════════════════════════════════════════════════
# ЖҮКТЕП АЛУ — CSV немесе JSON
# ═══════════════════════════════════════════════════════════════════════

def download_view(request, file_id: int):
    """
    Нәтижені CSV немесе JSON форматында жүктеп алуды қамтамасыз етеді.
    ТЗ 2.3 — Нәтижені жүктеп алуға болады.

    Query параметрі: ?format=csv немесе ?format=json
    """
    file_record = get_object_or_404(UploadedFile, pk=file_id)
    result = get_object_or_404(ProcessingResult, uploaded_file=file_record)

    fmt = request.GET.get('format', 'csv').lower()

    if fmt == 'json':
        content = export_result_as_json(result)
        content_type = 'application/json'
        filename = f'result_{file_record.pk}.json'
    else:
        content = export_result_as_csv(result)
        content_type = 'text/csv; charset=utf-8'
        filename = f'result_{file_record.pk}.csv'

    response = HttpResponse(content, content_type=content_type)
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


# ═══════════════════════════════════════════════════════════════════════
# ТАРИХ — барлық жүктелген файлдар
# ═══════════════════════════════════════════════════════════════════════

def history_view(request):
    """Жүктелген файлдар тарихын тізімдейді."""
    files = UploadedFile.objects.all()
    return render(request, 'dataprocessor/history.html', {'files': files})
