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


def build_cleaning_diff(original_df, cleaned_df):
    diff = []
    normalized_before = original_df.copy()
    normalized_before.columns = normalized_before.columns.str.lower()

    # Column renames
    for orig_col, clean_col in zip(original_df.columns, normalized_before.columns):
        if orig_col != clean_col:
            diff.append({
                'object': 'column name',
                'field': orig_col,
                'before': orig_col,
                'after': clean_col,
                'note': 'Column name lowercased',
            })

    # Duplicate rows removed
    duplicate_removed = len(normalized_before) - len(cleaned_df)
    if duplicate_removed > 0:
        diff.append({
            'object': 'rows',
            'field': None,
            'before': len(normalized_before),
            'after': len(cleaned_df),
            'note': f'{duplicate_removed} duplicate row(s) removed',
        })

    before_unique = normalized_before.drop_duplicates(keep='first').reset_index(drop=True)
    after_reset = cleaned_df.reset_index(drop=True)
    compare_rows = min(len(before_unique), len(after_reset))

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

    for col in after_reset.columns:
        if col not in before_unique.columns:
            continue
        for row_idx in range(compare_rows):
            before_value = before_unique[col].iloc[row_idx]
            after_value = after_reset[col].iloc[row_idx]
            if pd.isna(before_value) and pd.isna(after_value):
                continue
            if before_value == after_value:
                continue

            note = 'Changed value'
            if isinstance(before_value, str) and isinstance(after_value, str) and before_value.strip() == after_value:
                note = 'Whitespace trimmed'
            elif pd.isna(before_value) and not pd.isna(after_value):
                note = 'Missing value filled'
            elif not pd.isna(before_value) and pd.isna(after_value):
                note = 'Value removed'

            diff.append({
                'object': 'cell',
                'field': col,
                'row': row_idx + 1,
                'before': normalize_value(before_value),
                'after': normalize_value(after_value),
                'note': note,
            })
            if len(diff) >= 20:
                break
        if len(diff) >= 20:
            break

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
                    result_data['numeric_stats'] = get_numeric_stats(cleaned_df)

                # ТЗ 2.2 — Top-N мәндер
                if opts.get('show_top_values'):
                    top_n = opts.get('top_n') or 5
                    result_data['top_values'] = get_top_values(cleaned_df, top_n=top_n)

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
        cleaning_changes = {
            'rows': result.cleaning_after['shape']['row_count'] - result.cleaning_before['shape']['row_count'],
            'columns': result.cleaning_after['shape']['column_count'] - result.cleaning_before['shape']['column_count'],
            'nulls': result.cleaning_after['null_info']['total_nulls'] - result.cleaning_before['null_info']['total_nulls'],
        }

    context = {
        'file_record': file_record,
        'result': result,
        'download_form': ProcessingOptionsForm(),
        'cleaning_changes': cleaning_changes,
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
