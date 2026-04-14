"""
Деректер модельдері.

UploadedFile — жүктелген файл туралы ақпаратты сақтайды.
ProcessingResult — өңдеу нәтижелерін сақтайды.
"""

from django.db import models
import os


class UploadedFile(models.Model):
    """Пайдаланушы жүктеген файл."""

    # Файл дискте сақталатын жол
    file = models.FileField(upload_to='uploads/')

    # Оригинал файл атауы (браузерде)
    original_name = models.CharField(max_length=255)

    # Файл өлшемі байтпен
    file_size = models.BigIntegerField(default=0)

    # Файл типі: csv, json, txt, xlsx
    file_type = models.CharField(max_length=10, blank=True)

    # Жүктелген уақыт
    uploaded_at = models.DateTimeField(auto_now_add=True)

    # Өңделді ме?
    is_processed = models.BooleanField(default=False)

    class Meta:
        verbose_name = 'Жүктелген файл'
        verbose_name_plural = 'Жүктелген файлдар'
        ordering = ['-uploaded_at']

    def __str__(self):
        return self.original_name

    def save(self, *args, **kwargs):
        """Сақтау кезінде файл типін автоматты анықтайды."""
        if self.original_name:
            _, ext = os.path.splitext(self.original_name)
            self.file_type = ext.lower().lstrip('.')
        super().save(*args, **kwargs)

    def get_file_size_mb(self):
        """Файл өлшемін МБ форматында қайтарады."""
        return round(self.file_size / (1024 * 1024), 2)


class ProcessingResult(models.Model):
    """Файлды өңдеу нәтижесі."""

    # Қандай файлға байланысты
    uploaded_file = models.OneToOneField(
        UploadedFile,
        on_delete=models.CASCADE,
        related_name='result'
    )

    # ТЗ 2.2 — Жол/баған санын шығару
    row_count = models.IntegerField(null=True, blank=True)
    column_count = models.IntegerField(null=True, blank=True)
    column_names = models.JSONField(null=True, blank=True)  # баған атаулары тізімі

    # ТЗ 2.2 — Бос мәндерді (null) анықтау және есептеу
    null_counts = models.JSONField(null=True, blank=True)   # {баған: null саны}
    total_nulls = models.IntegerField(null=True, blank=True)

    # ТЗ 2.2 — Сандық бағандар бойынша статистика
    numeric_stats = models.JSONField(null=True, blank=True)  # {баған: {min, max, mean}}

    # ТЗ 2.2 — Ең жиі кездесетін мәндер (top-N)
    top_values = models.JSONField(null=True, blank=True)    # {баған: [(мән, саны), ...]}

    # Өңдеу уақыты (секунд)
    processing_time = models.FloatField(null=True, blank=True)

    # Өңдеу күні
    processed_at = models.DateTimeField(auto_now_add=True)

    # Деректерді тазалау алдындағы және одан кейінгі қорытындылар
    cleaning_before = models.JSONField(null=True, blank=True)
    cleaning_after = models.JSONField(null=True, blank=True)
    cleaning_diff = models.JSONField(null=True, blank=True)

    # Қате болса хабарлама
    error_message = models.TextField(blank=True)

    class Meta:
        verbose_name = 'Өңдеу нәтижесі'
        verbose_name_plural = 'Өңдеу нәтижелері'

    def __str__(self):
        return f"Нәтиже: {self.uploaded_file.original_name}"
