"""
Django management command - обработка результатов.

Использование:
    python manage.py clear_processing_results
"""

from django.core.management.base import BaseCommand
from dataprocessor.models import ProcessingResult, UploadedFile


class Command(BaseCommand):
    help = 'Өндеу нәтижелері кестесін тазалау (ескі қатесі бар деректерді өшіру)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--all',
            action='store_true',
            help='Барлық нәтижелерді өшіру',
        )

    def handle(self, *args, **options):
        if options['all']:
            count = ProcessingResult.objects.count()
            ProcessingResult.objects.all().delete()
            self.stdout.write(
                self.style.SUCCESS(f'✅ {count} нәтиже өшірілді')
            )
        else:
            # Ескі format-та (tuple) тұрғындарының санын есептеу
            old_results = []
            for result in ProcessingResult.objects.all():
                if result.cleaning_before and isinstance(result.cleaning_before.get('shape'), tuple):
                    old_results.append(result.id)

            if old_results:
                count = len(old_results)
                ProcessingResult.objects.filter(id__in=old_results).delete()
                self.stdout.write(
                    self.style.SUCCESS(f'✅ {count} ескі формат нәтижесі өшірілді')
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS('✅ Ескі формат деректері табылмады')
                )
