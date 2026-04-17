from django.contrib import admin
from .models import UploadedFile, ProcessingResult

admin.site.register(UploadedFile)
admin.site.register(ProcessingResult)
