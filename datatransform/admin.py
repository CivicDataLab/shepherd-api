from django.contrib import admin

# Register your models here.
from .models import Task, Pipeline

admin.site.register(Task)
admin.site.register(Pipeline)
