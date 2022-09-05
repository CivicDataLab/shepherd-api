import os

from datatransform.models import Task, Pipeline
import django
django.setup()


os.environ.setdefault("DJANGO_SETTINGS_MODULE", "dataplatform.settings")
print("done")