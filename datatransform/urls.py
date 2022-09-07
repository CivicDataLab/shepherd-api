from django.urls import path

from . import views
from django.views.decorators.csrf import csrf_exempt

urlpatterns = [
    path('trans_list', views.transformer_list, name='transformer_list'),
    path('pipe_create', csrf_exempt(views.pipe_create), name='pipe_create'),
    path('pipe_list', csrf_exempt(views.pipe_list), name='pipe_list'),
    path('res_transform', csrf_exempt(views.res_transform), name='res_transform'),
]
