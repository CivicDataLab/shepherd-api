from django.urls import path

from . import views
from django.views.decorators.csrf import csrf_exempt

urlpatterns = [
    path('trans_list', views.transformer_list, name='transformer_list'),
    path('pipe_create', csrf_exempt(views.pipe_create), name='pipe_create'),
    path('pipe_update', csrf_exempt(views.pipe_update), name='pipe_update'),
    path('pipe_list', csrf_exempt(views.pipe_list), name='pipe_list'),
    path('res_transform', csrf_exempt(views.res_transform), name='res_transform'),
    path('pipeline_filter', csrf_exempt(views.pipeline_filter), name='pipeline_filter'),
    path('custom_data_viewer', csrf_exempt(views.custom_data_viewer), name='custom_data_viewer'),
    path('api_source_query', csrf_exempt(views.api_source_query), name='api_source_query'),
    path('api_res_transform', csrf_exempt(views.api_res_transform), name='api_res_transform'),
    path('api_res_run_transform', csrf_exempt(views.api_res_run_transform), name='api_res_run_transform'),
    path('api_transformer_list', csrf_exempt(views.api_transformer_list), name='api_transformer_list'),
    path('delete_api_res_transform', csrf_exempt(views.delete_api_res_transform), name='delete_api_res_transform'),
    path('clone_pipe', csrf_exempt(views.clone_pipe), name='clone_pipe')
]