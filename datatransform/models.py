from django.db import models
import datetime

# Create your models here.


class Pipeline(models.Model):
    pipeline_id    = models.AutoField(primary_key=True)
    pipeline_name  = models.CharField(max_length=100, default="")
    output_id      = models.CharField(max_length=200, default="")
    created_at     = models.DateTimeField(default=datetime.datetime.now) 
    status         = models.CharField(max_length=50)
    dataset_id     = models.CharField(max_length=50)
    resource_identifier    = models.CharField(max_length=50, null=True)
    db_action = models.CharField(max_length=50, null=True)
    err_msg = models.CharField(max_length=200, null=True)
    resultant_res_id = models.CharField(max_length=20, null=True)
    # tasks = list()

class Task(models.Model):
    task_id        = models.AutoField(primary_key=True)
    task_name      = models.CharField(max_length=50)
    context        = models.CharField(max_length=500)
    status         = models.CharField(max_length=50)
    order_no       = models.IntegerField()
    created_at     = models.DateTimeField(default=datetime.datetime.now)
    result_url     = models.CharField(max_length=500)
    Pipeline_id    = models.ForeignKey(Pipeline, on_delete=models.CASCADE)
    output_id      = models.CharField(max_length=200, default="")


