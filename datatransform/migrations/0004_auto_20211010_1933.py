# Generated by Django 3.2.8 on 2021-10-10 19:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datatransform', '0003_pipeline_status'),
    ]

    operations = [
        migrations.AddField(
            model_name='pipeline',
            name='output_id',
            field=models.CharField(default='', max_length=200),
        ),
        migrations.AddField(
            model_name='pipeline',
            name='pipeline_name',
            field=models.CharField(default='', max_length=100),
        ),
        migrations.AddField(
            model_name='task',
            name='output_id',
            field=models.CharField(default='', max_length=200),
        ),
    ]
