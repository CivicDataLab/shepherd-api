# Generated by Django 3.2.8 on 2022-09-15 11:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datatransform', '0005_merge_0004_auto_20211009_0843_0004_auto_20211010_1933'),
    ]

    operations = [
        migrations.AddField(
            model_name='pipeline',
            name='dataset_id',
            field=models.CharField(default='', max_length=50),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='pipeline',
            name='resource_id',
            field=models.CharField(default='', max_length=50),
            preserve_default=False,
        ),
    ]
