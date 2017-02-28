# -*- coding: utf-8 -*-
# Generated by Django 1.10.1 on 2017-02-22 21:21
from __future__ import unicode_literals

from django.db import migrations, models
import django.db.models.deletion


def move_field(apps, schema_editor):
    fabato = apps.get_model("awards", "FinancialAccountsByAwardsTransactionObligations")
    for amount in fabato.objects.all():
        if amount.financial_accounts_by_awards:
            amount.financial_accounts_by_awards.transaction_obligated_amount = \
                amount.transaction_obligated_amount
            amount.save()
        else:  #if there's no associated financial account, skip.
            pass


def move_field_back(apps, schema_editor):
    faba = apps.get_model("awards", "FinancialAccountsByAwards")
    for amount in faba.objects.all():
        if amount.transaction_obligations:
            fabato = amount.transaction_obligations.last()
            fabato.transaction_obligated_amount = amount.transaction_obligated_amount
            fabato.save()


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0069_auto_20170222_2113'),
    ]

    operations = [
        migrations.RunPython(move_field, move_field_back),
    ]
