from django.db import models

# External CRM models
class Account(models.Model):
    class Meta:
        managed = False
        db_table = 'account'

class Campaign(models.Model):
    class Meta:
        managed = False
        db_table = 'campaign'

class Funnel(models.Model):
    class Meta:
        managed = False
        db_table = 'funnel'

class Step(models.Model):
    class Meta:
        managed = False
        db_table = 'step'

class Lead(models.Model):
    class Meta:
        managed = False
        db_table = 'lead' 