from django.db import models
from django.conf import settings

class MessageTemplate(models.Model):
    TEMPLATE_TYPES = [
        ('email', 'Email'),
        ('sms', 'SMS'),
        ('voice', 'Voice'),
        ('chat', 'Chat'),
    ]

    name = models.CharField(max_length=255)
    description = models.TextField(blank=True, null=True)
    template_type = models.CharField(max_length=20, choices=TEMPLATE_TYPES)
    subject = models.CharField(max_length=255, blank=True, null=True)
    content = models.TextField()
    variables = models.JSONField(blank=True, null=True, help_text="List of variables used in the template")
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name='created_templates')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'asc_messagetemplate'
        indexes = [
            models.Index(fields=['template_type']),
            models.Index(fields=['is_active']),
        ]

    def __str__(self):
        return f"{self.name} ({self.get_template_type_display()})" 