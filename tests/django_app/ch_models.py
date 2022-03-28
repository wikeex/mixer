from django_clickhouse.clickhouse_models import ClickHouseModel
from infi.clickhouse_orm import fields


class CHAccount(ClickHouseModel):
    """
    account data
    """
    id = fields.StringField()
    account_id = fields.UInt32Field()
    account_type = fields.UInt32Field()
    name = fields.StringField()
    update_time = fields.Int64Field()

    class Meta:
        verbose_name = 'account data'
        verbose_name_plural = verbose_name
        ordering = ('-update_time',)

    def __str__(self):
        return '{}_{}_{}'.format(self.account_id, self.account_type, self.name)