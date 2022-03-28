""" Django ClickHouse support. """
from __future__ import absolute_import

import datetime as dt
import decimal
import enum
import random
from types import GeneratorType
from django.contrib.contenttypes.fields import GenericForeignKey, GenericRelation   # noqa


from .. import mix_types as t, _compat as _
from ..main import (
    SKIP_VALUE, TypeMixerMeta, TypeMixer as BaseTypeMixer,
    GenFactory as BaseFactory, Mixer as BaseMixer)
from infi.clickhouse_orm import fields


class GenFactory(BaseFactory):

    """ Map a clickhouse classes to simple types. """

    types = {
        fields.StringField: str,
        fields.FixedStringField: str,
        fields.DateField: dt.date,
        fields.DateTimeField: dt.datetime,
        fields.DateTime64Field: dt.datetime,

        fields.Float32Field: float,
        fields.Float64Field: float,
        fields.DecimalField: decimal.Decimal,
        fields.Decimal32Field: decimal.Decimal,
        fields.Decimal64Field: decimal.Decimal,
        fields.Decimal128Field: decimal.Decimal,
        fields.Enum8Field: enum.Enum,
        fields.Enum16Field: enum.Enum,
        fields.ArrayField: list,
        fields.UUIDField: t.UUID,
        fields.IPv4Field: t.IP4String,
        fields.IPv6Field: t.IP6String,
        fields.NullableField: None
    }

    generators = {
        fields.UInt8Field: lambda: random.randint(0, 2**8-1),
        fields.UInt16Field: lambda: random.randint(0, 2**16-1),
        fields.UInt32Field: lambda: random.randint(0, 2**32-1),
        fields.UInt64Field: lambda: random.randint(0, 2**64-1),
        fields.Int8Field: lambda: random.randint(-2**7, 2**7-1),
        fields.Int16Field: lambda: random.randint(-2**15, 2**15-1),
        fields.Int32Field: lambda: random.randint(-2**31, 2**31-1),
        fields.Int64Field: lambda: random.randint(-2**63, 2**63-1),
        fields.LowCardinalityField: None
    }


class TypeMixer(_.with_metaclass(TypeMixerMeta, BaseTypeMixer)):

    """ TypeMixer for Django Clickhouse. """

    __metaclass__ = TypeMixerMeta

    factory = GenFactory

    def get_value(self, name, value):
        """ Set value to generated instance.

        :return : None or (name, value) for later use

        """
        field = self.__fields.get(name)
        if field:

            return self._get_value(name, value, field)

        return super(TypeMixer, self).get_value(name, value)

    def _get_value(self, name, value, field=None):

        if isinstance(value, GeneratorType):
            return self._get_value(name, next(value), field)

        if not isinstance(value, t.Mix) and value is not SKIP_VALUE:

            if callable(value):
                return self._get_value(name, value(), field)

        return name, value

    def get_fabric(self, field, field_name=None, fake=None):
        """ Get an objects fabric for field and cache it.

        :param field: Field for looking a fabric
        :param field_name: Name of field for generation
        :param fake: Generate fake data instead of random data.

        :return function:

        """
        if fake is None:
            fake = self.__fake

        if field.params:
            return self.make_fabric(field.scheme, field_name, fake, kwargs=field.params)

        key = (field.scheme.creation_counter, field_name, fake)

        if key not in self.__fabrics:
            self.__fabrics[key] = self.make_fabric(field.scheme, field_name, fake)

        return self.__fabrics[key]

    def make_fabric(self, field, fname=None, fake=False, kwargs=None): # noqa
        """ Make a fabric for field.

        :param field: A mixer field
        :param fname: Field name
        :param fake: Force fake data

        :return function:

        """
        kwargs = {} if kwargs is None else kwargs

        fcls = type(field)
        stype = self.__factory.cls_to_simple(fcls)

        if stype in (str, t.Text):
            fab = super(TypeMixer, self).make_fabric(
                fcls, field_name=fname, fake=fake, kwargs=kwargs)
            return lambda: fab()

        if stype is decimal.Decimal:
            kwargs['left_digits'] = field.max_digits - field.decimal_places
            kwargs['right_digits'] = field.decimal_places

        return super(TypeMixer, self).make_fabric(
            fcls, field_name=fname, fake=fake, kwargs=kwargs)

    def guard(self, *args, **kwargs):
        """ Look objects in database.

        :returns: A finded object or False

        """
        qs = self.__scheme.objects.filter(*args, **kwargs)
        count = qs.count()

        if count == 1:
            return qs.get()

        if count:
            return list(qs)

        return False

    def __load_fields(self):

        for field_name, field in self.__scheme._fields.items():

            yield field_name, t.Field(field, field_name)


class Mixer(BaseMixer):

    """ Integration with Django ClickHouse. """

    type_mixer_cls = TypeMixer

    def __init__(self, commit=True, **params):
        """Initialize Mixer instance.

        :param commit: (True) Save object to database.

        """
        super(Mixer, self).__init__(**params)
        self.params['commit'] = commit

    def postprocess(self, target):
        """ Save objects in db.

        :return value: A generated value

        """
        if self.params.get('commit'):
            db = target.get_database(for_write=True)
            db.insert([target], batch_size=1)

        return target


# Default mixer
mixer = Mixer()
