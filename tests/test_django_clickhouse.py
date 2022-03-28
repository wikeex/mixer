from __future__ import absolute_import
from tests.django_app.ch_models import CHAccount

import pytest

from mixer.backend.django_clickhouse import Mixer


@pytest.fixture(autouse=True)
def mixer(request):
    return Mixer()


def test_base(mixer):

    account = mixer.blend(CHAccount)
    assert isinstance(account.name, str)
