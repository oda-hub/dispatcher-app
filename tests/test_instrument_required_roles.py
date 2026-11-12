import pytest

from cdci_data_analysis.analysis.queries import InstrumentQuery
from cdci_data_analysis.flask_app import dispatcher_query
from cdci_data_analysis.flask_app.dispatcher_query import (
    INSTRUMENT_REQUIRED_ROLES,
    check_instrument_access_with_required_roles,
)


def test_ctao_sdc_requires_its_own_role():
    assert INSTRUMENT_REQUIRED_ROLES['ctao_sdc'] == ['ctao sdc']


@pytest.mark.parametrize('restricted_access', [True, False])
@pytest.mark.parametrize('roles, expected', [
    (None, False),
    ([], False),
    (['general'], False),
    (['oda workflow developer'], False),        # the usual development role is NOT enough
    (['ctao sdc user'], False),                 # exact role name only
    (['ctao sdc'], True),
    (['oda workflow developer', 'ctao sdc'], True),
])
def test_reserved_instrument_access(restricted_access, roles, expected):
    instr_query = InstrumentQuery('instr_query', restricted_access=restricted_access)
    assert check_instrument_access_with_required_roles('ctao_sdc', instr_query, roles) is expected


@pytest.mark.parametrize('restricted_access, roles, expected', [
    (False, None, True),
    (False, ['ctao sdc'], True),
    (True, None, False),
    (True, ['ctao sdc'], False),                # the CTAO role gives nothing on other instruments
    (True, ['oda workflow developer'], True),   # unchanged behaviour for other development instruments
])
def test_other_instruments_unchanged(restricted_access, roles, expected):
    instr_query = InstrumentQuery('instr_query', restricted_access=restricted_access)
    assert check_instrument_access_with_required_roles('some_other_instrument', instr_query, roles) is expected
    assert check_instrument_access_with_required_roles('some_other_instrument', instr_query, roles) \
        == instr_query.check_instrument_access(roles)


def test_mapping_is_used(monkeypatch):
    monkeypatch.setitem(dispatcher_query.INSTRUMENT_REQUIRED_ROLES, 'empty', ['special'])
    instr_query = InstrumentQuery('instr_query', restricted_access=False)
    assert not check_instrument_access_with_required_roles('empty', instr_query, None)
    assert check_instrument_access_with_required_roles('empty', instr_query, ['special'])
