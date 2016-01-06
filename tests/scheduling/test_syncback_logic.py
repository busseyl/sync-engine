import pytest

from inbox.ignition import engine_manager
from inbox.transactions.actions import SyncbackService
from inbox.models import Account, Namespace
from inbox.models.session import session_scope_by_shard_id, session_scope


@pytest.yield_fixture
def patched_enginemanager(monkeypatch):
    engines = {k: None for k in range(0, 6)}
    monkeypatch.setattr('inbox.ignition.engine_manager.engines', engines)
    yield
    monkeypatch.undo()


def test_all_accounts_are_assigned_exactly_once(patched_enginemanager):
    assigned_keys = []

    service = SyncbackService(cpu_id=0, total_cpus=2)
    assert service.keys == [0, 2, 4]
    assigned_keys.extend(service.keys)

    service = SyncbackService(cpu_id=1, total_cpus=2)
    assert service.keys == [1, 3, 5]
    assigned_keys.extend(service.keys)

    # All keys are assigned (therefore all accounts are assigned)
    assert set(engine_manager.engines.keys()) == set(assigned_keys)
    # No key is assigned more than once (and therefore, no account)
    assert len(assigned_keys) == len(set(assigned_keys))
