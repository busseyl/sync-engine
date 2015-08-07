import json
import time
from gevent import Greenlet

import pytest
from tests.util.base import add_fake_message
from inbox.models import Namespace
from inbox.util.url import url_concat
from tests.api_legacy.base import api_client

GEVENT_EPSILON = .5  # Greenlet switching time. VMs on Macs suck :()
LONGPOLL_EPSILON = 1 + GEVENT_EPSILON  # API implementation polls every second

__all__ = ['api_client']


@pytest.yield_fixture
def streaming_test_client(db):
    from inbox.api.srv import app
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


@pytest.fixture
def api_prefix(default_namespace):
    return '/n/{}/delta/streaming'.format(default_namespace.public_id)


@pytest.fixture
def longpoll_prefix(default_namespace):
    return '/n/{}/delta/longpoll'.format(default_namespace.public_id)


def get_cursor(api_client, timestamp, namespace):
    cursor_response = api_client.post(
        '/n/{}/delta/generate_cursor'.format(namespace.public_id),
        data=json.dumps({'start': timestamp}))
    return json.loads(cursor_response.data)['cursor']


def validate_response_format(response_string):
    response = json.loads(response_string)
    assert 'cursor' in response
    assert 'attributes' in response
    assert 'object' in response
    assert 'id' in response
    assert 'event' in response


def test_response_when_old_cursor_given(db, api_prefix, streaming_test_client,
                                        default_namespace):
    url = url_concat(api_prefix, {'timeout': .1,
                                  'cursor': '0'})
    r = streaming_test_client.get(url)
    assert r.status_code == 200
    responses = r.data.split('\n')
    for response_string in responses:
        if response_string:
            validate_response_format(response_string)


def test_empty_response_when_latest_cursor_given(db, api_prefix,
                                                 streaming_test_client,
                                                 default_namespace):
    cursor = get_cursor(streaming_test_client, int(time.time() + 22),
                        default_namespace)
    url = url_concat(api_prefix, {'timeout': .1,
                                  'cursor': cursor})
    r = streaming_test_client.get(url)
    assert r.status_code == 200
    assert r.data.strip() == ''


def test_gracefully_handle_new_namespace(db, streaming_test_client):
    new_namespace = Namespace()
    new_account = Account()
    new_namespace.account = new_account
    db.session.add(new_namespace)
    db.session.add(new_account)
    db.session.commit()
    cursor = get_cursor(streaming_test_client, int(time.time()),
                        new_namespace)
    url = url_concat('/n/{}/delta/streaming'.format(new_namespace.public_id),
                     {'timeout': .1, 'cursor': cursor})
    r = streaming_test_client.get(url)
    assert r.status_code == 200


def test_exclude_and_include_object_types(db, api_prefix,
                                          streaming_test_client, thread,
                                          default_namespace):

    add_fake_message(db.session, default_namespace.id, thread,
                     from_addr=[('Bob', 'bob@foocorp.com')])
    # Check that we do get message and contact changes by default.
    url = url_concat(api_prefix, {'timeout': .1,
                                  'cursor': '0'})
    r = streaming_test_client.get(url)
    assert r.status_code == 200
    responses = r.data.split('\n')
    parsed_responses = [json.loads(resp) for resp in responses if resp != '']
    assert any(resp['object'] == 'message' for resp in parsed_responses)
    assert any(resp['object'] == 'contact' for resp in parsed_responses)

    # And check that we don't get message/contact changes if we exclude them.
    url = url_concat(api_prefix, {'timeout': .1,
                                  'cursor': '0',
                                  'exclude_types': 'message,contact'})
    r = streaming_test_client.get(url)
    assert r.status_code == 200
    responses = r.data.split('\n')
    parsed_responses = [json.loads(resp) for resp in responses if resp != '']
    assert not any(resp['object'] == 'message' for resp in parsed_responses)
    assert not any(resp['object'] == 'contact' for resp in parsed_responses)

    # And check we only get message objects if we use include_types
    url = url_concat(api_prefix, {'timeout': .1,
                                  'cursor': '0',
                                  'include_types': 'message'})
    r = streaming_test_client.get(url)
    assert r.status_code == 200
    responses = r.data.split('\n')
    parsed_responses = [json.loads(resp) for resp in responses if resp != '']
    assert all(resp['object'] == 'message' for resp in parsed_responses)


def test_invalid_timestamp(streaming_test_client, default_namespace):
    # Valid UNIX timestamp
    response = streaming_test_client.post(
        '/n/{}/delta/generate_cursor'.format(default_namespace.public_id),
        data=json.dumps({'start': int(time.time())}))
    assert response.status_code == 200

    # Invalid timestamp
    response = streaming_test_client.post(
        '/n/{}/delta/generate_cursor'.format(default_namespace.public_id),
        data=json.dumps({'start': 1434591487647}))
    assert response.status_code == 400


def test_longpoll_delta_newitem(db, longpoll_prefix, streaming_test_client,
                                default_namespace, thread):
    cursor = get_cursor(streaming_test_client, int(time.time() + 22),
                        default_namespace)
    url = url_concat(longpoll_prefix, {'cursor': cursor})
    start_time = time.time()
    # Spawn the request in background greenlet
    longpoll_greenlet = Greenlet.spawn(streaming_test_client.get, url)
    # This should make it return immediately
    add_fake_message(db.session, default_namespace.id, thread,
                     from_addr=[('Bob', 'bob@foocorp.com')])
    longpoll_greenlet.join()  # now block and wait
    end_time = time.time()
    assert end_time - start_time < LONGPOLL_EPSILON
    parsed_responses = json.loads(longpoll_greenlet.value.data)
    assert len(parsed_responses['deltas']) == 3
    assert set(k['object'] for k in parsed_responses['deltas']) == \
           set([u'message', u'contact', u'thread'])


def test_longpoll_delta_timeout(db, longpoll_prefix, streaming_test_client,
                                default_namespace):
    test_timeout = 2
    cursor = get_cursor(streaming_test_client, int(time.time() + 22),
                        default_namespace)
    url = url_concat(longpoll_prefix, {'timeout': test_timeout,
                                       'cursor': cursor})
    start_time = time.time()
    resp = streaming_test_client.get(url)
    end_time = time.time()
    assert resp.status_code == 200

    assert end_time - start_time - test_timeout < GEVENT_EPSILON
    parsed_responses = json.loads(resp.data)
    assert len(parsed_responses['deltas']) == 0
    assert type(parsed_responses['deltas']) == list
    assert parsed_responses['cursor_start'] == cursor
    assert parsed_responses['cursor_end'] == cursor