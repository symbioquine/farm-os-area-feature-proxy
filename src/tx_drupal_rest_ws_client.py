#!/bin/env python3

from io import BytesIO

from twisted.internet import reactor, task, defer
from twisted.python import compat
from twisted.web.client import Agent, CookieAgent, readBody, FileBodyProducer, HTTPConnectionPool
from twisted.web.http_headers import Headers

from urllib.parse import urlencode, urlparse, parse_qs
from datetime import datetime, timezone, timedelta

import warnings, json

class TxDrupalRestWsClient(object):
    def __init__(self, drupal_url, user, password, reactor, tx_agent, cookie_jar, user_agent):
        self._drupal_url = drupal_url
        self._user = user
        self._password = password
        self._reactor = reactor
        self._tx_agent = tx_agent
        self._cookie_jar = cookie_jar
        self._user_agent = user_agent

        self._login_url = self.format_url('user/login')
        self._session_token_url = self.format_url('restws/session/token')

        self._session_lock = defer.DeferredLock()
        self._csrf_token = None

    @classmethod
    def create(cls, drupal_url, user, password, reactor=None, cookie_jar=None, user_agent="TxDrupalRestWsClient"):
        if cookie_jar is None:
            cookie_jar = compat.cookielib.CookieJar()

        if reactor is None:
            import twisted.internet
            reactor = twisted.internet.reactor

        pool = HTTPConnectionPool(reactor)

        tx_agent = CookieAgent(Agent(reactor, pool=pool), cookie_jar)

        return cls(drupal_url, user, password, reactor, tx_agent, cookie_jar, user_agent)

    @defer.inlineCallbacks
    def get_entity(self, entity_type, entity_id):
        headers = yield self.get_authenticated_headers()

        response = yield self._tx_agent.request(b'GET', 
            self.format_url('{entity_type}/{entity_id}.json', entity_type=entity_type, entity_id=entity_id),
            headers, None)

        if response.code != 200:
            raise Exception("Failed to get entity: " + entity_id)

        result = yield _read_body_no_warn(response)

        return json.loads(result)

    @defer.inlineCallbacks
    def get_entities(self, entity_type, filters):
        raw_page = yield self._get_entities(entity_type, filters)

        return TxDrupalEntityPage(self, entity_type, filters, raw_page)

    @defer.inlineCallbacks
    def _get_entities(self, entity_type, filters):
        headers = yield self.get_authenticated_headers()

        response = yield self._tx_agent.request(b'GET', 
            self.format_url('{entity_type}.json', query_params=filters, entity_type=entity_type),
            headers, None)

        if response.code != 200:
            raise Exception("Failed to get entities of type: " + entity_type)

        result = yield _read_body_no_warn(response)

        return json.loads(result)

    @defer.inlineCallbacks
    def get_all_entities(self, entity_type, filters):
        all_entities = []

        page = yield self.get_entities(entity_type, filters)

        while page:
            all_entities.extend(page)

            page = yield page.next_page(forgetful=True)

        return TxDrupalEntityPage(self, entity_type, filters, {'list': all_entities})

    @defer.inlineCallbacks
    def create_entity(self, entity_type, record):
        headers = yield self.get_authenticated_headers({'Content-Type': ['application/json']})

        body = FileBodyProducer(BytesIO(json.dumps(record).encode('utf-8')))

        response = yield self._tx_agent.request(b'POST', 
            self.format_url('{entity_type}', entity_type=entity_type),
            headers, body)

        result = yield _read_body_no_warn(response)

        if response.code != 201:
            raise Exception("".join(map(str, ("Failed to create entity of type", entity_type, response.code, result))))

        return json.loads(result)

    @defer.inlineCallbacks
    def update_entity(self, entity_type, entity_id, record):
        headers = yield self.get_authenticated_headers({'Content-Type': ['application/json']})

        body = FileBodyProducer(BytesIO(json.dumps(record).encode('utf-8')))

        response = yield self._tx_agent.request(b'PUT', 
            self.format_url('{entity_type}/{entity_id}', entity_type=entity_type, entity_id=entity_id),
            headers, body)

        if response.code != 200:
            result = yield _read_body_no_warn(response)

            raise Exception("".join(map(str, ("Failed to update entity of type", entity_type, response.code, result))))

    @defer.inlineCallbacks
    def delete_entity(self, entity_type, entity_id):
        headers = yield self.get_authenticated_headers()

        response = yield self._tx_agent.request(b'DELETE',
            self.format_url('{entity_type}/{entity_id}', entity_type=entity_type, entity_id=entity_id),
            headers, None)

        if response.code != 200:
            raise Exception("Failed to get entities of type: " + entity_type)

    def agent(self):
        return self._tx_agent

    def format_url(self, path_template, query_params=None, **kwargs):
        url = self._drupal_url + '/' + path_template.format(**kwargs)

        if query_params:
            url += '?' + urlencode(query_params)

        return url.encode('utf-8')

    @defer.inlineCallbacks
    def get_authenticated_headers(self, extra_headers=None):
        yield self._ensure_authenticated()

        header_values = {
            'User-Agent': [self._user_agent],
            'X-CSRF-Token': [self._csrf_token]
        }

        if extra_headers:
            header_values.update(extra_headers)

        defer.returnValue(Headers(header_values))

    @defer.inlineCallbacks
    def _ensure_authenticated(self):
        '''
        Returns a Deferred that will succeed when a RestWS session token is available or error if one cannot be retrieved.
        '''
        if self._csrf_token is not None and datetime.now(timezone.utc) < self._session_expiry:
            return defer.returnValue(True)

        yield self._session_lock.acquire()
        try:
            if self._csrf_token is not None and datetime.now(timezone.utc) < self._session_expiry:
                return defer.returnValue(True)

            login_args = {
                'name': self._user,
                'pass': self._password,
                'form_id': 'user_login'
            }

            body = FileBodyProducer(BytesIO(urlencode(login_args).encode('utf-8')))

            response = yield self._tx_agent.request(b'POST', self._login_url,
                Headers({
                    'User-Agent': [self._user_agent],
                    'Content-Type': ["application/x-www-form-urlencoded"]
                }), body)

            if response.code != 302:
                raise Exception("Login failed")

            response = yield self._tx_agent.request(b'GET', self._session_token_url,
                Headers({
                    'User-Agent': [self._user_agent]
                }), None)

            if response.code != 200:
                raise Exception("Session token retrieval failed")

            self._csrf_token = yield _read_body_no_warn(response)
            self._session_expiry = self._derive_session_expiry_date_time()

            return True
        finally:
            self._session_lock.release()

    def _derive_session_expiry_date_time(self):
        session_cookie_expiries = []
        all_cookie_expiries = []

        for cookie in self._cookie_jar:
            cookie_expiry = cookie.expires

            if cookie_expiry is None:
                continue

            cookie_expiry_date_time = datetime.fromtimestamp(cookie_expiry, timezone.utc)

            if cookie.name.startswith("SESS"):
                session_cookie_expiries.append(cookie_expiry_date_time)

            all_cookie_expiries.append(cookie_expiry_date_time)

        if session_cookie_expiries:
            return min(session_cookie_expiries)

        if all_cookie_expiries:
            return min(all_cookie_expiries)

        return datetime.now(timezone.utc) + timedelta(hours=24)

class TxDrupalEntityPage(object):
    def __init__(self, client, entity_type, filters, raw_current_page, prev_page_ref=None, next_page_ref=None):
        self._client = client
        self._entity_type = entity_type
        self._filters = filters
        self._raw_current_page = raw_current_page
        self._prev_page_ref = prev_page_ref
        self._next_page_ref = next_page_ref
        self.page_num = int(self._filters.get('page', '0'))
        self._max_page_num = int(parse_qs(urlparse(self._raw_current_page.get('last', '?page=0')).query).get('page', ['0'])[0])

    def __len__(self):
        return len(self._raw_current_page.get('list', []))

    def __iter__(self):
        return iter(self._raw_current_page.get('list', []))

    def prev_page(self, forgetful=False):
        return self._get_page(target_page_num=self.page_num - 1, forgetful=forgetful, page_ref_var='_prev_page_ref', back_ref_var='_next_page_ref')

    def has_prev_page(self):
        return self.page_num > 0

    def next_page(self, forgetful=False):
        return self._get_page(target_page_num=self.page_num + 1, forgetful=forgetful, page_ref_var='_next_page_ref', back_ref_var='_prev_page_ref')

    def has_next_page(self):
        return self.page_num < self._max_page_num

    @defer.inlineCallbacks
    def _get_page(self, target_page_num, forgetful, page_ref_var, back_ref_var):
        # Make sure this fn is always a generator
        yield defer.succeed(True)

        if target_page_num < 0 or target_page_num > self._max_page_num:
            return None

        target_page_ref = getattr(self, page_ref_var)

        if not target_page_ref is None:
            return target_page_ref

        target_page_filters = dict(**self._filters)
        target_page_filters['page'] = str(self.page_num + 1)

        raw_page = yield self._client._get_entities(self._entity_type, target_page_filters)

        this_page_ref = None if forgetful else self

        target_page_ref = TxDrupalEntityPage(self._client, self._entity_type, target_page_filters, raw_page)

        setattr(target_page_ref, back_ref_var, this_page_ref)
        setattr(self, page_ref_var, target_page_ref)

        return target_page_ref

def _read_body_no_warn(response):
    with warnings.catch_warnings():
        # readBody has a buggy DeprecationWarning:
        # hhttps://twistedmatrix.com/trac/ticket/8227
        warnings.simplefilter('ignore', category=DeprecationWarning)
        return readBody(response)
