import logging

from functools import wraps

from twisted.internet import defer
from twisted.web.server import NOT_DONE_YET


def deferred_rendering_fn(f):
    @wraps(f)
    def wrapper(self, request):
        @defer.inlineCallbacks
        def _inner(request):
            try:
                result = yield defer.maybeDeferred(f, self, request)
                request.write(result)
            except:
                logging.error(logging.traceback.format_exc())
                request.setResponseCode(500)
            request.finish()
        _inner(request)
        return NOT_DONE_YET
    return wrapper
