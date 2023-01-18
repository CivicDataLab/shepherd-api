class SimpleMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        response = self.get_response(request)
        response.__setitem__('Server', '')
        response['X-XSS-Protection'] = "1; mode=block"
        response.headers['Server'] = "Not Available!"
        return response