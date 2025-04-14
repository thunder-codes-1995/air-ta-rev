import traceback

from base.redis import Redis

from .routes import Route


class PreRequestSignal:
    @classmethod
    def run(cls):
        """will be triggered before each request"""
        Route.authorize()


class PostRequestSignal:
    @classmethod
    def run(cls, response):
        """will be triggered after each request"""
        ...


class OnInitSignal:
    @classmethod
    def run(cls):
        """will be triggered when the app first runs"""
        # logger.info(f'Starting Flask on port:{os.getenv("APP_PORT")}')
        # clear redis cache on start (if redis is enabled by configuration)
        Redis().clear()


class OnErrorSignal:
    @classmethod
    def run(cls, e: Exception):
        """will be triggered upon errors"""
        print(traceback.format_exc())
        return {"message": str(e)}
