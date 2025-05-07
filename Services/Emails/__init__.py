from .Notify import notify_bp
from .EmailWatcher import emailwatcher_bp   

# Make sure these are explicitly exported
__all__ = ['notify_bp', 'emailwatcher_bp']
