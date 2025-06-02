from flask import Blueprint
from flask_cors import CORS

connect_bp = Blueprint('connect', __name__, url_prefix='/connect')
listings_bp = Blueprint('listings', __name__, url_prefix='/listings')
data_bp = Blueprint('data', __name__, url_prefix='/data')

# Enable CORS for all blueprints
CORS(connect_bp, origins='*', methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allow_headers=['Content-Type', 'Authorization'])
CORS(listings_bp, origins='*', methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allow_headers=['Content-Type', 'Authorization'])
CORS(data_bp, origins='*', methods=['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allow_headers=['Content-Type', 'Authorization'])

from .Connect import *
from .Listings import *
from .Data import *
# Make sure these are explicitly exported
__all__ = ['connect_bp', 'listings_bp', 'data_bp']
