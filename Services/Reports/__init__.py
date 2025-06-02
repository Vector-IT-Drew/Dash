from flask import Blueprint
from flask_cors import CORS

reports_bp = Blueprint('reports', __name__, url_prefix='/reports')

# Enable CORS for all routes in this blueprint
CORS(reports_bp, origins='*', methods=['GET', 'POST', 'OPTIONS'], allow_headers=['Content-Type'])

from . import generate_report
