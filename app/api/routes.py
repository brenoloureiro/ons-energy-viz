from flask import jsonify
from app.api import bp

@bp.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'})