import os

# Configurações básicas
DEBUG = os.environ.get('FLASK_DEBUG', '0') == '1'
TESTING = False
SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_key_for_development')

# Configurações AWS
AWS_BUCKET_NAME = os.environ.get('AWS_BUCKET_NAME', 'ons-aws-prod-opendata')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Configurações de cache
CACHE_DIR = os.environ.get('CACHE_DIR', 'data/cache')