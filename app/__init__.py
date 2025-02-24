from flask import Flask

def create_app():
    app = Flask(__name__)
    
    # Configuração da aplicação
    app.config.from_object('config.settings')
    app.config.from_object('config.secure')
    
    # Registrar blueprints
    from app.api import bp as api_bp
    app.register_blueprint(api_bp, url_prefix='/api')
    
    @app.route('/')
    def index():
        return 'ONS Energy Viz está funcionando!'
    
    return app