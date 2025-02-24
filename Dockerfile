# Usar imagem Python oficial como base
FROM python:3.9-slim

# Definir variáveis de ambiente
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Definir diretório de trabalho
WORKDIR /app

# Copiar o arquivo requirements.txt primeiro
COPY requirements.txt /app/

# Instalar dependências do sistema e uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && export PATH="/root/.cargo/bin:$PATH"

# Criar ambiente virtual e instalar dependências com uv
RUN . ~/.bashrc && \
    uv venv && \
    uv pip install --system -r requirements.txt

# Copiar o resto do código
COPY . /app/

# Expor a porta que o Flask usará
EXPOSE 5000

# Comando para iniciar a aplicação
CMD ["python", "-m", "flask", "run", "--host=0.0.0.0"]