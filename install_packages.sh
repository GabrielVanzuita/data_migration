#!/bin/bash

# Atualizar e instalar dependências do sistema (caso necessário)
sudo apt update
sudo apt install python3-venv python3-pip -y

# Criar um novo ambiente virtual
python3 -m venv venv

# Ativar o ambiente virtual
source venv/bin/activate

# Atualizar o pip no ambiente virtual
pip install --upgrade pip

# Instalar os pacotes necessários
pip install pymongo mysql-connector-python pandas

# Instalar outros pacotes que você possa precisar (se houver alguma dependência específica)
# pip install <outro-pacote>

# Informações sobre o ambiente
echo "Ambiente virtual e pacotes instalados com sucesso!"

# Listar pacotes instalados
pip list

echo "Agora você pode usar seu ambiente virtual com os pacotes necessários!"

