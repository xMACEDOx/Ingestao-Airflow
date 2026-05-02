set -e
 
echo ">>> [1/4] Aguardando banco de dados ficar pronto..."
until airflow db check; do
  echo "    Banco ainda não está pronto, aguardando 3s..."
  sleep 3
done
 
echo ">>> [2/4] Inicializando/migrando schema do Airflow..."
airflow db migrate
 
echo ">>> [3/4] Criando usuário admin (ignora se já existir)..."
airflow users create \
  --username "${AIRFLOW_ADMIN_USER}" \
  --password "${AIRFLOW_ADMIN_PASSWORD}" \
  --email    "${AIRFLOW_ADMIN_EMAIL}" \
  --firstname Admin \
  --lastname  User \
  --role Admin || echo "    Usuário já existe, continuando..."
 
echo ">>> [4/4] Subindo webserver e scheduler..."
airflow scheduler &
airflow webserver --port 8080