# Create mysql container
docker pull mysql
docker volume create lab4_sql_data

# Starts the docker container containing the MySQL database
docker run --name lab4_sql_container \
  --network de300_net \
  -e MYSQL_ROOT_PASSWORD=de3002024spring \
  -e MYSQL_DATABASE=cars_lab4 \
  -e MYSQL_USER=de300_lab04 \
  -e MYSQL_PASSWORD=de300u1springaccess \
  -v lab4_sql_data:/var/lib/mysql \
  -d mysql \