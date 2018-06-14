# Dados de voos já utilizados em outra disciplina.
# Esta versão possui algumas diferenças em nomes de colunas
install.packages( "nycflights13" )

format( object.size(nycflights13::flights), "MB" )

library(sparklyr)
library(dbplyr)

# Inicializa processo do Spark
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark.
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )

# Lista tabelas do catálogo
src_tbls(sc)

# Abre a página principal da interface gráfica do Spark. 
spark_web(sc)

# Summary funciona?
summary(flights_tbl)

# Neste caso precisamos de uma função diferente.
sdf_describe(flights_tbl)

spark_disconnect(sc)
