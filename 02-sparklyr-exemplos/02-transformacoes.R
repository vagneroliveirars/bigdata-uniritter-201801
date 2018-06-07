library(sparklyr)
library(tidyverse)

# Inicializa processo do Spark (ver processo Java em monitor de tarefas / monitor de atividades)
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark. Ver na aba Connections, navegar no conteúdo
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )

# Abre a página principal da interface gráfica do Spark. 
spark_web(sc)

# Uma operação simples de média.

# Criar uma operação com dplyr para calcular a média de atraso de partida (dep_delay) por companhia aérea (carrier) em nycflights13::flights

# Repetir esta mesma operação com o flights_tbl

# Agora atribuir o resultado da operação sparklyr para uma variável chamada media_atraso_partidas_companhias

tbl_atraso_medio <- sdf_register( media_atraso_partidas_companhias, name = "media_atraso" )

# Persistindo (cache) em memória
sdf_persist( tbl_atraso_medio, storage.level = "MEMORY_ONLY" )

# Qual o tipo de dado do retorno do collect?
collect(media_atraso_partidas_companhias)

# Como o spark irá processar este Data Frame?
show_query( media_atraso_partidas_companhias )

# E este?
show_query( tbl_atraso_medio )

# Um pouco mais de complexidade.

# Criar uma variável chamada atraso_chegada_maior_partida, que será 1 quando arr_delay for maior que dep_delay e 0 caso contrário
# Calcular as seguintes sumarizações por companhia aérea, no mesmo dataframe:
#   Média de atraso de partida (dep_delay)
#   Média de atraso de chegada (arr_delay)
#   Quantidade de voos por companhia
#   Quantidade de voos onde houve atraso na partida e a companhia não conseguiu reduzir o atraso na chegada (usar a nova variável criada)
#   Ordenar o resultado pela taxa de voos nos quais a companhia não conseguiu reduzir o atraso na chegada, em ordem decrescente
# atribuiro resultado a uma variável chamada ranking_piores_atrasos
ranking_piores_atrasos <- ...

show_query( ranking_piores_atrasos )

sdf_describe( ranking_piores_atrasos )

# Visualização
ranking_piores_atrasos %>%
  collect() %>%
  View( title = "Spark" )

# Comparação com resultado local
# Repetir a mesma operação com o data frame local, nycflights13::flights. Vamos comparar o resultado

spark_disconnect(sc)
