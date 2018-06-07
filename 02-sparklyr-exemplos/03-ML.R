library(sparklyr)
library(tidyverse)

# Inicializa processo do Spark 
sc <- spark_connect(master = "local")

# Cria uma tabela no catálogo do Spark. 
flights_tbl <- copy_to( sc, nycflights13::flights, "flights" )

# Regressão linear, exemplo das disciplinas anteriores
model_data_tbl <- 
  flights_tbl %>%
  filter( !is.na( arr_delay ) & !is.na( dep_delay ) & !is.na( distance )) %>%
  filter( dep_delay > 15 & dep_delay < 240 ) %>%
  filter( arr_delay > -60 & arr_delay < 360 ) %>%
  mutate( gain = dep_delay - arr_delay ) %>%
  filter( gain > -100 ) %>%
  select( arr_delay, dep_delay, distance, carrier, origin, dest, gain )

train_tbl <- sdf_sample(model_data_tbl, fraction = 0.7, seed = 1235)
test_tbl <- setdiff(model_data_tbl, train_tbl)

sparklyr::ml_linear_regression( train_tbl, gain ~ distance + dep_delay + carrier ) -> lm_model_spark

# A mesma regressão com dplyr e base R
model_data <- 
  nycflights13::flights %>%
  filter( !is.na( arr_delay ) & !is.na( dep_delay ) & !is.na( distance )) %>%
  filter( dep_delay > 15 & dep_delay < 240 ) %>%
  filter( arr_delay > -60 & arr_delay < 360 ) %>%
  mutate( gain = dep_delay - arr_delay ) %>%
  filter( gain > -100 ) %>%
  select( year, month, arr_delay, dep_delay, distance, carrier, origin, dest, gain )

train_local <- collect( train_tbl ) 
test_local <- collect( test_tbl )

lm( gain ~ distance + dep_delay + carrier, train_local ) -> lm_model_local

# Comparação dos resultados
sparklyr::ml_predict( lm_model_spark, test_tbl )

test_local %>% 
  add_column( prediction = predict( lm_model_local, test_local ))

# E os modelos???
sparklyr::ml_summary(lm_model_spark)
lm_model_spark  
summary(lm_model_local)

# criar um spark data frame chamado erros_previsao_recuperacao_atraso, onde contaremos em quantos casos o valor previsto teve o mesmo 
# sinal da variavel gain, e em quantos casos o sinal foi diferente
erros_previsao_recuperacao_atraso <- ...

collect( erros_previsao_recuperacao_atraso )

# Vamos olhar os detalhes
show_query(erros_previsao_recuperacao_atraso)

# Regressão com Random Forest. features per tree = 1/3.
ml_random_forest_regressor( x = train_tbl
                            , formula = gain ~ distance + dep_delay + carrier
                            , num_trees = 75
                            , subsampling_rate = 0.8
                            , max_depth = 15
                            , min_instances_per_node = 2
                            , seed = 1235 ) -> spark_rf_regressor

# Ops, e agora?
sparklyr::ml_summary( spark_rf_regressor )

print(spark_rf_regressor)

ml_tree_feature_importance( spark_rf_regressor )

sparklyr::ml_predict( spark_rf_regressor, test_tbl ) %>%
  ml_regression_evaluator( label_col = "gain" )

sparklyr::ml_predict( lm_model_spark, test_tbl ) %>%
  ml_regression_evaluator( label_col = "gain" )

# Usando o ggplot, criar gráficos de comparação previsto vs gain nos resultados dos modelos linear e RF aplicados no dataset de testes 




# Criar outra regressão com Random Forest utilizando as seguintes features: dep_delay + distance + carrier + origin + dest
# Calcular a métrica de RMSE no spark data frame de testes
# Listar a importância das features
# Criar gráfico comparativo previsto vs gain



spark_disconnect(sc)
