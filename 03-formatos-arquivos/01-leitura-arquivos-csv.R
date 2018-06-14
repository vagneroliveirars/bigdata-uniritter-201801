library(sparklyr)
sc <- spark_connect(master = "local")
library(tidyverse)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE )

head(ted_main)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE, delimiter = ",", quote = '"', escape = '\\')

head(ted_main)

ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main.csv"
                          , header = TRUE, infer_schema = TRUE, memory = FALSE
                          , columns = c( "comments", "description", "duration", "event", "film_date", "languages", "main_speaker", "name"
                                       , "num_speaker", "published_date", "speaker_occupation", "title", "url", "views" ))

local_ted_main <- read_csv("03-formatos-arquivos/data/ted_main.csv")

head(local_ted_main)

# Lê o arquivo problemático com a biblioteca local do R, remove todos as quebras de linha, vírgulas
# e gera um novo arquivo sem as partes problemáticos
local_ted_main %>%
  mutate(url = str_replace_all(url, "\n", "")) %>%
  mutate_if(is_character, ~ str_replace_all(.x, ",", " ")) %>%
  select(-ratings, -related_talks, -tags) %>%
  write_csv("03-formatos-arquivos/data/ted_main_no_json.csv")

# Agora consegue ler o arquivo CSV com a lib do spark
ted_main <- spark_read_csv( sc, name = "ted_main", path = "03-formatos-arquivos/data/ted_main_no_json.csv"
                            , header = TRUE, infer_schema = TRUE, memory = FALSE )

head(ted_main)

ted_main %>%
  mutate_at(vars(comments, duration, languages, published_date), as.integer) %>% 
  head()

spark_disconnect(sc)
