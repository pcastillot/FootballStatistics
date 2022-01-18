import unicodedata

import streamlit as st
import pandas as pd
import pydeck as pdk
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg
from pyspark.sql.functions import regexp_replace, col, concat, lit
from pyspark.sql.types import StringType, FloatType, IntegerType


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

def getEquipoFromID(id):
    return data_clubs.select("pretty_name").where(col('club_id') == id).collect()[0][0]

st.title('Futbol')

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

DATA_APPEARANCES = "datos/appearances.csv"
DATA_CLUBS = "datos/clubs.csv"
DATA_COMPETITIONS = "datos/competitions.csv"
DATA_GAMES = "datos/games.csv"
DATA_LEAGUES = "datos/leagues.csv"
DATA_PLAYERS = "datos/players.csv"
DATA_FINAL = "datos/Final.csv"

DICT_MES = {
    'Enero': '01',
    'Febrero': '02',
    'Marzo': '03',
    'Abril': '04',
    'Mayo': '05',
    'Junio': '06',
    'Julio': '07',
    'Agosto': '08',
    'Septiembre': '09',
    'Octubre': '10',
    'Noviembre': '11',
    'Diciembre': '12'
}

# COMIENZO

# DataFrame del csv appearances
data_appearances = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_APPEARANCES)
data_appearances = data_appearances.withColumn('goals', col('goals').cast(IntegerType()))
data_appearances = data_appearances.withColumn('assists', col('assists').cast(IntegerType()))

# DataFrame del csv clubs
data_clubs = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_CLUBS)

# DataFrame del csv competitions
data_competitions = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_COMPETITIONS)

# DataFrame del csv games
data_games = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_GAMES)

# DataFrame del csv leagues
data_leagues = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_LEAGUES)

# DataFrame del csv players
data_players = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_PLAYERS)

# DataFrame del xlsx final
data_final = spark.read.options(delimiter=",", header=True, encoding='UTF-8').csv(DATA_FINAL)

data_rendimiento_valor = data_final.select('Player', 'club_x').collect()

jugadores = []
for jugador in data_rendimiento_valor:
    if jugador['club_x'] == None:
        jugadores.append(jugador['Player'])
    else:
        jugadores.append('' + jugador['Player'] + ', ' + jugador['club_x'])


st.subheader('Rendimiento y valor de un jugador')
jugador_seleccionado = st.selectbox('Seleccione el jugador', jugadores)
nombre_jugador = jugador_seleccionado.split(',')[0]
valor_jugador = data_final.select('Market value').where(col('Player') == nombre_jugador).collect()[0][0]

# Eliminamos los signos de acentuacion
nombre_jugador = strip_accents(nombre_jugador)

# Obtenemos el id del jugador, accedemos a la primera coincidencia del filter y luego a la primera del select
id_jugador = data_players.filter(col('pretty_name') == nombre_jugador).select('player_id').collect()[0][0]

ano = st.select_slider('Selecciona el año', options=range(2014, 2021), value=2014)

fecha = str(ano)

if st.checkbox('Seleccionar mes'):

    mes = st.select_slider('Selecciona el mes', options=DICT_MES.keys(), value='Enero')

    fecha = str(ano)+'-'+DICT_MES[mes]

partidos_filtrados = data_games.select('game_id').filter(col('date').startswith(fecha))

# Creamos una lista con todos los partidos
list_partidos = []
for partido in partidos_filtrados.collect():
    list_partidos.append(partido['game_id'])

# Apariciones del jugador en el campo
rendimiento_jugador = data_appearances.where((col('player_id') == id_jugador))

# Partidos jugados en la fecha filtrada
rendimiento_jugador = rendimiento_jugador.filter(col('game_id').isin(list_partidos))
rendimiento_jugador = rendimiento_jugador.select('game_id', 'goals', 'assists')

# Unimos este dataset con el de los partidos, para obtener los equipos que disputaron el partido
rendimiento_jugador = rendimiento_jugador.join(data_games, rendimiento_jugador.game_id == data_games.game_id)
# rendimiento_jugador = rendimiento_jugador.withColumn('home_club_id', getEquipoFromID(col('home_club_id')))
# rendimiento_jugador = rendimiento_jugador.withColumn('away_club_id', getEquipoFromID(col('away_club_id')))

# Preparamos dataset con la información relevante y formar una nueva columna uniendo las columnas de equipos que jugaron
rendimiento_jugador = rendimiento_jugador.select('goals', 'assists', 'home_club_id', 'away_club_id',
                                                 concat(col("home_club_id"), lit(" - "), col("away_club_id"))
                                                 .alias("enfrentamiento"))

rendimiento_jugador = rendimiento_jugador.select('enfrentamiento', 'goals', 'assists').toPandas().set_index('enfrentamiento')


st.line_chart(rendimiento_jugador)
