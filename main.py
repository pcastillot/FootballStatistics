import datetime
import unicodedata
import re
import streamlit as st
import pandas as pd
import pydeck as pdk
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg, udf, lower
from pyspark.sql.functions import regexp_replace, col, concat, lit
from pyspark.sql.types import StringType, FloatType, IntegerType


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s)
                   if unicodedata.category(c) != 'Mn')

def getEquipoFromID(id):
    try:
        equipo = data_clubs.select("pretty_name").where(col('club_id') == id).collect()[0][0]
    except:
        equipo = "Unknown"

    return equipo

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
    'Agosto': '08',
    'Septiembre': '09',
    'Octubre': '10',
    'Noviembre': '11',
    'Diciembre': '12',
    'Enero': '01',
    'Febrero': '02',
    'Marzo': '03',
    'Abril': '04',
    'Mayo': '05',
    'Junio': '06',
    'Julio': '07'
}

DICT_TEMPORADA = {
    '2013-2014': '2013',
    '2014-2015': '2014',
    '2015-2016': '2015',
    '2016-2017': '2016',
    '2017-2018': '2017',
    '2018-2019': '2018',
    '2019-2020': '2019',
    '2020-2021': '2020',
    '2021-2022': '2021',
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


# RENDIMIENTO JUGADOR
data_rendimiento_valor = data_final.select('Player', 'club_x').collect()

jugadores = []
for jugador in data_rendimiento_valor:
    if jugador['club_x'] == None:
        jugadores.append(jugador['Player'])
    else:
        jugadores.append(jugador['Player'])


st.subheader('Rendimiento y valor de un jugador')
jugador_seleccionado = st.selectbox('Seleccione el jugador', jugadores)
with st.spinner('Cargando datos...'):
    nombre_jugador = jugador_seleccionado.split(',')[0]

    # Eliminamos los signos de acentuacion
    nombre_jugador_no_acentos = strip_accents(nombre_jugador)
    if re.match(r'(.*-.*)', nombre_jugador_no_acentos):
        nombre_jugador_no_acentos = nombre_jugador_no_acentos.replace('-', ' ')

    if re.match(r"(.*\'.*)", nombre_jugador_no_acentos):
        nombre_jugador_no_acentos = nombre_jugador_no_acentos.replace("'", '')

    # Obtenemos el id del jugador, accedemos a la primera coincidencia del filter y luego a la primera del select
    id_jugador = data_players.filter(lower(col('pretty_name')) == nombre_jugador_no_acentos.lower()).select('player_id').collect()[0][0]

    # Obtenemos estadisticas del jugador
    valor_jugador = data_final.select('Market value').where(col('Player') == nombre_jugador).collect()[0][0]
    goles_total_jugador = data_appearances.groupBy('player_id').sum('goals').where(col('player_id') == id_jugador).collect()[0][1]
    asistencias_total_jugador = data_appearances.groupBy('player_id').sum('assists').where(col('player_id') == id_jugador).collect()[0][1]
    nacionalidad_jugador = data_players.select('country_of_citizenship').where(col('player_id') == id_jugador).collect()[0][0]
    equipo_jugador = data_players.join(data_clubs, data_clubs.club_id == data_players.current_club_id)\
                                    .select(data_clubs.pretty_name).where(col('player_id') == id_jugador).collect()[0][0]
    primera_aparicion_jugador = data_appearances.join(data_games, data_games.game_id == data_appearances.game_id)\
                                    .select(col('season')).where(col('player_id') == id_jugador).collect()[0][0]

indice = int(primera_aparicion_jugador) - 2013

# Mostramos las estadisticas en pantalla
st.write('Nacionalidad: ', nacionalidad_jugador)
st.write('Equipo actual: ', equipo_jugador)
st.write('Goles totales del jugador: ', int(goles_total_jugador))
st.write('Asistencias totales del jugador: ', int(asistencias_total_jugador))
st.write('Valor del jugador: ', valor_jugador)

# Slider para que el usuario elija la temporada
temporada = st.select_slider('Selecciona la temporada', options=list(DICT_TEMPORADA.keys())[indice:len(DICT_TEMPORADA)])
with st.spinner('Cargando datos...'):
    # Formateamos la temporada para buscarla en el dataset
    temporada_largo = DICT_TEMPORADA.get(temporada)

    # Primero obtenemos los partidos jugados esa temporada
    partidos_filtrados = data_games.select('game_id', 'date', 'home_club_id', 'away_club_id').filter(col('season') == temporada_largo)

    # Luego por separado obtenemos los partidos jugados por el jugador
    apariciones_filtrado = data_appearances.select('game_id', 'player_id', 'goals', 'assists').where(col('player_id') == id_jugador)

    # Hacemos un join sobre los partidos de esa temporada añadiendo las apariciones del jugador por el id del partido
    partidos_filtrados = partidos_filtrados.join(apariciones_filtrado, apariciones_filtrado.game_id == partidos_filtrados.game_id)

    # Eliminamos la columna duplicada por el join
    partidos_filtrados = partidos_filtrados.drop(apariciones_filtrado.game_id)

    # Ya podemos sacar los goles y asistencias del jugador en la temporada
    goles_temporada_jugador = partidos_filtrados.groupBy('player_id').sum('goals').collect()[0][1]
    asistencias_temporada_jugador = partidos_filtrados.groupBy('player_id').sum('assists').collect()[0][1]

st.write('Goles en la temporada ', temporada, ': ', int(goles_temporada_jugador))
st.write('Asistencias en la temporada ', temporada, ': ', int(asistencias_temporada_jugador))
isError = False

if st.checkbox('Seleccionar mes'):

    mes = st.select_slider('Selecciona el mes', options=DICT_MES.keys(), value='Agosto')
    try:
        with st.spinner('Cargando datos...'):
            partidos_filtrados = partidos_filtrados.filter(col('date').substr(6, 2) == DICT_MES.get(mes))

            goles_mes_jugador = partidos_filtrados.groupBy('player_id').sum('goals').collect()[0][1]
            asistencias_mes_jugador = partidos_filtrados.groupBy('player_id').sum('assists').collect()[0][1]

        st.write('Goles en el mes ', mes, ': ', int(goles_mes_jugador))
        st.write('Asistencias en el mes ', mes, ': ', int(asistencias_mes_jugador))

    except:
        st.error('No hay datos de ' + jugador_seleccionado + ' en el mes ' + mes + ' en la temporada ' + temporada)
        isError = True


with st.spinner('Cargando datos...'):
    # Seleccionamos solo los datos que nos interesan
    rendimiento_jugador = partidos_filtrados.select('game_id', 'goals', 'assists')

    # Unimos este dataset con el de los partidos, para obtener los equipos que disputaron el partido
    rendimiento_jugador = rendimiento_jugador.join(data_games, rendimiento_jugador.game_id == data_games.game_id)
    rendimiento_jugador = rendimiento_jugador.toPandas()

    # Preparamos dataset con la información relevante y formar una nueva columna uniendo las columnas de equipos que jugaron
    rendimiento_jugador['home_club_id'] = rendimiento_jugador['home_club_id'].apply(getEquipoFromID)
    rendimiento_jugador['away_club_id'] = rendimiento_jugador['away_club_id'].apply(getEquipoFromID)
    rendimiento_jugador['enfrentamiento'] = rendimiento_jugador['home_club_id'] + " - " + rendimiento_jugador["away_club_id"]
    rendimiento_jugador = rendimiento_jugador[['goals', 'assists', "enfrentamiento"]]
    rendimiento_jugador = rendimiento_jugador.set_index('enfrentamiento')

    # Si no han habido errores pinta la grafica
    if not isError:
        st.line_chart(rendimiento_jugador)


# st.title("Date range")
#
# min_date = datetime.datetime(2013,1,1)
# max_date = datetime.date(2022,2,1)
#
# a_date = st.date_input("Pick a date", min_value=min_date, max_value=max_date)


#