import datetime
import unicodedata
import re
import streamlit as st
import pandas as pd
import pydeck as pdk
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg, udf, lower, when, count
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
data_appearances = data_appearances.withColumn('minutes_played', col('minutes_played').cast(IntegerType()))

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

user_select = st.selectbox('Seleccione qué estadísticas quiere visualizar', ['Jugador', 'Equipo'])

if user_select == 'Jugador':

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
        id_jugador = data_players.filter(lower(col('pretty_name')) == nombre_jugador_no_acentos.lower()).select(
            'player_id').collect()[0][0]

        # Obtenemos estadisticas del jugador
        valor_jugador = data_final.select('Market value').where(col('Player') == nombre_jugador).collect()[0][0]
        goles_total_jugador = \
        data_appearances.groupBy('player_id').sum('goals').where(col('player_id') == id_jugador).collect()[0][1]
        asistencias_total_jugador = \
        data_appearances.groupBy('player_id').sum('assists').where(col('player_id') == id_jugador).collect()[0][1]
        nacionalidad_jugador = \
        data_players.select('country_of_citizenship').where(col('player_id') == id_jugador).collect()[0][0]
        equipo_jugador = data_players.join(data_clubs, data_clubs.club_id == data_players.current_club_id) \
            .select(data_clubs.pretty_name).where(col('player_id') == id_jugador).collect()[0][0]
        primera_aparicion_jugador = data_appearances.join(data_games, data_games.game_id == data_appearances.game_id) \
            .select(col('season')).where(col('player_id') == id_jugador).collect()[0][0]

    indice = int(primera_aparicion_jugador) - 2013

    # Mostramos las estadisticas en pantalla
    st.write('Nacionalidad: ', nacionalidad_jugador)
    st.write('Equipo actual: ', equipo_jugador)
    st.write('Goles totales del jugador: ', int(goles_total_jugador))
    st.write('Asistencias totales del jugador: ', int(asistencias_total_jugador))
    st.write('Valor del jugador: ', valor_jugador)

    # Slider para que el usuario elija la temporada
    temporada = st.select_slider('Selecciona la temporada',
                                 options=list(DICT_TEMPORADA.keys())[indice:len(DICT_TEMPORADA)])
    with st.spinner('Cargando datos...'):
        # Formateamos la temporada para buscarla en el dataset
        temporada_largo = DICT_TEMPORADA.get(temporada)

        # Primero obtenemos los partidos jugados esa temporada
        partidos_filtrados = data_games.select('game_id', 'date', 'home_club_id', 'away_club_id').filter(
            col('season') == temporada_largo)

        # Luego por separado obtenemos los partidos jugados por el jugador
        apariciones_filtrado = data_appearances.select('game_id', 'player_id', 'goals', 'assists').where(
            col('player_id') == id_jugador)

        # Hacemos un join sobre los partidos de esa temporada añadiendo las apariciones del jugador por el id del partido
        partidos_filtrados = partidos_filtrados.join(apariciones_filtrado,
                                                     apariciones_filtrado.game_id == partidos_filtrados.game_id)

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

        # Si no han habido errores pinta la grafica
    if not isError:
        with st.spinner('Cargando datos...'):
            # Seleccionamos solo los datos que nos interesan
            rendimiento_jugador = partidos_filtrados.select('game_id', 'goals', 'assists')

            # Unimos este dataset con el de los partidos, para obtener los equipos que disputaron el partido
            rendimiento_jugador = rendimiento_jugador.join(data_games,
                                                           rendimiento_jugador.game_id == data_games.game_id)
            rendimiento_jugador = rendimiento_jugador.toPandas()

            # Preparamos dataset con la información relevante y formar una nueva columna uniendo las columnas de equipos que jugaron
            rendimiento_jugador['home_club_id'] = rendimiento_jugador['home_club_id'].apply(getEquipoFromID)
            rendimiento_jugador['away_club_id'] = rendimiento_jugador['away_club_id'].apply(getEquipoFromID)
            rendimiento_jugador['enfrentamiento'] = rendimiento_jugador['home_club_id'] + " - " + rendimiento_jugador[
                "away_club_id"]
            rendimiento_jugador = rendimiento_jugador[['goals', 'assists', "enfrentamiento"]]
            rendimiento_jugador = rendimiento_jugador.set_index('enfrentamiento')

            st.line_chart(rendimiento_jugador)

    # st.title("Date range")
    #
    # min_date = datetime.datetime(2013,1,1)
    # max_date = datetime.date(2022,2,1)
    #
    # a_date = st.date_input("Pick a date", min_value=min_date, max_value=max_date)


# Sacar victorias y derrotas, comprobar si el jugador esta dando un beneficio al equipo
#

elif user_select == 'Equipo':
    equipos = []

    equipos_data = data_clubs.select(col('pretty_name'), col('club_id'))

    for equipo in equipos_data.collect():
        equipos.append(equipo['pretty_name'])

    st.subheader('Estadísticas de equipo')
    equipo_seleccionado = st.selectbox('Seleccione el equipo', equipos)

    # Obtenemos el id del equipo
    id_equipo = equipos_data.select(col('club_id')).where(col('pretty_name') == equipo_seleccionado).collect()[0][0]

    # Obtenemos victorias empates y derrotas como local
    datos_equipo_local = data_games.select(col('home_club_id'), col('home_club_goals'), col('away_club_goals')) \
                                        .where((col('home_club_id') == id_equipo))

    victorias_equipo_local = datos_equipo_local.groupBy('home_club_id').agg(
        count(when(col('home_club_goals') > col('away_club_goals'), True).otherwise(None)))
    victorias_equipo_local = victorias_equipo_local.collect()[0][1]

    derrotas_equipo_local = datos_equipo_local.groupBy('home_club_id').agg(
        count(when(col('home_club_goals') < col('away_club_goals'), True).otherwise(None)))
    derrotas_equipo_local = derrotas_equipo_local.collect()[0][1]

    empates_equipo_local = datos_equipo_local.groupBy('home_club_id').agg(
        count(when(col('home_club_goals') == col('away_club_goals'), True).otherwise(None)))
    empates_equipo_local = empates_equipo_local.collect()[0][1]


    # Obtenemos victorias empates y derrotas como visitante
    datos_equipo_visitante = data_games.select(col('away_club_id'), col('home_club_goals'), col('away_club_goals')) \
                                            .where((col('away_club_id') == id_equipo))

    victorias_equipo_visitante = datos_equipo_visitante.groupBy('away_club_id').agg(
        count(when(col('away_club_goals') > col('home_club_goals'), True).otherwise(None)))
    victorias_equipo_visitante = victorias_equipo_visitante.collect()[0][1]

    derrotas_equipo_visitante = datos_equipo_visitante.groupBy('away_club_id').agg(
        count(when(col('away_club_goals') < col('home_club_goals'), True).otherwise(None)))
    derrotas_equipo_visitante = derrotas_equipo_visitante.collect()[0][1]

    empates_equipo_visitante = datos_equipo_visitante.groupBy('away_club_id').agg(
        count(when(col('away_club_goals') == col('home_club_goals'), True).otherwise(None)))
    empates_equipo_visitante = empates_equipo_visitante.collect()[0][1]

    # Imprimimos las estadisticas del equipo
    st.write('Estadísticas como equipo local (V/E/D):       ', victorias_equipo_local, '/', empates_equipo_local, '/', derrotas_equipo_local)
    st.write('Estadísticas como equipo visitante (V/E/D):   ', victorias_equipo_visitante, '/', empates_equipo_visitante, '/', derrotas_equipo_visitante)

    # Indice para ajustar el slider
    indice = int(data_games.select(col('season')).where((col('home_club_id') == id_equipo) | (col('away_club_id') == id_equipo)).collect()[0][0])
    indice = indice - 2013

    # Slider para que el usuario elija la temporada
    temporada_largo = st.select_slider('Selecciona la temporada',
                                 options=list(DICT_TEMPORADA.keys())[indice:len(DICT_TEMPORADA)])
    temporada = DICT_TEMPORADA.get(temporada_largo)

    # Seleccionar jgador del equipo
    if st.checkbox('Seleccionar jugador del equipo'):
        # Obtenemos los partidos de la temporada seleccionada
        partidos_temporada = data_games.select(col('game_id'), col('season')).where(col('season') == temporada)
        # Juntamos la tabla de apariciones a la de partidos de la temporada para obtener el id de los jugadores
        partidos_temporada = partidos_temporada.join(data_appearances, data_appearances.game_id == partidos_temporada.game_id).drop(data_appearances.game_id)
        # Obtenemos las apariciones de los jugadores del equipo seleccionado
        jugadores_equipo_temporada = partidos_temporada.where(col('player_club_id') == id_equipo)
        # Eliminamos las multiples apariciones y nos quedamos solo con los player id que aparecen
        jugadores_equipo_temporada = jugadores_equipo_temporada.select(col('player_id'), col('player_club_id')).dropDuplicates(['player_id'])
        # Juntamos la tabla de jugadores para obtener toda la informacion de cada jugador
        jugadores_equipo_temporada = jugadores_equipo_temporada.join(data_players, data_players.player_id == jugadores_equipo_temporada.player_id).drop(data_players.player_id)

        list_jugadores_equipo_temporada = []
        for jugador in jugadores_equipo_temporada.collect():
            list_jugadores_equipo_temporada.append(jugador['pretty_name'])

        jugador_seleccionado = st.selectbox('Seleccione jugador', list_jugadores_equipo_temporada)
        id_jugador = jugadores_equipo_temporada.select('player_id').where(col('pretty_name') == jugador_seleccionado).collect()[0][0]

        partidos_jugador_temporada = partidos_temporada.where(col('player_id') == id_jugador)

        # Ya podemos sacar los goles y asistencias del jugador en la temporada ademas de otras estadisticas
        goles_temporada_jugador = int(partidos_jugador_temporada.groupBy('player_id').sum('goals').collect()[0][1])
        asistencias_temporada_jugador = int(partidos_jugador_temporada.groupBy('player_id').sum('assists').collect()[0][1])
        minutos_temporada_jugador = int(partidos_jugador_temporada.groupBy('player_id').sum('minutes_played').collect()[0][1])
        num_partidos_temporada_jugador = int(partidos_jugador_temporada.groupBy('player_id').count().collect()[0][1])

        goles_minuto = goles_temporada_jugador/minutos_temporada_jugador
        asistencias_minuto = asistencias_temporada_jugador/minutos_temporada_jugador
        goles_partido = goles_temporada_jugador/num_partidos_temporada_jugador
        asistencias_partido = asistencias_temporada_jugador/num_partidos_temporada_jugador

        st.write('Goles en la temporada ', temporada_largo, ': ', goles_temporada_jugador)
        st.write('Asistencias en la temporada ', temporada_largo, ': ', asistencias_temporada_jugador)
        st.write(" ")
        st.write('Relacion goles/minuto jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(goles_minuto)))
        st.write('Relacion asistencias/minuto jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(asistencias_minuto)))
        st.write(" ")
        st.write('Relacion goles/partido jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(goles_partido)))
        st.write('Relacion asistencias/partido jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(asistencias_partido)))

