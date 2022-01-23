import datetime
import unicodedata
import re
import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, avg, udf, lower, when, count, to_date, first, last
from pyspark.sql.functions import regexp_replace, col, concat, lit
from pyspark.sql.types import StringType, FloatType, IntegerType, DoubleType


def strip_accents(string):
    return ''.join(c for c in unicodedata.normalize('NFD', string)
                   if unicodedata.category(c) != 'Mn')


def getEquipoFromID(id):
    try:
        equipo = data_clubs.select("pretty_name").where(col('club_id') == id).collect()[0][0]
    except:
        equipo = "Unknown"

    return equipo

def getPosicionJugador(id_jugador):
    posicion_jugador = data_players.select('position').where(col('player_id') == id_jugador).collect()[0][0]
    subposicion_jugador = data_players.select('sub_position').where(col('player_id') == id_jugador).collect()[0][0]
    return posicion_jugador + " - " + subposicion_jugador

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

DICT_POSICION = {
    'FW': 'Delantero',
    'MF': 'Centrocampista',
    'DF': 'Defensa',
    'GK': 'Portero'
}

# COMIENZO
with st.spinner('Cargando datos...'):
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
    data_final = data_final.replace(',', '.')
    data_final = data_final.withColumn('Gls90', regexp_replace('Gls90', ',', '.').cast('float'))
    data_final = data_final.withColumn('Ast90', regexp_replace('Ast90', ',', '.').cast('float'))
    data_final = data_final.withColumn('SoT%', regexp_replace('SoT%', ',', '.').cast('float'))
    data_final = data_final.withColumn('G/Sh', regexp_replace('G/Sh', ',', '.').cast('float'))
    data_final = data_final.withColumn('Blocks', regexp_replace('Blocks', ',', '.').cast('int'))
    data_final = data_final.withColumn('Cmp%', regexp_replace('Cmp%', ',', '.').cast('float'))


user_select = st.selectbox('Seleccione qué estadísticas quiere visualizar', ['Jugador', 'Equipo', 'Temporada 19-20'])

#############################################################
# Jugador
#############################################################

if user_select == 'Jugador':

    # RENDIMIENTO JUGADOR
    data_rendimiento_valor = data_final.select('Player').collect()

    jugadores = []
    for jugador in data_rendimiento_valor:
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
        posicion_jugador = getPosicionJugador(id_jugador)
        goles_total_jugador = data_appearances.groupBy('player_id').sum('goals').where(col('player_id') == id_jugador).collect()[0][1]
        asistencias_total_jugador = data_appearances.groupBy('player_id').sum('assists').where(col('player_id') == id_jugador).collect()[0][1]
        nacionalidad_jugador = data_players.select('country_of_citizenship').where(col('player_id') == id_jugador).collect()[0][0]

        equipo_jugador = data_players.join(data_clubs, data_clubs.club_id == data_players.current_club_id) \
                                        .select(data_clubs.pretty_name).where(col('player_id') == id_jugador).collect()[0][0]

        primera_aparicion_jugador = data_appearances.join(data_games, data_games.game_id == data_appearances.game_id) \
            .select(col('season')).where(col('player_id') == id_jugador).collect()[0][0]
        ultima_aparicion_jugador = data_appearances.join(data_games, data_games.game_id == data_appearances.game_id) \
            .select(col('season')).where(col('player_id') == id_jugador)
        ultima_aparicion_jugador = ultima_aparicion_jugador.collect()[ultima_aparicion_jugador.count() - 1][0]

    indice_inicio = int(primera_aparicion_jugador) - 2013
    indice_final = int(ultima_aparicion_jugador) - 2013

    # Mostramos las estadisticas en pantalla
    st.write('Nacionalidad: ', nacionalidad_jugador)
    st.write('Equipo actual: ', equipo_jugador)
    st.write('Posición: ', posicion_jugador)
    st.write('Goles totales del jugador: ', int(goles_total_jugador))
    st.write('Asistencias totales del jugador: ', int(asistencias_total_jugador))
    st.write('Valor del jugador: ', valor_jugador)

    # Slider para que el usuario elija la temporada
    temporada = st.select_slider('Selecciona la temporada',
                                 options=list(DICT_TEMPORADA.keys())[indice_inicio:indice_final+1])
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


#############################################################
# Equipo
#############################################################

elif user_select == 'Equipo':
    equipos = []
    with st.spinner('Cargando datos...'):
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

        datos_equipo = data_clubs.where(col('club_id') == id_equipo)
        valor_equipo = datos_equipo.select('total_market_value').collect()[0][0]
        estadio_equipo = datos_equipo.select('stadium_name').collect()[0][0]
        link_equipo = datos_equipo.select('url').collect()[0][0]

        # Obtenemos todos los partidos del club
        partidos_liga_equipo = data_games.where((col('home_club_id') == id_equipo) | (col('away_club_id') == id_equipo))
        # Unimos la tabla competiciones para obtener solo los partidos de liga
        partidos_liga_equipo = partidos_liga_equipo.join(data_competitions, data_competitions.competition_id == partidos_liga_equipo.competition_code)
        partidos_liga_equipo = partidos_liga_equipo.where(col('type') == 'first_tier')
        # Transformamos la columna date para hacer comprobaciones de fecha y la ordenamos por fecha
        partidos_liga_equipo = partidos_liga_equipo.withColumn('date', to_date(col('date'),'yyyy-MM-dd'))
        partidos_liga_equipo = partidos_liga_equipo.orderBy('date')
        # Agrupamos por temporada obteniendo la fecha del ultimo partido de la temporada
        ultimo_partido_liga_equipo = partidos_liga_equipo.groupBy(col('season')).agg(last('date'))
        # Unimos la tabla de partidos utilizando las fechas y asi obtenemos todos los detalles del ultimo partido que jugo el club
        posiciones_liga_equipo = ultimo_partido_liga_equipo.join(data_games, (data_games.date == col('last(date)'))
                                                                 & ((data_games.home_club_id == id_equipo) | (data_games.away_club_id == id_equipo)))
        posiciones_liga_equipo = posiciones_liga_equipo.drop(data_games.season)
        posiciones_liga_equipo = posiciones_liga_equipo.orderBy('date')

        # Pasamos a pandas la columna season y creamos una lista vacia para ingresar la posicion del equipo en cada temporada
        posiciones_liga_equipo_pandas = posiciones_liga_equipo.select('season').toPandas()
        list_posiciones_liga_equipo = []

        # Por cada partido del dataframe detallado comprobamos si el equipo era local o visitante
        # Añadimos la posición del equipo despues de ese partido en la lista
        for partido in posiciones_liga_equipo.collect():
            if partido['home_club_id'] == id_equipo:
                list_posiciones_liga_equipo.append(partido['home_club_position'])
            else:
                list_posiciones_liga_equipo.append(partido['away_club_position'])

        # Unimos esta lista al dataframe de pandas creado anteriormente
        posiciones_liga_equipo_pandas = posiciones_liga_equipo_pandas.join(pd.DataFrame(list_posiciones_liga_equipo))
        # Formateamos el texto para obtener el texto de la temporada completa
        posiciones_liga_equipo_pandas['season'] = posiciones_liga_equipo_pandas['season'].map(lambda x: '{}-{}'.format(x, str(int(x)+1)))
        # Asignamos la columna season como indice para representar el dataframe en una grafica
        posiciones_liga_equipo_pandas = posiciones_liga_equipo_pandas.set_index('season')



    # Imprimimos las estadisticas del equipo
    st.write('Valor de mercado (Mill €): ', float(valor_equipo))
    st.write('Estadio: ', estadio_equipo)
    st.write('Enlace TransferMarkt: ', link_equipo)
    st.write('Estadísticas como equipo local (V/E/D):       ', victorias_equipo_local, '/', empates_equipo_local, '/', derrotas_equipo_local)
    st.write('Estadísticas como equipo visitante (V/E/D):   ', victorias_equipo_visitante, '/', empates_equipo_visitante, '/', derrotas_equipo_visitante)

    # Grafica con la posicion del equipo en primera division en las distintas temporadas
    st.caption('Posicion en primera división durante las temporadas')
    st.line_chart(posiciones_liga_equipo_pandas)

    # Indices para ajustar el slider
    primera_aparicion_equipo = int(data_games.select(col('season')).where((col('home_club_id') == id_equipo) | (col('away_club_id') == id_equipo)).collect()[0][0])
    indice_inicio = primera_aparicion_equipo - 2013

    ultima_aparicion_equipo = data_games.select(col('season')).where((col('home_club_id') == id_equipo) | (col('away_club_id') == id_equipo))
    ultima_aparicion_equipo = int(ultima_aparicion_equipo.collect()[ultima_aparicion_equipo.count() - 1][0])
    indice_final = ultima_aparicion_equipo - 2013

    # Comprobamos si solo hay datos de una temporada
    if indice_final == indice_inicio:
        temporada = ultima_aparicion_equipo
        temporada_largo = str(temporada) + "-" + str(temporada+1)
        st.subheader('Temporada ' + temporada_largo)
    else:
        # Si hay datos de mas de una temporada ofrecemos el slider para elegir
        options_slider = list(DICT_TEMPORADA.keys())[indice_inicio:indice_final+1]
        # Slider para que el usuario elija la temporada
        temporada_largo = st.select_slider('Selecciona la temporada', options=options_slider)
        temporada = DICT_TEMPORADA.get(temporada_largo)



    with st.spinner('Cargando datos...'):

        # Obtenemos victorias empates y derrotas como local
        datos_temporada_equipo_local = data_games.select(col('home_club_id'), col('home_club_goals'), col('away_club_goals')) \
            .where((col('home_club_id') == id_equipo) & (col('season') == temporada))

        victorias_temporada_equipo_local = datos_temporada_equipo_local.groupBy('home_club_id').agg(
            count(when(col('home_club_goals') > col('away_club_goals'), True).otherwise(None)))
        victorias_temporada_equipo_local = victorias_temporada_equipo_local.collect()[0][1]

        derrotas_temporada_equipo_local = datos_temporada_equipo_local.groupBy('home_club_id').agg(
            count(when(col('home_club_goals') < col('away_club_goals'), True).otherwise(None)))
        derrotas_temporada_equipo_local = derrotas_temporada_equipo_local.collect()[0][1]

        empates_temporada_equipo_local = datos_temporada_equipo_local.groupBy('home_club_id').agg(
            count(when(col('home_club_goals') == col('away_club_goals'), True).otherwise(None)))
        empates_temporada_equipo_local = empates_temporada_equipo_local.collect()[0][1]

        # Obtenemos victorias empates y derrotas como visitante
        datos_temporada_equipo_visitante = data_games.select(col('away_club_id'), col('home_club_goals'), col('away_club_goals')) \
            .where((col('away_club_id') == id_equipo) & (col('season') == temporada))

        victorias_temporada_equipo_visitante = datos_temporada_equipo_visitante.groupBy('away_club_id').agg(
            count(when(col('away_club_goals') > col('home_club_goals'), True).otherwise(None)))
        victorias_temporada_equipo_visitante = victorias_temporada_equipo_visitante.collect()[0][1]

        derrotas_temporada_equipo_visitante = datos_temporada_equipo_visitante.groupBy('away_club_id').agg(
            count(when(col('away_club_goals') < col('home_club_goals'), True).otherwise(None)))
        derrotas_temporada_equipo_visitante = derrotas_temporada_equipo_visitante.collect()[0][1]

        empates_temporada_equipo_visitante = datos_temporada_equipo_visitante.groupBy('away_club_id').agg(
            count(when(col('away_club_goals') == col('home_club_goals'), True).otherwise(None)))
        empates_temporada_equipo_visitante = empates_temporada_equipo_visitante.collect()[0][1]

        # Obtenemos todos los partidos de la temporada del equipo
        partidos_temporada_equipo = data_games.select('home_club_id', 'away_club_id', 'home_club_position', 'away_club_position', 'competition_code', 'date')\
                                                .where((col('season') == temporada) & ((col('home_club_id') == id_equipo) | (col('away_club_id') == id_equipo)))
        # Unimos la tabla competiciones para saber si los partidos son de liga
        partidos_temporada_equipo = partidos_temporada_equipo.join(data_competitions, data_competitions.competition_id == partidos_temporada_equipo.competition_code)

        partidos_temporada_equipo = partidos_temporada_equipo.withColumn('date', to_date(col('date'),'yyyy-MM-dd'))

        # Obtenemos todos los partidos de liga
        partidos_liga_temporada_equipo = partidos_temporada_equipo.where(col('type') == 'first_tier').orderBy('date', ascending=False)

        # Comprobamos si hay datos de la posicion del equipo en su liga y asignamos valor
        try:
            # Comprobamos si el equipo jugo como local o visitante
            if partidos_liga_temporada_equipo.select('home_club_id').collect()[0][0] == id_equipo:
                posicion_temporada_equipo = int(partidos_liga_temporada_equipo.select('home_club_position').collect()[0][0])

            else:
                posicion_temporada_equipo = int(partidos_liga_temporada_equipo.select('away_club_position').collect()[0][0])

        except:
            posicion_temporada_equipo = 'No hay datos'

        # Comprobamos si hay informacion de la liga que jugó el equipo
        try:
            liga_equipo = partidos_liga_temporada_equipo.select('name').collect()[0][0]
        except:
            liga_equipo = 'segunda division'

    # Imprimimos las estadisticas del equipo
    st.write('Posición final en ', liga_equipo, ': ', posicion_temporada_equipo)
    st.write('Estadísticas como equipo local durante la temporada (V/E/D):       ', victorias_temporada_equipo_local, '/', empates_temporada_equipo_local, '/',
             derrotas_temporada_equipo_local)
    st.write('Estadísticas como equipo visitante durante la temporada (V/E/D):   ', victorias_temporada_equipo_visitante, '/', empates_temporada_equipo_visitante,
             '/', derrotas_temporada_equipo_visitante)

    # Seleccionar jugador del equipo
    if st.checkbox('Seleccionar jugador del equipo'):
        with st.spinner('Cargando datos...'):
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
            posicion_jugador = getPosicionJugador(id_jugador)

            goles_minuto = goles_temporada_jugador/minutos_temporada_jugador
            asistencias_minuto = asistencias_temporada_jugador/minutos_temporada_jugador
            goles_partido = goles_temporada_jugador/num_partidos_temporada_jugador
            asistencias_partido = asistencias_temporada_jugador/num_partidos_temporada_jugador

        st.write('Posición: ', posicion_jugador)
        st.write('Partidos jugados: ', num_partidos_temporada_jugador)
        st.write('Goles en la temporada ', temporada_largo, ': ', goles_temporada_jugador)
        st.write('Asistencias en la temporada ', temporada_largo, ': ', asistencias_temporada_jugador)
        st.write(" ")
        st.write('Relacion goles/partido jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(goles_partido)))
        st.write('Relacion asistencias/partido jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(asistencias_partido)))
        st.write(" ")
        st.write('Relacion goles/minuto jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(goles_minuto)))
        st.write('Relacion asistencias/minuto jugado en la temporada ', temporada_largo, ': ', float('{:.5f}'.format(asistencias_minuto)))

#############################################################
# Temporada 2019-2020
#############################################################

elif user_select == 'Temporada 19-20':
    # Obtenemos los jugadores del archivo
    data_jugadores_19_20 = data_final
    jugadores_19_20 = data_jugadores_19_20.select('Player').collect()

    # Creamos la lista que mostraremos en el selectbox metiendo los nombres de todos los jugadores del archivo
    jugadores = []
    for jugador in jugadores_19_20:
        jugadores.append(jugador['Player'])

    st.subheader('Rendimiento y valor de un jugador')
    jugador_seleccionado = st.selectbox('Seleccione el jugador', jugadores)

    # Obtenemos la posicion del jugador
    posicion_jugador = data_jugadores_19_20.select('Pos').where(col('Player') == jugador_seleccionado).collect()[0][0]
    # Comprobamos que hayan datos
    if posicion_jugador == None:
        st.error('No hay datos del jugador seleccionado')
    else:
        # Comprobamos si juega en mas de una posicion
        if len(posicion_jugador.split(',')) == 2:
            # Obtenemos la posición version extendida del jugador
            posicion_jugador_extendido = DICT_POSICION.get(posicion_jugador.split(',')[0]) + " - " + DICT_POSICION.get(posicion_jugador.split(',')[1])
        else:
            posicion_jugador_extendido = DICT_POSICION.get(posicion_jugador)

        # Obtenemos el valor del jugador
        valor_jugador = data_jugadores_19_20.select('Market value').where(col('Player') == jugador_seleccionado).collect()[0][0]

        equipo = data_jugadores_19_20.select('club_x').where(col('Player') == jugador_seleccionado).collect()[0][0]
        liga = data_jugadores_19_20.select('League').where(col('Player') == jugador_seleccionado).collect()[0][0]
        nacion = data_jugadores_19_20.select('Nation').where(col('Player') == jugador_seleccionado).collect()[0][0]

        st.write('Posición: ', posicion_jugador_extendido)
        st.write('Valor: ', valor_jugador)
        st.write('Equipo: ', equipo)
        st.write('Liga: ', liga)
        st.write('Nacionalidad: ', nacion)

        medias_jugador = data_jugadores_19_20.select(col('Player').alias('Tipo'), 'Gls90', 'Ast90', 'SoT%', 'G/Sh', 'Blocks', 'Cmp%').where(
            col('Player') == jugador_seleccionado)

        comparacion_seleccionada = st.selectbox('Seleccione con qué desea comparar al jugador (la comparación se realiza con el resto de jugadores de su posición)', ['Equipo', 'Liga', 'Nacionalidad'])
        if comparacion_seleccionada == 'Equipo':
            # Obtener estadisticas del resto de compañeros del equipo en su misma posicion
            medias_equipo_posicion = data_jugadores_19_20.where((col('Club_x') == equipo) & (col('Pos') == posicion_jugador)).groupBy('Pos')\
                                                    .agg(first('Club_x').alias('Tipo'), avg('Gls90').alias('Gls90'),
                                                         avg('Ast90').alias('Ast90'), avg('SoT%').alias('SoT%'),
                                                         avg('G/Sh').alias('G/Sh'), avg('Blocks').alias('Blocks'),
                                                         avg('Cmp%').alias('Cmp%'))
            medias_equipo_posicion = medias_equipo_posicion.drop('Pos')
            df_comparado = medias_equipo_posicion.toPandas().append(medias_jugador.toPandas())

            st.write("Goles por partido (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['Gls90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Gls90'])))

            st.write("Asistencias por partido (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['Ast90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Ast90'])))

            st.write("Porcentaje de acierto a puerta (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['SoT%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['SoT%'])))

            st.write("Goles por disparo (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['G/Sh'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['G/Sh'])))

            st.write("Pases completados (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['Cmp%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Cmp%'])))

            st.write("Bloqueos exitosos (Equipo/Jugador): ",
                     float('{:.2f}'.format(medias_equipo_posicion.collect()[0]['Blocks'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Blocks'])))

            fig = px.bar(df_comparado, x="Tipo", y=df_comparado.columns, barmode='group', height=400)
            st.plotly_chart(fig)

            st.write(df_comparado)


        elif comparacion_seleccionada == 'Liga':
            # Obtener estadisticas del resto de compañeros del equipo en su misma posicion
            medias_liga_posicion = data_jugadores_19_20.where(
                (col('League') == liga) & (col('Pos') == posicion_jugador)).groupBy('Pos') \
                .agg(first('League').alias('Tipo'), avg('Gls90').alias('Gls90'), avg('Ast90').alias('Ast90'), avg('SoT%').alias('SoT%'),
                     avg('G/Sh').alias('G/Sh'), avg('Blocks').alias('Blocks'), avg('Cmp%').alias('Cmp%'))
            medias_liga_posicion = medias_liga_posicion.drop('Pos')
            df_comparado = medias_liga_posicion.toPandas().append(medias_jugador.toPandas())

            st.write("Goles por partido (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['Gls90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Gls90'])))

            st.write("Asistencias por partido (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['Ast90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Ast90'])))

            st.write("Porcentaje de acierto a puerta (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['SoT%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['SoT%'])))

            st.write("Goles por disparo (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['G/Sh'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['G/Sh'])))

            st.write("Pases completados (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['Cmp%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Cmp%'])))

            st.write("Bloqueos exitosos (Liga/Jugador): ",
                     float('{:.2f}'.format(medias_liga_posicion.collect()[0]['Blocks'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Blocks'])))


            df_comparado.columns = ['Tipo', 'Goles por partido', 'Asistencias por partido', '% Disparos a puerta', 'Goles por disparo', 'Bloqueos', '% Pases acertados']
            fig = px.bar(df_comparado, x="Tipo", y=df_comparado.columns, barmode='group', height=400)
            st.plotly_chart(fig)
            st.write(df_comparado)

        elif comparacion_seleccionada == 'Nacionalidad':
            # Obtener estadisticas del resto de compañeros del equipo en su misma posicion
            medias_nacion_posicion = data_jugadores_19_20.where(
                (col('Nation') == nacion) & (col('Pos') == posicion_jugador)).groupBy('Pos') \
                .agg(first('Nation').alias('Tipo'), avg('Gls90').alias('Gls90'), avg('Ast90').alias('Ast90'), avg('SoT%').alias('SoT%'),
                     avg('G/Sh').alias('G/Sh'), avg('Blocks').alias('Blocks'), avg('Cmp%').alias('Cmp%'))
            medias_nacion_posicion = medias_nacion_posicion.drop('Pos')
            df_comparado = medias_nacion_posicion.toPandas().append(medias_jugador.toPandas())

            st.write("Goles por partido (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['Gls90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Gls90'])))

            st.write("Asistencias por partido (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['Ast90'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Ast90'])))

            st.write("Porcentaje de acierto a puerta (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['SoT%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['SoT%'])))

            st.write("Goles por disparo (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['G/Sh'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['G/Sh'])))

            st.write("Pases completados (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['Cmp%'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Cmp%'])))

            st.write("Bloqueos exitosos (Nacionalidad/Jugador): ",
                     float('{:.2f}'.format(medias_nacion_posicion.collect()[0]['Blocks'])), '/',
                     float('{:.2f}'.format(medias_jugador.collect()[0]['Blocks'])))

            fig = px.bar(df_comparado, x="Tipo", y=df_comparado.columns, barmode='group', height=400)
            st.plotly_chart(fig)

            st.write(df_comparado)