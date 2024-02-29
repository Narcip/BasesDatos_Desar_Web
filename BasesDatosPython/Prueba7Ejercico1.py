import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import matplotlib.pyplot as plt

# Clase para extraer datos de una URL
class ExtractorDatos:
    def __init__(self, url):
        # Almacena la URL y configura el atributo privado _datos como None
        self.url = url
        self._datos = None

    # Método privado para realizar la solicitud HTTP y obtener el contenido de la página
    def _realizar_solicitud_http(self):
        try:
            # Realiza la solicitud HTTP usando la biblioteca requests
            response = requests.get(self.url)
            response.raise_for_status()  # Verifica si la solicitud fue exitosa
            return response.text
        except requests.exceptions.RequestException as e:
            # Maneja cualquier excepción que ocurra durante la solicitud HTTP
            raise RuntimeError(f"Error al realizar la solicitud HTTP: {e}")

    # Método para extraer datos de la página web y almacenarlos en un DataFrame
    def extraer_datos(self):
        response = self._realizar_solicitud_http()
        if response:
            soup = BeautifulSoup(response, 'html.parser')
            tabla_sencillos = soup.find('table', {'class': 'wikitable'})
            if tabla_sencillos:
                # Extrae los nombres de las columnas de la tabla
                nombres_columnas = [th.text.replace('\n', '').strip() for th in tabla_sencillos.find('tr').find_all('th')]
                # Utiliza una list comprehension para crear la lista de datos
                datos = [self._extraer_datos_fila(fila) for fila in tabla_sencillos.find_all('tr')[1:]]
                self._datos = pd.DataFrame(datos, columns=nombres_columnas)
                return self._datos

    # Método estático para extraer datos de una fila de la tabla
    @staticmethod
    def _extraer_datos_fila(fila):
        celdas = fila.select('td')
        # Inicializa un diccionario para almacenar los datos de la fila
        diccionario_fila = {
            'Tema': celdas[0].text.strip(),
            'Intérprete': celdas[1].find('a', title=True).get('title', '').strip(),
            'Año': celdas[2].text.strip().split('/')[0],
            'Semanas': celdas[3].text.strip(),
            'País': ExtractorDatos._extraer_pais(celdas[4])
        }
        return diccionario_fila

    # Método estático para extraer el país de una celda
    @staticmethod
    def _extraer_pais(celda_pais):
        elementos_pais = celda_pais.find_all('span', style=['display:none;'])
        if elementos_pais:
            return elementos_pais[0].text.strip()
        else:
            mas_paises = celda_pais.find_all('a', title=True)
            return mas_paises[-1].get('title', '') if mas_paises else ''

# Clase para condicionar datos según ciertas reglas
class CondicionandoDatos:
    
    # Diccionario de mapeo que asocia países con idioma y continente
    mapeo_condiciones = {
            'España': {'Idioma': 'Español', 'Continente': 'Europa'},
            'Argentina': {'Idioma': 'Español', 'Continente': 'América del Sur'},
            'Francia': {'Idioma': 'Francés', 'Continente': 'Europa'},
            'Colombia': {'Idioma': 'Español', 'Continente': 'América del Sur'},
            'Venezuela': {'Idioma': 'Español', 'Continente': 'Europa'},
            'Puerto Rico': {'Idioma': 'Inglés', 'Continente': 'Europa'},
            'Estados Unidos': {'Idioma': 'Inglés', 'Continente': 'Europa'},
            'Canadá': {'Idioma': 'Inglés', 'Continente': 'América del Norte'},
            'Guyana': {'Idioma': 'Inglés', 'Continente': 'América del Sur'},
            'Brasil': {'Idioma': 'Portugués', 'Continente': 'América del Sur'},
            'Alemania': {'Idioma': 'Alemán', 'Continente': 'Europa'},
            'Cuba': {'Idioma': 'Español', 'Continente': 'América Central'},
            'Suecia': {'Idioma': 'Sueco', 'Continente': 'Europa'},
            'Reino Unido': {'Idioma': 'Inglés', 'Continente': 'Europa'},
            # Puedes agregar más países y sus condiciones según tus necesidades
    }
    
    def __init__(self, extractor):
        self.extractor = extractor
        self.datos_condicionados = pd.DataFrame()

    def ingresar_idioma_continente(self):
        try:
            # Intenta obtener el DataFrame original usando el extractor
            df_original = self.extractor.extraer_datos()

            # Obtiene idiomas y continentes
            idiomas, continentes = self._obtener_idiomas_y_continentes(df_original['País'])

            # Agrega las nuevas columnas al DataFrame original
            df_original['Idioma'] = idiomas
            df_original['Continente'] = continentes

            # Almacena el DataFrame condicionado en la propiedad de la clase
            self.datos_condicionados = df_original
            return df_original  # Devuelve el DataFrame condicionado

        except Exception as e:
            # Manejo de excepciones en caso de error
            print(f"Error al condicionar datos: {e}")
            return None  # Devuelve None en caso de error

    def _obtener_idiomas_y_continentes(self, paises):
        # Listas para almacenar idiomas y continentes
        idiomas = []
        continentes = []

        # Itera sobre los países
        for pais in paises:
            if pais in self.mapeo_condiciones:
                # Si el país está en el diccionario de mapeo, agrega idioma y continente
                idiomas.append(self.mapeo_condiciones[pais]['Idioma'])
                continentes.append(self.mapeo_condiciones[pais]['Continente'])
            else:
                # Si el país no está en el diccionario, agrega valores nulos
                idiomas.append(None)
                continentes.append(None)

        return idiomas, continentes


# Clase para interactuar con una base de datos SQLite
class DataBase:
    def __init__(self):
        try:
            # Establecer una conexión a la base de datos SQLite
            self.conexion = sqlite3.connect('database.db')

            # Crear un objeto cursor para ejecutar comandos SQL
            self.cursor = self.conexion.cursor()

            # Llamar al método para crear la tabla si no existe
            self.crear_data()

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la conexión
            print(f"Error al conectar a la base de datos: {e}")

    # Método para crear la tabla 'sencillos' si no existe
    def crear_data(self):
        try:
            # Ejecutar el comando SQL para crear la tabla 'sencillos' si no existe
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sencillos (
                    id INTEGER PRIMARY KEY,
                    Tema TEXT,
                    Intérprete TEXT,
                    Año INTEGER,
                    Semanas INTEGER,
                    País TEXT,
                    Idioma TEXT,
                    Continente TEXT
                );
            ''')

            # Confirmar los cambios en la base de datos
            self.conexion.commit()

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la creación de la tabla
            print(f"Error al crear la tabla en la base de datos: {e}")

    # Método para insertar datos en la tabla 'sencillos'
    def insertar_datos(self, df):
        try:
            # Insertar los datos del DataFrame en la tabla 'sencillos'
            df.to_sql('sencillos', self.conexion, if_exists='replace', index=False)

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la inserción de datos
            print(f"Error al insertar datos en la base de datos: {e}")

    # Método para cerrar la conexión a la base de datos
    def close_connection(self):
        try:
            # Cerrar la conexión a la base de datos
            self.conexion.close()

        except Exception as e:
            # Manejar cualquier excepción que ocurra al cerrar la conexión
            print(f"Error al cerrar la conexión a la base de datos: {e}")

# Clase para introducir datos en la base de datos después de condicionarlos
class IntroductorData:
    def __init__(self, condicionador, database):
        # Almacena instancias de CondicionandoDatos y DataBase
        self.condicionador = condicionador
        self.database = database

        try:
            # Llamar al método para introducir datos
            self.introducir_datos()

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la inicialización
            print(f"Error al introducir datos: {e}")

    # Método para introducir datos condicionados en la base de datos
    def introducir_datos(self):
        try:
            # Llamar al método para ingresar idioma y continente en el objeto CondicionandoDatos
            self.condicionador.ingresar_idioma_continente()

            # Insertar los datos condicionados en la base de datos utilizando el objeto DataBase
            self.database.insertar_datos(self.condicionador.datos_condicionados)

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la introducción de datos
            print(f"Error al introducir datos: {e}")

# Clase para realizar consultas SQL en la base de datos
class ConsultasSql:
    def __init__(self, database):
        # Almacena una instancia de DataBase
        self.database = database

    # Método para ejecutar una consulta SQL y devolver el resultado
    def consultar(self, consulta):
        try:
            # Ejecutar la consulta SQL y obtener el resultado
            resultado = self.database.cursor.execute(consulta).fetchall()
            return resultado

        except Exception as e:
            # Manejar cualquier excepción que ocurra durante la ejecución de la consulta SQL
            print(f"Error al ejecutar la consulta SQL: {e}")
            return None

    # Método para mostrar los resultados de la consulta en un DataFrame y gráficamente
    def entrega_de_datos(self, resultado):
        if resultado:
            try:
                # Crear un DataFrame a partir de los resultados y mostrarlo por pantalla
                df_resultado = pd.DataFrame(resultado, columns=[desc[0] for desc in self.database.cursor.description])
                
                # Visualizar la tabla utilizando matplotlib (opcional)
                plt.rcParams['figure.autolayout'] = True
                plt.rcParams['figure.figsize'] = [6.0, 6.0]

                # Crear y mostrar la tabla utilizando matplotlib
                plt.title('TABLA DE RESULTADOS', fontsize=20)
                plt.table(cellText=df_resultado.values, colLabels=df_resultado.columns, cellLoc='center', loc='center')
                plt.axis('off')  # Desactivar ejes para que se vea solo la tabla
                plt.show()

            except Exception as e:
                # Manejar cualquier excepción que ocurra al mostrar resultados
                print(f"Error al mostrar resultados: {e}")
        else:
            print("La consulta no devolvió resultados.")

    # Método para cerrar la conexión a la base de datos
    def close_connection(self):
        try:
            self.database.close_connection()

        except Exception as e:
            print(f"Error al cerrar la conexión a la base de datos: {e}")

# Función principal para interactuar con el programa
def menu():
    try:
        # URL de la página a extraer datos
        url = 'https://es.wikipedia.org/wiki/Anexo:Sencillos_n%C3%BAmero_uno_en_Espa%C3%B1a#canciones_c0n_m%C3%A1s_semanas_en_el_n%C3%BAmero_uno'
        
        # Crear instancia del ExtractorDatos
        extractor = ExtractorDatos(url)

        # Crear instancia del CondicionandoDatos y pasarle el ExtractorDatos
        condicionador = CondicionandoDatos(extractor)

        # Crear instancia de la base de datos
        database = DataBase()

        # Crear instancia de IntroductorData y pasarle el CondicionandoDatos y la base de datos
        introductor = IntroductorData(condicionador, database)

        # Crear instancia de ConsultasSql y pasarle la base de datos
        consultas = ConsultasSql(database)

        while True:
            print("MENU PRINCIPAL")
            print("1. Realizar consulta SQL")
            print("2. Salir")

            opcion = input("Dime qué opción eliges: ")

            if opcion == '1':
                consulta_usuario = input("Ingrese su consulta SQL: ")
                resultado_consulta = consultas.consultar(consulta_usuario)
                consultas.entrega_de_datos(resultado_consulta)
            elif opcion == '2':

                break
            else:
                print("Opción no válida. Intente nuevamente.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        if 'database' in locals():
            database.close_connection()

if __name__ == '__main__':
    menu()
