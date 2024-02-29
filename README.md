Vamos a trabajar con una base de datos musical, en concreto, con la lista de canciones con más semanas en el número uno de las listas de España. Dicha información la podemos encontrar en la tabla de:

https://es.wikipedia.org/wiki/Anexo:Sencillos_n%C3%BAmero_uno_en_Espa%C3%B1a#Canciones_con_m%C3%A1s_semanas_en_el_n%C3%BAmero_uno

La idea es hacer una aplicación para extraer resultados e indicadores de la misma. A grandes rasgos, esto implica una serie de destrezas ya aprendidas

Identificar los registros
Diseñar la tabla
Crear la base de datos
Introducir los datos
Hacer las consultas pertienentes
Recordamos el comando COUNT, porque va a ser especialmente útiles para este ejercicio:

Para contar todos los registros de la tabla: SELECT COUNT(*) FROM tabla;
Para contar los registros que contienen un valor en el campo 'campo' (no cuenta los nulos): SELECT COUNT(campo) FROM tabla;
Para contar los registros de la tabla pero sin repeticiones: SELECT COUNT(DISTINCT campo) FROM tabla;
TAREAS (parte 1)

Crear la base de datos
Crear la tabla
Añadir una columna nueva con el campo IDIOMA
Añadir una columna nueva con el campo CONTINENTE
Insertar todos los datos
CONSIDERACIONES

Se considera como 'artista' al artista principal, el que sale en primer lugar.
Se considera como 'año' el primer año en el que fue número uno.
Se consideran los idiomas español, inglés, alemán, sueco, portugués y francés.
Se consideran los continentes Europa, Asia, África, América del Norte, América del Sur y Oceanía.
Un año tiene 52 semanas.
Se pueden añadir más columnas o actualizar los datos de forma que, sin alterar el resultado, el alumno encuentre más facilidad para obtener la respuesta correcta.
PREGUNTAS (parte 2)

¿Cuál es la canción más antigua de la lista?
¿Qué artista aparece más veces en esta lista?
¿Qué país tiene más artistas en esta lista?
¿Cuantas canciones distintas hay por cada idioma?
¿Cuál es el continente con más apariciones en la lista?
¿Qué canción ha estado más % de tiempo al año como número 1?
