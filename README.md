# Ejemplos de consultas a bases de datos

Extracción y transformación de información a partir de bases de datos **MySQL** y **MongoDB** para su carga en una aplicación BI, Tabulae. El *dataset* resultante, de formato tabular, puede ser transformado adicionalmente haciendo uso del lenguaje de programación propio de la aplicación, y a partir de él se pueden elaborar cuadros de mandos que permitan representar y visualizar los datos de un modo intuitivo y asequible al usuario final.

### MySQL

- **gestiones.sql**: Uso de subconsultas para rehacer una tabla de la base de datos y darle el formato apropiado.</br>
- **intervenciones.sql**: Uso de subconsultas para obtener diversas métricas.</br>
- **intervenciones_tecnicos.sql**: Uso de *UNION* para crear una tabla de formato único a partir de dos subconsultas diferentes.</br>
- **notificaciones.sql**: Uso de subconsultas anidadas para reorganizar en varias columnas información dispersa en la base de datos.</br>
- **percentil_80.sql**: Uso de variables y tablas temporales.</br>

### MongoDB

- **emisiones.js**: Uso de *$group* para el manejo de millones de registros.</br>
- **emisiones_reacciones.js**: Uso de *$lookup* sobre una misma colección para comprobar el solapamiento temporal entre dos tipos diferentes de archivos.</br>