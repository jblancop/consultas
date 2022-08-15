/* 

Consulta SQL para crear un cuadro de mandos en nuestro BI (para uso de nuestros clientes) que permita el cálculo del percentil 80 (registro que deja por debajo 
el 80 % de la distribución) del tiempo de espera de los usuarios atendidos mediante nuestro gestor de turnos

*/

/* Variables iniciales */

SET @counter:= 0; #Contador, que empieza en 0
SET @previous:= ''; #Variable que guarda el concatenado tienda + día, que empieza como una cadena de texto vacía

/* Tablas temporales */

CREATE TEMPORARY TABLE IF NOT EXISTS ordered_stores_per_day AS #Registros numerados ordenados por tienda, día y tiempo de espera (ascendente)
(
	SELECT 
		@counter:= CASE 
				       WHEN @previous = concat(t.queue_id, '|', date(t.issue_time)) 
				       THEN @counter + 1 ELSE 1 
				   END AS id_row, #Si @previous tiene almacenado el mismo valor que el concatenado del registro actual, suma uno al contador [1]
		@previous:= concat(t.queue_id, '|', date(t.issue_time)) id_store_dt_day, #Almacena el concatenado para servir de referencia a @counter
		t.queue_id AS id_store,
		date(t.issue_time) AS dt_day,
		timestampdiff(second, t.issue_time, t.called_in_time) AS qt_waiting_time_s #Tiempo de espera por cliente individual
	FROM 
		mysql3.ticket AS t
	WHERE
        timestampdiff(second, t.issue_time, t.called_in_time) IS NOT NULL #Se excluyen los registros para los que no hay tiempo de espera [2]
	ORDER BY 
		id_store, 
        dt_day, 
        qt_waiting_time_s
);

CREATE TEMPORARY TABLE IF NOT EXISTS grouped_ordered_stores_per_day AS #Registros agrupados por tienda y día
(
	SELECT 
		id_store_dt_day,
		count(id_row) AS max_id_row, #Número total de registros (es decir, de clientes atendidos)
        round(avg(qt_waiting_time_s), 2) AS qt_waiting_time_avg_s #Tiempo de espera promedio
	FROM 
		ordered_stores_per_day #Tabla temporal anterior 
	GROUP BY 
		id_store_dt_day
);

/* JOIN entre ambas tablas temporales para calcular el P80 por tienda y día */

SELECT
	q.id_code AS co_queue, #Código de tienda
    q.name AS na_queue, #Nombre de tienda
	o.dt_day AS dt_day, #Parámetros temporales
	cast(year(o.dt_day) AS CHAR) AS na_year,
	CASE 
        WHEN month(o.dt_day) = 1 THEN 'Enero'
        WHEN month(o.dt_day) = 2 THEN 'Febrero'
        WHEN month(o.dt_day) = 3 THEN 'Marzo'
        WHEN month(o.dt_day) = 4 THEN 'Abril'
        WHEN month(o.dt_day) = 5 THEN 'Mayo'
        WHEN month(o.dt_day) = 6 THEN 'Junio'
        WHEN month(o.dt_day) = 7 THEN 'Julio'
        WHEN month(o.dt_day) = 8 THEN 'Agosto'
        WHEN month(o.dt_day) = 9 THEN 'Septiembre'
        WHEN month(o.dt_day) = 10 THEN 'Octubre'
        WHEN month(o.dt_day) = 11 THEN 'Noviembre'
        WHEN month(o.dt_day) = 12 THEN 'Diciembre'
        ELSE NULL 
    END AS na_month,
    CASE 
        WHEN weekday(o.dt_day) = 0 THEN 'Lunes'
        WHEN weekday(o.dt_day) = 1 THEN 'Martes'
        WHEN weekday(o.dt_day) = 2 THEN 'Miércoles'
        WHEN weekday(o.dt_day) = 3 THEN 'Jueves'
        WHEN weekday(o.dt_day) = 4 THEN 'Viernes'
        WHEN weekday(o.dt_day) = 5 THEN 'Sábado'
        WHEN weekday(o.dt_day) = 6 THEN 'Domingo'
        ELSE NULL
    END AS na_dayweek,
    timestampdiff(month, date_format(o.dt_day, '%Y-%m-01'), date_format(curdate(), '%Y-%m-01')) AS qt_months_from_now, #Permite filtrar los registros en función del tiempo transcurrido [3]
    round(g.qt_waiting_time_avg_s, 2) AS qt_waiting_time_avg_s, #Tiempo de espera medio
	o.qt_waiting_time_s AS qt_waiting_time_s_P80, #Tiempo que marca el percentil 80 (el filtrado se realiza en el WHERE)
    if(g.qt_waiting_time_avg_s > o.qt_waiting_time_s, 'Valores atípicos', '') AS co_outliers #Si el tiempo medio supera al percentil 80, se considera un valor atípico
FROM 
	ordered_stores_per_day AS o
		INNER JOIN grouped_ordered_stores_per_day AS g ON o.id_store_dt_day = g.id_store_dt_day
        INNER JOIN mysql3.queue AS q ON o.id_store = q.id
WHERE
	o.id_row = ceiling(g.max_id_row * 0.8) #Busca en el contador de la primera tabla temporal el valor que equivale al 80 % del máximo [4]
;

/*

	[1] En caso contrario, se trata de una tienda nueva y hay que empezar la cuenta en 1
	[2] Por no disponer de called_in_time; es decir, un usuario cuyo turno no fue llamado
	[3] Para crear datasets hijos más ligeros (limitados a los últimos 12 meses, por ejemplo) en el BI
	[4] Redondeado hacia arriba, que en principio es el criterio matemático

*/