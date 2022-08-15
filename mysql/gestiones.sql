/* Consulta SQL para crear un cuadro de mandos en nuestro BI (para uso de nuestros clientes) que permita analizar los servicios prestados/productos vendidos (gestiones) y el importe total de los mismos para cada tique expedido mediante nuestro gestor de turnos */

SELECT
    qu.id_code AS co_store, #Información de la tienda
    qu.name AS na_store,
    ti.id AS id_ticket, #Información del tique
    ti.name AS co_ticket,
    ti.issue_time AS dt_ticket, #Fecha de emisión del tique
    t1.na_service AS na_service, #Nombre del servicio/producto
    t1.qt_service AS qt_service, #Cantidad de cada servicio/producto 
    t1.qt_price AS qt_price, #Importe total (no individual de cada servicio/producto) de las adquisiciones
    us.name AS co_agent, #Empleado responsable de la gestión
    CASE WHEN us.id IS NULL THEN 'S/A' ELSE concat(us.first_name, ' ', us.family_name) END AS na_agent,
    ti.visitor_mobile AS co_phone, #Teléfono del cliente
    date(ti.issue_time) AS dt_day, #Resto de paramétros temporales
    CASE 
        WHEN weekday(ti.issue_time) = 0 THEN 'Lunes'
        WHEN weekday(ti.issue_time) = 1 THEN 'Martes'
        WHEN weekday(ti.issue_time) = 2 THEN 'Miércoles'
        WHEN weekday(ti.issue_time) = 3 THEN 'Jueves'
        WHEN weekday(ti.issue_time) = 4 THEN 'Viernes'
        WHEN weekday(ti.issue_time) = 5 THEN 'Sábado'
        WHEN weekday(ti.issue_time) = 6 THEN 'Domingo'
        ELSE NULL
    END AS na_dayweek,
    CASE 
        WHEN month(ti.issue_time) = 1 THEN 'Enero'
        WHEN month(ti.issue_time) = 2 THEN 'Febrero'
        WHEN month(ti.issue_time) = 3 THEN 'Marzo'
        WHEN month(ti.issue_time) = 4 THEN 'Abril'
        WHEN month(ti.issue_time) = 5 THEN 'Mayo'
        WHEN month(ti.issue_time) = 6 THEN 'Junio'
        WHEN month(ti.issue_time) = 7 THEN 'Julio'
        WHEN month(ti.issue_time) = 8 THEN 'Agosto'
        WHEN month(ti.issue_time) = 9 THEN 'Septiembre'
        WHEN month(ti.issue_time) = 10 THEN 'Octubre'
        WHEN month(ti.issue_time) = 11 THEN 'Noviembre'
        WHEN month(ti.issue_time) = 12 THEN 'Diciembre'
        ELSE NULL 
    END AS na_month,
    cast(year(ti.issue_time) AS CHAR) AS na_year,
    CASE 
        WHEN 0 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 3600 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[00:00, 01:00]'
        WHEN 3600 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 7200 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[01:00, 02:00]'
        WHEN 7200 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 10800 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[02:00, 03:00]'
        WHEN 10800 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 14400 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[03:00, 04:00]'
        WHEN 14400 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 18000 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[04:00, 05:00]'
        WHEN 18000 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 21600 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[05:00, 06:00]'
        WHEN 21600 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 25200 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[06:00, 07:00]'
        WHEN 25200 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 28800 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[07:00, 08:00]'
        WHEN 28800 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 32400 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[08:00, 09:00]'
        WHEN 32400 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 36000 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[09:00, 10:00]'
        WHEN 36000 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 39600 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[10:00, 11:00]'
        WHEN 39600 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 43200 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[11:00, 12:00]'
        WHEN 43200 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 46800 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[12:00, 13:00]'
        WHEN 46800 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 50400 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[13:00, 14:00]'
        WHEN 50400 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 54000 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[14:00, 15:00]'
        WHEN 54000 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 57600 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[15:00, 16:00]'
        WHEN 57600 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 61200 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[16:00, 17:00]'
        WHEN 61200 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 64800 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[17:00, 18:00]'
        WHEN 64800 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 68400 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[18:00, 19:00]'
        WHEN 68400 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 72000 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[19:00, 20:00]'
        WHEN 72000 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 75600 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[20:00, 21:00]'
        WHEN 75600 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 79200 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[21:00, 22:00]'
        WHEN 79200 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 82800 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[22:00, 23:00]'
        WHEN 82800 <= timestampdiff(second, date(ti.issue_time), ti.issue_time) AND 86400 > timestampdiff(second, date(ti.issue_time), ti.issue_time) THEN '[23:00, 24:00]'
        ELSE 's/a'
    END AS co_hour_interval,
    timestampdiff(month, date_format(ti.issue_time, '%Y-%m-01'), date_format(curdate(), '%Y-%m-01')) AS qt_months_from_now #Permite filtrar los registros en función del tiempo transcurrido [1]
FROM
    (
        SELECT
            t11.id_ticket,
            t11.id_service,
            t11.na_service,
            t11.qt_service,
            t12.qt_price
        FROM
            (
                SELECT
                    ticket_id AS id_ticket,
                    id AS id_service,
                    description AS na_service,
                    sum(amount) AS qt_service
                FROM 
                    ticket_close_info
                WHERE
                    type != 'PRICE' #El resto de tipos corresponden a servicios/productos
                GROUP BY
                    ticket_id,
                    description
            ) t11 #Subsubconsulta con los servicios/productos y las cantidades
            LEFT JOIN
            (
                SELECT
                    ticket_id AS id_ticket,
                    amount AS qt_price #En principio no debería haber más de un registro de tipo 'PRICE' por tique
                FROM 
                    ticket_close_info
                WHERE
                    type = 'PRICE'
            ) t12 ON t11.id_ticket = t12.id_ticket #Subconsulta con los importes [2]
    ) t1 #Subconsulta equivalente a ticket_close_info
        INNER JOIN mysql3.ticket AS ti ON t1.id_ticket = ti.id #No aparecen los tiques que no lleven aparejadas gestiones
            LEFT JOIN mysql3.user AS us ON ti.serviced_by_id = us.id
            LEFT JOIN mysql3.queue AS qu ON ti.queue_id = qu.id
WHERE
    ti.issue_time < curdate()
;

/*

Notas:

[1] Para crear datasets hijos más ligeros (limitados a los últimos 12 meses, por ejemplo) en el BI
[2] De esta manera se convierten en una nueva columna que acompaña al resto de productos/servicios

*/