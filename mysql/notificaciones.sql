/* 

Consulta SQL para crear un cuadro de mandos en nuestro BI (para uso propio) que permita el análisis de las notificaciones (de un tipo específico de todas las 
posibles, de ahí las restricciones en el WHERE final) que recibe el servicio de atención técnica de nuestra empresa 

*/

SELECT
    t.code AS co_ticket, #La notificación se convierte en un tique (consulta agrupada a este nivel) en el sistema
    1 AS qt_ticket,
    date(t.creationDate) AS dt_day, #Parámetros temporales generales
    cast(year(t.creationDate) AS CHAR) AS na_year,
    CASE 
        WHEN month(t.creationDate) = 1 THEN 'Enero'
        WHEN month(t.creationDate) = 2 THEN 'Febrero'
        WHEN month(t.creationDate) = 3 THEN 'Marzo'
        WHEN month(t.creationDate) = 4 THEN 'Abril'
        WHEN month(t.creationDate) = 5 THEN 'Mayo'
        WHEN month(t.creationDate) = 6 THEN 'Junio'
        WHEN month(t.creationDate) = 7 THEN 'Julio'
        WHEN month(t.creationDate) = 8 THEN 'Agosto'
        WHEN month(t.creationDate) = 9 THEN 'Septiembre'
        WHEN month(t.creationDate) = 10 THEN 'Octubre'
        WHEN month(t.creationDate) = 11 THEN 'Noviembre'
        WHEN month(t.creationDate) = 12 THEN 'Diciembre'
        ELSE 'S/A' 
    END AS na_month,
    CASE 
        WHEN weekday(t.creationDate) = 0 THEN 'Lunes'
        WHEN weekday(t.creationDate) = 1 THEN 'Martes'
        WHEN weekday(t.creationDate) = 2 THEN 'Miércoles'
        WHEN weekday(t.creationDate) = 3 THEN 'Jueves'
        WHEN weekday(t.creationDate) = 4 THEN 'Viernes'
        WHEN weekday(t.creationDate) = 5 THEN 'Sábado'
        WHEN weekday(t.creationDate) = 6 THEN 'Domingo'
        ELSE 'S/A'
    END AS na_dayweek,
    CASE 
        WHEN 0 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 3600 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[00:00, 01:00]'
        WHEN 3600 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 7200 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[01:00, 02:00]'
        WHEN 7200 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 10800 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[02:00, 03:00]'
        WHEN 10800 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 14400 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[03:00, 04:00]'
        WHEN 14400 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 18000 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[04:00, 05:00]'
        WHEN 18000 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 21600 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[05:00, 06:00]'
        WHEN 21600 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 25200 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[06:00, 07:00]'
        WHEN 25200 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 28800 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[07:00, 08:00]'
        WHEN 28800 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 32400 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[08:00, 09:00]'
        WHEN 32400 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 36000 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[09:00, 10:00]'
        WHEN 36000 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 39600 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[10:00, 11:00]'
        WHEN 39600 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 43200 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[11:00, 12:00]'
        WHEN 43200 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 46800 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[12:00, 13:00]'
        WHEN 46800 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 50400 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[13:00, 14:00]'
        WHEN 50400 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 54000 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[14:00, 15:00]'
        WHEN 54000 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 57600 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[15:00, 16:00]'
        WHEN 57600 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 61200 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[16:00, 17:00]'
        WHEN 61200 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 64800 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[17:00, 18:00]'
        WHEN 64800 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 68400 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[18:00, 19:00]'
        WHEN 68400 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 72000 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[19:00, 20:00]'
        WHEN 72000 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 75600 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[20:00, 21:00]'
        WHEN 75600 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 79200 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[21:00, 22:00]'
        WHEN 79200 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 82800 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[22:00, 23:00]'
        WHEN 82800 <= timestampdiff(second, date(t.creationDate), t.creationDate) AND 86400 > timestampdiff(second, date(t.creationDate), t.creationDate) THEN '[23:00, 24:00]'
        ELSE 'S/A'
    END AS co_hour_interval,
    timestampdiff(month, date_format(t.creationDate, '%Y-%m-01'), date_format(curdate(), '%Y-%m-01')) AS qt_months_from_now, #Permite filtrar los registros en función del tiempo transcurrido [1]
    CASE
        WHEN inc.id IS NOT NULL THEN 'Incidencia'
        WHEN ins.id IS NOT NULL THEN 'Instalación'
        WHEN p.id IS NOT NULL THEN 'Preparación'
        WHEN s.id IS NOT NULL THEN 'Envío'
        WHEN r.id IS NOT NULL THEN 'Solicitud'
        WHEN o.id IS NOT NULL THEN 'Pedido'
        ELSE 'S/A'
    END AS na_type_ticket, #Categorización de la notificación; debido a las restricciones impuestas, todas son de un tipo concreto [2]
    tsp.name AS na_provider, #Empresa que se encarga de atender el tique (puede ser la nuestra o una externa)
    if(tsp.name = 'NuestraEmpresa', 1, 0) AS qt_own, #Recuento de tiques que dependen de nuestra empresa
    CASE
        WHEN t2.qt_providers = 1 THEN 'Único'
        WHEN t2.qt_providers > 1 THEN 'Múltiple'
        ELSE 'S/A'
    END AS co_provider, #En algunas ocasiones, el servicio puede ser prestado por más de una empresa
    c.name AS na_customer, #Cliente al que se presta el servicio; debido a las restricciones impuestas, es siempre el mismo [2]
    l.code AS co_store, #Tienda que envía la notificación
    l.commercialName AS na_store,
	t11.na_channel AS na_channel, #Posibles niveles de agrupación de las tiendas de este cliente: canal, tipología, concepto, categorización [3]
    t12.na_typology AS na_typology,
    t13.na_concept AS na_concept,
    t14.na_categorization AS na_categorization,
    tor.name AS na_origin, #Vía de entrada de la notificación (teléfono, correo electrónico, etc.); debido a las restricciones impuestas, el origen es siempre el mismo, una aplicación interna [2]
    ts.name AS na_service, #Nombre del servicio prestado
    itt.name AS na_category, #Categorías en que se agrupan los servicios
    t.title AS na_title_ticket, #Detalles sobre el servicio a prestar
    t.description AS na_description_ticket,
    tu.name AS na_priority,
    t.creationDate AS dt_creation, #Fecha de creación del tique
    t.revisionDate AS dt_revision, #Diferentes estados por los que puede pasar un tique hasta que termina la prestación del servicio [4] 
    if(t.revisionDate IS NOT NULL, 1, 0) AS qt_revised, 
    t.escalationDateStart AS dt_escalation_start,
    if(t.escalationDateStart IS NOT NULL, 1, 0) AS qt_escalated_start,
    t.escalationDateEnd AS dt_escalation_end,
    if(t.escalationDateEnd IS NOT NULL, 1, 0) AS qt_escalated_end,
    t.resolutionDate AS dt_resolution,
    if(t.resolutionDate IS NOT NULL, 1, 0) AS qt_resolved,
    t.closeDate AS dt_closing,
    if(t.closeDate IS NOT NULL, 1, 0) AS qt_closed,
    t.reopeningDate AS dt_reopening,
    if(t.reopeningDate IS NOT NULL, 1, 0) AS qt_reopened,
    if(t.paused = 1, 1, 0) AS qt_paused,
    if(t.paused = 1, 'Sí', 'No') AS co_paused
FROM 
    mysql1.tickets AS t
        LEFT JOIN mysql1.installations AS ins ON t.id = ins.id
        LEFT JOIN mysql1.preparations AS p ON t.id = p.id
        LEFT JOIN mysql1.shippings AS s ON t.id = s.id
        LEFT JOIN mysql1.requests AS r ON t.id = r.id
        LEFT JOIN mysql1.orders AS o ON t.id = o.id
        LEFT JOIN mysql1.ticketorigins AS tor ON t.idTicketOrigin = tor.id
        LEFT JOIN mysql1.ticketservices AS ts ON t.idTicketService = ts.id
        LEFT JOIN mysql1.incidents AS inc ON t.id = inc.id
            LEFT JOIN mysql1.incidenttypetranslations AS itt ON inc.idIncidentType = itt.idIncidentType AND itt.idLanguage = 1
        LEFT JOIN mysql1.locations AS l ON t.idLocation = l.id
			LEFT JOIN mysql1.companies AS c ON l.idCompany = c.id
			LEFT JOIN
            (
				SELECT
					llcv.idLocation AS id_store,
					max(if(lcv.idLocationCategory = 13, llcv.idLocationCategoryValue, NULL)) AS id_channel,
					max(if(lcv.idLocationCategory = 14, llcv.idLocationCategoryValue, NULL)) AS id_typology,
					max(if(lcv.idLocationCategory = 15, llcv.idLocationCategoryValue, NULL)) AS id_concept,
					max(if(lcv.idLocationCategory = 16, llcv.idLocationCategoryValue, NULL)) AS id_categorization
				FROM
					mysql1.locations_locationcategoryvalues AS llcv
						LEFT JOIN mysql1.locationcategoryvalues AS lcv ON llcv.idLocationCategoryValue = lcv.id
				WHERE
					lcv.idLocationCategory IN (13, 14, 15, 16)
				GROUP BY
					llcv.idLocation
            ) t1 ON l.id = t1.id_store #Subconsulta que crea las cuatro columnas correspondientes a las categorías en que se agrupan las tiendas de este cliente [5]
    			LEFT JOIN
                (
    				SELECT
    					id AS id_channel,
    					value AS na_channel
    				FROM
    					mysql1.locationcategoryvalues
    				WHERE
    					idLocationCategory = 13
                ) t11 ON t1.id_channel = t11.id_channel #Valores posibles para canal
    			LEFT JOIN
                (
    				SELECT
    					id AS id_typology,
    					value AS na_typology
    				FROM
    					mysql1.locationcategoryvalues
    				WHERE
    					idLocationCategory = 14
                ) t12 ON t1.id_typology = t12.id_typology #Valores posibles para tipología
    			LEFT JOIN
                (
    				SELECT
    					id AS id_concept,
    					value AS na_concept
    				FROM
    					mysql1.locationcategoryvalues
    				WHERE
    					idLocationCategory = 15
                ) t13 ON t1.id_concept = t13.id_concept #Valores posibles para concepto
    			LEFT JOIN
                (
    				SELECT
    					id AS id_categorization,
    					value AS na_categorization
    				FROM
    					mysql1.locationcategoryvalues
    				WHERE
    					idLocationCategory = 16
                ) t14 ON t1.id_categorization = t14.id_categorization #Valores posibles para categorización
        LEFT JOIN
        (
            SELECT
                ttsp.idTicket AS id_ticket,
                count(DISTINCT idTicketServiceProvider) AS qt_providers
            FROM 
                mysql1.tickets_ticketserviceproviders AS ttsp
                    LEFT JOIN mysql1.ticketserviceproviders AS tsp ON ttsp.idTicketServiceProvider = tsp.id
            GROUP BY
                ttsp.idTicket
        ) t2 ON t.id = t2.id_ticket #Recuento de proveedores
        LEFT JOIN mysql1.tickets_ticketserviceproviders AS ttsp ON t.id = ttsp.idTicket
            LEFT JOIN mysql1.ticketserviceproviders AS tsp ON ttsp.idTicketServiceProvider = tsp.id 
        LEFT JOIN mysql1.ticketurgencies AS tu ON t.idTicketUrgency = tu.id
        LEFT JOIN mysql1.users AS u ON t.idUser = u.id
WHERE 
    t.code NOT LIKE 'DR%' 
    AND t.idTicketOrigin = 7
    AND c.id = 85
;

/*

Notas:

[1] Para crear datasets hijos más ligeros (limitados a los últimos 12 meses, por ejemplo) en el BI
[2] Estos parámetros en realidad no varían pero podrían hacerlo si se eliminan las restricciones de la cláusula WHERE; se utilizan a modo de comprobación
[3] Para la creación de filtros segmentadores que permitan discriminar fácilmente para una determinada categoría de tiendas dentro del BI
[4] Revisada, escalada, resuelta, cerrada, reabierta y pausada. El BI los controla mediante una estructura condicional, de manera que cada tique sólo se 
    representa con un único estado en cada momento (salvo la pausa, que es aplicable a un tique en cualquier etapa del proceso)
[5] t1 crea una tabla en la que se relaciona cada id de tienda con los ids de las categorías que le corresponden, y las siguientes subconsultas (t11 a t14) 
    relacionan esos ids con sus valores de tipo cadena, entendibles por el usuario final

*/