/* Consulta SQL para crear un cuadro de mandos en nuestro BI (para uso propio) que permita el análisis del rendimiento de los técnicos (tanto propios como externos) implicados en las intervenciones ejecutadas por el servicio de atención al cliente de nuestra empresa */

SELECT
	concat(u.name, ' ', u.surname) AS na_user, #Técnico que realiza la acción
    1 AS qt_action, #Consulta agrupada al nivel de las acciones (una intervención puede implicar una o más acciones)
    t1.dt_action AS dt_action, #Momento de ejecución de la acción
    date(t1.dt_action) AS dt_day, #Resto de parámetros temporales
    cast(year(t1.dt_action) AS CHAR) AS na_year,
    CASE 
        WHEN month(t1.dt_action) = 1 THEN 'Enero'
        WHEN month(t1.dt_action) = 2 THEN 'Febrero'
        WHEN month(t1.dt_action) = 3 THEN 'Marzo'
        WHEN month(t1.dt_action) = 4 THEN 'Abril'
        WHEN month(t1.dt_action) = 5 THEN 'Mayo'
        WHEN month(t1.dt_action) = 6 THEN 'Junio'
        WHEN month(t1.dt_action) = 7 THEN 'Julio'
        WHEN month(t1.dt_action) = 8 THEN 'Agosto'
        WHEN month(t1.dt_action) = 9 THEN 'Septiembre'
        WHEN month(t1.dt_action) = 10 THEN 'Octubre'
        WHEN month(t1.dt_action) = 11 THEN 'Noviembre'
        WHEN month(t1.dt_action) = 12 THEN 'Diciembre'
        ELSE 'S/A' 
    END AS na_month,
    CASE 
        WHEN weekday(t1.dt_action) = 0 THEN 'Lunes'
        WHEN weekday(t1.dt_action) = 1 THEN 'Martes'
        WHEN weekday(t1.dt_action) = 2 THEN 'Miércoles'
        WHEN weekday(t1.dt_action) = 3 THEN 'Jueves'
        WHEN weekday(t1.dt_action) = 4 THEN 'Viernes'
        WHEN weekday(t1.dt_action) = 5 THEN 'Sábado'
        WHEN weekday(t1.dt_action) = 6 THEN 'Domingo'
        ELSE 'S/A'
    END AS na_dayweek,
    CASE 
        WHEN 0 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 3600 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[00:00, 01:00]'
        WHEN 3600 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 7200 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[01:00, 02:00]'
        WHEN 7200 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 10800 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[02:00, 03:00]'
        WHEN 10800 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 14400 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[03:00, 04:00]'
        WHEN 14400 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 18000 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[04:00, 05:00]'
        WHEN 18000 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 21600 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[05:00, 06:00]'
        WHEN 21600 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 25200 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[06:00, 07:00]'
        WHEN 25200 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 28800 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[07:00, 08:00]'
        WHEN 28800 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 32400 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[08:00, 09:00]'
        WHEN 32400 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 36000 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[09:00, 10:00]'
        WHEN 36000 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 39600 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[10:00, 11:00]'
        WHEN 39600 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 43200 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[11:00, 12:00]'
        WHEN 43200 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 46800 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[12:00, 13:00]'
        WHEN 46800 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 50400 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[13:00, 14:00]'
        WHEN 50400 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 54000 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[14:00, 15:00]'
        WHEN 54000 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 57600 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[15:00, 16:00]'
        WHEN 57600 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 61200 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[16:00, 17:00]'
        WHEN 61200 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 64800 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[17:00, 18:00]'
        WHEN 64800 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 68400 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[18:00, 19:00]'
        WHEN 68400 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 72000 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[19:00, 20:00]'
        WHEN 72000 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 75600 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[20:00, 21:00]'
        WHEN 75600 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 79200 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[21:00, 22:00]'
        WHEN 79200 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 82800 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[22:00, 23:00]'
        WHEN 82800 <= timestampdiff(second, date(t1.dt_action), t1.dt_action) AND 86400 > timestampdiff(second, date(t1.dt_action), t1.dt_action) THEN '[23:00, 24:00]'
        ELSE 'S/A'
    END AS co_hour_interval,
    timestampdiff(month, date_format(t1.dt_action, '%Y-%m-01'), date_format(curdate(), '%Y-%m-01')) AS qt_months_from_now, #Permite filtrar los registros en función del tiempo transcurrido [1]
	t1.na_company AS na_company, #Empresa para la que trabaja el técnico
	i.code AS co_intervention, #Intervención de la que forma parte la acción
    itt.name AS na_type, #Tipo de intervención (auditoría, desmontaje, instalación, etc.)
    ipt.name AS na_priority, #Grado de urgencia de la intervención
    c.name AS na_customer, #Cliente atendido
	t1.qt_time AS qt_time #Tiempo empleado por acción
FROM
( 
	SELECT #Técnicos en remoto
		idUser AS id_user,
		"NuestraEmpresa" AS na_company, #Las acciones en remoto dependen exclusivamente del servicio de atención técnica de nuestra empresa
		idIntervention AS id_intervention,
		createDate AS dt_action,
	    timeSpent * 60 AS qt_time
	FROM 
		mysql2.interventionlogbookrecords

	UNION #Se ponen al mismo nivel todos los técnicos independientemente de su rol

	SELECT #Técnicos de campo
		t.idUser AS id_user,
		o.name AS na_company, #Las acciones de campo son ejecutadas por diferentes compañías externas
		ia.idIntervention AS id_intervention,
		ia.startDate AS dt_action,
		timestampdiff(SECOND, ia.startDate, ia.endDate) AS qt_time
	FROM 
		mysql2.technicians AS t
			LEFT JOIN mysql2.interventionactions AS ia ON t.id = ia.idTechnician
				LEFT JOIN mysql2.outsourcers_interventions AS oi ON ia.idIntervention = oi.idIntervention
					LEFT JOIN mysql2.outsourcers AS o ON oi.idOutsourcer = o.id
) t1
	LEFT JOIN mysql1.users AS u ON t1.id_user = u.id
	LEFT JOIN mysql2.interventions AS i ON t1.id_intervention = i.id
		LEFT JOIN mysql2.interventiontypes AS it ON i.idInterventionType = it.id
			LEFT JOIN mysql2.interventiontypetranslations AS itt ON it.id = itt.idInterventionType
        LEFT JOIN mysql1.companies AS c ON i.idCompany = c.id
        LEFT JOIN mysql2.interventionpriorities AS ip ON i.idInterventionPriority = ip.id
            LEFT JOIN mysql2.interventionprioritytranslations AS ipt ON ip.id = ipt.idInterventionPriority
WHERE
	itt.name != 'Actualización' #Se excluyen este tipo de intervenciones
ORDER BY
	t1.id_intervention
;

/*

Notas:

[1] Para crear datasets hijos más ligeros (limitados a los últimos 12 meses, por ejemplo) en el BI

*/