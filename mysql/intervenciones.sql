/* 

Consulta SQL para crear un cuadro de mandos en nuestro BI (para uso propio) que permita el análisis de las intervenciones llevadas a cabo por el servicio de 
atención técnica de nuestra empresa 

*/

SELECT 
	i.id AS id_intervention,
	1 AS qt_intervention, #Consulta agrupada al nivel de las intervenciones
    i.code AS co_intervention,
    i.createDate AS dt_creation, #Fecha de registro de la intervención (no de su inicio efectivo)
    date(i.createDate) AS dt_day, #Resto de parámetros temporales
    cast(year(i.createDate) AS CHAR) AS na_year,
    CASE 
        WHEN month(i.createDate) = 1 THEN 'Enero'
        WHEN month(i.createDate) = 2 THEN 'Febrero'
        WHEN month(i.createDate) = 3 THEN 'Marzo'
        WHEN month(i.createDate) = 4 THEN 'Abril'
        WHEN month(i.createDate) = 5 THEN 'Mayo'
        WHEN month(i.createDate) = 6 THEN 'Junio'
        WHEN month(i.createDate) = 7 THEN 'Julio'
        WHEN month(i.createDate) = 8 THEN 'Agosto'
        WHEN month(i.createDate) = 9 THEN 'Septiembre'
        WHEN month(i.createDate) = 10 THEN 'Octubre'
        WHEN month(i.createDate) = 11 THEN 'Noviembre'
        WHEN month(i.createDate) = 12 THEN 'Diciembre'
        ELSE 'S/A' 
    END AS na_month,
    CASE 
        WHEN weekday(i.createDate) = 0 THEN 'Lunes'
        WHEN weekday(i.createDate) = 1 THEN 'Martes'
        WHEN weekday(i.createDate) = 2 THEN 'Miércoles'
        WHEN weekday(i.createDate) = 3 THEN 'Jueves'
        WHEN weekday(i.createDate) = 4 THEN 'Viernes'
        WHEN weekday(i.createDate) = 5 THEN 'Sábado'
        WHEN weekday(i.createDate) = 6 THEN 'Domingo'
        ELSE 'S/A'
    END AS na_dayweek,
    CASE 
        WHEN 0 <= timestampdiff(second, date(i.createDate), i.createDate) AND 3600 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[00:00, 01:00]'
        WHEN 3600 <= timestampdiff(second, date(i.createDate), i.createDate) AND 7200 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[01:00, 02:00]'
        WHEN 7200 <= timestampdiff(second, date(i.createDate), i.createDate) AND 10800 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[02:00, 03:00]'
        WHEN 10800 <= timestampdiff(second, date(i.createDate), i.createDate) AND 14400 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[03:00, 04:00]'
        WHEN 14400 <= timestampdiff(second, date(i.createDate), i.createDate) AND 18000 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[04:00, 05:00]'
        WHEN 18000 <= timestampdiff(second, date(i.createDate), i.createDate) AND 21600 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[05:00, 06:00]'
        WHEN 21600 <= timestampdiff(second, date(i.createDate), i.createDate) AND 25200 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[06:00, 07:00]'
        WHEN 25200 <= timestampdiff(second, date(i.createDate), i.createDate) AND 28800 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[07:00, 08:00]'
        WHEN 28800 <= timestampdiff(second, date(i.createDate), i.createDate) AND 32400 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[08:00, 09:00]'
        WHEN 32400 <= timestampdiff(second, date(i.createDate), i.createDate) AND 36000 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[09:00, 10:00]'
        WHEN 36000 <= timestampdiff(second, date(i.createDate), i.createDate) AND 39600 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[10:00, 11:00]'
        WHEN 39600 <= timestampdiff(second, date(i.createDate), i.createDate) AND 43200 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[11:00, 12:00]'
        WHEN 43200 <= timestampdiff(second, date(i.createDate), i.createDate) AND 46800 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[12:00, 13:00]'
        WHEN 46800 <= timestampdiff(second, date(i.createDate), i.createDate) AND 50400 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[13:00, 14:00]'
        WHEN 50400 <= timestampdiff(second, date(i.createDate), i.createDate) AND 54000 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[14:00, 15:00]'
        WHEN 54000 <= timestampdiff(second, date(i.createDate), i.createDate) AND 57600 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[15:00, 16:00]'
        WHEN 57600 <= timestampdiff(second, date(i.createDate), i.createDate) AND 61200 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[16:00, 17:00]'
        WHEN 61200 <= timestampdiff(second, date(i.createDate), i.createDate) AND 64800 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[17:00, 18:00]'
        WHEN 64800 <= timestampdiff(second, date(i.createDate), i.createDate) AND 68400 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[18:00, 19:00]'
        WHEN 68400 <= timestampdiff(second, date(i.createDate), i.createDate) AND 72000 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[19:00, 20:00]'
        WHEN 72000 <= timestampdiff(second, date(i.createDate), i.createDate) AND 75600 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[20:00, 21:00]'
        WHEN 75600 <= timestampdiff(second, date(i.createDate), i.createDate) AND 79200 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[21:00, 22:00]'
        WHEN 79200 <= timestampdiff(second, date(i.createDate), i.createDate) AND 82800 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[22:00, 23:00]'
        WHEN 82800 <= timestampdiff(second, date(i.createDate), i.createDate) AND 86400 > timestampdiff(second, date(i.createDate), i.createDate) THEN '[23:00, 24:00]'
        ELSE 'S/A'
    END AS co_hour_interval,
    timestampdiff(month, date_format(i.createDate, '%Y-%m-01'), date_format(curdate(), '%Y-%m-01')) AS qt_months_from_now, #Permite filtrar los registros en función del tiempo transcurrido [1]
    itt.name AS na_type, #Tipo de intervención (auditoría, desmontaje, instalación, etc.)
    ipt.name AS na_priority, #Grado de urgencia de la intervención
    c.name AS na_customer, #Cliente atendido
    i.plannedStartDate AS dt_planned_start, #Inicio planeado
    i.plannedEndDate AS dt_planned_end,
    t3.dt_field_start AS dt_start, #Inicio efectivo
    i.pauseDate AS dt_pause,
    if(i.completeDate IS NOT NULL, i.completeDate, i.cancelDate) AS dt_end, #El fin efectivo puede ser el momento de resolución o el de cancelación
    1 AS qt_open, #Toda intervención se considera abierta por el hecho de haber sido registrada en la BD
    if(t2.qt_field_companies IS NOT NULL AND i.plannedStartDate IS NOT NULL, 1, 0) AS qt_assigned, #Diferentes estados por los que puede pasar una intervención hasta su resolución [2]
    if(t3.qt_field_users IS NOT NULL OR t5.qt_field_users IS NOT NULL, 1, 0) AS qt_managed,
    if(t3.qt_field_actions IS NOT NULL, 1, 0) AS qt_ongoing,
    if(i.pauseDate IS NOT NULL, 1, 0) AS qt_paused,
    if(i.completed = 1, 1, 0) AS qt_completed,
    if(i.cancelled = 1, 1, 0) AS qt_canceled, 
    t1.qt_office_users AS qt_office_users, #Número de técnicos en remoto de nuestra empresa que intervienen en la resolución de la intervención
    t1.qt_office_actions AS qt_office_actions, #Número de acciones en remoto tomadas (una intervención implica una o más acciones)
    t1.qt_office_time AS qt_office_time, #Tiempo total empleado en remoto
    t2.qt_field_companies AS qt_field_companies, #Número de empresas externas implicadas en la resolución de la invervención
    if(t3.qt_field_users <= t5.qt_field_users, t5.qt_field_users, coalesce(t5.qt_field_users, t3.qt_field_users)) AS qt_field_users, #Número de técnicos de campo implicados en la resolución de la invervención [3]
    t3.qt_field_actions AS qt_field_actions, #Número de acciones de campo tomadas
    t4.qt_planned_field_tasks AS qt_planned_field_tasks, #Las acciones de campo se dividen en diferentes tareas, que los técnicos en remoto planifican y asignan para que los técnico de campo completen
    t5.qt_assigned_field_tasks AS qt_assigned_field_tasks,
    t4.qt_completed_field_tasks AS qt_completed_field_tasks,
    t3.qt_field_time AS qt_field_time #Tiempo total empleado en campo
FROM 
	mysql2.interventions AS i
		LEFT JOIN mysql2.interventiontypes AS it ON i.idInterventionType = it.id
			LEFT JOIN mysql2.interventiontypetranslations AS itt ON it.id = itt.idInterventionType
		LEFT JOIN mysql2.interventionpriorities AS ip ON i.idInterventionPriority = ip.id
			LEFT JOIN mysql2.interventionprioritytranslations AS ipt ON ip.id = ipt.idInterventionPriority
		LEFT JOIN mysql1.companies AS c ON i.idCompany = c.id
		LEFT JOIN
		(
			SELECT
				idIntervention AS id_intervention,
				min(createDate) AS dt_office_start,
			    count(DISTINCT idUser) AS qt_office_users,
			    count(id) AS qt_office_actions,
			    sum(timeSpent) * 60 AS qt_office_time
			FROM 
				mysql2.interventionlogbookrecords
			GROUP BY
				id_intervention
		) t1 ON i.id = t1.id_intervention #Subconsulta con información sobre los técnicos en remoto del servicio de atención técnica de la empresa
		LEFT JOIN
		(
			SELECT
				idIntervention AS id_intervention,
				count(DISTINCT idOutsourcer) AS qt_field_companies
			FROM 
				mysql2.outsourcers_interventions
			GROUP BY
				idIntervention
		) t2 ON i.id = t2.id_intervention #Subconsulta sobre las empresas externas implicadas en la resolución de la intervención
		LEFT JOIN 
		(
			SELECT
				idIntervention AS id_intervention,
			    min(startDate) AS dt_field_start,
			    count(DISTINCT idTechnician) AS qt_field_users,
				sum(1) AS qt_field_actions,
			    sum(timestampdiff(SECOND, startDate, endDate)) AS qt_field_time
			FROM
				mysql2.interventionactions
			GROUP BY
				id_intervention
		) t3 ON i.id = t3.id_intervention #Subconsulta sobre los técnicos (pertenecientes a las empresas externas) y acciones de campo
		LEFT JOIN
		(
			SELECT
				idIntervention AS id_intervention,
			    count(id) AS qt_planned_field_tasks,
			    min(createDate) AS dt_tasks_start,
			    sum(completed) AS qt_completed_field_tasks
			FROM 
				mysql2.interventiontasks
			GROUP BY
				id_intervention
		) t4 ON i.id = t4.id_intervention #Subconsulta sobre las tareas planificadas y completadas
		LEFT JOIN
		(
			SELECT
				idIntervention AS id_intervention,
				count(DISTINCT idTechnician) AS qt_field_users,
    			count(DISTINCT idTask) AS qt_assigned_field_tasks
			FROM
				mysql2.technicians_interventions
			GROUP BY
				id_intervention
		) t5 ON i.id = t5.id_intervention #Subconsulta sobre los técnicos de campo y las tareas asignadas [3]
WHERE
	itt.name != 'Actualización' #Se excluyen este tipo de intervenciones
;

/*

Notas:

[1] Para crear datasets hijos más ligeros (limitados a los últimos 12 meses, por ejemplo) en el BI
[2] Abierta, asignada, gestionada, en marcha, completa, cancelada, pausada. El BI controla los estados mediante una estructura condicional, de manera que cada 
	intervención se representa con sólo uno de ellos en cada momento (salvo la pausa, que es aplicable en cualquier etapa del proceso)
[3] Por limitaciones de la aplicación que gestiona las intervenciones, no es obligatorio asignar una tarea a un técnico concreto aunque luego siempre se 
	verifique que ha sido completada, pero sin especificar quién lo ha hecho; de ahí que haya que contrastar en qt_field_users si aparecen más 
	técnicos de campo en t5 que en t3

*/