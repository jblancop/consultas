/*

Consulta AF de MongoDB para crear un cuadro de mandos en nuestro BI que permita analizar las reacciones (obtenidas mediante reconocimiento facial) de los 
usuarios a los vídeos emitidos en las tiendas de nuestros clientes

*/

var pipeline = 
[
	{
		"$match": //Se ataca a la colección "sensorsData", que almacena los registros individuales de los diferentes sensores
		{
		    "idSensor": {"$in": [2,3]} //Se deja pasar sólo aquellos relacionados con los de tipo 3, que emiten, y los de tipo 2, que registran las reacciones
		}
	},
    {
        "$lookup": //Se une cada documento con los demás -incluyendo consigo mismo- y se agrupan en un "array" con tantas posiciones como documentos
        {
            "from": "sensorsData", //Se trata de un "autojoin"
            "localField": "idDevice", //La unión se hace en función del dispositivo, que es el que contiene ambos tipos sensores
            "foreignField": "idDevice",
            "as": "array" //Arreglo de tipo diccionario
        }
    },
    {
        "$unwind": "$array" //Descompone los archivos en función de las posiciones del "array"
    },
    {
        "$match": //Se deja pasar sólo aquellos archivos con el patrón emisión-reacción [1] 
        {
            "$and": 
            [
                {"data.inicio": {"$exists": true}}, //Este campo proviene de un registro de un sensor de tipo 3 (emisor)
                {"array.data.BeginTime": {"$exists": true}} //Éste, de uno de tipo 2 (receptor)
            ]
        }
    },
    {
        "$addFields": //Creación de parámetros temporales
        {
            "dtBroadcastStart": {"$toDate": "$data.inicio"}, //Comienzo (y fin) de la emisión del vídeo
            "dtBroadcastEnd": {"$toDate": "$data.fin"},
            "dtTrackingStart": {"$toDate": "$array.data.BeginTime"}, //Comienzo (y fin) del seguimiento del usuario
			"dtTrackingEnd": {"$add": [{"$toDate": "$array.data.BeginTime"}, {"$multiply": ["$array.data.TrackingDuration", 1000]}]}
        }
    },
    {
        "$match": //Filtrado de los documentos que cumplen las condiciones de solapamiento temporal [2]
        {
            "$expr":
            {
                "$and": 
                [
                    {"$lt": ["$dtBroadcastStart", "$dtTrackingEnd"]},
                    {"$gt": ["$dtBroadcastEnd", "$dtTrackingStart"]}
                ]
            }
        }
    },
    {
        "$project": //Selección final de parámetros
        {
            "_id": 0,
            "idDevice": "$idDevice", //Dispositivo que ha emitido el vídeo y ha captado la reacción de la persona
            "naBroadcast": "$data.archivo", //Nombre del archivo de vídeo emitido
			"naType": "$data.tipo",
            "dtBroadcastStart": 1,
            "dtBroadcastEnd": 1,
            "qtBroadcastDurationS": {"$divide": [{"$subtract": [{"$toDate": "$dtBroadcastEnd"}, {"$toDate": "$dtBroadcastStart"}]}, 1000]} //Duración de la emisión en segundos
            "naWeekday": //Parámetros temporales generales: día de la semana y mes
			{
			    "$switch": 
			    {
			        "branches": 
			        [
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 1]}, "then": "Domingo"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 2]}, "then": "Lunes"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 3]}, "then": "Martes"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 4]}, "then": "Miércoles"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 5]}, "then": "Jueves"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 6]}, "then": "Viernes"},
			            {"case": {"$eq": [{"$dayOfWeek": {"$toDate": "$data.inicio"}}, 7]}, "then": "Sábado"}
			        ],
			        "default": "Fecha incorrecta"
			    }
			},
			"naMonth":
			{
			    "$switch": 
			    {
			        "branches": 
			        [
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "01"]}, "then": "Enero"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "02"]}, "then": "Febrero"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "03"]}, "then": "Marzo"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "04"]}, "then": "Abril"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "05"]}, "then": "Mayo"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "06"]}, "then": "Junio"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "07"]}, "then": "Julio"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "08"]}, "then": "Agosto"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "09"]}, "then": "Septiembre"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "10"]}, "then": "Octubre"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "11"]}, "then": "Noviembre"},
			            {"case": {"$eq": [{"$substr": ["$data.inicio", 5, 2]}, "12"]}, "then": "Diciembre"}
			        ],
			        "default": "Fecha incorrecta"
			    }
			},
            "idPerson": "$array.data.PersonID", //La aplicación de reconocimiento le asigna un id a cada persona que detecta
			"dtTrackingStart": 1,
			"dtTrackingEnd": 1,
			"qtFaceDetected": //La aplicación puede detectar una persona pero no ser capaz de captar claramente su cara
			{
				"$cond": 
				{
					"if": {"$eq": ["$array.data.FaceInfo.IsDetected", true]}, 
					"then": 1, 
					"else": 0
				}
			},
			"qtMaleProbability": "$array.data.FaceInfo.MaleProbability", //Probabilidad de que la persona reconocida sea hombre [3]
			"qtAge": "$array.data.FaceInfo.Age", //Edad estimada de la persona detectada
			"naEmotion": "$array.data.FaceInfo.Emotion", //Emoción predominante en su rostro (tristeza, alegría, ira, etc.)
			"qtFaceDurationS": "$array.data.LookingDuration", //Duración en segundos de la mirada a cámara de la persona detectada
			"qtTrackingDurationS": "$array.data.TrackingDuration", //Duración en segundos del movimiento de seguimiento que hace el sensor
			"qtReId": "$array.data.ReIDInfo.NumReIDs" //Número de reidentificaciones de la persona
        }
	}
]

db.sensorsData.aggregate(pipeline)

/*

Notas:

[1] Se eliminan las demás combinaciones: emisión-emisión (innecesaria), reacción-reacción (innecesaria) y reacción-emisión (redundante)
[2] Para que un vídeo haya podido captar la atención de un usuario, su emisión ha de haber empezado antes que el fin del seguimiento del usuario y finalizado 
	después del inicio del seguimiento
[3] Si no supera el 50 %, se habría de considerar mujer

*/