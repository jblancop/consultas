/*

Consulta AF de MongoDB para crear un cuadro de mandos en nuestro BI (para uso de nuestros clientes) que permita analizar las emisiones de vídeo y audio 
realizadas en sus tiendas.

*/

var pipeline =
[
	{
	   "$match": {"group.$id": 11} //Se ataca a la colección "devices", por lo que se hace un filtrado inicial de los dispositivos instalados para el cliente nº 11
	},
	{
	   "$lookup": //Unión con la colección que almacena los registros individuales de los diferentes sensores (cada dispositivo consta de uno o más sensores)
	   {
	      "from": "sensorsDataHistoric",
	      "localField": "_id", //Clave primaria de "devices"
	      "foreignField": "idDevice", //Campo equivalente de "sensorsDataHistoric"
	      "as": "array" //Para cada registro -es decir, cada dispositivo- se crea un arreglo de tipo diccionario que almacena las relaciones encontradas entre ese dispositivo y "sensorsDataHistoric"
	   }
	},
	{
	   "$unwind": "$array" //Descompone los registros en función de las posiciones de su "array" [1]
	},
	{
	   "$match": {"array.idSensor": 3} //Se filtra para los sensores de tipo 3, que son los que realizan las emisiones
	},
	{
	   "$addFields": //Por cuestiones de rendimiento, se crean los parámetros imprescindibles para que la consulta sea funcional [2]
	   {
	      "coInterval": //Intervalo horario de formato [x:00, x+1:00] en que ha ocurrido la emisión [3]
	      {
	         "$concat":
	         [
	            "[",
	            {"$substrBytes": ["$array.data.inicio", 11, 2]},
	            ":00, ",
	            {"$dateToString": {"format": "%H", "date": {"$add": [{"$toDate": "$array.data.inicio"}, 3600000]}, "timezone": "$array.dateTime.offset"}},
	            ":00]"
	         ]
	      },
	      "naDay": {"$substrBytes": ["$array.data.inicio", 0, 10]}, //Día de emisión
	      "qtBroadcastDurationS": {"$divide": [{"$subtract": [{"$toDate": "$array.data.fin"}, {"$toDate": "$array.data.inicio"}]}, 1000]}, //Duración de la emisión en segundos
	      "idLocation": {"$toString": "$idLocation"},
	      "naBroadcast": "$array.data.archivo", //Nombre del archivo emitido
	      "coNaLocation": {"$ifNull": ["$locationAddress", null]}, //Toda la información relativa a la localización de la tienda en la que se ha realizado la emisión [2]
	      "idDevice": {"$toString": "$array.idDevice"},
	      "naType": "$array.data.tipo" //Tipo de emisión (vídeo, canción, cuña de corta duración, etc.)
	   }
	},
	{
	   "$group": //Agrupación de la consulta en función de los campos creados en la anterior etapa [4]
	   {
	      "_id":
	      {
	         "idLocation": "$idLocation",
	         "idDevice": "$idDevice",
	         "coNaLocation": "$coNaLocation",
	         "naDay": "$naDay",
	         "coInterval": "$coInterval",
	         "naBroadcast": "$naBroadcast",
	         "naType": "$naType"
	      },
	      "qtBroadcast": {"$sum": 1}, //Se calcula el número de emisiones
	      "qtBroadcastDurationS": {"$avg": "$qtBroadcastDurationS"} //Y su tiempo medio de duración [5]
	   }
	},
	{
	   "$project": //Se determina qué campos ha de contener cada registro final
	   {
	      "_id": 0,
	      "idLocation": "$_id.idLocation",
	      "idDevice": "$_id.idDevice",
	      "coNaLocation": "$_id.coNaLocation",
	      "naDay": "$_id.naDay",
	      "coInterval": "$_id.coInterval",
	      "naBroadcast": "$_id.naBroadcast",
	      "naType": "$_id.naType",
	      "qtBroadcast": "$qtBroadcast",
	      "qtBroadcastDurationS": "$qtBroadcastDurationS"
	   }
	}
]

db.devices.aggregate(pipeline)

/*

Notas:

[1] Si el cliente tiene instalados 5 dispositivos y para cada uno de ellos se encuentran 100 registros en la colección de "sensorsDataHistoric", en esta etapa 
	se pasaría a tener 500 registros diferentes (en realidad en este punto y para este cliente lo normal es tener millones de registros); de ahí que haya que 
	agrupar la información
[2] A partir de ellos se pueden crear nuevos parámetros haciendo uso del lenguaje de nuestro BI
[3] La cantidad de registros para este cliente es tan grande que se agrupan por intervalos horarios
[4] Para cada tienda y dispositivo, por día, se determina cuántas veces se ha realizado una misma emisión -un mismo archivo de audio o vídeo- por cada intervalo 
	horario
[5] En ocasiones hay variaciones de un segundo para el mismo tipo de registro

*/