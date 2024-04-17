import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.bson.Document

@Serializable
data class Student(
    val student_id: Int,
    val name: String,
    val surname: String,
    val class_id: String,
    val group: String,
    val scores: List<Score>,
    val interests: List<String>
)

@Serializable
data class Score(val type: String, val score: Int)


fun main() {
    var mongoClient: MongoClient? = null

    try {
        // Connectar-se a servidor de mongoDB
        val connectionString = "mongodb+srv://elkin:pepoClown123@sergioherrador.bwwhoy4.mongodb.net/?retryWrites=true&w=majority&appName=SergioHerrador"
        // Crear connexió
        mongoClient = MongoClients.create(connectionString)
        // Obtenir database SergioHerradorDiazLopez de la connexió a servidor
        val db = mongoClient.getDatabase("SergioHerradorDiazLopez")
        // Obtenir col·lecció  grades de la database
        val coll: MongoCollection<Document> = db.getCollection("grades")

        exercici1(coll)
        exercici2(coll)

        // Control de errors.
    } catch (e: MongoTimeoutException) {
        println("No arribem a la base de dades")
    } catch (e: MongoException) {
        println(e.message)
    } catch (e: Exception) {
        println(e.message)
    } finally {
        // Cerrar la conexión de MongoDB al finalizar
        mongoClient?.close()
    }

}
// Exercici 1. Insertar dos estudiants més a la col·lecció "grades"
// Afegir dos nous estudiants a la colecció grades.
fun exercici1(coll: MongoCollection<Document>) {
    // Creamos la lista de estudiantes
    val students = listOf(
        Student(
            student_id = 111333444,
            name = "Sergi",
            surname = "Herrador Díaz",
            class_id = "DAM",
            group = "1A",
            scores = listOf(Score(type = "exam", score = 100), Score(type = "teamWork", score = 50)),
            interests = listOf("music", "gym", "code", "electronics")
        ),
        Student(
            student_id = 111222333,
            name = "Elkin",
            surname = "David",
            class_id = "ASIX",
            group = "2A",
            scores = listOf(Score(type = "exam", score = 33), Score(type = "teamWork", score = 33)),
            interests = listOf("music", "gym", "code", "electronics")
        )
    )

    // Convertimos cada objeto `Student` a JSON y los agregamos a la lista `jsonDocuments`
    val jsonDocuments : MutableList<String> = mutableListOf()
    val jsonPersonaUno = Json.encodeToString(students[0]) ; jsonDocuments.add(jsonPersonaUno)
    val jsonPersonaDos = Json.encodeToString(students[1]) ; jsonDocuments.add(jsonPersonaDos)

    // Inserción de los documentos a la colección
    coll.insertMany(jsonDocuments.map { Document.parse(it) })
    // El método `map` transforma cada cadena JSON en un documento `Document` de MongoDB, luego se inserta en la colección.
}


// Exercici 2. Realitza les següents consultes:
// Mostrar dades dels almne sde la colecció grades amb diferents filtres
fun exercici2(coll: MongoCollection<Document>) {


    //Ex 2.1
    // Mostrar las datos de los estudiantes del mismo grupo
    val studentsInGroupCursor = coll.find(eq("group", "1A"))
    println("Estudiantes del grupo 1A:")
    studentsInGroupCursor.forEach { println(it.toJson()) }



    //Ex. 2.2
    // Mostrar las datos de los estudiantes que tienen un 100 en el examen
    val perfectScoreStudentsCursor = coll.find(and(eq("scores.type", "exam"), eq("scores.score", 100)))
    println("\nEstudiantes con 100 en el examen:")
    perfectScoreStudentsCursor.forEach { println(it.toJson()) }



    //Ex. 2.3
    // Mostrar las datos de los estudiantes que tienen menos de 50 en el examen
    val failExamStudentsCursor = coll.find(and(eq("scores.type", "exam"), lt("scores.score", 50)))
    println("\nEstudiantes con menos de 50 en el examen:")
    failExamStudentsCursor.forEach { println(it.toJson()) }



    //Ex. 2.4
    // Mostrar los intereses del estudiante con student_id=111222333
    val studentInterestsCursor = coll.find(eq("student_id", 111222333)).projection(Document("interests", 1))
    println("\nIntereses del estudiante con student_id=111222333:")
    studentInterestsCursor.forEach { println(it.toJson()) }
}