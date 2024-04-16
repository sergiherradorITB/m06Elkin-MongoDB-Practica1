package org.example

import com.mongodb.MongoException
import com.mongodb.MongoTimeoutException
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.bson.Document
import java.io.File

@Serializable
data class Student(
    val student_id: Int,
    val name: String,
    val surname: String,
    val class_id: String,
    val group: String,
    val scores: List<Score>,
    val interests: List<String>
) : java.io.Serializable

@Serializable
data class Score(val type: String, val score: Int) : java.io.Serializable


fun main() {
    var mongoClient: MongoClient? = null

    try {
        var connectionString =
            "mongodb+srv://elkin:pepoClown123@sergioherrador.bwwhoy4.mongodb.net/?retryWrites=true&w=majority&appName=SergioHerrador"
        mongoClient = MongoClients.create(connectionString)
        val db = mongoClient.getDatabase("SergioHerradorDiazLopez")
        val coll: MongoCollection<Document> = db.getCollection("grades")

        exercici1(coll)
        exercici2(coll)

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

fun exercici1(coll: MongoCollection<Document>) {
    // Lectura del archivo JSON
    val jsonFile = File("src/main/kotlin/Alumne1.json")
    println(jsonFile.absolutePath)
    val jsonText = jsonFile.readText()
    val students = Json.decodeFromString<List<Student>>(jsonText)

    for (i in students) {
        println(i)
    }

    // Creación de la lista de documentos MongoDB
    val documents = students.map { student ->
        Document("student_id", student.student_id)
            .append("name", student.name)
            .append("surname", student.surname)
            .append("class_id", student.class_id)
            .append("group", student.group)
            .append("scores", student.scores.map { score ->
                Document("type", score.type)
                    .append("score", score.score)
            })
            // Añadir los intereses como una lista de documentos
            .append("interests", student.interests)
    }

    // Inserción de los documentos a la colección
    coll.insertMany(documents)
}

fun exercici2(coll: MongoCollection<Document>) {
    // Mostrar las datos de los estudiantes del mismo grupo
    val studentsInGroupCursor = coll.find(eq("group", "1A"))
    println("Estudiantes del grupo 1A:")
    studentsInGroupCursor.forEach { println(it.toJson()) }

    // Mostrar las datos de los estudiantes que tienen un 100 en el examen
    val perfectScoreStudentsCursor = coll.find(and(eq("scores.type", "exam"), eq("scores.score", 100)))
    println("\nEstudiantes con 100 en el examen:")
    perfectScoreStudentsCursor.forEach { println(it.toJson()) }

    // Mostrar las datos de los estudiantes que tienen menos de 50 en el examen
    val failExamStudentsCursor = coll.find(and(eq("scores.type", "exam"), lt("scores.score", 50)))
    println("\nEstudiantes con menos de 50 en el examen:")
    failExamStudentsCursor.forEach { println(it.toJson()) }

    // Mostrar los intereses del estudiante con student_id=111222333
    val studentInterestsCursor = coll.find(eq("student_id", 111222333)).projection(Document("interests", 1))
    println("\nIntereses del estudiante con student_id=111222333:")
    studentInterestsCursor.forEach { println(it.toJson()) }
}