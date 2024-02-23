package org.jetbrains.exposed.sql.tests.shared.entities

import org.jetbrains.exposed.dao.CompositeEntity
import org.jetbrains.exposed.dao.CompositeEntityClass
import org.jetbrains.exposed.dao.IntEntity
import org.jetbrains.exposed.dao.IntEntityClass
import org.jetbrains.exposed.dao.id.CompositeID
import org.jetbrains.exposed.dao.id.CompositeIdTable
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IdTable
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.insertAndGetId
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.junit.Test
import java.util.*

@Suppress("UnusedPrivateProperty")
class CompositeIdEntityTest : DatabaseTestsBase() {
    // composite id has 2 columns - types Int, UUID
    object Publishers : CompositeIdTable("publishers") {
        val pubId = integer("pub_id").autoIncrement().compositeEntityId()
        val isbn = uuid("isbn_code").autoGenerate().compositeEntityId()
        val name = varchar("publisher_name", 32)

        override val primaryKey = PrimaryKey(pubId, isbn)
    }
    class Publisher(id: EntityID<CompositeID>) : CompositeEntity(id) {
        companion object : CompositeEntityClass<Publisher>(Publishers)

        var name by Publishers.name
    }

    // single id has 1 column - type Int
    object Authors : IdTable<Int>("authors") {
        override val id = integer("id").autoIncrement().entityId()
        val publisherId = integer("publisher_id")
        val publisherIsbn = uuid("publisher_isbn")
        val penName = varchar("pen_name", 32)

        override val primaryKey = PrimaryKey(id)
        init {
            foreignKey(publisherId, publisherIsbn, target = Publishers.primaryKey)
        }
    }
    class Author(id: EntityID<Int>) : IntEntity(id) {
        companion object : IntEntityClass<Author>(Authors)

        var publisher by Publisher referencedOn Authors
        var penName by Authors.penName
    }

    // composite id has 1 column - type Int
    object Books : CompositeIdTable("books") {
        val bookId = integer("book_id").autoIncrement().compositeEntityId()
        val title = varchar("title", 32)
        val author = reference("author", Authors)

        override val primaryKey = PrimaryKey(bookId)
    }
    class Book(id: EntityID<CompositeID>) : CompositeEntity(id) {
        companion object : CompositeEntityClass<Book>(Books)

        var title by Books.title
        var author by Author referencedOn Books.author
    }

    @Test
    fun entityIdUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers, Authors, Books) {
            // entities
            val publisherA = Publisher.new {
                name = "Publisher A"
            }
            val authorA = Author.new {
                publisher = publisherA
                penName = "Author A"
            }
            val bookA = Book.new {
                title = "Book A"
                author = authorA
            }

            // entity id properties
            val publisherId = publisherA.id
            val authorId = authorA.id
            val bookId = bookA.id

            // access wrapped entity id values
            val publisherIdValue = publisherId.value
            val authorIdValue = authorId.value // no type erasure
            val bookIdValue = bookId.value

            // access individual composite entity id values - type erasure
            val publisherIdComponent1 = publisherIdValue[Publishers.pubId]
            val publisherIdComponent2 = publisherIdValue[Publishers.isbn]
            val bookIdComponent1 = bookIdValue[Books.bookId]

            // find entity by its id property - argument type EntityID<T> must match invoking type EntityClass<T, _>
            val foundPublisherA = Publisher.findById(publisherId)
            val foundAuthorA = Author.findById(authorId)
            val foundBookA = Book.findById(bookId)
        }
    }

    @Test
    fun tableIdColumnUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers, Authors, Books) {
            // id columns
            val publisherIdColumn = Publishers.id
            val authorIdColumn = Authors.id
            val bookIdColumn = Books.id

            // entity id values
            val publisherA = Publishers.insertAndGetId {
                it[name] = "Publisher A"
            }
            val authorA = Authors.insertAndGetId {
                // cast necessary due to type erasure
                it[publisherId] = publisherA.value[Publishers.pubId] as Int
                it[publisherIsbn] = publisherA.value[Publishers.isbn] as UUID
                it[penName] = "Author A"
            }
            val bookA = Books.insertAndGetId {
                it[title] = "Book A"
                it[author] = authorA.value // cast not necessary
            }

            // access entity id with single result row access
            val publisherResult = Publishers.selectAll().single()[Publishers.id]
            val authorResult = Authors.selectAll().single()[Authors.id]
            val bookResult = Books.selectAll().single()[Books.id]

            // add all id components to query builder with single column op - EntityID<T> == EntityID<T>
            Publishers.selectAll().where { Publishers.id eq publisherResult }.single() // deconstructs to use compound AND
            Authors.selectAll().where { Authors.id eq authorResult }.single()
            Books.selectAll().where { Books.id eq bookResult }.single()
        }
    }

    @Test
    fun manualEntityIdUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers, Authors, Books) {
            // manual using DSL
            val code = UUID.randomUUID()
            Publishers.insert {
                it[pubId] = 725
                it[isbn] = code
                it[name] = "Publisher A"
            }
            Authors.insert {
                it[id] = EntityID(1, Authors)
                it[publisherId] = 725
                it[publisherIsbn] = code
                it[penName] = "Author A"
            }
            Books.insert {
                it[bookId] = 1
                it[title] = "Book A"
                it[author] = 1
            }

            // manual using DAO
            val publisherIdValue = CompositeID(mapOf(Publishers.pubId to 611, Publishers.isbn to UUID.randomUUID()))
            val publisherA = Publisher.new(publisherIdValue) {
                name = "Publisher B"
            }
            val authorA = Author.new(2) {
                publisher = publisherA
                penName = "Author B"
            }
            val bookIdValue = CompositeID(mapOf(Books.bookId to 2))
            Book.new(bookIdValue) {
                title = "Book B"
                author = authorA
            }

            // equality check - EntityID<T> == T
            Publishers.selectAll().where { Publishers.id eq publisherIdValue }.single()
            Authors.selectAll().where { Authors.id eq 2 }.single()
            Books.selectAll().where { Books.id eq bookIdValue }.single()

            // find entity by its id value - argument type T must match invoking type EntityClass<T, _>
            val foundPublisherA = Publisher.findById(publisherIdValue)
            val foundAuthorA = Author.findById(2)
            val foundBookA = Book.findById(bookIdValue)
        }
    }
}
