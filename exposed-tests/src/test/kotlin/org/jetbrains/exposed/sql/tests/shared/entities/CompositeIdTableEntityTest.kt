package org.jetbrains.exposed.sql.tests.shared.entities

import org.jetbrains.exposed.dao.Entity
import org.jetbrains.exposed.dao.EntityClass
import org.jetbrains.exposed.dao.id.CompositeIdTable
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.currentTestDB
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.junit.Test
import java.util.*
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class CompositeIdTableEntityTest : DatabaseTestsBase() {
    // composite id has 2 columns - types Int, UUID
    object Publishers : CompositeIdTable<PubId>("publishers") {
        val number = integer("id_number").autoIncrement().compositeEntityId()
        val isbn = uuid("isbn").autoGenerate().compositeEntityId()
        val name = varchar("publisher_name", 32)

        override val primaryKey = PrimaryKey(number, isbn)
        override fun readToKey(readValues: Map<Column<*>, Any>) = PubId(readValues[number] as Int, readValues[isbn] as UUID)
        override fun writeFromKey(key: PubId): Map<Column<*>, Any> = mapOf(number to key.number, isbn to key.isbn)
    }
    class Publisher(id: EntityID<PubId>) : Entity<PubId>(id) {
        companion object : EntityClass<PubId, Publisher>(Publishers)

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
    class Author(id: EntityID<Int>) : Entity<Int>(id) {
        companion object : EntityClass<Int, Author>(Authors)

        var publisher by Publisher referencedOn Authors
        var penName by Authors.penName
    }

    // composite id has 1 column - type Int
    object Books : CompositeIdTable<Int>("books") {
        val bookId = integer("book_id").autoIncrement().compositeEntityId()
        val title = varchar("title", 32)
        val author = reference("author_id", Authors)

        override val primaryKey = PrimaryKey(bookId)
    }
    class Book(id: EntityID<Int>) : Entity<Int>(id) {
        companion object : EntityClass<Int, Book>(Books)

        var title by Books.title
        var author by Author referencedOn Books.author
    }

    // TYPE & USE CASE TESTS

    @Test
    fun entityIdUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers, Authors, Books) {
            // entities
            val publisherA: Publisher = Publisher.new {
                name = "Publisher A"
            }
            val authorA: Author = Author.new {
                publisher = publisherA
                penName = "Author A"
            }
            val bookA: Book = Book.new {
                title = "Book A"
                author = authorA
            }

            // entity id properties
            val publisherId: EntityID<PubId> = publisherA.id
            val authorId: EntityID<Int> = authorA.id
            val bookId: EntityID<Int> = bookA.id

            // access wrapped entity id values - No Type Erasure
            val publisherIdValue: PubId = publisherId.value
            val authorIdValue: Int = authorId.value
            val bookIdValue = bookId.value

            // access individual composite entity id values - No Type Erasure
            val publisherIdComponent1: Int = publisherIdValue.number
            val publisherIdComponent2: UUID = publisherIdValue.isbn
            val bookIdComponent1: Int = bookIdValue

            // find entity by its id property - argument type EntityID<T> must match invoking type EntityClass<T, _>
            val foundPublisherA: Publisher? = Publisher.findById(publisherId)
            val foundAuthorA: Author? = Author.findById(authorId)
            val foundBookA: Book? = Book.findById(bookId)
        }
    }

    @Test
    fun tableIdColumnUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers, Authors, Books) {
            // id columns
            val publisherIdColumn: Column<EntityID<PubId>> = Publishers.id
            val authorIdColumn: Column<EntityID<Int>> = Authors.id
            val bookIdColumn: Column<EntityID<Int>> = Books.id

            // entity id values
            val publisherA: EntityID<PubId> = Publishers.insertAndGetId {
                it[name] = "Publisher A"
            }
            val authorA: EntityID<Int> = Authors.insertAndGetId {
                // cast no longer necessary due to no type erasure
                it[publisherId] = publisherA.value.number
                it[publisherIsbn] = publisherA.value.isbn
                it[penName] = "Author A"
            }
            val bookA: EntityID<Int> = Books.insertAndGetId {
                it[title] = "Book A"
                it[author] = authorA
            }

            // access entity id with single result row access
            val publisherResult: EntityID<PubId> = Publishers.selectAll().single()[Publishers.id]
            val authorResult: EntityID<Int> = Authors.selectAll().single()[Authors.id]
            val bookResult: EntityID<Int> = Books.selectAll().single()[Books.id]

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
                it[number] = 725
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
                it[title] = "Book A"
                it[author] = 1
            }

            // manual using DAO
            val publisherIdValue = PubId(611, UUID.randomUUID())
            val publisherA: Publisher = Publisher.new(publisherIdValue) {
                name = "Publisher B"
            }
            val authorA: Author = Author.new(2) {
                publisher = publisherA
                penName = "Author B"
            }
            Book.new(2) {
                title = "Book A"
                author = authorA
            }

            // equality check - EntityID<T> == T
            Publishers.selectAll().where { Publishers.id eq publisherIdValue }.single()
            Authors.selectAll().where { Authors.id eq 2 }.single()
            Books.selectAll().where { Books.id eq 2 }.single()

            // find entity by its id value - argument type T must match invoking type EntityClass<T, _>
            val foundPublisherA: Publisher? = Publisher.findById(publisherIdValue)
            val foundAuthorA: Author? = Author.findById(2)
            val foundBookA: Book? = Book.findById(2)
        }
    }

    // DATABASE OPERATIONS TESTS

    @Test
    fun testCreateAndDropCompositeIdTable() {
        withDb(excludeSettings = listOf(TestDB.SQLITE)) {
            try {
                addLogger(StdOutSqlLogger)
                SchemaUtils.create(Publishers, Authors, Books)

                assertTrue(Publishers.exists())
                assertTrue(Authors.exists())
                assertTrue(Books.exists())
            } finally {
                SchemaUtils.drop(Authors, Publishers, Books)
            }
        }
    }

    @Test
    fun testInsertAndSelectUsingDAO() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            val p1 = Publisher.new {
                name = "Publisher A"
            }

            val result1 = Publisher.all().single()
            assertEquals("Publisher A", result1.name)
            // can compare entire entities
            assertEquals(p1, result1)
            // or entire entity ids
            assertEquals(p1.id, result1.id)
            // or the value wrapped by entity id
            assertEquals(p1.id.value, result1.id.value)
            // or the composite id components
            assertEquals(p1.id.value.number, result1.id.value.number)
            assertEquals(p1.id.value.isbn, result1.id.value.isbn)

            Publisher.new { name = "Publisher B" }
            Publisher.new { name = "Publisher C" }

            val result2 = Publisher.all().toList()
            assertEquals(3, result2.size)
        }
    }

    @Test
    fun testInsertAndSelectUsingDSL() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            Publishers.insert {
                it[name] = "Publisher A"
            }

            val result = Publishers.selectAll().single()
            assertEquals("Publisher A", result[Publishers.name])

            // test all id column components are accessible from single ResultRow access
            val idResult = result[Publishers.id]
            assertIs<EntityID<PubId>>(idResult)
            assertEquals(result[Publishers.number], idResult.value.number)
            assertEquals(result[Publishers.isbn], idResult.value.isbn)

            // test that using composite id column in DSL query builder works
            val dslQuery = Publishers.select(Publishers.id).where { Publishers.id eq idResult }.prepareSQL(this)
            val selectClause = dslQuery.substringAfter("SELECT ").substringBefore("FROM")
            // id column should deconstruct to 2 columns from PK
            assertEquals(2, selectClause.split(", ", ignoreCase = true).size)
            val whereClause = dslQuery.substringAfter("WHERE ")
            // 2 column in composite PK to check, joined by single AND operator
            assertEquals(2, whereClause.split("AND", ignoreCase = true).size)
        }
    }

    @Test
    fun testInsertAndGetCompositeIds() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers) {
            // no need to wrap inserted DSL value in EntityID
            val id1 = Publishers.insertAndGetId {
                it[number] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }
            assertEquals(725, id1.value.number)

            val id2 = Publishers.insertAndGetId {
                it[name] = "Publisher B"
            }
            val expectedNextVal = if (currentTestDB in TestDB.mySqlRelatedDB) 726 else 1
            assertEquals(expectedNextVal, id2.value.number)
        }
    }

    @Test
    fun testInsertUsingManualCompositeIds() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers) {
            // manual using DSL
            Publishers.insert {
                it[number] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }

            assertEquals(725, Publishers.selectAll().single()[Publishers.number])

            // manual using DAO
            val fullId = PubId(611, UUID.randomUUID())
            val p2 = Publisher.new(fullId) {
                name = "Publisher B"
            }
            assertEquals(611, p2.id.value.number)
            assertEquals(611, Publisher.findById(p2.id)?.id?.value?.number)
        }
    }

    @Test
    fun testFindByCompositeId() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers) {
            val id1: EntityID<PubId> = Publishers.insertAndGetId {
                it[number] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }

            val p1 = Publisher.findById(id1)
            assertNotNull(p1)
            assertEquals(725, p1.id.value.number)

            val id2: EntityID<PubId> = Publisher.new {
                name = "Publisher B"
            }.id

            val p2 = Publisher.findById(id2)
            assertNotNull(p2)
            assertEquals("Publisher B", p2.name)
            assertEquals(id2.value.number, p2.id.value.number)

            // test findById() using CompositeID value
            val compositeId1: PubId = id1.value
            val p3 = Publisher.findById(compositeId1)
            assertNotNull(p3)
            assertEquals(p1, p3)
        }
    }

    @Test
    fun testFindWithDSLBuilder() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            val p1 = Publisher.new {
                name = "Publisher A"
            }

            assertEquals(p1.id, Publisher.find { Publishers.name like "% A" }.single().id)

            val p2 = Publisher.find { Publishers.id eq p1.id }.single()
            assertEquals(p1, p2)
        }
    }

    @Test
    fun testUpdateCompositeEntity() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            val p1 = Publisher.new {
                name = "Publisher A"
            }

            p1.name = "Publisher B"

            assertEquals("Publisher B", Publisher.all().single().name)
        }
    }

    @Test
    fun testDeleteCompositeEntity() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            val p1 = Publisher.new {
                name = "Publisher A"
            }
            val p2 = Publisher.new {
                name = "Publisher B"
            }

            assertEquals(2, Publisher.all().count())

            p1.delete()

            val result = Publisher.all().single()
            assertEquals("Publisher B", result.name)
            assertEquals(p2.id, result.id)
        }
    }

    @Test
    fun testInsertAndSelectReferencedEntities() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers, Authors, Books) {
            val publisherA = Publisher.new {
                name = "Publisher A"
            }
            val authorA = Author.new {
                publisher = publisherA
                penName = "Author A"
            }
            val authorB = Author.new {
                publisher = publisherA
                penName = "Author B"
            }
            val bookA = Book.new {
                title = "Book A"
                author = authorB
            }

            assertEquals(publisherA.id.value.number, authorA.publisher.id.value.number)
            assertEquals(publisherA, authorA.publisher)
            assertEquals(publisherA, authorB.publisher)
            assertEquals(publisherA, bookA.author.publisher)
        }
    }
}

data class PubId(val number: Int, val isbn: UUID) : Comparable<PubId> {
    override fun compareTo(other: PubId): Int = compareValuesBy(this, other) { it.number }
}
