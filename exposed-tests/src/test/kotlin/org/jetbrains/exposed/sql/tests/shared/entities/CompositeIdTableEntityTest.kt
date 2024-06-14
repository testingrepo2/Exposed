package org.jetbrains.exposed.sql.tests.shared.entities

import org.jetbrains.exposed.dao.Entity
import org.jetbrains.exposed.dao.EntityClass
import org.jetbrains.exposed.dao.id.CompositeID
import org.jetbrains.exposed.dao.id.CompositeIdTable
import org.jetbrains.exposed.dao.id.EntityID
import org.jetbrains.exposed.dao.id.IdTable
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.tests.DatabaseTestsBase
import org.jetbrains.exposed.sql.tests.TestDB
import org.jetbrains.exposed.sql.tests.currentTestDB
import org.jetbrains.exposed.sql.tests.shared.assertEquals
import org.jetbrains.exposed.sql.tests.shared.assertTrue
import org.junit.Test
import java.util.*
import kotlin.test.assertIs
import kotlin.test.assertNotNull

@Suppress("UnusedPrivateProperty")
class CompositeIdTableEntityTest : DatabaseTestsBase() {
    // composite id has 2 columns - types Int, UUID
    object Publishers : CompositeIdTable("publishers") {
        val pubId = integer("pub_id").autoIncrement().compositeEntityId()
        val isbn = uuid("isbn_code").autoGenerate().compositeEntityId()
        val name = varchar("publisher_name", 32)

        override val primaryKey = PrimaryKey(pubId, isbn)
    }

    class Publisher(id: EntityID<CompositeID>) : Entity<CompositeID>(id) {
        companion object : EntityClass<CompositeID, Publisher>(Publishers)

        var name by Publishers.name
    }

    // single id has 1 column - type Int
    object Authors : IdTable<Int>("authors") {
        override val id = integer("id").autoIncrement().entityId()
        val publisherId = integer("pub_id")
        val publisherIsbn = uuid("isbn_code")
        val penName = varchar("pen_name", 32)

        override val primaryKey = PrimaryKey(id)

        init {
            foreignKey(publisherId, publisherIsbn, target = Publishers.primaryKey)
        }
    }

    class Author(id: EntityID<Int>) : Entity<Int>(id) {
        companion object : EntityClass<Int, Author>(Authors)

        var penName by Authors.penName
        var publisher by Publisher referencedOn Authors
    }

    private val allTables = arrayOf(Publishers, Authors)

    // EXCLUDED DB:
    // SQLite temporarily excluded because auto-increment can be applied only to a single column primary key
    // SQL Server temporarily excluded when inserting explicit value for identity column (not allowed without mode)

    // ID TYPE & USE CASES

    @Test
    fun entityIdUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), tables = allTables) {
            // entities
            val publisherA: Publisher = Publisher.new {
                name = "Publisher A"
            }
            val authorA: Author = Author.new {
                publisher = publisherA
                penName = "Author A"
            }

            // entity id properties
            val publisherId: EntityID<CompositeID> = publisherA.id
            val authorId: EntityID<Int> = authorA.id

            // access wrapped entity id values
            val publisherIdValue: CompositeID = publisherId.value
            val authorIdValue: Int = authorId.value

            // access individual composite entity id values - no type erasure
            val publisherIdComponent1: Int = publisherIdValue[Publishers.pubId]
            val publisherIdComponent2: UUID = publisherIdValue[Publishers.isbn]

            // find entity by its id property - argument type EntityID<T> must match invoking type EntityClass<T, _>
            val foundPublisherA: Publisher? = Publisher.findById(publisherId)
            val foundAuthorA: Author? = Author.findById(authorId)
        }
    }

    @Test
    fun tableIdColumnUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), tables = allTables) {
            // id columns
            val publisherIdColumn: Column<EntityID<CompositeID>> = Publishers.id
            val authorIdColumn: Column<EntityID<Int>> = Authors.id

            // entity id values
            val publisherA: EntityID<CompositeID> = Publishers.insertAndGetId {
                it[name] = "Publisher A"
            }
            val authorA: EntityID<Int> = Authors.insertAndGetId {
                // no cast needed as no type erasure
                it[publisherId] = publisherA.value[Publishers.pubId]
                it[publisherIsbn] = publisherA.value[Publishers.isbn]
                it[penName] = "Author A"
            }

            // access entity id with single result row access
            val publisherResult: EntityID<CompositeID> = Publishers.selectAll().single()[Publishers.id]
            val authorResult: EntityID<Int> = Authors.selectAll().single()[Authors.id]

            // add all id components to query builder with single column op - EntityID<T> == EntityID<T>
            Publishers.selectAll().where { Publishers.id eq publisherResult }.single() // deconstructs to use compound AND
            Authors.selectAll().where { Authors.id eq authorResult }.single()
        }
    }

    @Test
    fun manualEntityIdUseCases() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), tables = allTables) {
            // manual using DSL
            val code = UUID.randomUUID()
            Publishers.insert {
                it[pubId] = 725
                it[isbn] = code
                it[name] = "Publisher A"
            }
            Authors.insert {
                it[publisherId] = 725
                it[publisherIsbn] = code
                it[penName] = "Author A"
            }

            // manual using DAO
            val publisherIdValue = CompositeID {
                it[Publishers.pubId] = 611
                it[Publishers.isbn] = UUID.randomUUID()
            }
            val publisherA: Publisher = Publisher.new(publisherIdValue) {
                name = "Publisher B"
            }
            val authorA: Author = Author.new(12345) {
                publisher = publisherA
                penName = "Author B"
            }

            // equality check - EntityID<T> == T
            Publishers.selectAll().where { Publishers.id eq publisherIdValue }.single()
            Authors.selectAll().where { Authors.id eq authorA.id }.single()

            // find entity by its id value - argument type T must match invoking type EntityClass<T, _>
            val foundPublisherA: Publisher? = Publisher.findById(publisherIdValue)
            val foundAuthorA: Author? = Author.findById(authorA.id.value)
        }
    }

    // DATABASE OPERATIONS TESTS

    @Test
    fun testCreateAndDropCompositeIdTable() {
        withDb(excludeSettings = listOf(TestDB.SQLITE)) {
            try {
                SchemaUtils.create(tables = allTables)

                assertTrue(Publishers.exists())
                assertTrue(Authors.exists())

                assertTrue(SchemaUtils.statementsRequiredToActualizeScheme(tables = allTables).isEmpty())
            } finally {
                SchemaUtils.drop(tables = allTables)
            }
        }
    }

    @Test
    fun testInsertAndSelectUsingDAO() {
        withTables(excludeSettings = listOf(TestDB.SQLITE), Publishers) {
            val p1: Publisher = Publisher.new {
                name = "Publisher A"
            }

            val result1: Publisher = Publisher.all().single()

            assertEquals("Publisher A", result1.name)
            // can compare entire entity objects
            assertEquals(p1, result1)
            // or entire entity ids
            assertEquals(p1.id, result1.id)
            // or the value wrapped by entity id
            assertEquals(p1.id.value, result1.id.value)
            // or the composite id components
            assertEquals(p1.id.value[Publishers.pubId], result1.id.value[Publishers.pubId])
            assertEquals(p1.id.value[Publishers.isbn], result1.id.value[Publishers.isbn])

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

            val result: ResultRow = Publishers.selectAll().single()
            assertEquals("Publisher A", result[Publishers.name])

            // test all id column components are accessible from single ResultRow access
            val idResult = result[Publishers.id]
            assertIs<EntityID<CompositeID>>(idResult)
            assertEquals(result[Publishers.pubId], idResult.value[Publishers.pubId])
            assertEquals(result[Publishers.isbn], idResult.value[Publishers.isbn])

            // test that using composite id column in DSL query builder works
            val dslQuery = Publishers
                .select(Publishers.id) // should deconstruct to 2 columns
                .where { Publishers.id eq idResult } // should deconstruct to 2 ops
                .prepareSQL(this)
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
            // insert individual components
            val id1: EntityID<CompositeID> = Publishers.insertAndGetId {
                it[pubId] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }
            assertEquals(725, id1.value[Publishers.pubId])

            // insert components as defaults
            val id2: EntityID<CompositeID> = Publishers.insertAndGetId {
                it[name] = "Publisher B"
            }
            val expectedNextVal = if (currentTestDB in TestDB.mySqlRelatedDB) 726 else 1
            assertEquals(expectedNextVal, id2.value[Publishers.pubId])

            // insert as composite ID
            // this is possible with single PK IDs - should it also be possible here?
            val id3: EntityID<CompositeID> = Publishers.insertAndGetId {
                it[id] = CompositeID { id ->
                    id[pubId] = 999
                    id[isbn] = UUID.randomUUID()
                }
                it[name] = "Publisher C"
            }
            assertEquals(999, id3.value[Publishers.pubId])

            // should this also be possible?
//            Publishers.insertAndGetId {
//                it[id] = EntityID(
//                    CompositeID { id ->
//                        id[pubId] = 12345
//                        id[isbn] = UUID.randomUUID()
//                    },
//                    Publishers
//                )
//                it[name] = "Publisher C"
//            }
        }
    }

    @Test
    fun testInsertUsingManualCompositeIds() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers) {
            // manual using DSL
            Publishers.insert {
                it[pubId] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }

            assertEquals(725, Publishers.selectAll().single()[Publishers.pubId])

            // manual using DAO
            val fullId = CompositeID {
                it[Publishers.pubId] = 611
                it[Publishers.isbn] = UUID.randomUUID()
            }
            val p2Id = Publisher.new(fullId) {
                name = "Publisher B"
            }.id
            assertEquals(611, p2Id.value[Publishers.pubId])
            assertEquals(611, Publisher.findById(p2Id)?.id?.value?.get(Publishers.pubId))
        }
    }

    @Test
    fun testFindByCompositeId() {
        withTables(excludeSettings = listOf(TestDB.SQLITE, TestDB.SQLSERVER), Publishers) {
            val id1: EntityID<CompositeID> = Publishers.insertAndGetId {
                it[pubId] = 725
                it[isbn] = UUID.randomUUID()
                it[name] = "Publisher A"
            }

            val p1 = Publisher.findById(id1)
            assertNotNull(p1)
            assertEquals(725, p1.id.value[Publishers.pubId])

            val id2: EntityID<CompositeID> = Publisher.new {
                name = "Publisher B"
            }.id

            val p2 = Publisher.findById(id2)
            assertNotNull(p2)
            assertEquals("Publisher B", p2.name)
            assertEquals(id2.value[Publishers.pubId], p2.id.value[Publishers.pubId])

            // test findById() using CompositeID value
            val compositeId1: CompositeID = id1.value
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
}
