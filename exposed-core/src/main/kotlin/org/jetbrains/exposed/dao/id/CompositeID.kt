package org.jetbrains.exposed.dao.id

import org.jetbrains.exposed.sql.Column

/** Class representing a mapping of each composite primary key column to its stored identity value. */
class CompositeID private constructor() : Comparable<CompositeID> {
    internal val values: MutableMap<Column<*>, Comparable<*>?> = LinkedHashMap()

    operator fun <T : Comparable<T>, S : T?> set(column: Column<T>, value: S) {
        require(values.isEmpty() || values.keys.first().table == column.table) {
            "CompositeID key columns must all come from the same IdTable"
        }
        require(column in (column.table as CompositeIdTable).idColumns) {
            "Column ${column.name} must be a component column in ${column.table.tableName}"
        }
        values[column] = value
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <T : Comparable<T>> get(column: Column<T>): T {
        return values[column] as T
    }

    operator fun contains(column: Column<*>): Boolean = values.contains(column)

    override fun toString(): String = "CompositeID(${values.entries.joinToString { "${it.key.name}=${it.value}" }})"

    override fun hashCode(): Int = values.entries.fold(0) { acc, entry ->
        (acc * 31) + entry.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        if (other is EntityID<*>) return this == other._value
        if (other !is CompositeID) return false

        return values == other.values
    }

    override fun compareTo(other: CompositeID): Int {
        val compareSize = compareValues(other.values.size, values.size)
        if (compareSize != 0) return compareSize

        values.entries.forEach { (column, idValue) ->
            if (!other.values.containsKey(column)) return -1
            compareValues(idValue, other.values[column]).let {
                if (it != 0) return it
            }
        }
        return 0
    }

    companion object {
        operator fun invoke(body: (CompositeID) -> Unit): CompositeID {
            return CompositeID().apply(body)
        }
    }
}
