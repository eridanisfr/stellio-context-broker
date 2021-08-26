package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.AttributeType
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATASET_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_POINT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_POLYGON_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromObject
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueAsDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueAsString
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.toUri
import jakarta.json.JsonArray
import jakarta.json.JsonNumber
import jakarta.json.JsonObject
import jakarta.json.JsonString
import jakarta.json.JsonValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.ZonedDateTime

private val logger: Logger = LoggerFactory.getLogger(NgsiLdEntity::class.java)

class NgsiLdEntity private constructor(
    val id: URI,
    val type: String,
    val relationships: List<NgsiLdRelationship>,
    val properties: List<NgsiLdProperty>,
    // TODO by 5.2.4, it is at most one location, one observationSpace and one operationSpace (to be enforced)
    //      but nothing prevents to add other user-defined geo properties
    val geoProperties: List<NgsiLdGeoProperty>,
    val contexts: List<String>
) {
    companion object {
        operator fun invoke(parsedKeys: JsonObject, contexts: List<String>): NgsiLdEntity {
            if (!parsedKeys.containsKey(JSONLD_ID))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain an id property")
            val id = parsedKeys.getJsonString(JSONLD_ID).string.toUri()

            if (!parsedKeys.containsKey(JSONLD_TYPE))
                throw BadRequestDataException("The provided NGSI-LD entity does not contain a type property")
            val type = parsedKeys.getJsonArray(JSONLD_TYPE).getJsonString(0).string
            if (!type.extractShortTypeFromExpanded().isNgsiLdSupportedName())
                throw BadRequestDataException("The provided NGSI-LD entity has a type with invalid characters")

            val attributes = getNonCoreAttributes(parsedKeys, NGSILD_ENTITY_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            val geoProperties = getAttributesOfType<NgsiLdGeoProperty>(attributes, NGSILD_GEOPROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size + geoProperties.size)
                throw BadRequestDataException("Entity has unknown attributes types: $attributes")

            return NgsiLdEntity(id, type, relationships, properties, geoProperties, contexts)
        }
    }

    val attributes: List<NgsiLdAttribute> = properties.plus(relationships).plus(geoProperties)

    /**
     * Gets linked entities ids.
     * Entities can be linked either by a relation or a property.
     */
    fun getLinkedEntitiesIds(): List<URI> =
        properties.flatMap {
            it.getLinkedEntitiesIds()
        }.plus(
            relationships.flatMap { it.getLinkedEntitiesIds() }
        ).plus(
            geoProperties.flatMap { it.getLinkedEntitiesIds() }
        )

    fun getLocation(): NgsiLdGeoProperty? =
        geoProperties.find { it.name == NGSILD_LOCATION_PROPERTY }
}

sealed class NgsiLdAttribute(val name: String) {
    val compactName: String = name.extractShortTypeFromExpanded()

    init {
        if (!compactName.isNgsiLdSupportedName())
            throw BadRequestDataException("Entity has an invalid attribute name: $compactName")
    }

    abstract fun getLinkedEntitiesIds(): List<URI>
    abstract fun getAttributeInstances(): List<NgsiLdAttributeInstance>
}

class NgsiLdProperty private constructor(
    name: String,
    val instances: List<NgsiLdPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: JsonArray): NgsiLdProperty {
            checkInstancesAreOfSameType(name, instances, NGSILD_PROPERTY_TYPE)

            val ngsiLdPropertyInstances = instances.map { instance ->
                NgsiLdPropertyInstance(name, instance.asJsonObject())
            }

            checkAttributeDefaultInstance(name, ngsiLdPropertyInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdPropertyInstances)

            return NgsiLdProperty(name, ngsiLdPropertyInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdAttributeInstance> = instances
}

class NgsiLdRelationship private constructor(
    name: String,
    val instances: List<NgsiLdRelationshipInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: JsonArray): NgsiLdRelationship {
            checkInstancesAreOfSameType(name, instances, NGSILD_RELATIONSHIP_TYPE)

            val ngsiLdRelationshipInstances = instances.map { instance ->
                NgsiLdRelationshipInstance(name, instance.asJsonObject())
            }

            checkAttributeDefaultInstance(name, ngsiLdRelationshipInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdRelationshipInstances)

            return NgsiLdRelationship(name, ngsiLdRelationshipInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdAttributeInstance> = instances
}

class NgsiLdGeoProperty private constructor(
    name: String,
    val instances: List<NgsiLdGeoPropertyInstance>
) : NgsiLdAttribute(name) {
    companion object {
        operator fun invoke(name: String, instances: JsonArray): NgsiLdGeoProperty {
            checkInstancesAreOfSameType(name, instances, NGSILD_GEOPROPERTY_TYPE)

            val ngsiLdGeoPropertyInstances = instances.map { instance ->
                NgsiLdGeoPropertyInstance(instance.asJsonObject())
            }

            checkAttributeDefaultInstance(name, ngsiLdGeoPropertyInstances)
            checkAttributeDuplicateDatasetId(name, ngsiLdGeoPropertyInstances)

            return NgsiLdGeoProperty(name, ngsiLdGeoPropertyInstances)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        instances.flatMap { it.getLinkedEntitiesIds() }

    override fun getAttributeInstances(): List<NgsiLdAttributeInstance> = instances
}

sealed class NgsiLdAttributeInstance(
    val observedAt: ZonedDateTime?,
    val datasetId: URI?,
    val properties: List<NgsiLdProperty>,
    val relationships: List<NgsiLdRelationship>
) {

    fun isTemporalAttribute(): Boolean = observedAt != null

    open fun getLinkedEntitiesIds(): List<URI> =
        properties.flatMap {
            it.getLinkedEntitiesIds()
        }
}

class NgsiLdPropertyInstance private constructor(
    val value: Any,
    val unitCode: String?,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    properties: List<NgsiLdProperty>,
    relationships: List<NgsiLdRelationship>
) : NgsiLdAttributeInstance(observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(name: String, values: JsonObject): NgsiLdPropertyInstance {
            // TODO for short-handed properties, the value is directly accessible from the map under the @value key ?
            val value = getPropertyValueFromObject(values, NGSILD_PROPERTY_VALUE)
                ?: throw BadRequestDataException("Property $name has an instance without a value")

            val unitCode = getPropertyValueAsString(values, NGSILD_UNIT_CODE_PROPERTY)
            val observedAt = getPropertyValueAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_PROPERTIES_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Property has unknown attributes types: $attributes")

            return NgsiLdPropertyInstance(value, unitCode, observedAt, datasetId, properties, relationships)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }
}

class NgsiLdRelationshipInstance private constructor(
    val objectId: URI,
    observedAt: ZonedDateTime?,
    datasetId: URI?,
    properties: List<NgsiLdProperty>,
    relationships: List<NgsiLdRelationship>
) : NgsiLdAttributeInstance(observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(name: String, values: JsonObject): NgsiLdRelationshipInstance {
            val objectId = extractRelationshipObject(name, values).fold({ throw it }, { it })
            val observedAt = getPropertyValueAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val attributes = getNonCoreAttributes(values, NGSILD_RELATIONSHIPS_CORE_MEMBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Relationship has unknown attributes: $attributes")

            return NgsiLdRelationshipInstance(objectId, observedAt, datasetId, properties, relationships)
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }.plus(objectId)
}

class NgsiLdGeoPropertyInstance(
    observedAt: ZonedDateTime? = null,
    datasetId: URI? = null,
    val geoPropertyType: GeoPropertyType,
    val coordinates: List<Any>,
    properties: List<NgsiLdProperty> = emptyList(),
    relationships: List<NgsiLdRelationship> = emptyList()
) : NgsiLdAttributeInstance(observedAt, datasetId, properties, relationships) {
    companion object {
        operator fun invoke(values: JsonObject): NgsiLdGeoPropertyInstance {
            val observedAt = getPropertyValueAsDateTime(values, NGSILD_OBSERVED_AT_PROPERTY)
            val datasetId = values.getDatasetId()

            val geoPropertyValue = values[NGSILD_GEOPROPERTY_VALUE]!!.asJsonArray()[0].asJsonObject()
            val expandedGeoPropertyType = (geoPropertyValue[JSONLD_TYPE]!!.asJsonArray()[0] as JsonString).string
            val geoPropertyType = GeoPropertyType.valueOf(expandedGeoPropertyType.extractShortTypeFromExpanded())
            val coordinates = extractCoordinates(expandedGeoPropertyType, geoPropertyValue)

            val attributes = getNonCoreAttributes(values, NGSILD_GEOPROPERTIES_CORE_MEMEBERS)
            val relationships = getAttributesOfType<NgsiLdRelationship>(attributes, NGSILD_RELATIONSHIP_TYPE)
            val properties = getAttributesOfType<NgsiLdProperty>(attributes, NGSILD_PROPERTY_TYPE)
            if (attributes.size > relationships.size + properties.size)
                throw BadRequestDataException("Geoproperty has unknown attributes: $attributes")

            return NgsiLdGeoPropertyInstance(
                observedAt,
                datasetId,
                geoPropertyType,
                coordinates,
                properties,
                relationships
            )
        }

        // TODO this lacks sanity checks
        private fun extractCoordinates(
            expandedGeoPropertyType: String,
            geoPropertyValue: JsonObject
        ): List<Any> {
            val coordinates =
                if (geoPropertyValue.containsKey(NGSILD_COORDINATES_PROPERTY))
                    geoPropertyValue[NGSILD_COORDINATES_PROPERTY]!!.asJsonArray()
                else
                    throw BadRequestDataException(
                        "Geoproperty of type $expandedGeoPropertyType does not contain coordinates"
                    )
            when (expandedGeoPropertyType) {
                NGSILD_POINT_PROPERTY -> {
                    val innerCoordinates = coordinates[0].asJsonObject()["@list"]!!.asJsonArray()
                    val longitude = (innerCoordinates[0].asJsonObject()["@value"]!! as JsonNumber).doubleValue()
                    val latitude = (innerCoordinates[1].asJsonObject()["@value"]!! as JsonNumber).doubleValue()
                    return listOf(longitude, latitude)
                }
                NGSILD_POLYGON_PROPERTY -> {
                    val innerCoordinates = coordinates[0].asJsonObject()["@list"]!!.asJsonArray()
                    return innerCoordinates.map {
                        val coordinatesPair = (it.asJsonObject()["@list"]!!.asJsonArray())
                        listOf(
                            (coordinatesPair[0].asJsonObject()["@value"] as JsonNumber).doubleValue(),
                            (coordinatesPair[1].asJsonObject()["@value"] as JsonNumber).doubleValue()
                        )
                    }
                }
                else -> {
                    logger.warn("Not yet supported geometry: $expandedGeoPropertyType")
                    throw NotImplementedException("Not yet supported geometry: $expandedGeoPropertyType")
                }
            }
        }

        fun toWktFormat(geoPropertyType: GeoPropertyType, coordinates: List<Any>): String {
            return if (geoPropertyType == GeoPropertyType.Point) {
                "${geoPropertyType.value.uppercase()} (${coordinates.joinToString(" ")})"
            } else {
                val formattedCoordinates = coordinates.map {
                    it as List<*>
                    it.joinToString(" ")
                }
                "${geoPropertyType.value.uppercase()} ((${formattedCoordinates.joinToString(", ")}))"
            }
        }
    }

    override fun getLinkedEntitiesIds(): List<URI> =
        relationships.flatMap { it.getLinkedEntitiesIds() }
}

/**
 * Given an attribute values, returns whether it is of the given attribute type
 * (i.e. property, geo property or relationship)
 */
fun isAttributeOfType(attributeValues: JsonObject, type: AttributeType): Boolean =
    // TODO move some of these checks to isValidAttribute()
    attributeValues.containsKey(JSONLD_TYPE) &&
        attributeValues[JSONLD_TYPE]?.valueType == JsonValue.ValueType.ARRAY &&
        attributeValues.getJsonArray(JSONLD_TYPE).getJsonString(0).string == type.uri

private inline fun <reified T : NgsiLdAttribute> getAttributesOfType(
    attributes: Map<String, JsonValue>,
    type: AttributeType
): List<T> =
    attributes.mapValues {
        it.value.asJsonArray()
    }.filter {
        // only check the first entry, multi-attribute consistency is later checked by each attribute
        isAttributeOfType(it.value.getJsonObject(0), type)
    }.map {
        when (type) {
            NGSILD_PROPERTY_TYPE -> NgsiLdProperty(it.key, it.value) as T
            NGSILD_RELATIONSHIP_TYPE -> NgsiLdRelationship(it.key, it.value) as T
            NGSILD_GEOPROPERTY_TYPE -> NgsiLdGeoProperty(it.key, it.value) as T
            else -> throw BadRequestDataException("Unrecognized type: $type")
        }
    }

private fun getNonCoreAttributes(parsedKeys: JsonObject, keysToFilter: List<String>): Map<String, JsonValue> =
    parsedKeys.filterKeys {
        !keysToFilter.contains(it)
    }

// TODO to be refactored with validation
fun checkInstancesAreOfSameType(name: String, values: JsonArray, type: AttributeType) {
    if (!values.all { isAttributeOfType(it.asJsonObject(), type) })
        throw BadRequestDataException("Attribute $name instances must have the same type")
}

// TODO to be refactored with validation
fun checkAttributeDefaultInstance(name: String, instances: List<NgsiLdAttributeInstance>) {
    if (instances.count { it.datasetId == null } > 1)
        throw BadRequestDataException("Attribute $name can't have more than one default instance")
}

// TODO to be refactored with validation
fun checkAttributeDuplicateDatasetId(name: String, instances: List<NgsiLdAttributeInstance>) {
    val datasetIds = instances.map {
        it.datasetId
    }
    if (datasetIds.toSet().count() != datasetIds.count())
        throw BadRequestDataException("Attribute $name can't have more than one instance with the same datasetId")
}

fun parseToNgsiLdAttributes(attributes: JsonObject): List<NgsiLdAttribute> =
    attributes.map {
        val firstInstanceOfAttribute = it.value.asJsonArray().getJsonObject(0)
        when {
            isAttributeOfType(firstInstanceOfAttribute, NGSILD_PROPERTY_TYPE) ->
                NgsiLdProperty(it.key, it.value.asJsonArray())
            isAttributeOfType(firstInstanceOfAttribute, NGSILD_RELATIONSHIP_TYPE) ->
                NgsiLdRelationship(it.key, it.value.asJsonArray())
            isAttributeOfType(firstInstanceOfAttribute, NGSILD_GEOPROPERTY_TYPE) ->
                NgsiLdGeoProperty(it.key, it.value.asJsonArray())
            else -> throw BadRequestDataException("Unrecognized type for ${it.key}")
        }
    }

fun String.isNgsiLdSupportedName() =
    this.all { char -> char.isLetterOrDigit() || listOf(':', '_').contains(char) }

fun JsonLdEntity.toNgsiLdEntity(): NgsiLdEntity =
    NgsiLdEntity(this.properties, this.contexts)

fun JsonObject.getDatasetId(): URI? =
    this[NGSILD_DATASET_ID_PROPERTY]?.asJsonArray()?.get(0)?.asJsonObject()?.getString(JSONLD_ID)?.toUri()

val NGSILD_ENTITY_CORE_MEMBERS = listOf(
    JSONLD_ID,
    JSONLD_TYPE
)

val NGSILD_ATTRIBUTES_CORE_MEMBERS = listOf(
    JSONLD_TYPE,
    NGSILD_CREATED_AT_PROPERTY,
    NGSILD_MODIFIED_AT_PROPERTY,
    NGSILD_OBSERVED_AT_PROPERTY,
    NGSILD_DATASET_ID_PROPERTY
)

val NGSILD_PROPERTIES_CORE_MEMBERS = listOf(
    NGSILD_PROPERTY_VALUE,
    NGSILD_UNIT_CODE_PROPERTY
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_RELATIONSHIPS_CORE_MEMBERS = listOf(
    NGSILD_RELATIONSHIP_HAS_OBJECT
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

val NGSILD_GEOPROPERTIES_CORE_MEMEBERS = listOf(
    NGSILD_GEOPROPERTY_VALUE
).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

enum class GeoPropertyType(val value: String) {
    Point("Point"),
    Polygon("Polygon")
}
