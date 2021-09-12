package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.apicatalog.jsonld.JsonLd
import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdErrorCode
import com.apicatalog.jsonld.JsonLdOptions
import com.apicatalog.jsonld.context.cache.LruCache
import com.apicatalog.jsonld.document.JsonDocument
import com.apicatalog.jsonld.http.DefaultHttpClient
import com.apicatalog.jsonld.loader.HttpLoader
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.GeoPropertyType
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.toKeyValues
import com.egm.stellio.shared.util.JsonLdUtils.NgsiLdAttributeType
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import jakarta.json.Json
import jakarta.json.JsonArray
import jakarta.json.JsonNumber
import jakarta.json.JsonObject
import jakarta.json.JsonString
import jakarta.json.JsonValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import java.io.StringWriter
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import kotlin.reflect.full.safeCast

data class AttributeType(val uri: String)

object JsonLdUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    const val EGM_BASE_CONTEXT_URL =
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/feature/357-prepare-core-context-upgrade-to-1_3/"
    const val NGSILD_EGM_CONTEXT = "$EGM_BASE_CONTEXT_URL/shared-jsonld-contexts/egm.jsonld"

    enum class NgsiLdAttributeType(val value: String) {
        PROPERTY("Property"),
        GEOPROPERTY("GeoProperty"),
        RELATIONSHIP("Relationship");

        companion object {
            fun forString(key: String): NgsiLdAttributeType? =
                values().toList().find { it.value == key }
        }
    }

    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_PROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    const val JSONLD_ID = "@id"
    const val JSONLD_TYPE = "@type"
    const val JSONLD_VALUE_KW = "@value"
    const val JSONLD_CONTEXT = "@context"
    val JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS = setOf(JSONLD_ID, JSONLD_TYPE, JSONLD_CONTEXT)
    val JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS = setOf("id", "type", JSONLD_CONTEXT)

    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/createdAt"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/modifiedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/observedAt"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_COORDINATES_PROPERTY = "https://purl.org/geojson/vocab#coordinates"
    const val NGSILD_POINT_PROPERTY = "https://purl.org/geojson/vocab#Point"
    const val NGSILD_POLYGON_PROPERTY = "https://purl.org/geojson/vocab#Polygon"
    const val NGSILD_COMPACT_POINT_PROPERTY = "Point"
    const val NGSILD_INSTANCE_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/instanceId"
    const val NGSILD_DATASET_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/datasetId"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"
    const val NGSILD_DATE_TYPE = "https://uri.etsi.org/ngsi-ld/Date"
    const val NGSILD_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/Time"

    const val EGM_OBSERVED_BY = "https://ontology.eglobalmark.com/egm#observedBy"
    const val EGM_RAISED_NOTIFICATION = "https://ontology.eglobalmark.com/egm#raised"
    const val EGM_SPECIFIC_ACCESS_POLICY = "https://ontology.eglobalmark.com/egm#specificAccessPolicy"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    // TODO our JSON-LD contexts are hosted on GH where the content type is set to text/plain
    //  so allow any content type (by default the lib only allows JSON or JSON-LD)
    private val httpLoader = HttpLoader(DefaultHttpClient.defaultInstance()).apply {
        setFallbackContentType(com.apicatalog.jsonld.http.media.MediaType.ANY)
    }

    // TODO see if this can be improved, for instance by preloading the core context?
    private val jsonLdOptions = JsonLdOptions().apply {
        this.contextCache = LruCache(1024)
        this.documentLoader = httpLoader
    }

    fun expandJsonLdEntity(input: String, contexts: List<String>): JsonLdEntity {
        return JsonLdEntity(expandJsonLdFragment(input, contexts), contexts)
    }

    fun expandJsonLdEntity(input: String): JsonLdEntity {
        // TODO find a way to avoid this extra parsing
        return JsonLdEntity(expandJsonLdFragment(input), extractContextFromInput(input))
    }

    fun expandJsonLdEntities(entities: List<Map<String, Any>>): List<JsonLdEntity> {
        return entities.map {
            expandJsonLdEntity(serializeObject(it))
        }
    }

    fun expandJsonLdEntities(entities: List<Map<String, Any>>, contexts: List<String>): List<JsonLdEntity> {
        return entities.map {
            expandJsonLdEntity(serializeObject(it), contexts)
        }
    }

    fun expandAttributeFragment(attributeName: String, attributePayload: String, contexts: List<String>): JsonObject =
        expandJsonLdFragment(
            serializeObject(mapOf(attributeName to deserializeAs<Any>(attributePayload))),
            contexts
        )

    fun expandJsonLdKey(type: String, context: String): String? =
        expandJsonLdKey(type, listOf(context))

    fun expandJsonLdKey(type: String, contexts: List<String>): String? {
        val fragment =
            """
                { "$type": "$type" }
            """.trimIndent()
        val expandedFragment = expandJsonLdFragment(fragment, contexts)

        return expandedFragment.keys.first()
    }

    fun expandJsonLdFragment(
        fragment: String,
        contexts: List<String> = emptyList(),
        addCoreContextIfMissing: Boolean = true
    ): JsonObject {
        val documentFragment = try {
            JsonDocument.of(fragment.byteInputStream())
        } catch (e: JsonLdError) {
            throw InvalidRequestException("Unexpected error while parsing payload : ${e.cause?.message ?: e.message}")
        }
        val expandedFragment = try {
            if (contexts.isNotEmpty())
                JsonLd.expand(documentFragment)
                    .options(jsonLdOptions)
                    .context(createContextDocument(contexts, addCoreContextIfMissing))
                    .get()
            else
                JsonLd.expand(documentFragment)
                    .options(jsonLdOptions)
                    .get()
        } catch (e: JsonLdError) {
            if (e.code == JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED)
                throw LdContextNotAvailableException("Unable to load remote context (cause was: ${e.message ?: e.code})")
            else
                throw BadRequestDataException("Unexpected error while parsing payload (cause was: $e)")
        }
        if (expandedFragment.isEmpty())
            throw BadRequestDataException("Unable to parse input payload")

        return expandedFragment.getJsonObject(0)
    }

    fun expandJsonLdFragment(fragment: String, context: String): JsonObject {
        return expandJsonLdFragment(fragment, listOf(context))
    }

    fun List<String>.toJsonArray(): JsonArray =
        Json.createArrayBuilder(this).build()

    fun Map<String, Any>.toJsonObject(): JsonObject =
        Json.createObjectBuilder(this).build()

    fun String.toJsonString(): JsonString =
        Json.createValue(this)

    fun addContextToListOfElements(listOfElements: String, contexts: List<String>): String {
        val updatedPayload = deserializeListOfObjects(listOfElements)
            .map {
                it.plus(Pair(JSONLD_CONTEXT, contexts))
            }
        return serializeObject(updatedPayload)
    }

    fun addContextToElement(element: String, contexts: List<String>): String {
        val updatedPayload = deserializeObject(element).plus(Pair(JSONLD_CONTEXT, contexts))
        return serializeObject(updatedPayload)
    }

    fun addContextsToEntity(
        compactedJsonLdEntity: CompactedJsonLdEntity,
        contexts: List<String>
    ): CompactedJsonLdEntity =
        compactedJsonLdEntity.plus(Pair(JSONLD_CONTEXT, contexts.toJsonArray()))

    fun addContextsToEntity(element: CompactedJsonLdEntity, contexts: List<String>, mediaType: MediaType) =
        if (mediaType == MediaType.APPLICATION_JSON)
            element
        else
            element.plus(Pair(JSONLD_CONTEXT, contexts))

    fun extractContextFromInput(input: String): List<String> {
        val parsedInput = deserializeObject(input)

        return if (!parsedInput.containsKey(JSONLD_CONTEXT))
            emptyList()
        else if (parsedInput[JSONLD_CONTEXT] is List<*>)
            parsedInput[JSONLD_CONTEXT] as List<String>
        else if (parsedInput[JSONLD_CONTEXT] is String)
            listOf(parsedInput[JSONLD_CONTEXT] as String)
        else
            emptyList()
    }

    fun removeContextFromInput(input: String): String {
        val parsedInput = deserializeObject(input)

        return if (parsedInput.containsKey(JSONLD_CONTEXT))
            serializeObject(parsedInput.minus(JSONLD_CONTEXT))
        else input
    }

    /**
     * Extract the actual value (@value) of a property from the properties map of an expanded property.
     *
     * @param value a map similar to:
     * {
     *   https://uri.etsi.org/ngsi-ld/hasValue=[{
     *     @type=[https://uri.etsi.org/ngsi-ld/Property],
     *     @value=250
     *   }],
     *   https://uri.etsi.org/ngsi-ld/unitCode=[{
     *     @value=kg
     *   }],
     *   https://uri.etsi.org/ngsi-ld/observedAt=[{
     *     @value=2019-12-18T10:45:44.248755Z
     *   }]
     * }
     *
     * @return the actual value, e.g. "kg" if provided #propertyKey is https://uri.etsi.org/ngsi-ld/unitCode
     */
    fun getPropertyValueFromObject(value: JsonObject, propertyKey: String): Any? =
        if (value[propertyKey] != null) {
            val intermediateList = value[propertyKey]?.asJsonArray()!!
            if (intermediateList.size == 1) {
                val firstListEntry = intermediateList[0].asJsonObject()
                val finalValueType = firstListEntry[JSONLD_TYPE] as? JsonString
                when {
                    finalValueType != null -> {
                        val finalValue = JsonString::class.safeCast(firstListEntry[JSONLD_VALUE_KW])
                        when (finalValueType.string) {
                            NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue?.string)
                            NGSILD_DATE_TYPE -> LocalDate.parse(finalValue?.string)
                            NGSILD_TIME_TYPE -> LocalTime.parse(finalValue?.string)
                            else -> firstListEntry[JSONLD_VALUE_KW]
                        }
                    }
                    firstListEntry[JSONLD_VALUE_KW] != null -> {
                        when (val propertyValue = firstListEntry[JSONLD_VALUE_KW]) {
                            is JsonString -> propertyValue.string
                            // TODO how to deal properly with int / floating?
                            is JsonNumber ->
                                if (propertyValue.isIntegral) propertyValue.intValue()
                                else propertyValue.bigDecimalValue()
                            else -> propertyValue.toString()
                        }
                    }
                    firstListEntry[JSONLD_ID] != null -> {
                        // Used to get the value of datasetId property,
                        // since it is mapped to "@id" key rather than "@value"
                        (firstListEntry[JSONLD_ID] as JsonString).string
                    }
                    else -> {
                        // it is a map / JSON object, keep it as is
                        // {https://uri.etsi.org/ngsi-ld/default-context/key=[{@value=value}], ...}
                        firstListEntry
                    }
                }
            } else {
                intermediateList.map {
                    (it as? JsonObject)?.get(JSONLD_VALUE_KW)
                }
            }
        } else
            null

    fun getPropertyValueAsDateTime(values: JsonObject, propertyKey: String): ZonedDateTime? =
        ZonedDateTime::class.safeCast(getPropertyValueFromObject(values, propertyKey))

    fun getPropertyValueAsString(values: JsonObject, propertyKey: String): String? =
        String::class.safeCast(getPropertyValueFromObject(values, propertyKey))

    fun getAttributeFromExpandedFragment(
        expandedFragment: JsonObject,
        expandedAttributeName: String,
        datasetId: URI?
    ): JsonValue? {
        if (!expandedFragment.containsKey(expandedAttributeName))
            return null

        return (expandedFragment[expandedAttributeName]!!.asJsonArray())
            .find { jsonValue ->
                when {
                    jsonValue.valueType == JsonValue.ValueType.OBJECT && datasetId == null ->
                        !jsonValue.asJsonObject().containsKey(NGSILD_DATASET_ID_PROPERTY)
                    jsonValue.valueType == JsonValue.ValueType.OBJECT && datasetId != null ->
                        getPropertyValueFromObject(
                            jsonValue.asJsonObject(),
                            NGSILD_DATASET_ID_PROPERTY
                        ) == datasetId.toString()
                    else -> false
                }
            }
    }

    /**
     * Utility but basic method to find if given contexts can resolve a known term from the core context.
     */
    internal fun containsCoreContext(contexts: List<String>): Boolean {
        val fragment =
            """
                { "datasetId": "datasetId" }
            """.trimIndent()

        return try {
            val expandedFragment = expandJsonLdFragment(fragment, contexts, false)
            expandedFragment.keys.first() == NGSILD_DATASET_ID_PROPERTY
        } catch (e: BadRequestDataException) {
            false
        }
    }

    private fun createContextDocument(contexts: List<String>, addCoreContextIfMissing: Boolean = true): JsonDocument {
        var jsonArrayBuilder = Json.createArrayBuilder(contexts)
        // if the core context is not in the list, add it at the end
        if (addCoreContextIfMissing && !containsCoreContext(contexts))
            jsonArrayBuilder = jsonArrayBuilder.add(NGSILD_CORE_CONTEXT)

        return JsonDocument.of(jsonArrayBuilder.build())
    }

    private fun Map<String, Any>.toJsonDocument(): JsonDocument =
        JsonDocument.of(serializeObject(this).byteInputStream())

    private fun String.toJsonDocument(): JsonDocument =
        JsonDocument.of(Json.createObjectBuilder().add(this, this).build())

    /**
     * Compact a term (type, attribute name, ...) using the provided context.
     */
    fun compactTerm(term: String, contexts: List<String>): String {
        val compactedFragment =
            JsonLd.compact(term.toJsonDocument(), createContextDocument(contexts))
                .options(jsonLdOptions)
                .get()
        return compactedFragment.keys.first()
    }

    fun compactFragment(value: Map<String, Any>, contexts: List<String>): Map<String, JsonValue> =
        JsonLd.compact(value.toJsonDocument(), createContextDocument(contexts))
            .options(jsonLdOptions)
            .get()

    fun compactAndSerializeFragment(key: String, value: Any, contexts: List<String>): String =
        compactAndSerializeFragment(mapOf(key to value), contexts)

    fun compactAndSerializeFragment(value: Map<String, Any>, contexts: List<String>): String {
        val compactedFragment = compactFragment(value, contexts)
        return compactedFragment.minus(JSONLD_CONTEXT).serialize()
    }

    fun compactAndSerialize(
        jsonLdEntity: JsonLdEntity,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): String =
        compact(jsonLdEntity, contexts, mediaType).serialize()

    fun CompactedJsonLdEntity.serialize(): String {
        val stringWriter = StringWriter()
        val jsonWriter = Json.createWriter(stringWriter)
        jsonWriter.writeObject(Json.createObjectBuilder(this).build())
        jsonWriter.close()
        return stringWriter.toString()
    }

    fun compact(
        jsonLdEntity: JsonLdEntity,
        context: String? = null,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedJsonLdEntity =
        compact(jsonLdEntity, listOfNotNull(context), mediaType)

    fun compact(
        jsonLdEntity: JsonLdEntity,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedJsonLdEntity {
        val allContexts =
            when {
                contexts.isEmpty() -> listOf(NGSILD_CORE_CONTEXT)
                // to ensure core @context comes last
                contexts.contains(NGSILD_CORE_CONTEXT) ->
                    contexts.filter { it != NGSILD_CORE_CONTEXT }.plus(NGSILD_CORE_CONTEXT)
                // to check if the core @context is included / embedded in one of the link
                containsCoreContext(contexts) -> contexts
                else -> contexts.plus(NGSILD_CORE_CONTEXT)
            }

        return if (mediaType == MediaType.APPLICATION_JSON)
            JsonLd.compact(jsonLdEntity.properties.toJsonDocument(), createContextDocument(allContexts))
                .options(jsonLdOptions)
                .get()
                .minus(JSONLD_CONTEXT)
        else
            JsonLd.compact(jsonLdEntity.properties.toJsonDocument(), createContextDocument(allContexts))
                .options(jsonLdOptions)
                .get()
                .minus(JSONLD_CONTEXT)
                .plus(JSONLD_CONTEXT to Json.createArrayBuilder(allContexts).build())
    }

    fun compactEntities(
        entities: List<JsonLdEntity>,
        useSimplifiedRepresentation: Boolean,
        context: String,
        mediaType: MediaType
    ): List<CompactedJsonLdEntity> =
        entities.map {
            if (useSimplifiedRepresentation)
                compact(it, context, mediaType).toKeyValues()
            else
                compact(it, context, mediaType)
        }

    fun filterCompactedEntityOnAttributes(
        input: CompactedJsonLdEntity,
        includedAttributes: Set<String>
    ): Map<String, Any> {
        val identity: (CompactedJsonLdEntity) -> CompactedJsonLdEntity = { it }
        return filterEntityOnAttributes(input, identity, includedAttributes, false)
    }

    fun filterJsonLdEntityOnAttributes(
        input: JsonLdEntity,
        includedAttributes: Set<String>
    ): Map<String, JsonValue> {
        val inputToMap = { i: JsonLdEntity -> i.properties }
        return filterEntityOnAttributes(input, inputToMap, includedAttributes, true)
    }

    private fun <T> filterEntityOnAttributes(
        input: T,
        inputToMap: (T) -> Map<String, JsonValue>,
        includedAttributes: Set<String>,
        isExpandedForm: Boolean
    ): Map<String, JsonValue> {
        return if (includedAttributes.isEmpty()) {
            inputToMap(input)
        } else {
            val mandatoryFields = if (isExpandedForm)
                JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS
            else
                JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS
            val includedKeys = mandatoryFields.plus(includedAttributes)
            inputToMap(input).filterKeys { includedKeys.contains(it) }
        }
    }

    fun extractRelationshipObject(name: String, values: JsonObject): Either<BadRequestDataException, URI> {
        return values.right()
            .flatMap {
                if (!it.containsKey(NGSILD_RELATIONSHIP_HAS_OBJECT))
                    BadRequestDataException("Relationship $name does not have an object field").left()
                else it[NGSILD_RELATIONSHIP_HAS_OBJECT]!!.right()
            }
            .flatMap {
                if (it.asJsonArray().isEmpty())
                    BadRequestDataException("Relationship $name is empty").left()
                else it.asJsonArray()[0].right()
            }
            .flatMap {
                if (it.valueType != JsonValue.ValueType.OBJECT)
                    BadRequestDataException("Relationship $name has an invalid object type: ${it.javaClass}").left()
                else it.asJsonObject()[JSONLD_ID]?.right()
                    ?: BadRequestDataException("Relationship $name has an invalid or no object id: $it").left()
            }
            .flatMap {
                if (it.valueType != JsonValue.ValueType.STRING)
                    BadRequestDataException("Relationship $name has an invalid object id type: $it").left()
                else (it as JsonString).string.toUri().right()
            }
    }

    fun reconstructPolygonCoordinates(compactedJsonLdEntity: MutableMap<String, JsonValue>) =
        compactedJsonLdEntity
            .filterValues {
                it is JsonObject &&
                    it.typeOfAttribute() == NgsiLdAttributeType.GEOPROPERTY &&
                    (it.getJsonObject("value")["type"] as JsonString).string == GeoPropertyType.Polygon.value
            }.forEach {
                val geoPropertyValue = (it.value as JsonObject).getJsonObject("value").asJsonObject()
                val geoPropertyCoordinates = geoPropertyValue.getJsonArray("coordinates")

                compactedJsonLdEntity.replace(
                    it.key,
                    Json.createObjectBuilder(
                        mapOf(
                            "type" to Json.createValue(NgsiLdAttributeType.GEOPROPERTY.value),
                            "value" to Json.createObjectBuilder(
                                mapOf(
                                    "type" to GeoPropertyType.Polygon.value,
                                    "coordinates" to geoPropertyCoordinates
                                )
                            )
                        )
                    ).build()
                )
            }
}

fun String.extractShortTypeFromExpanded(): String =
    /*
     * TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
     * TODO do a clean implementation using info from @context
     */
    this.substringAfterLast("/").substringAfterLast("#")

fun JsonObject.typeOfAttribute(): NgsiLdAttributeType? =
    NgsiLdAttributeType.forString((this["type"] as JsonString).string)

// expanded values are typically wrapped in an array of one element
fun JsonValue.extractJsonObject(): JsonObject =
    if (this is JsonArray)
        this.asJsonArray().getJsonObject(0)
    else
        this.asJsonObject()
