package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NgsiLdAttributeType
import com.egm.stellio.shared.util.extractShortTypeFromExpanded
import com.egm.stellio.shared.util.typeOfAttribute
import jakarta.json.JsonArray
import jakarta.json.JsonObject
import jakarta.json.JsonString
import jakarta.json.JsonValue
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.net.URI

private val logger: Logger = LoggerFactory.getLogger(JsonLdEntity::class.java)

typealias CompactedJsonLdEntity = Map<String, JsonValue>
typealias CompactedJsonLdAttribute = Map<String, Any>

data class JsonLdEntity(
    val properties: Map<String, JsonValue>,
    val contexts: List<String>
) {
    fun containsAnyOf(expandedAttributes: Set<String>): Boolean =
        expandedAttributes.isEmpty() || properties.keys.any { expandedAttributes.contains(it) }

    // FIXME kinda hacky but we often just need the id or type... how can it be improved?
    val id by lazy {
        (properties[JSONLD_ID] as JsonString).string
            ?: throw InternalErrorException("Could not extract id from JSON-LD entity")
    }

    val type by lazy {
        (properties[JSONLD_TYPE] as JsonArray).getJsonString(0).string
            ?: throw InternalErrorException("Could not extract type from JSON-LD entity")
    }
}

fun CompactedJsonLdEntity.getType(): String =
    (this["type"] as JsonString).string

fun CompactedJsonLdEntity.toKeyValues(): Map<String, JsonValue> =
    this.mapValues { simplifyRepresentation(it.value) }

fun CompactedJsonLdEntity.extractAttributeInstance(
    compactedAttributeName: String,
    datasetId: URI?
): CompactedJsonLdAttribute {
    return if (this[compactedAttributeName] is List<*>) {
        val attributePayload = this[compactedAttributeName] as List<CompactedJsonLdAttribute>
        attributePayload.first { it["datasetId"] as String? == datasetId?.toString() }
    } else if (this[compactedAttributeName] != null)
        this[compactedAttributeName] as CompactedJsonLdAttribute
    else {
        // Since some attributes cannot be well compacted, to be improved later
        logger.warn(
            "Could not find entry for attribute: $compactedAttributeName, " +
                "trying on the 'guessed' short form instead: ${compactedAttributeName.extractShortTypeFromExpanded()}"
        )
        this[compactedAttributeName.extractShortTypeFromExpanded()] as CompactedJsonLdAttribute
    }
}

private fun simplifyRepresentation(value: JsonValue): JsonValue {
    return when (value) {
        // entity attributes are always JSON objects
        is JsonObject -> simplifyValue(value)
        // we keep id, type and @context values as they are (String and List<String>)
        else -> value
    }
}

private fun simplifyValue(value: JsonObject): JsonValue {
    return when (value.typeOfAttribute()) {
        NgsiLdAttributeType.PROPERTY, NgsiLdAttributeType.GEOPROPERTY -> value.getOrDefault("value", value)
        NgsiLdAttributeType.RELATIONSHIP -> value.getOrDefault("object", value)
        else -> throw BadRequestDataException("Unknown type for attribute: $value")
    }
}
