package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.toJsonObject
import com.egm.stellio.shared.util.JsonLdUtils.toJsonString
import jakarta.json.Json
import jakarta.json.JsonValue
import org.springframework.stereotype.Service
import java.net.URI

typealias SimplifiedTemporalAttribute = Map<String, JsonValue>
typealias TemporalEntityAttributeInstancesResult = Map<TemporalEntityAttribute, List<AttributeInstanceResult>>

@Service
class TemporalEntityService {

    fun buildTemporalEntities(
        queryResult: List<Pair<URI, TemporalEntityAttributeInstancesResult>>,
        temporalQuery: TemporalQuery,
        contexts: List<String>,
        withTemporalValues: Boolean
    ): List<CompactedJsonLdEntity> {
        return queryResult.map { buildTemporalEntity(it.first, it.second, temporalQuery, contexts, withTemporalValues) }
    }

    fun buildTemporalEntity(
        entityId: URI,
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalQuery: TemporalQuery,
        contexts: List<String>,
        withTemporalValues: Boolean
    ): CompactedJsonLdEntity {
        val temporalAttributes = buildTemporalAttributes(
            attributeAndResultsMap,
            temporalQuery,
            contexts,
            withTemporalValues
        )

        return mapOf(
            "id" to entityId.toString().toJsonString(),
            "type" to JsonLdUtils.compactTerm(attributeAndResultsMap.keys.first().type, contexts).toJsonString()
        ).plus(temporalAttributes)
    }

    private fun buildTemporalAttributes(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        temporalQuery: TemporalQuery,
        contexts: List<String>,
        withTemporalValues: Boolean
    ): Map<String, JsonValue> {
        return if (withTemporalValues || temporalQuery.timeBucket != null) {
            val attributes = buildAttributesSimplifiedRepresentation(attributeAndResultsMap)
            mergeSimplifiedTemporalAttributesOnAttributeName(attributes)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    if (it.value.size == 1) it.value.first().toJsonObject()
                    else Json.createArrayBuilder(it.value).build()
                }
        } else {
            mergeFullTemporalAttributesOnAttributeName(attributeAndResultsMap)
                .mapKeys { JsonLdUtils.compactTerm(it.key, contexts) }
                .mapValues {
                    val values = it.value.map { attributeInstanceResult ->
                        attributeInstanceResult as FullAttributeInstanceResult
                        Json.createReader(attributeInstanceResult.payload.byteInputStream()).readArray()
                    }
                    Json.createArrayBuilder(values).build()
                }
        }
    }

    /**
     * Creates the simplified representation for each temporal entity attribute in the input map.
     *
     * The simplified representation is created from the attribute instance results of the temporal entity attribute.
     *
     * It returns a Map with the same keys as the input map and values corresponding to simplified representation
     * of the temporal entity attribute.
     */
    private fun buildAttributesSimplifiedRepresentation(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult
    ): Map<TemporalEntityAttribute, SimplifiedTemporalAttribute> {
        return attributeAndResultsMap.mapValues {
            val attributeInstance = mutableMapOf<String, Any>(
                "type" to it.key.attributeType.toString()
            )
            val valuesKey =
                if (it.key.attributeType == TemporalEntityAttribute.AttributeType.Property)
                    "values"
                else
                    "objects"
            it.key.datasetId?.let { attributeInstance["datasetId"] = it }
            attributeInstance[valuesKey] = it.value.map { attributeInstanceResult ->
                attributeInstanceResult as SimplifiedAttributeInstanceResult
                listOf(attributeInstanceResult.value, attributeInstanceResult.observedAt)
            }
            attributeInstance.toMap().toJsonObject()
        }
    }

    /**
     * Group the attribute instances results by temporal entity attribute name and return a map with:
     * - Key: (expanded) name of the temporal entity attribute
     * - Value: list of the full representation of the attribute instances
     */
    private fun mergeFullTemporalAttributesOnAttributeName(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult
    ): Map<String, List<AttributeInstanceResult>> =
        attributeAndResultsMap.toList()
            .groupBy { (temporalEntityAttribute, _) ->
                temporalEntityAttribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, attributeInstancesResults) ->
                    attributeInstancesResults
                }.flatten()
            }

    /**
     * Group the simplified representations by temporal entity attribute name and return a map with:
     * - Key: (expanded) name of the temporal entity attribute
     * - Value: list of the simplified temporal representation of the attribute instances
     */
    private fun mergeSimplifiedTemporalAttributesOnAttributeName(
        attributeAndResultsMap: Map<TemporalEntityAttribute, SimplifiedTemporalAttribute>
    ): Map<String, List<SimplifiedTemporalAttribute>> =
        attributeAndResultsMap.toList()
            .groupBy { (temporalEntityAttribute, _) ->
                temporalEntityAttribute.attributeName
            }
            .toMap()
            .mapValues {
                it.value.map { (_, simplifiedTemporalAttribute) ->
                    simplifiedTemporalAttribute
                }
            }
}
