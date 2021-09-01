package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Attribute
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.UpdateAttributeResult
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.toRelationshipTypeName
import com.egm.stellio.entity.model.updateResultFromDetailedResult
import com.egm.stellio.entity.repository.AttributeSubjectNode
import com.egm.stellio.entity.repository.EntitySubjectNode
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.NGSILD_PROPERTIES_CORE_MEMBERS
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIPS_CORE_MEMBERS
import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.model.getDatasetId
import com.egm.stellio.shared.model.isAttributeOfType
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import jakarta.json.Json
import jakarta.json.JsonObject
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI

@Component
class EntityAttributeService(
    private val entityService: EntityService,
    private val neo4jRepository: Neo4jRepository,
    private val propertyRepository: PropertyRepository,
    private val relationshipRepository: RelationshipRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun partialUpdateEntityAttribute(
        entityId: URI,
        expandedPayload: JsonObject,
        contexts: List<String>
    ): UpdateResult {
        val (expandedAttributeName, attributeValues) = expandedPayload.entries.first()

        logger.debug("Updating attribute $expandedAttributeName of entity $entityId with values: $attributeValues")

        val updateResult = attributeValues.asJsonArray().map { attributeInstanceValues ->
            val attributeInstanceObject = attributeInstanceValues.asJsonObject()
            val datasetId = attributeInstanceObject.getDatasetId()
            when {
                neo4jRepository.hasRelationshipInstance(
                    EntitySubjectNode(entityId),
                    expandedAttributeName.toRelationshipTypeName(),
                    datasetId
                ) ->
                    partialUpdateEntityRelationship(
                        entityId,
                        expandedAttributeName.toRelationshipTypeName(),
                        attributeInstanceObject,
                        datasetId,
                        contexts
                    )
                neo4jRepository.hasPropertyInstance(
                    EntitySubjectNode(entityId),
                    expandedAttributeName,
                    datasetId
                ) ->
                    partialUpdateEntityProperty(
                        entityId,
                        expandedAttributeName,
                        attributeInstanceObject,
                        datasetId,
                        contexts
                    )
                else -> UpdateAttributeResult(
                    expandedAttributeName,
                    datasetId,
                    UpdateOperationResult.IGNORED,
                    "Unknown attribute $expandedAttributeName with datasetId $datasetId in entity $entityId"
                )
            }
        }

        // still throwing an exception to trigger the rollback of the transactions of other instances
        if (updateResult.any { it.updateOperationResult == UpdateOperationResult.FAILED })
            throw InternalErrorException("Partial update operation failed to perform the whole update")

        // update modifiedAt in entity if at least one attribute has been updated
        if (updateResult.any { it.isSuccessfullyUpdated() })
            neo4jRepository.updateEntityModifiedDate(entityId)

        return updateResultFromDetailedResult(updateResult)
    }

    internal fun partialUpdateEntityRelationship(
        entityId: URI,
        relationshipType: String,
        relationshipValues: JsonObject,
        datasetId: URI?,
        contexts: List<String>
    ): UpdateAttributeResult {
        val relationship =
            if (datasetId != null)
                relationshipRepository.getRelationshipOfSubject(entityId, relationshipType, datasetId)
            else
                relationshipRepository.getRelationshipOfSubject(entityId, relationshipType)
        val relationshipUpdates = partialUpdateRelationshipOfAttribute(
            relationship,
            entityId,
            relationshipType,
            datasetId,
            relationshipValues
        )
        val attributesOfRelationshipUpdates = partialUpdateAttributesOfAttribute(
            relationship,
            relationshipValues,
            contexts
        )
        return if (relationshipUpdates && attributesOfRelationshipUpdates)
            UpdateAttributeResult(
                expandJsonLdKey(relationshipType, contexts)!!,
                datasetId,
                UpdateOperationResult.UPDATED,
                null
            ) else UpdateAttributeResult(
            expandJsonLdKey(relationshipType, contexts)!!,
            datasetId,
            UpdateOperationResult.FAILED,
            "Partial update operation failed to perform the whole update"
        )
    }

    internal fun partialUpdateEntityProperty(
        entityId: URI,
        expandedPropertyName: String,
        propertyValues: JsonObject,
        datasetId: URI?,
        contexts: List<String>
    ): UpdateAttributeResult {
        val property =
            if (datasetId != null)
                propertyRepository.getPropertyOfSubject(entityId, expandedPropertyName, datasetId)
            else
                propertyRepository.getPropertyOfSubject(entityId, expandedPropertyName)
        val propertyUpdates = partialUpdatePropertyOfAttribute(
            property,
            propertyValues
        )
        val attributesOfPropertyUpdates = partialUpdateAttributesOfAttribute(property, propertyValues, contexts)
        return if (propertyUpdates && attributesOfPropertyUpdates)
            UpdateAttributeResult(
                expandedPropertyName,
                datasetId,
                UpdateOperationResult.UPDATED,
                null
            ) else UpdateAttributeResult(
            expandedPropertyName,
            datasetId,
            UpdateOperationResult.FAILED,
            "Partial update operation failed to perform the whole update"
        )
    }

    internal fun partialUpdateAttributesOfAttribute(
        attribute: Attribute,
        attributeValues: JsonObject,
        contexts: List<String>
    ): Boolean {
        return attributeValues.filterKeys {
            if (attribute is Relationship)
                !NGSILD_RELATIONSHIPS_CORE_MEMBERS.contains(it)
            else
                !NGSILD_PROPERTIES_CORE_MEMBERS.contains(it)
        }.map {
            val attributeOfAttributeName = it.key
            val attributeOfAttributeObject = it.value.asJsonObject()
            when {
                neo4jRepository.hasRelationshipInstance(
                    AttributeSubjectNode(attribute.id()),
                    attributeOfAttributeName.toRelationshipTypeName()
                ) -> {
                    val relationshipOfAttribute = relationshipRepository.getRelationshipOfSubject(
                        attribute.id(),
                        attributeOfAttributeName.toRelationshipTypeName()
                    )
                    partialUpdateRelationshipOfAttribute(
                        relationshipOfAttribute,
                        attribute.id(),
                        attributeOfAttributeName.toRelationshipTypeName(),
                        null,
                        attributeOfAttributeObject
                    )
                }
                neo4jRepository.hasPropertyInstance(
                    AttributeSubjectNode(attribute.id()),
                    expandJsonLdKey(attributeOfAttributeName, contexts)!!
                ) -> {
                    val propertyOfAttribute = propertyRepository.getPropertyOfSubject(
                        attribute.id(),
                        expandJsonLdKey(attributeOfAttributeName, contexts)!!
                    )
                    partialUpdatePropertyOfAttribute(
                        propertyOfAttribute,
                        attributeOfAttributeObject
                    )
                }
                else -> {
                    if (isAttributeOfType(attributeOfAttributeObject, JsonLdUtils.NGSILD_RELATIONSHIP_TYPE)) {
                        val ngsiLdRelationship = NgsiLdRelationship(
                            attributeOfAttributeName,
                            Json.createArrayBuilder(listOf(it.value)).build()
                        )
                        entityService.createAttributeRelationships(attribute.id(), listOf(ngsiLdRelationship))
                    } else if (isAttributeOfType(attributeOfAttributeObject, JsonLdUtils.NGSILD_PROPERTY_TYPE)) {
                        val ngsiLdProperty = NgsiLdProperty(
                            expandJsonLdKey(attributeOfAttributeName, contexts)!!,
                            Json.createArrayBuilder(listOf(it.value)).build()
                        )
                        entityService.createAttributeProperties(attribute.id(), listOf(ngsiLdProperty))
                    } else false
                }
            }
        }.all { it }
    }

    internal fun partialUpdateRelationshipOfAttribute(
        relationshipOfAttribute: Relationship,
        attributeId: URI,
        relationshipType: String,
        datasetId: URI?,
        relationshipValues: JsonObject
    ): Boolean {
        updateRelationshipTargetOfAttribute(
            attributeId,
            relationshipType,
            datasetId,
            relationshipValues
        )
        val updatedRelationship = relationshipOfAttribute.updateValues(relationshipValues)
        relationshipRepository.save(updatedRelationship)
        return true
    }

    internal fun partialUpdatePropertyOfAttribute(
        propertyOfAttribute: Property,
        propertyValues: JsonObject
    ): Boolean {
        val updatedProperty = propertyOfAttribute.updateValues(propertyValues)
        propertyRepository.save(updatedProperty)
        return true
    }

    internal fun updateRelationshipTargetOfAttribute(
        attributeId: URI,
        relationshipType: String,
        datasetId: URI?,
        relationshipValues: JsonObject
    ): Boolean =
        JsonLdUtils.extractRelationshipObject(relationshipType, relationshipValues)
            .map { objectId ->
                neo4jRepository.updateRelationshipTargetOfSubject(
                    attributeId,
                    relationshipType,
                    objectId,
                    datasetId
                )
            }
            .fold({ false }, { it })
}
