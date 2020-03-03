package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.*
import com.egm.datahub.context.registry.repository.*
import com.egm.datahub.context.registry.util.*
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.EGM_VENDOR_ID
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_COORDINATES_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_ENTITY_ID
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_ENTITY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_GEOPROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTIES_CORE_MEMBERS
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIPS_CORE_MEMBERS
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_RELATIONSHIP_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandJsonLdFragment
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandJsonLdKey
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandRelationshipType
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.expandValueAsMap
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.extractShortTypeFromPayload
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.extractTypeFromPayload
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMapAsDateTime
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMapAsString
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getRelationshipObjectId
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.isAttributeOfType
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.parseJsonLdFragment
import com.egm.datahub.context.registry.web.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.utils.JsonUtils
import org.neo4j.ogm.types.spatial.GeographicPoint2d
import org.slf4j.LoggerFactory
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional

@Component
class Neo4jService(
    private val neo4jRepository: Neo4jRepository,
    private val entityRepository: EntityRepository,
    private val propertyRepository: PropertyRepository,
    private val relationshipRepository: RelationshipRepository,
    private val attributeRepository: AttributeRepository,
    private val applicationEventPublisher: ApplicationEventPublisher
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun createEntity(expandedPayload: Map<String, Any>, contexts: List<String>): Entity {
        val mapper = jacksonObjectMapper()
        val rawEntity = mapper.readValue(JsonUtils.toString(expandedPayload), Entity::class.java)
        // we have to re-inject the contexts as the expanded form does not ship them (by definition)
        rawEntity.contexts = contexts
        val entity = entityRepository.save(rawEntity)

        // filter the unwanted entries and expand all attributes for easier later processing
        val propertiesAndRelationshipsMap = expandedPayload.filterKeys {
            !listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).contains(it)
        }.mapValues {
            expandValueAsMap(it.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            isAttributeOfType(entry.value, NGSILD_RELATIONSHIP_TYPE)
        }.forEach { entry ->
            val relationshipType = entry.key
            val objectId = getRelationshipObjectId(entry.value)
            val objectEntity = entityRepository.findById(objectId)

            if (!objectEntity.isPresent) {
                    throw BadRequestDataException("Target entity $objectId in relationship $relationshipType does not exist, create it first")
            } else
                createEntityRelationship(entity, relationshipType, entry.value, objectEntity.get().id)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            isAttributeOfType(entry.value, NGSILD_PROPERTY_TYPE)
        }.forEach { entry ->
            createEntityProperty(entity, entry.key, entry.value)
        }

        propertiesAndRelationshipsMap.filter { entry ->
            isAttributeOfType(entry.value, NGSILD_GEOPROPERTY_TYPE)
        }.forEach { entry ->
            createLocationProperty(entity, entry.key, entry.value)
        }

        val entityType = extractShortTypeFromPayload(expandedPayload)
        val entityEvent = EntityEvent(entityType, entity.id, EventType.POST, JsonUtils.toString(expandedPayload))
        applicationEventPublisher.publishEvent(entityEvent)

        return entity
    }

    internal fun createEntityProperty(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>, isInBatchContext: Boolean = false, validEntities: Map<String, String> = mutableMapOf()): Property {
        val propertyValue = getPropertyValueFromMap(propertyValues, NGSILD_PROPERTY_VALUE)
            ?: throw BadRequestDataException("Key $NGSILD_PROPERTY_VALUE not found in $propertyValues")

        logger.debug("Creating property $propertyKey with value $propertyValue")

        val rawProperty = Property(
            name = propertyKey, value = propertyValue,
            unitCode = getPropertyValueFromMapAsString(propertyValues, NGSILD_UNIT_CODE_PROPERTY),
            observedAt = getPropertyValueFromMapAsDateTime(propertyValues, NGSILD_OBSERVED_AT_PROPERTY)
        )

        val property = propertyRepository.save(rawProperty)

        entity.properties.add(property)
        entityRepository.save(entity)

        createAttributeProperties(property, propertyValues)
        createAttributeRelationships(property, propertyValues, isInBatchContext, validEntities)

        return property
    }

    /**
     * Create the relationship between two entities, as two relationships : a generic one from source entity to a relationship node,
     * and a typed one (with the relationship type) from the relationship node to the target entity.
     *
     * @return the created Relationship object
     */
    private fun createEntityRelationship(
        entity: Entity,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>,
        targetEntityId: String
    ): Relationship {

        // TODO : finish integration with relationship properties (https://redmine.eglobalmark.com/issues/847)
        val rawRelationship = Relationship(type = listOf(relationshipType),
            observedAt = getPropertyValueFromMapAsDateTime(relationshipValues, NGSILD_OBSERVED_AT_PROPERTY))
        val relationship = relationshipRepository.save(rawRelationship)
        entity.relationships.add(relationship)
        entityRepository.save(entity)

        neo4jRepository.createRelationshipToEntity(relationship.id, relationshipType.toRelationshipTypeName(), targetEntityId)

        createAttributeProperties(relationship, relationshipValues)
        createAttributeRelationships(relationship, relationshipValues)

        return relationship
    }

    private fun createAttributeProperties(subject: Attribute, values: Map<String, List<Any>>) {
        values.filterValues {
            it[0] is Map<*, *>
        }
        .mapValues {
            expandValueAsMap(it.value)
        }
        .filter {
            !(NGSILD_PROPERTIES_CORE_MEMBERS.plus(NGSILD_RELATIONSHIPS_CORE_MEMBERS)).contains(it.key) &&
                    isAttributeOfType(it.value, NGSILD_PROPERTY_TYPE)
        }
        .forEach { entry ->
            logger.debug("Creating property ${entry.key} with values ${entry.value}")

            // for short-handed properties, the value is directly accessible from the map under the @value key
            val propertyValue = getPropertyValueFromMap(entry.value, NGSILD_PROPERTY_VALUE) ?: entry.value["@value"]!!

            val rawProperty = Property(
                name = entry.key, value = propertyValue,
                unitCode = getPropertyValueFromMapAsString(entry.value, NGSILD_UNIT_CODE_PROPERTY),
                observedAt = getPropertyValueFromMapAsDateTime(entry.value, NGSILD_OBSERVED_AT_PROPERTY)
            )

            val property = propertyRepository.save(rawProperty)
            subject.properties.add(property)
            attributeRepository.save(subject)
        }
    }

    private fun createAttributeRelationships(subject: Attribute, values: Map<String, List<Any>>, isInBatchContext: Boolean = false, validEntities: Map<String, String> = mutableMapOf()) {
        values.forEach { propEntry ->
            val propEntryValue = propEntry.value[0]
            logger.debug("Looking at prop entry ${propEntry.key} with value $propEntryValue")
            if (propEntryValue is Map<*, *>) {
                val propEntryValueMap = propEntryValue as Map<String, List<Any>>
                if (isAttributeOfType(propEntryValueMap, NGSILD_RELATIONSHIP_TYPE)) {
                    val objectId = getRelationshipObjectId(propEntryValueMap)
                    val objectEntity = entityRepository.findById(objectId)
                    if (objectEntity.isPresent) {
                        val rawRelationship = Relationship(
                            type = listOf(propEntry.key),
                            observedAt = getPropertyValueFromMapAsDateTime(
                                propEntryValueMap,
                                NGSILD_OBSERVED_AT_PROPERTY
                            )
                        )
                        val relationship = relationshipRepository.save(rawRelationship)
                        subject.relationships.add(relationship)
                        attributeRepository.save(subject)

                        neo4jRepository.createRelationshipToEntity(relationship.id, propEntry.key.toRelationshipTypeName(), objectEntity.get().id
                        )
                    } else {
                        if (!isInBatchContext)
                            throw BadRequestDataException("Target entity $objectId in property $subject does not exist, create it first")
                        else {
                            if (!validEntities.containsKey(objectId)) {
                                throw BadRequestDataException("Target entity $objectId in relationship $subject does not exist, create it first")
                            } else {
                                val rawRelationship = Relationship(
                                    type = listOf(propEntry.key),
                                    observedAt = getPropertyValueFromMapAsDateTime(
                                        propEntryValueMap,
                                        NGSILD_OBSERVED_AT_PROPERTY
                                    )
                                )
                                val relationship = relationshipRepository.save(rawRelationship)
                                subject.relationships.add(relationship)
                                attributeRepository.save(subject)

                                neo4jRepository.createRelationshipToEntity(relationship.id, propEntry.key.toRelationshipTypeName(), createTempEntityInBatch(objectId, validEntities.getValue(objectId)).id)
                            }
                        }
                    }
                } else {
                    logger.debug("Ignoring ${propEntry.key} entry as it can't be a relationship ($propEntryValue)")
                }
            }
        }
    }

    internal fun createLocationProperty(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>): Int {

        logger.debug("Geo property $propertyKey has values $propertyValues")
        val geoPropertyValue = expandValueAsMap(propertyValues[NGSILD_GEOPROPERTY_VALUE]!!)
        val geoPropertyType = geoPropertyValue["@type"]!![0] as String
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        return if (geoPropertyType == "https://uri.etsi.org/ngsi-ld/default-context/Point") {
            val geoPropertyCoordinates = geoPropertyValue[NGSILD_COORDINATES_PROPERTY]!!
            val longitude = (geoPropertyCoordinates[0] as Map<String, Double>)["@value"]
            val latitude = (geoPropertyCoordinates[1] as Map<String, Double>)["@value"]
            logger.debug("Point has coordinates $latitude, $longitude")

            neo4jRepository.addLocationPropertyToEntity(entity.id, Pair(longitude!!, latitude!!))
        } else {
            logger.warn("Unsupported geometry type : $geoPropertyType")
            0
        }
    }

    fun exists(entityId: String): Boolean = entityRepository.existsById(entityId)

    /**
     * @return a pair consisting of a map representing the entity keys and attributes and the list of contexts
     * associated to the entity
     */
    fun getFullEntityById(entityId: String): Pair<Map<String, Any>, List<String>> {
        val entity = entityRepository.getEntityCoreById(entityId)[0]["entity"] as Entity
        val resultEntity = entity.serializeCoreProperties()

        // TODO test with a property having more than one relationship (https://redmine.eglobalmark.com/issues/848)
        entityRepository.getEntitySpecificProperties(entityId)
            .groupBy {
                (it["property"] as Property).id
            }
            .values
            .forEach {
                val property = it[0]["property"]!! as Property
                val propertyKey = property.name
                val propertyValues = property.serializeCoreProperties()

                it.filter { relEntry -> relEntry["propValue"] != null }.forEach {
                    val propertyOfProperty = it["propValue"] as Property
                    propertyValues[propertyOfProperty.name] = propertyOfProperty.serializeCoreProperties()
                }

                it.filter { relEntry -> relEntry["relOfProp"] != null }.forEach {
                    val relationship = it["relOfProp"] as Relationship
                    val targetEntity = it["relOfPropObject"] as Entity
                    val relationshipKey = (it["relType"] as String).toNgsiLdRelationshipKey()
                    logger.debug("Adding relOfProp to ${targetEntity.id} with type $relationshipKey")

                    val relationshipValue = mapOf(
                        NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to targetEntity.id)
                    )
                    val relationshipValues = relationship.serializeCoreProperties()
                    relationshipValues.putAll(relationshipValue)
                    val expandedRelationshipKey = expandRelationshipType(mapOf(relationshipKey to relationshipValue), entity.contexts)
                    propertyValues[expandedRelationshipKey] = relationshipValues
                }
                resultEntity[propertyKey] = propertyValues
            }

        entityRepository.getEntityRelationships(entityId)
            .groupBy {
                (it["rel"] as Relationship).id
            }.values
            .forEach {
                val relationship = it[0]["rel"] as Relationship
                val primaryRelType = (it[0]["rel"] as Relationship).type[0]
                val primaryRelation =
                    it.find { relEntry -> relEntry["relType"] == primaryRelType.toRelationshipTypeName() }!!
                val relationshipTargetId = (primaryRelation["relObject"] as Entity).id
                val relationshipValue = mapOf(
                    NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to relationshipTargetId)
                )

                val relationshipValues = relationship.serializeCoreProperties()
                relationshipValues.putAll(relationshipValue)

                it.filter { relEntry -> relEntry["relOfRel"] != null }.forEach {
                    val relationship = it["relOfRel"] as Relationship
                    val innerRelType = (it["relOfRelType"] as String).toNgsiLdRelationshipKey()
                    val innerTargetEntityId = (it["relOfRelObject"] as Entity).id

                    val innerRelationship = mapOf(
                        NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to innerTargetEntityId)
                    )

                    val innerRelationshipValues = relationship.serializeCoreProperties()
                    innerRelationshipValues.putAll(innerRelationship)
                    val expandedInnerRelationshipType =
                        expandRelationshipType(mapOf(innerRelType to relationshipValue), entity.contexts)

                    relationshipValues[expandedInnerRelationshipType] = innerRelationshipValues
                }

                resultEntity[primaryRelType] = relationshipValues
            }

        return Pair(resultEntity, entity.contexts)
    }

    fun searchEntities(type: String, query: List<String>, contextLink: String): List<Pair<Map<String, Any>, List<String>>> =
        searchEntities(type, query, listOf(contextLink))

    /**
     * Search entities by type and query parameters
     *
     * @param type the short-hand type (e.g "Measure")
     * @param query the list of raw query parameters (e.g "name==test")
     * @param contexts the list of contexts to consider
     *
     * @return a list of entities represented as per #getFullEntityById result
     */
    fun searchEntities(type: String, query: List<String>, contexts: List<String>): List<Pair<Map<String, Any>, List<String>>> {
        val expandedType = expandJsonLdKey(type, contexts)!!

        val queryCriteria = query
            .map {
                val splitted = extractComparaisonParametersFromQuery(it)
                val expandedParam =
                    if (splitted[2].startsWith("urn:"))
                        splitted[0].toRelationshipTypeName()
                    else
                        expandJsonLdKey(splitted[0], contexts)!!
                Triple(expandedParam, splitted[1], splitted[2])
            }
            .partition {
                it.third.startsWith("urn:")
            }
        return neo4jRepository.getEntitiesByTypeAndQuery(expandedType, queryCriteria)
            .map { getFullEntityById(it) }
    }

    fun appendEntityAttributes(entityId: String, attributes: Map<String, Any>, disallowOverwrite: Boolean, isInBatchContext: Boolean = false, validEntities: Map<String, String> = mutableMapOf()): UpdateResult {
        val updateStatuses = attributes.map {
            val attributeValue = expandValueAsMap(it.value)
            if (!attributeValue.containsKey("@type"))
                throw BadRequestDataException("@type not found in $attributeValue")
            val attributeType = attributeValue["@type"]!![0]
            logger.debug("Fragment is of type $attributeType")
            if (attributeType == NGSILD_RELATIONSHIP_TYPE.uri) {
                val relationshipTypeName = it.key.extractShortTypeFromExpanded()
                if (!neo4jRepository.hasRelationshipOfType(entityId, relationshipTypeName.toRelationshipTypeName())) {
                    val objectId = getRelationshipObjectId(attributeValue)
                    val objectEntity = entityRepository.findById(objectId)
                    if (!objectEntity.isPresent && isInBatchContext) {
                        if (!validEntities.containsKey(objectId)) {
                            throw BadRequestDataException("Target entity $objectId in relationship $relationshipTypeName does not exist, create it first")
                        } else
                            createEntityRelationship(entityRepository.findById(entityId).get(), it.key, attributeValue, createTempEntityInBatch(objectId, validEntities.getValue(objectId)).id)
                    } else
                        createEntityRelationship(entityRepository.findById(entityId).get(), it.key, attributeValue, getRelationshipObjectId(attributeValue))
                    Triple(it.key, true, null)
                } else if (disallowOverwrite) {
                    logger.info("Relationship $relationshipTypeName already exists on $entityId and overwrite is not allowed, ignoring")
                    Triple(it.key, false, "Relationship $relationshipTypeName already exists on $entityId and overwrite is not allowed, ignoring")
                } else {
                    neo4jRepository.deleteRelationshipFromEntity(entityId, relationshipTypeName.toRelationshipTypeName())
                    val objectId = getRelationshipObjectId(attributeValue)
                    val objectEntity = entityRepository.findById(objectId)
                    if (!objectEntity.isPresent && isInBatchContext) {
                        if (!validEntities.containsKey(objectId)) {
                            throw BadRequestDataException("Target entity $objectId in relationship $relationshipTypeName does not exist, create it first")
                        } else
                            createEntityRelationship(entityRepository.findById(entityId).get(), it.key, attributeValue, createTempEntityInBatch(objectId, validEntities.getValue(objectId)).id)
                    } else
                        createEntityRelationship(
                            entityRepository.findById(entityId).get(), it.key, attributeValue,
                            getRelationshipObjectId(attributeValue))
                    Triple(it.key, true, null)
                }
            } else if (attributeType == NGSILD_PROPERTY_TYPE.uri) {
                if (!neo4jRepository.hasPropertyOfName(entityId, it.key)) {
                    createEntityProperty(entityRepository.findById(entityId).get(), it.key, attributeValue, isInBatchContext, validEntities)
                    Triple(it.key, true, null)
                } else if (disallowOverwrite) {
                    logger.info("Property ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                    Triple(it.key, false, "Property ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                } else {
                    neo4jRepository.deletePropertyFromEntity(entityId, it.key)
                    createEntityProperty(entityRepository.findById(entityId).get(), it.key, attributeValue, isInBatchContext, validEntities)
                    Triple(it.key, true, null)
                }
            } else if (attributeType == NGSILD_GEOPROPERTY_TYPE.uri) {
                if (!neo4jRepository.hasGeoPropertyOfName(entityId, it.key.extractShortTypeFromExpanded())) {
                    createLocationProperty(entityRepository.findById(entityId).get(), it.key, attributeValue)
                    Triple(it.key, true, null)
                } else if (disallowOverwrite) {
                    logger.info("GeoProperty ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                    Triple(it.key, false, "GeoProperty ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                } else {
                    updateLocationPropertyOfEntity(entityRepository.findById(entityId).get(), it.key, attributeValue)
                    Triple(it.key, true, null)
                }
            } else {
                Triple(it.key, false, "Unknown attribute type $attributeType")
            }
        }
        .toList()

        val updated = updateStatuses.filter { it.second }.map { it.first }
        val notUpdated = updateStatuses.filter { !it.second }.map { NotUpdatedDetails(it.first, it.third!!) }

        return UpdateResult(updated, notUpdated)
    }

    fun updateEntityAttribute(id: String, attribute: String, payload: String, contextLink: String): Int {
        val expandedAttributeName = expandJsonLdKey(attribute, contextLink)!!
        val attributeValue = parseJsonLdFragment(payload)["value"]!!
        return neo4jRepository.updateEntityAttribute(id, expandedAttributeName, attributeValue)
    }

    fun updateEntityAttributes(id: String, payload: String, contextLink: String): UpdateResult {
        val updatedAttributes = mutableListOf<String>()
        val notUpdatedAttributes = mutableListOf<NotUpdatedDetails>()
        val entity = entityRepository.findById(id).get()

        val expandedPayload = expandJsonLdFragment(payload, contextLink)
        logger.debug("Expanded entity fragment to $expandedPayload (in $contextLink context)")
        expandedPayload.forEach {
            val shortAttributeName = it.key.extractShortTypeFromExpanded()
            try {
                val attributeValue = expandValueAsMap(it.value)
                val attributeType = attributeValue["@type"]!![0]
                logger.debug("Trying to update attribute $shortAttributeName of type $attributeType")
                if (attributeType == NGSILD_RELATIONSHIP_TYPE.uri) {
                    if (neo4jRepository.hasRelationshipOfType(id, it.key.toRelationshipTypeName())) {
                        updateRelationshipOfEntity(entity, it.key, attributeValue, getRelationshipObjectId(expandValueAsMap(it.value)))
                        updatedAttributes.add(shortAttributeName)
                    } else
                        notUpdatedAttributes.add(NotUpdatedDetails(shortAttributeName, "Relationship does not exist"))
                } else if (attributeType == NGSILD_PROPERTY_TYPE.uri) {
                    if (neo4jRepository.hasPropertyOfName(id, it.key)) {
                        updatePropertyOfEntity(entity, it.key, attributeValue)
                        updatedAttributes.add(shortAttributeName)
                    } else
                        notUpdatedAttributes.add(NotUpdatedDetails(shortAttributeName, "Property does not exist"))
                } else if (attributeType == NGSILD_GEOPROPERTY_TYPE.uri) {
                    if (neo4jRepository.hasGeoPropertyOfName(id, shortAttributeName)) {
                        updateLocationPropertyOfEntity(entity, it.key, attributeValue)
                        updatedAttributes.add(shortAttributeName)
                    } else
                        notUpdatedAttributes.add(NotUpdatedDetails(shortAttributeName, "GeoProperty does not exist"))
                }
            } catch (e: BadRequestDataException) {
                notUpdatedAttributes.add(NotUpdatedDetails(shortAttributeName,
                    e.message ?: "Unexpected error while updating property $shortAttributeName"))
            }
        }
        return UpdateResult(updatedAttributes, notUpdatedAttributes)
    }

    private fun updatePropertyOfEntity(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>): Property {
        val property = updatePropertyValues(entity.id, propertyKey, propertyValues)
        updatePropertiesOfAttribute(property, propertyValues)
        updateRelationshipsOfAttribute(property, propertyValues)

        return property
    }

    private fun updatePropertyValues(subjectId: String, propertyKey: String, propertyValues: Map<String, List<Any>>): Property {
        val unitCode = getPropertyValueFromMapAsString(propertyValues, NGSILD_UNIT_CODE_PROPERTY)
        val value = getPropertyValueFromMap(propertyValues, NGSILD_PROPERTY_VALUE) ?: propertyValues["@value"]!!
        val observedAt = getPropertyValueFromMapAsDateTime(propertyValues, NGSILD_OBSERVED_AT_PROPERTY)

        val property = neo4jRepository.getPropertyOfSubject(subjectId, propertyKey)
        property.updateValues(unitCode, value, observedAt)

        return propertyRepository.save(property)
    }

    private fun updateRelationshipOfEntity(
        entity: Entity,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>,
        targetEntityId: String
    ): Relationship {
        val relationship = updateRelationshipValues(entity.id, relationshipType, relationshipValues, targetEntityId)
        updatePropertiesOfAttribute(relationship, relationshipValues)
        updateRelationshipsOfAttribute(relationship, relationshipValues)

        return relationship
    }

    private fun updateRelationshipValues(
        subjectId: String,
        relationshipType: String,
        relationshipValues: Map<String, List<Any>>,
        targetEntityId: String
    ): Relationship {

        if (!entityRepository.findById(targetEntityId).isPresent)
            throw BadRequestDataException("Target entity $targetEntityId does not exist, create it first")

        val relationship = neo4jRepository.getRelationshipOfSubject(subjectId, relationshipType.toRelationshipTypeName())
        val oldRelationshipTarget = neo4jRepository.getRelationshipTargetOfSubject(subjectId, relationshipType.toRelationshipTypeName())
        relationship.observedAt = getPropertyValueFromMapAsDateTime(relationshipValues, NGSILD_OBSERVED_AT_PROPERTY)
        neo4jRepository.updateRelationshipTargetOfAttribute(relationship.id, relationshipType.toRelationshipTypeName(),
            oldRelationshipTarget.id, targetEntityId)

        return relationshipRepository.save(relationship)
    }

    private fun updatePropertiesOfAttribute(subject: Attribute, values: Map<String, List<Any>>) {
        values.filterValues {
            it[0] is Map<*, *>
        }
        .mapValues {
            expandValueAsMap(it.value)
        }
        .filter {
            !(NGSILD_PROPERTIES_CORE_MEMBERS.plus(NGSILD_RELATIONSHIPS_CORE_MEMBERS)).contains(it.key) &&
                    isAttributeOfType(it.value, NGSILD_PROPERTY_TYPE)
        }
        .forEach { entry ->
            if (!neo4jRepository.hasPropertyOfName(subject.id, entry.key))
                throw BadRequestDataException("Property ${entry.key.extractShortTypeFromExpanded()} does not exist")
            updatePropertyValues(subject.id, entry.key, entry.value)
        }
    }

    private fun updateRelationshipsOfAttribute(subject: Attribute, values: Map<String, List<Any>>) {
        values.forEach { propEntry ->
            val propEntryValue = propEntry.value[0]
            logger.debug("Looking at prop entry ${propEntry.key} with value $propEntryValue")
            if (propEntryValue is Map<*, *>) {
                val propEntryValueMap = propEntryValue as Map<String, List<Any>>
                if (isAttributeOfType(propEntryValueMap, NGSILD_RELATIONSHIP_TYPE)) {
                    if (!neo4jRepository.hasRelationshipOfType(subject.id, propEntry.key.toRelationshipTypeName()))
                        throw BadRequestDataException("Relationship ${propEntry.key.toRelationshipTypeName()} does not exist")
                    val objectId = getRelationshipObjectId(propEntryValueMap)
                    updateRelationshipValues(subject.id, propEntry.key, propEntryValueMap, objectId)
                }
            } else {
                logger.debug("Ignoring ${propEntry.key} entry as it can't be a relationship ($propEntryValue)")
            }
        }
    }

    internal fun updateLocationPropertyOfEntity(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>) {
        logger.debug("Geo property $propertyKey has values $propertyValues")
        val geoPropertyValue = expandValueAsMap(propertyValues[NGSILD_GEOPROPERTY_VALUE]!!)
        val geoPropertyType = geoPropertyValue["@type"]!![0] as String
        // TODO : point is not part of the NGSI-LD core context (https://redmine.eglobalmark.com/issues/869)
        if (geoPropertyType == "https://uri.etsi.org/ngsi-ld/default-context/Point") {
            val geoPropertyCoordinates = geoPropertyValue[NGSILD_COORDINATES_PROPERTY]!!
            val longitude = (geoPropertyCoordinates[0] as Map<String, Double>)["@value"]
            val latitude = (geoPropertyCoordinates[1] as Map<String, Double>)["@value"]
            logger.debug("Point has coordinates $latitude, $longitude")

            neo4jRepository.updateLocationPropertyOfEntity(entity.id, Pair(longitude!!, latitude!!))
        } else {
            throw BadRequestDataException("Unsupported geometry type : $geoPropertyType")
        }
    }

    fun deleteEntity(entityId: String): Pair<Int, Int> {
        return neo4jRepository.deleteEntity(entityId)
    }

    fun processBatchOfEntities(existingEntities: List<Pair<Map<String, Any>, List<String>>>, newEntities: List<Pair<Map<String, Any>, List<String>>>, validEntities: Map<String, String>): BatchOperationResult {
        val batchCreationResult = createBatchOfEntities(newEntities, validEntities)
        existingEntities.forEach {
            val urn = it.first.getOrElse("@id") { "" } as String
            batchCreationResult.errors.add(BatchEntityError(urn, arrayListOf("Entity already exists")))
        }
        return BatchOperationResult(batchCreationResult.success, batchCreationResult.errors)
    }

    fun createBatchOfEntities(entities: List<Pair<Map<String, Any>, List<String>>>, validEntities: Map<String, String>): BatchOperationResult {
        val createdEntities: ArrayList<String> = ArrayList()
        val failedEntities: ArrayList<BatchEntityError> = ArrayList()

        entities.forEach {
            val urn = it.first.getOrElse("@id") { "" } as String
            val type = extractTypeFromPayload(it.first)
            val entity = createTempEntityInBatch(urn, type, it.second)
            val propertiesAndRelationshipsMap = it.first.filterKeys {
                !listOf(NGSILD_ENTITY_ID, NGSILD_ENTITY_TYPE).contains(it)
            }
            try {
                appendEntityAttributes(entity.id, propertiesAndRelationshipsMap, false, true, validEntities)
                createdEntities.add(urn)
            } catch (e: BadRequestDataException) {
                deleteEntity(urn)
                failedEntities.add(BatchEntityError(urn, arrayListOf(e.message ?: "Unexpected error while creating entity $urn")))
            }
        }
        return BatchOperationResult(createdEntities, failedEntities)
    }

    fun createTempEntityInBatch(entityId: String, entityType: String, contexts: List<String> = listOf()): Entity {
        val entity = Entity(id = entityId, type = listOf(entityType), contexts = contexts)
        entityRepository.save(entity)
        return entity
    }

    fun updateEntityLastMeasure(observation: Observation) {
        val observingEntity = neo4jRepository.getObservingSensorEntity(observation.observedBy, EGM_VENDOR_ID, observation.attributeName)
        if (observingEntity == null) {
            logger.warn("Unable to find observing entity ${observation.observedBy}")
            return
        }
        // Find the previous observation of the same unit for the given sensor, then create or update it
        val observedByRelationshipType = EGM_OBSERVED_BY.extractShortTypeFromExpanded().toRelationshipTypeName()
        val observedProperty = neo4jRepository.getObservedProperty(observingEntity.id, observedByRelationshipType)
        if (observedProperty == null || observedProperty.name.extractShortTypeFromExpanded() != observation.attributeName) {
            logger.warn("Found no property named ${observation.attributeName} observed by ${observation.observedBy}, ignoring it")
        } else {
            observedProperty.updateValues(observation.unitCode, observation.value, observation.observedAt)
            if (observation.latitude != null && observation.longitude != null) {
                val observedEntity = neo4jRepository.getEntityByProperty(observedProperty)
                observedEntity.location = GeographicPoint2d(observation.latitude, observation.longitude)
                entityRepository.save(observedEntity)
            }
            propertyRepository.save(observedProperty)
        }
    }
}
