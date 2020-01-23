package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.*
import com.egm.datahub.context.registry.repository.*
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.EGM_OBSERVED_BY
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
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMap
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMapAsDateTime
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getPropertyValueFromMapAsString
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.getRelationshipObjectId
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.isAttributeOfType
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.parseJsonLdFragment
import com.egm.datahub.context.registry.util.extractComparaisonParametersFromQuery
import com.egm.datahub.context.registry.util.extractShortTypeFromExpanded
import com.egm.datahub.context.registry.util.toNgsiLdRelationshipKey
import com.egm.datahub.context.registry.util.toRelationshipTypeName
import com.egm.datahub.context.registry.web.BadRequestDataException
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

            if (!objectEntity.isPresent)
                throw BadRequestDataException("Target entity $objectId in relationship $relationshipType does not exist, create it first")

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

    internal fun createEntityProperty(entity: Entity, propertyKey: String, propertyValues: Map<String, List<Any>>): Property {
        val propertyValue = getPropertyValueFromMap(propertyValues, NGSILD_PROPERTY_VALUE)!!

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
        createAttributeRelationships(property, propertyValues)

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

    private fun createAttributeRelationships(subject: Attribute, values: Map<String, List<Any>>) {
        values.forEach { propEntry ->
            val propEntryValue = propEntry.value[0]
            logger.debug("Looking at prop entry ${propEntry.key} with value $propEntryValue")
            if (propEntryValue is Map<*, *>) {
                val propEntryValueMap = propEntryValue as Map<String, List<Any>>
                if (isAttributeOfType(propEntryValueMap, NGSILD_RELATIONSHIP_TYPE)) {
                    val objectId = getRelationshipObjectId(propEntryValueMap)
                    val objectEntity = entityRepository.findById(objectId)
                    if (!objectEntity.isPresent)
                        throw BadRequestDataException("Target entity $objectId in property $subject does not exist, create it first")

                    val rawRelationship = Relationship(type = listOf(propEntry.key),
                        observedAt = getPropertyValueFromMapAsDateTime(propEntryValueMap, NGSILD_OBSERVED_AT_PROPERTY))
                    val relationship = relationshipRepository.save(rawRelationship)
                    subject.relationships.add(relationship)
                    attributeRepository.save(subject)

                    neo4jRepository.createRelationshipToEntity(relationship.id, propEntry.key.toRelationshipTypeName(), objectEntity.get().id)
                }
            } else {
                logger.debug("Ignoring ${propEntry.key} entry as it can't be a relationship ($propEntryValue)")
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
                    val targetEntity = it["relOfPropObject"] as Entity
                    val relationshipKey = (it["relType"] as String).toNgsiLdRelationshipKey()
                    logger.debug("Adding relOfProp to ${targetEntity.id} with type $relationshipKey")

                    val relationshipValue = mapOf(
                        NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to targetEntity.id)
                    )
                    val relationship = mapOf(relationshipKey to relationshipValue)
                    val expandedRelationshipKey = expandRelationshipType(relationship, entity.contexts)
                    propertyValues[expandedRelationshipKey] = relationshipValue
                }
                resultEntity[propertyKey] = propertyValues
            }

        entityRepository.getEntityRelationships(entityId)
            .groupBy {
                (it["rel"] as Relationship).id
            }.values
            .forEach {
                val primaryRelType = (it[0]["rel"] as Relationship).type[0]
                val primaryRelation =
                    it.find { relEntry -> relEntry["relType"] == primaryRelType.toRelationshipTypeName() }!!
                val relationshipTargetId = (primaryRelation["relObject"] as Entity).id
                val relationship = mapOf(
                    NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                    NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to relationshipTargetId)
                )

                val relationshipValues = mutableMapOf<String, Any>()
                relationshipValues.putAll(relationship)
                val expandedRelationshipType =
                    expandRelationshipType(
                        mapOf(primaryRelType.extractShortTypeFromExpanded().toNgsiLdRelationshipKey() to relationship),
                        entity.contexts
                    )

                it.filter { relEntry -> relEntry["relOfRel"] != null }.forEach {
                    val innerRelType = (it["relOfRelType"] as String).toNgsiLdRelationshipKey()
                    val innerTargetEntityId = (it["relOfRelObject"] as Entity).id

                    val innerRelationship = mapOf(
                        NGSILD_ENTITY_TYPE to NGSILD_RELATIONSHIP_TYPE.uri,
                        NGSILD_RELATIONSHIP_HAS_OBJECT to mapOf(NGSILD_ENTITY_ID to innerTargetEntityId)
                    )

                    val innerRelationshipValues = mutableMapOf<String, Any>()
                    innerRelationshipValues.putAll(innerRelationship)
                    val expandedInnerRelationshipType =
                        expandRelationshipType(mapOf(innerRelType to relationship), entity.contexts)

                    relationshipValues[expandedInnerRelationshipType] = innerRelationship
                }

                resultEntity[expandedRelationshipType] = relationshipValues
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

    fun appendEntityAttributes(entityId: String, attributes: Map<String, Any>, disallowOverwrite: Boolean): UpdateResult {
        val updateStatuses = attributes.map {
            val attributeValue = expandValueAsMap(it.value)
            val attributeType = attributeValue["@type"]!![0]
            logger.debug("Fragment is of type $attributeType")
            if (attributeType == NGSILD_RELATIONSHIP_TYPE.uri) {
                val relationshipTypeName = it.key.extractShortTypeFromExpanded()
                if (!neo4jRepository.hasRelationshipOfType(entityId, relationshipTypeName.toRelationshipTypeName())) {
                    createEntityRelationship(
                        entityRepository.findById(entityId).get(), it.key, attributeValue,
                        getRelationshipObjectId(attributeValue))
                    Triple(it.key, true, null)
                } else if (disallowOverwrite) {
                    logger.info("Relationship $relationshipTypeName already exists on $entityId and overwrite is not allowed, ignoring")
                    Triple(it.key, false, "Relationship $relationshipTypeName already exists on $entityId and overwrite is not allowed, ignoring")
                } else {
                    neo4jRepository.deleteRelationshipFromEntity(entityId, relationshipTypeName.toRelationshipTypeName())
                    createEntityRelationship(
                        entityRepository.findById(entityId).get(), it.key, attributeValue,
                        getRelationshipObjectId(attributeValue))
                    Triple(it.key, true, null)
                }
            } else if (attributeType == NGSILD_PROPERTY_TYPE.uri) {
                if (!neo4jRepository.hasPropertyOfName(entityId, it.key)) {
                    createEntityProperty(entityRepository.findById(entityId).get(), it.key, attributeValue)
                    Triple(it.key, true, null)
                } else if (disallowOverwrite) {
                    logger.info("Property ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                    Triple(it.key, false, "Property ${it.key} already exists on $entityId and overwrite is not allowed, ignoring")
                } else {
                    neo4jRepository.deletePropertyFromEntity(entityId, it.key)
                    createEntityProperty(entityRepository.findById(entityId).get(), it.key, attributeValue)
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

    fun updateEntityAttributes(id: String, payload: String, contextLink: String): List<Int> {
        val expandedPayload = expandJsonLdFragment(payload, contextLink)
        logger.debug("Expanded entity fragment to $expandedPayload (in $contextLink context)")
        return expandedPayload.map {
            // TODO check existence before eventually replacing (https://redmine.eglobalmark.com/issues/849)
            val attributeValue = expandValueAsMap(it.value)
            val attributeType = attributeValue["@type"]!![0]
            logger.debug("Fragment is of type $attributeType")
            if (attributeType == NGSILD_RELATIONSHIP_TYPE.uri) {
                val relationshipTypeName = it.key.extractShortTypeFromExpanded()
                neo4jRepository.deleteRelationshipFromEntity(id, relationshipTypeName.toRelationshipTypeName())
                createEntityRelationship(
                    entityRepository.findById(id).get(), it.key, attributeValue,
                    getRelationshipObjectId(expandValueAsMap(it.value)))
            } else if (attributeType == NGSILD_PROPERTY_TYPE.uri) {
                neo4jRepository.deletePropertyFromEntity(id, it.key)
                createEntityProperty(entityRepository.findById(id).get(), it.key, attributeValue)
            }
            1
        }.toList()
    }

    fun deleteEntity(entityId: String): Pair<Int, Int> {
        return neo4jRepository.deleteEntity(entityId)
    }

    fun updateEntityLastMeasure(observation: Observation) {
        val observingEntity = entityRepository.findById(observation.observedBy.target)
        if (observingEntity.isEmpty) {
            logger.warn("Unable to find observing entity ${observation.observedBy.target} for observation ${observation.id}")
            return
        }

        // Find the previous observation of the same unit for the given sensor, then create or update it
        val observedByRelationshipType = EGM_OBSERVED_BY.extractShortTypeFromExpanded().toRelationshipTypeName()
        val observedProperty = neo4jRepository.getObservedProperty(observingEntity.get().id, observedByRelationshipType)

        if (observedProperty == null) {
            logger.warn("Found no property observed by ${observation.observedBy.target}, ignoring it")
        } else {
            val observedEntity = neo4jRepository.getEntityByProperty(observedProperty)

            observedProperty.value = observation.value
            observedProperty.observedAt = observation.observedAt
            observation.location?.let {
                observedEntity.location = GeographicPoint2d(observation.location.value.coordinates[0], observation.location.value.coordinates[1])
                entityRepository.save(observedEntity)
            }
            propertyRepository.save(observedProperty)
        }
    }
}
