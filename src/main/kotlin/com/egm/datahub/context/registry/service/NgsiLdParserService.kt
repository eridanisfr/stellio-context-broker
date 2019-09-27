package com.egm.datahub.context.registry.service

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.neo4j.ogm.response.model.NodeModel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern

typealias EntityStatements = List<String>
typealias RelationshipStatements = List<String>

@Component
class NgsiLdParserService {

    class Entity(var label: String, initialAttrs: Map<String, Any>, var ns: String?) {

        var attrs: MutableMap<String, Any> = initialAttrs.toMutableMap()

        fun getLabelWithPrefix(): String {
            return this.ns + "__" + this.label
        }

        fun getUri(): String {
            return this.attrs["uri"].toString()
        }
    }

    private val gson = GsonBuilder().setPrettyPrinting().create()
    private val logger = LoggerFactory.getLogger(NgsiLdParserService::class.java)

    companion object {
        val namespacesMapping: Map<String, List<String>> = mapOf(
            "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "Sensor", "Observation", "ObservedBy", "ManagedBy", "hasMeasure"),
            "ngsild" to listOf("connectsTo", "hasObject", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship", "name"),
            "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle", "isParked", "providedBy", "Camera") // this is property of property in order to allow nested property we need to add it to model
        )

        val contextsMap = mapOf(
            "@context" to listOf(
                "https://diatomic.eglobalmark.com/diatomic-context.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld"
            )
        )
    }

    fun parseEntity(ngsiLdPayload: String): Pair<String, Pair<EntityStatements, RelationshipStatements>> {
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        val entityUrn = entityMap["id"] as String

        val statements = transformNgsiLdToCypher(
            entityMap,
            null,
            null,
            mutableListOf(),
            mutableListOf()
        )

        return Pair(entityUrn, statements)
    }

    fun transformNgsiLdToCypher(
        node: Map<String, Any>,
        uuid: String?,
        parentAttribute: String?,
        accEntityStatements: MutableList<String>,
        accRelationshipStatements: MutableList<String>
    ): Pair<EntityStatements, RelationshipStatements> {

        logger.info("Traversing node $node")

        val parentIsRelationship: Boolean = node["type"]?.toString().equals("Relationship")

        val nodeType = node["type"].toString()
        val nodeUuid = node.getOrDefault("id", uuid).toString()
        val nsSubj = getLabelNamespace(nodeType)
        val nodeEntity = Entity(
            nodeType,
            getAttributes(node).plus("uri" to nodeUuid),
            nsSubj
        )

        // if is Property override the generic Property type with the attribute
        if (node["type"].toString() == "Property") {
            parentAttribute?.let {
                nodeEntity.ns = getLabelNamespace(it)
                nodeEntity.label = parentAttribute
            }
        }

        for (item in node) {
            // foreach attribute get the Map and check type is Property
            val content = expandObjToMap(item.value)
            if (isAttribute(content)) {
                logger.debug(item.key + " is attribute")
                // add to attr map
            }
            if (isRelationship(content)) {
                logger.debug(item.key + " is relationship")
                // THIS IS THE NODE --> REL --> NODE (object)
                val rel = item.key
                val nsPredicate = getLabelNamespace(rel)
                val predicate = nsPredicate + "__" + rel

                // a Relationship witemhout a object? not possible skip!
                if (content.get("object") == null)
                    continue

                content.get("object").let {
                    val urn: String = it.toString()
                    val typeObj = urn.split(":")[2]
                    val nsObj = getLabelNamespace(typeObj)

                    if (parentIsRelationship) {
                        parentAttribute?.let {
                            // nodeEntity.ns = getLabelNamespace(parentAttribute)
                            val newStatements = buildInsert(
                                nodeEntity,
                                predicate,
                                Entity(typeObj, hashMapOf("uri" to urn), nsObj)
                            )
                            if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                            if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                        }
                    } else {
                        val ns = getLabelNamespace(typeObj)
                        val newStatements = buildInsert(
                            nodeEntity,
                            predicate,
                            Entity(typeObj, hashMapOf("uri" to urn), ns)
                        )
                        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                    }

                    // DowntownParking can exist or not
                    val newStatements = buildInsert(
                        Entity(
                            rel,
                            hashMapOf("uri" to urn),
                            nsPredicate
                        ), null, null
                    )
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

                    // create random uri for mat rel
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()
                    // add materialized relationship NODE
                    val urnMatRel = "urn:$nsPredicate:$rel:$str"
                    if (hasAttributes(content)) {
                        // go deeper using the materialized rel Node
                        transformNgsiLdToCypher(
                            content,
                            urnMatRel,
                            item.key,
                            accEntityStatements,
                            accRelationshipStatements
                        )
                    }
                }
            }
            if (isProperty(content)) {
                logger.debug(item.key + " is property")

                // is not a map or the only attributes are type and value
                if (hasAttributes(content)) {
                    // has attributes or just value and type? if so store as attribute  (es. name and available spot number in vehicle)
                    logger.debug("this property has just type and value, it is already in node entity")
                } else {
                    // this property has one ore more nested objects ==> use the attr. key (es. availableSpotNumber) as object to create a Relationship between entity and Property
                    // MATERIALIZED PROPERTY
                    val labelObj = item.key
                    val nsObj = getLabelNamespace(labelObj)

                    // create uri for object
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()

                    val urn = "urn:$nsObj:$labelObj:$str"
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"

                    // object attributes will be set in the next travestPropertiesIteration with a match on URI
                    // ADD THE RELATIONSHIP
                    val newStatements = buildInsert(
                        nodeEntity,
                        predicate,
                        Entity(labelObj, hashMapOf("uri" to urn), nsObj)
                    )
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

                    // go deeper
                    transformNgsiLdToCypher(
                        content,
                        urn,
                        item.key,
                        accEntityStatements,
                        accRelationshipStatements
                    )
                }
            }
        }

        val newStatements = buildInsert(nodeEntity, null, null)
        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

        return Pair(accEntityStatements, accRelationshipStatements)
    }

    private fun isProperty(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("Property")
    }

    private fun isGeoProperty(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("GeoProperty")
    }

    private fun isRelationship(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("Relationship")
    }

    private fun isAttribute(prop: Map<String, Any>): Boolean {
        val type = prop["type"]?.toString()
        if (type.equals("Property") || type.equals("Relationship") || type.equals("GeoProperty")) return false
        if (prop.isEmpty()) return true
        return hasAttributes(prop)
    }

    private fun getAttributes(node: Map<String, Any>): Map<String, Any> {

        return node.filterKeys { key ->
            !listOf("type", "@context", "id").contains(key)
        }.filter {
            hasAttributes(expandObjToMap(it.value))
        }.mapValues {
            if (it.value is String) {
                it.value
            } else if (isGeoProperty(expandObjToMap(it.value))) {
                val value = expandObjToMap(it.value)["value"] as Map<String, Any>
                val coordinates = value["coordinates"] as List<Double>
                val lon = coordinates[0]
                val lat = coordinates[1]
                "point({ x: $lon , y: $lat, crs: 'WGS-84' })"
            } else if (expandObjToMap(it.value).containsKey("value")) {
                val innerValue = expandObjToMap(it.value)["value"]
                innerValue!!
            } else {
                ""
            }
        }
    }

    private fun formatAttributes(attributes: Map<String, Any>): String {
        var attrs = gson.toJson(attributes)
        val p = Pattern.compile("\\\"(\\w+)\\\"\\:")
        attrs = p.matcher(attrs).replaceAll("$1:")
        attrs = attrs.replace("\n", "")
        return attrs
    }

    private fun buildInsert(subject: Entity, predicate: String?, obj: Entity?): Pair<EntityStatements, RelationshipStatements> {
        return if (predicate == null || obj == null) {
            val labelSubject = subject.getLabelWithPrefix()
            val uri = subject.getUri()
            val attrsUriSubj = formatAttributes(mapOf("uri" to uri))
            val timeStamp = SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(Date())
            if (!subject.attrs.containsKey("createdAt")) {
                subject.attrs["createdAt"] = timeStamp
            }
            subject.attrs["modifiedAt"] = timeStamp
            val attrsSubj = formatAttributes(subject.attrs)
            val entityStatement = """
                MERGE (a : $labelSubject $attrsUriSubj) 
                ON CREATE SET a = $attrsSubj 
                ON MATCH  SET a += $attrsSubj 
                return a
            """.trimIndent()

            Pair(listOf(entityStatement), emptyList())
        } else {
            val labelObj = obj.getLabelWithPrefix()
            val labelSubject = subject.getLabelWithPrefix()
            val uriSubj = subject.getUri()
            val uriObj = obj.getUri()
            val attrsSubj = formatAttributes(mapOf("uri" to uriSubj))
            val attrsObj = formatAttributes(mapOf("uri" to uriObj))
            val relationshipStatement = """
                MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj ) 
                MERGE (a)-[r:$predicate]->(b) 
                return a,b
            """.trimIndent()
            Pair(emptyList(), listOf(relationshipStatement))
        }
    }

    fun ngsiLdToUpdateQuery(payload: String, uri: String, attr: String): Pair<String, String> {
        val payloadMap = expandObjToMap(payload)
        payloadMap[attr]?.let {
            val value = it.toString()
            val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).toString()
            val attrsUriMatch = formatAttributes(mapOf("uri" to uri))
            val attrsUriSubj = formatAttributes(mapOf("uri" to uri, "modifiedAt" to timestamp, attr to value))

            return Pair(uri, "MERGE (a $attrsUriMatch) ON  MATCH  SET a += $attrsUriSubj return a")
        }

        // TODO : validation error ?
        return Pair(uri, "")
    }

    private fun hasAttributes(node: Map<String, Any>): Boolean {
        // if a Property has just type and value we save it as attribute value in the parent entity
        return node.size == 1 || (node.size == 2 && node.containsKey("type") && node.containsKey("value"))
    }

    private fun expandObjToMap(obj: Any?): Map<String, Any> {
        return when (obj) {
            is Map<*, *> -> obj as Map<String, Any>
            is String -> mapOf(obj to obj)
            else -> emptyMap()
        }
    }

    private fun getLabelNamespace(label: String): String {
        namespacesMapping.forEach {
            if (it.value.contains(label)) {
                return it.key
            }
        }

        // fallback to default core NGSI-LD namespace
        // TODO : we should instead raise a 400-like exception
        return "ngsild"
    }

    /**
     * Sample (simple) query result :
     *
     *  {"n": {
     *      "id":57,
     *      "version":null,
     *      "labels":["diat__Door"],
     *      "primaryIndex":null,
     *      "previousDynamicLabels":[],
     *      "propertyList":[
     *          {"key":"createdAt","value":"2019.09.26.14.31.44"},
     *          {"key":"doorNumber","value":"15"},
     *          {"key":"modifiedAt","value":"2019.09.26.14.31.44"},
     *          {"key":"uri","value":"urn:diat:Door:0015"}
     *      ]
     *   }}
     */
    fun queryResultToNgsiLd(queryResult: Map<String, Any>): Map<String, Any> {
        val nodeModel = queryResult["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        logger.debug("Transforming node ${nodeModel.id} ($uriProperty)")
        val properties = nodeModel.propertyList
            .filter { it.key != "uri" }
            .map {
                it.key to mapOf(
                    "type" to "Property",
                    "value" to it.value
                )
            }

        return mapOf(
            "id" to uriProperty.value,
            "type" to nodeModel.labels[0]
        ).plus(properties).plus(contextsMap)
    }
}
