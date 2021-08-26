package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.toKeyValues
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_EGM_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_HAS_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.compact
import com.egm.stellio.shared.util.JsonLdUtils.containsCoreContext
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdKey
import com.egm.stellio.shared.util.JsonLdUtils.extractRelationshipObject
import com.egm.stellio.shared.util.JsonLdUtils.getAttributeFromExpandedFragment
import com.egm.stellio.shared.util.JsonLdUtils.reconstructPolygonCoordinates
import com.egm.stellio.shared.util.JsonLdUtils.serialize
import jakarta.json.Json
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.http.MediaType

class JsonLdUtilsTests {

    private val normalizedJson =
        """
        {
            "id": "urn:ngsi-ld:Vehicle:A4567",
            "type": "Vehicle",
            "brandName": {
                "type": "Property",
                "value": "Mercedes"
            },
            "isParked": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "observedAt": "2017-07-29T12:00:04Z",
                "providedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Person:Bob"
                    }
            },
           "location": {
              "type": "GeoProperty",
              "value": {
                 "type": "Point",
                 "coordinates": [
                    24.30623,
                    60.07966
                 ]
              }
           },
            "@context": [
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
            ]
        }
        """.trimIndent()

    @Test
    fun `it should render an entity as a keyValues object`() {
        val expectedKeyValuesEntity =
            """
            {
                "id": "urn:ngsi-ld:Vehicle:A4567",
                "type": "Vehicle",
                "brandName": "Mercedes",
                "isParked": "urn:ngsi-ld:OffStreetParking:Downtown1",
                "location": {
                 "type": "Point",
                 "coordinates": [
                    24.30623,
                    60.07966
                 ]
               },
                "@context": [
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
                ]
            }
            """.trimIndent()

        val keyValuesEntity = compact(expandJsonLdEntity(normalizedJson)).toKeyValues().serialize()

        assertTrue(expectedKeyValuesEntity.matchContent(keyValuesEntity))
    }

    @Test
    fun `it should filter a JSON-LD Map on the attributes specified as well as the mandatory attributes`() {
        val resultMap = JsonLdUtils.filterCompactedEntityOnAttributes(
            compact(expandJsonLdEntity(normalizedJson)),
            setOf("brandName", "location")
        )

        assertTrue(resultMap.containsKey("id"))
        assertTrue(resultMap.containsKey("type"))
        assertTrue(resultMap.containsKey("@context"))
        assertTrue(resultMap.containsKey("brandName"))
        assertTrue(resultMap.containsKey("location"))
        assertFalse(resultMap.containsKey("isParked"))
    }

    @Test
    fun `it should throw an InvalidRequest exception if the JSON-LD fragment is not a valid JSON document`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",,
                "type": "Device"
            }
            """.trimIndent()

        val exception = assertThrows<InvalidRequestException> {
            expandJsonLdFragment(rawEntity)
        }
        assertEquals(
            "Unexpected error while parsing payload : " +
                "Invalid token=COMMA at (line no=2, column no=38, offset=39). Expected tokens are: [STRING]",
            exception.message
        )
    }

    @Test
    fun `it should throw a LdContextNotAvailable exception if the provided JSON-LD context is not absolute`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": [
                    "unknownContext"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<LdContextNotAvailableException> {
            expandJsonLdFragment(rawEntity)
        }
        assertEquals(
            "Unable to load remote context (cause was: Context URI is not absolute [unknownContext].)",
            exception.message
        )
    }

    @Test
    fun `it should throw a LdContextNotAvailable exception if the provided JSON-LD context is not resolvable`() {
        val rawEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": [
                    "https://localhost/unknown.jsonld"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<LdContextNotAvailableException> {
            expandJsonLdFragment(rawEntity)
        }
        assertEquals(
            "Unable to load remote context (cause was: " +
                "There was a problem encountered loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED].)",
            exception.message
        )
    }

    @Test
    fun `it should throw a BadRequestData exception if the expanded JSON-LD fragment is empty`() {
        val rawEntity =
            """
            {
                "@context": [
                    "$NGSILD_CORE_CONTEXT"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdFragment(rawEntity)
        }
        assertEquals(
            "Unable to parse input payload",
            exception.message
        )
    }

    @Test
    fun `it should not find the core context if it is not included`() {
        assertFalse(containsCoreContext(listOf(NGSILD_EGM_CONTEXT)))
    }

    @Test
    fun `it should find the core context if it is included`() {
        assertTrue(containsCoreContext(DEFAULT_CONTEXTS))
    }

    @Test
    fun `it should return an error if a relationship has no object field`() {
        val relationshipValues =
            Json.createObjectBuilder().add("value", Json.createArrayBuilder().add("something")).build()

        val result = extractRelationshipObject("isARelationship", relationshipValues)

        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship does not have an object field", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an empty object`() {
        val relationshipValues =
            Json.createObjectBuilder().add(NGSILD_RELATIONSHIP_HAS_OBJECT, Json.createArrayBuilder()).build()

        val result = extractRelationshipObject("isARelationship", relationshipValues)

        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship is empty", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an invalid object type`() {
        val relationshipValues =
            Json.createObjectBuilder()
                .add(NGSILD_RELATIONSHIP_HAS_OBJECT, Json.createArrayBuilder().add("invalid"))
                .build()

        val result = extractRelationshipObject("isARelationship", relationshipValues)

        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals(
                "Relationship isARelationship has an invalid object type: class org.glassfish.json.JsonStringImpl",
                it.message
            )
        }
    }

    @Test
    fun `it should return an error if a relationship has object without id`() {
        val relationshipValues =
            Json.createObjectBuilder()
                .add(
                    NGSILD_RELATIONSHIP_HAS_OBJECT,
                    Json.createArrayBuilder()
                        .add(Json.createObjectBuilder().add(JSONLD_VALUE_KW, "urn:ngsi-ld:T:misplacedRelationshipObject"))
                )
                .build()

        val result = extractRelationshipObject("isARelationship", relationshipValues)

        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals(
                "Relationship isARelationship has an invalid or no object id: {\"@value\":\"urn:ngsi-ld:T:misplacedRelationshipObject\"}",
                it.message
            )
        }
    }

    @Test
    fun `it should extract the target object of a relationship`() {
        val relationshipValues =
            Json.createObjectBuilder()
                .add(
                    NGSILD_RELATIONSHIP_HAS_OBJECT,
                    Json.createArrayBuilder()
                        .add(Json.createObjectBuilder().add(JSONLD_ID, "urn:ngsi-ld:T:1"))
                )
                .build()

        val result = extractRelationshipObject("isARelationship", relationshipValues)

        assertTrue(result.isRight())
        result.map {
            assertEquals("urn:ngsi-ld:T:1".toUri(), it)
        }
    }

    @Test
    fun `it should compact and return a JSON entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity, listOf(NGSILD_CORE_CONTEXT))
        val compactedEntity = compact(jsonLdEntity, listOf(NGSILD_CORE_CONTEXT), MediaType.APPLICATION_JSON)

        assertTrue(compactedEntity.serialize().matchContent(entity))
    }

    @Test
    fun `it should compact and return a JSON-LD entity`() {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            {
                "id":"urn:ngsi-ld:Device:01234",
                "type":"Device",
                "@context":[
                    "$NGSILD_CORE_CONTEXT"
                ]
            }
            """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity, listOf(NGSILD_CORE_CONTEXT))
        val compactedEntity = compact(jsonLdEntity, listOf(NGSILD_CORE_CONTEXT))

        assertTrue(compactedEntity.serialize().matchContent(expectedEntity))
    }

    @Test
    fun `it should not find an unknown attribute instance in a list of attributes`() {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value"
                }            
            }
            """.trimIndent()

        val expandedAttributes = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        assertNull(getAttributeFromExpandedFragment(expandedAttributes, "unknownAttribute", null))
    }

    @Test
    fun `it should find an attribute instance from a list of attributes without multi-attributes`() {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                "name": {
                    "value": 12
                }
            }
            """.trimIndent()

        val expandedAttributes = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        val expandedBrandName = expandJsonLdKey("brandName", DEFAULT_CONTEXTS)!!

        assertNotNull(getAttributeFromExpandedFragment(expandedAttributes, expandedBrandName, null))
        assertNull(
            getAttributeFromExpandedFragment(expandedAttributes, expandedBrandName, "urn:datasetId".toUri())
        )
    }

    @Test
    fun `it should find an attribute instance from a list of attributes with multi-attributes`() {
        val entityFragment =
            """
            {
                "brandName": [{
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z",
                    "datasetId": "urn:datasetId:1"
                }],
                "name": {
                    "value": 12,
                    "datasetId": "urn:datasetId:1"
                }
            }
            """.trimIndent()

        val expandedFragment = expandJsonLdFragment(entityFragment, DEFAULT_CONTEXTS)
        val expandedBrandName = expandJsonLdKey("brandName", DEFAULT_CONTEXTS)!!
        val expandedName = expandJsonLdKey("name", DEFAULT_CONTEXTS)!!

        assertNotNull(getAttributeFromExpandedFragment(expandedFragment, expandedBrandName, null))
        assertNotNull(
            getAttributeFromExpandedFragment(expandedFragment, expandedBrandName, "urn:datasetId:1".toUri())
        )
        assertNull(
            getAttributeFromExpandedFragment(expandedFragment, expandedBrandName, "urn:datasetId:2".toUri())
        )
        assertNotNull(
            getAttributeFromExpandedFragment(expandedFragment, expandedName, "urn:datasetId:1".toUri())
        )
        assertNull(getAttributeFromExpandedFragment(expandedFragment, expandedName, null))
    }

    @Test
    fun `it should reconstruct Polygon coordinates`() {
        val entity =
            """
            {
               "id":"urn:ngsi-ld:Device:01234",
               "type":"Device",
               "operationSpace":{
                  "type":"GeoProperty",
                  "value":{
                     "type":"Polygon",
                     "coordinates":[
                        [[100.0,0.0],
                        [101.0,0.0],
                        [101.0,1.0],
                        [100.0,1.0],
                        [100.0,0.0]]
                     ]
                  }
               }
            }
            """.trimIndent()
        val expectedEntity =
            """
            {
               "id":"urn:ngsi-ld:Device:01234",
               "type":"Device",
               "operationSpace":{
                  "type":"GeoProperty",
                  "value":{
                     "type":"Polygon",
                     "coordinates":[
                        [[100.0,0.0],
                        [101.0,0.0],
                        [101.0,1.0],
                        [100.0,1.0],
                        [100.0,0.0]]
                     ]
                  }
               }
            }
            """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity, DEFAULT_CONTEXTS)
        val compactedEntity = compact(jsonLdEntity, DEFAULT_CONTEXTS, MediaType.APPLICATION_JSON).toMutableMap()
        reconstructPolygonCoordinates(compactedEntity)
        assertTrue(compactedEntity.serialize().matchContent(expectedEntity))
    }
}
