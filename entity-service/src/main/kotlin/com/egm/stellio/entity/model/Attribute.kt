package com.egm.stellio.entity.model

import jakarta.json.JsonValue
import org.springframework.data.neo4j.core.schema.Node
import java.net.URI

@Node
interface Attribute {
    fun serializeCoreProperties(includeSysAttrs: Boolean): MutableMap<String, JsonValue>
    fun nodeProperties(): MutableMap<String, Any>
    fun id(): URI
}
