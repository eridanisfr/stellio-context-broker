package com.egm.stellio.entity.service

import arrow.core.Validated
import arrow.core.invalid
import arrow.core.valid
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdateResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.shared.model.AttributeAppendEvent
import com.egm.stellio.shared.model.AttributeReplaceEvent
import com.egm.stellio.shared.model.AttributeUpdateEvent
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonUtils
import jakarta.json.JsonObject
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.internals.Topic
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Async
import org.springframework.stereotype.Component
import java.net.URI

@Component
class EntityEventService(
    private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    internal fun composeTopicName(entityType: String, contexts: List<String>): Validated<Unit, String> {
        val topicName = entityChannelName(compactTerm(entityType, contexts))
        return try {
            Topic.validate(topicName)
            topicName.valid()
        } catch (e: InvalidTopicException) {
            logger.error("Invalid topic name generated for entity type $entityType in contexts $contexts", e)
            return Unit.invalid()
        }
    }

    @Async
    fun publishEntityEvent(event: EntityEvent, entityType: String): java.lang.Boolean =
        composeTopicName(entityType, event.contexts)
            .fold(
                {
                    false as java.lang.Boolean
                },
                { topic ->
                    kafkaTemplate.send(topic, event.entityId.toString(), JsonUtils.serializeObject(event))
                    return true as java.lang.Boolean
                }
            )

    private fun entityChannelName(channelSuffix: String) =
        "cim.entity.$channelSuffix"

    fun publishAppendEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: JsonObject,
        appendResult: UpdateResult,
        updatedEntity: JsonLdEntity,
        contexts: List<String>
    ) {
        appendResult.updated.forEach { updatedDetails ->
            val attributeName = updatedDetails.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedFragment(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetails.datasetId
                )
            if (updatedDetails.updateOperationResult == UpdateOperationResult.APPENDED)
                publishEntityEvent(
                    AttributeAppendEvent(
                        entityId,
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        JsonLdUtils.compactAndSerializeFragment(
                            attributeName,
                            attributePayload!!,
                            contexts
                        ),
                        JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                        contexts
                    ),
                    updatedEntity.type
                )
            else
                publishEntityEvent(
                    AttributeReplaceEvent(
                        entityId,
                        compactTerm(attributeName, contexts),
                        updatedDetails.datasetId,
                        JsonLdUtils.compactAndSerializeFragment(
                            attributeName,
                            attributePayload!!,
                            contexts
                        ),
                        JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                        contexts
                    ),
                    updatedEntity.type
                )
        }
    }

    fun publishUpdateEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: JsonObject,
        updateResult: UpdateResult,
        updatedEntity: JsonLdEntity,
        contexts: List<String>
    ) {
        updateResult.updated.forEach { updatedDetails ->
            val attributeName = updatedDetails.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedFragment(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetails.datasetId
                )
            publishEntityEvent(
                AttributeReplaceEvent(
                    entityId,
                    compactTerm(attributeName, contexts),
                    updatedDetails.datasetId,
                    JsonLdUtils.compactAndSerializeFragment(
                        attributeName,
                        attributePayload!!,
                        contexts
                    ),
                    JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                    contexts
                ),
                updatedEntity.type
            )
        }
    }

    fun publishPartialUpdateEntityAttributesEvents(
        entityId: URI,
        jsonLdAttributes: JsonObject,
        updatedDetails: List<UpdatedDetails>,
        updatedEntity: JsonLdEntity,
        contexts: List<String>
    ) {
        updatedDetails.forEach { updatedDetail ->
            val attributeName = updatedDetail.attributeName
            val attributePayload =
                JsonLdUtils.getAttributeFromExpandedFragment(
                    jsonLdAttributes,
                    attributeName,
                    updatedDetail.datasetId
                ) as Map<String, Any>
            publishEntityEvent(
                AttributeUpdateEvent(
                    entityId,
                    compactTerm(attributeName, contexts),
                    updatedDetail.datasetId,
                    JsonLdUtils.compactAndSerializeFragment(
                        attributePayload,
                        contexts
                    ),
                    JsonLdUtils.compactAndSerialize(updatedEntity, contexts, MediaType.APPLICATION_JSON),
                    contexts
                ),
                updatedEntity.type
            )
        }
    }
}
