package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.model.NotificationParams.FormatType
import java.time.Instant
import java.time.ZoneOffset

fun gimmeRawSubscription(
    withQueryAndGeoQuery: Pair<Boolean, Boolean> = Pair(true, true),
    withEndpointInfo: Boolean = true,
    withNotifParams: Pair<FormatType, List<String>> = Pair(FormatType.NORMALIZED, emptyList()),
    withModifiedAt: Boolean = false,
    georel: String = "within",
    geometry: String = "Polygon",
    coordinates: String = "[[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]]",
    timeInterval: Int? = null,
    contexts: List<String> = listOf(NGSILD_CORE_CONTEXT)
): Subscription {
    val q =
        if (withQueryAndGeoQuery.first)
            "speed>50;foodName==dietary fibres"
        else
            null

    val geoQ =
        if (withQueryAndGeoQuery.second)
            GeoQ(
                georel = georel,
                geometry = geometry,
                coordinates = coordinates,
                pgisGeometry = "100000101000101010100010100054300"
            )
        else
            null

    val endpointInfo =
        if (withEndpointInfo)
            listOf(EndpointInfo(key = "Authorization-token", value = "Authorization-token-value"))
        else
            null

    val modifiedAtValue = if (withModifiedAt) Instant.now().atZone(ZoneOffset.UTC) else null
    return Subscription(
        type = NGSILD_SUBSCRIPTION_TERM,
        subscriptionName = "My Subscription",
        modifiedAt = modifiedAtValue,
        description = "My beautiful subscription",
        q = q,
        timeInterval = timeInterval,
        entities = emptySet(),
        geoQ = geoQ,
        notification = NotificationParams(
            attributes = withNotifParams.second,
            format = withNotifParams.first,
            endpoint = Endpoint(
                uri = "http://localhost:8089/notification".toUri(),
                accept = Endpoint.AcceptType.JSONLD,
                info = endpointInfo
            )
        ),
        contexts = contexts
    )
}
