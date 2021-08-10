package com.egm.stellio.entity.util

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHORIZATION_CONTEXT
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHORIZATION_ONTOLOGY
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.CREATION_ROLE_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.EGM_ROLES
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.USER_PREFIX
import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.repository.EntityRepository
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.toUri
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component

@Profile("api-tests")
@Component
class ApiTestsBootstrapper(
    private val entityRepository: EntityRepository
) : CommandLineRunner {

    @Value("\${application.apitests.userid}")
    val apiTestUserId: String? = null

    companion object {
        val AUTHORIZATION_CONTEXTS: List<String> = listOf(AUTHORIZATION_CONTEXT, NGSILD_CORE_CONTEXT)
        val USER_ROLES = listOf(CREATION_ROLE_LABEL)
    }

    override fun run(vararg args: String?) {
        // well, this should not happen in api-tests profile as we start from a fresh database on each run
        val ngsiLdUserId = (USER_PREFIX + apiTestUserId!!).toUri()
        val apiTestsUser = entityRepository.getEntityCoreById(ngsiLdUserId.toString())
        if (apiTestsUser == null) {
            val entity = Entity(
                id = ngsiLdUserId,
                type = listOf(USER_LABEL),
                contexts = AUTHORIZATION_CONTEXTS,
                properties = mutableListOf(
                    Property(
                        name = EGM_ROLES,
                        value = USER_ROLES
                    ),
                    Property(
                        name = AUTHORIZATION_ONTOLOGY + "username",
                        value = "API Tests"
                    )
                )
            )

            entityRepository.save(entity)
        }
    }
}
