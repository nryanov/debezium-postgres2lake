package com.ignite.cache.model.repositories.igniterepository

import com.ignite.cache.model.entity.Client
import org.apache.ignite.springdata.repository.IgniteRepository
import org.apache.ignite.springdata.repository.config.RepositoryConfig

/**
 * Created by GrIfOn on 23.07.2017.
 */
@RepositoryConfig(cacheName = "ClientCache")
interface IgniteClientRepository : IgniteRepository<Client, Long> {
}