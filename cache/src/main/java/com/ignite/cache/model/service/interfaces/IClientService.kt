package com.ignite.cache.model.service.interfaces

import com.ignite.cache.model.entity.Client
import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.NoRepositoryBean

/**
 * Created by GrIfOn on 23.07.2017.
 */
@NoRepositoryBean
interface IClientService : CrudRepository<Client, Long>