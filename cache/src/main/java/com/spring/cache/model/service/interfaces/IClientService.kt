package com.spring.cache.model.service.interfaces

import org.springframework.data.repository.CrudRepository
import org.springframework.data.repository.NoRepositoryBean
import com.spring.cache.model.entity.Client

/**
 * Created by GrIfOn on 24.07.2017.
 */
@NoRepositoryBean
interface IClientService : CrudRepository<Client, Long> {
}