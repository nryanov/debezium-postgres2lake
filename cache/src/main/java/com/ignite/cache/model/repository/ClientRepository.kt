package com.ignite.cache.model.repository

import com.ignite.cache.model.entity.Client
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository

/**
 * Created by GrIfOn on 31.07.2017.
 */
@Repository
interface ClientRepository : CrudRepository<Client, Long> {
}