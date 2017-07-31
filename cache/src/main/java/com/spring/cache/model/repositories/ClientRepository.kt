package com.spring.cache.model.repositories

import com.spring.cache.model.entity.Client
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository

/**
 * Created by GrIfOn on 24.07.2017.
 */
@Repository
interface ClientRepository : CrudRepository<Client, Long> {
}