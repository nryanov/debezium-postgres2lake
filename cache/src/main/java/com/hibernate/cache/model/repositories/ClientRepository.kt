package com.hibernate.cache.model.repositories

import com.hibernate.cache.model.entity.Client
import org.springframework.data.repository.CrudRepository
import org.springframework.stereotype.Repository

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Repository
interface ClientRepository : CrudRepository<Client, Long>