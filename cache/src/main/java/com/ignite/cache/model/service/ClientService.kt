package com.ignite.cache.model.service

import com.ignite.cache.model.entity.Client
import com.ignite.cache.model.repositories.springdatarepository.ClientRepository
import com.ignite.cache.model.service.interfaces.IClientService
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Service
@Transactional
class ClientService : IClientService, Serializable {
    private val logger: Logger = Logger.getLogger(ClientService::class.java)

    @Autowired
    lateinit var clientRepository: ClientRepository

    override fun delete(p0: Long?) {
        clientRepository.delete(p0)
    }

    override fun delete(p0: MutableIterable<Client>?) {
        clientRepository.delete(p0)
    }

    override fun delete(p0: Client?) {
        clientRepository.delete(p0)
    }

    override fun <S : Client?> save(p0: MutableIterable<S>?): MutableIterable<S> = clientRepository.save(p0)

    override fun <S : Client?> save(p0: S): S = clientRepository.save(p0)

    override fun findAll(p0: MutableIterable<Long>?): MutableIterable<Client> = clientRepository.findAll(p0)

    override fun findAll(): MutableIterable<Client> = clientRepository.findAll()

    override fun exists(p0: Long?): Boolean = clientRepository.exists(p0)

    override fun findOne(p0: Long?): Client = clientRepository.findOne(p0)

    override fun count(): Long = clientRepository.count()

    override fun deleteAll() {
        clientRepository.deleteAll()
    }
}