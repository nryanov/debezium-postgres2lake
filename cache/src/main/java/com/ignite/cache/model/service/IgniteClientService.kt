package com.ignite.cache.model.service

import com.ignite.cache.model.entity.Client
import com.ignite.cache.model.repositories.igniterepository.IgniteClientRepository
import com.ignite.cache.model.service.interfaces.IClientServiceIgnite
import org.apache.ignite.resources.SpringResource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Service
@Transactional
class IgniteClientService : IClientServiceIgnite {

    @Autowired
    @SpringResource
    lateinit var clientRepository: IgniteClientRepository

    override fun deleteAll() {
        clientRepository.deleteAll()
    }

    override fun deleteAll(p0: MutableIterable<Long>?) {
        clientRepository.deleteAll(p0)

    }

    override fun findAll(p0: MutableIterable<Long>?): MutableIterable<Client> = clientRepository.findAll(p0)

    override fun findAll(): MutableIterable<Client> = clientRepository.findAll()

    override fun <S : Client?> save(p0: S): S = clientRepository.save(p0)

    override fun <S : Client?> save(p0: Long?, p1: S): S = clientRepository.save(p0, p1)

    override fun <S : Client?> save(p0: MutableIterable<S>?): MutableIterable<S> = clientRepository.save(p0)

    override fun <S : Client?> save(p0: MutableMap<Long, S>?): MutableIterable<S> = clientRepository.save(p0)

    override fun delete(p0: Client?) {
        clientRepository.delete(p0?.id)
    }

    override fun delete(p0: MutableIterable<Client>?) {
        if (p0 != null) {
            for(client: Client in p0) {
                clientRepository.delete(client.id)
            }
        }
    }

    override fun delete(p0: Long?) {
        clientRepository.delete(p0)
    }

    override fun count(): Long = clientRepository.count()

    override fun findOne(p0: Long?): Client = clientRepository.findOne(p0)

    override fun exists(p0: Long?): Boolean = clientRepository.exists(p0)
}