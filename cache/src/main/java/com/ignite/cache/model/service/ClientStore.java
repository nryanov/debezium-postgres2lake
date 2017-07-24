package com.ignite.cache.model.service;

import com.ignite.cache.model.entity.Client;
import com.ignite.cache.model.service.interfaces.IClientService;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.Transient;

import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by GrIfOn on 23.07.2017.
 */
public class ClientStore implements CacheStore<Long, Client>, Serializable {
    private Logger logger = Logger.getLogger(ClientStore.class);

//    @SpringResource(resourceClass = ClientService.class)
    private transient IClientService clientRepository;

    @SpringApplicationContextResource
    private ApplicationContext context;

    @Override
    public void loadCache(IgniteBiInClosure<Long, Client> igniteBiInClosure, @Nullable Object... objects) throws CacheLoaderException {
        clientRepository = context.getBean(ClientService.class);
        Iterable<Client> clients = clientRepository.findAll();

        for(Client client : clients) {
            igniteBiInClosure.apply(client.getId(), client);
        }
    }

    @Override
    public void sessionEnd(boolean b) throws CacheWriterException {

    }

    @Override
    public Client load(Long aLong) throws CacheLoaderException {
        return clientRepository.findOne(aLong);
    }

    @Override
    public Map<Long, Client> loadAll(Iterable<? extends Long> iterable) throws CacheLoaderException {
        Map<Long, Client> clientMap = new HashMap<>();
        Iterable<Client> clients = clientRepository.findAll();

        for(Client client : clients) {
            clientMap.put(client.getId(), client);
        }

        return clientMap;
    }

    @Override
    public void write(Cache.Entry<? extends Long, ? extends Client> entry) throws CacheWriterException {
        clientRepository.save(entry.getValue());
    }

    @Override
    public void writeAll(Collection<Cache.Entry<? extends Long, ? extends Client>> collection) throws CacheWriterException {
        for(Cache.Entry<? extends Long, ? extends Client> entry : collection) {
            clientRepository.save(entry.getValue());
        }
    }

    @Override
    public void delete(Object o) throws CacheWriterException {
        if(o instanceof Long) {
            clientRepository.delete((Long) o);
        }

        if(o instanceof Client) {
            clientRepository.delete((Client) o);
        }
    }

    @Override
    public void deleteAll(Collection<?> collection) throws CacheWriterException {
        for(Object object : collection) {
            if(object instanceof Long) {
                clientRepository.delete((Long) object);
            }

            if(object instanceof Client) {
                clientRepository.delete((Client) object);
            }
        }
    }
}
