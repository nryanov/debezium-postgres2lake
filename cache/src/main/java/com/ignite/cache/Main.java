package com.ignite.cache;

import com.ignite.cache.configuration.AppConfiguration;
import com.ignite.cache.configuration.DataSourceConfiguration;
import com.ignite.cache.model.entity.Client;
import com.ignite.cache.model.repositories.igniterepository.IgniteClientRepository;
import org.apache.ignite.Ignite;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by GrIfOn on 23.07.2017.
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfiguration.class, DataSourceConfiguration.class);
        Ignite ignite = context.getBean(Ignite.class);

        (ignite.getOrCreateCache("ClientCache")).loadCache(null);

        IgniteClientRepository clientRepository = context.getBean(IgniteClientRepository.class);

//        IgniteCache<Long, Client> clientIgniteCache = igniterepository.getOrCreateCache("ClientCache");

//        try {
//            clientIgniteCache.loadCache(null);
//        } catch (Exception e) {
//            logger.info(e);
//        }
//
//        Map<Long, Client> clientMap = clientIgniteCache.getAll(new HashSet(Arrays.asList(1, 2)));
//
//        for(Map.Entry<Long, Client> entry : clientMap.entrySet()) {
//            logger.info(entry.getValue());
//        }
//        clientRepository.save(10l, new Client(10, "1134", "424", null));
        Iterable<Client> clients = clientRepository.findAll();
        for(Client client : clients) {
            logger.info(client);
        }

        logger.info("Finish");
    }
}
