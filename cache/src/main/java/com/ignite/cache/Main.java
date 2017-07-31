package com.ignite.cache;

import com.ignite.cache.configuration.AppConfig;
import com.ignite.cache.model.entity.Client;
import com.ignite.cache.service.interfaces.IClientService;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by GrIfOn on 31.07.2017.
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        IClientService clientService = context.getBean(IClientService.class);

        logger.info("Ignite Spring cache example");
        logger.info("First query find one");
        Client clients = clientService.findOne(1l);

        logger.info(clients);

        logger.info("Cached query");
        clients = clientService.findOne(1l);

        logger.info(clients);

        logger.info("First query find all");
        Iterable<Client> clientIterable = clientService.findAll();

        logger.info("Cached query");
        clientIterable = clientService.findAll();

        for(Client client : clientIterable) {
            logger.info(client);
        }
    }
}
