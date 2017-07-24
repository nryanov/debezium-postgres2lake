package com.ignite.cache;

import com.ignite.cache.configuration.AppConfiguration;
import com.ignite.cache.configuration.DataSourceConfiguration;
import com.ignite.cache.model.entity.Client;
import com.ignite.cache.model.service.interfaces.IClientService;
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
        IClientService clientService = context.getBean(IClientService.class);

        logger.info("First query");
        Iterable<Client> clients = clientService.findAll();

        for(Client client : clients) {
            logger.info(client);
        }

        logger.info("Cached query");
        clients = clientService.findAll();

        for(Client client : clients) {
            logger.info(client);
        }
    }
}
