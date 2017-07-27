package com.hibernate.cache;

import com.hibernate.cache.configuration.AppConfiguration;
import com.hibernate.cache.model.entity.Client;
import com.hibernate.cache.model.service.interfaces.IClientService;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by GrIfOn on 23.07.2017.
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfiguration.class);
        IClientService clientService = context.getBean(IClientService.class);

        logger.info("First query");
        Client clients = clientService.findOne(1l);

        logger.info(clients);

        logger.info("Cached query");
        clients = clientService.findOne(1l);

        logger.info(clients);
    }
}
