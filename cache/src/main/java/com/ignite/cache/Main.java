package com.ignite.cache;

import com.ignite.cache.configuration.AppConfiguration;
import com.ignite.cache.model.entity.Client;
import com.ignite.cache.model.repositories.igniterepository.IgniteClientRepository;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by GrIfOn on 23.07.2017.
 */
public class Main {
    private static Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        ApplicationContext context = new ClassPathXmlApplicationContext("context.xml");
        Ignite ignite = context.getBean(Ignite.class);
//
//        (ignite.getOrCreateCache("ClientCache")).loadCache(null);
//
//        IgniteClientRepository clientRepository = context.getBean(IgniteClientRepository.class);
//
//        Iterable<Client> clients = clientRepository.findAll();
//        for(Client client : clients) {
//            logger.info(client);
//        }
//
//        logger.info("Finish");
//        Ignition.start("ignite.xml");
    }
}
