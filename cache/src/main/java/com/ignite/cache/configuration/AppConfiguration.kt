package com.ignite.cache.configuration

import com.ignite.cache.model.entity.Client
import com.ignite.cache.model.service.ClientStore
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.springdata.repository.config.EnableIgniteRepositories
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import java.io.Serializable
import javax.cache.configuration.FactoryBuilder

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Configuration
@ComponentScan(basePackages = arrayOf("com.ignite.cache"))
open class AppConfiguration : Serializable {
    private val logger: Logger = Logger.getLogger(AppConfiguration::class.java)

    @Autowired
    lateinit var context: ApplicationContext

    @Bean
    open fun igniteInstance(): Ignite {
//        val cfg = IgniteConfiguration()
//
//        cfg.igniteInstanceName = "springDataNode"
//
//        cfg.isPeerClassLoadingEnabled = true
//
//
//        var clientCache: CacheConfiguration<Long, Client> = CacheConfiguration("ClientCache")
//        clientCache.apply {
//            setIndexedTypes(Long::class.java, Client::class.java)
//            setCacheStoreFactory(FactoryBuilder.factoryOf(ClientStore::class.java))
//            isReadThrough = true
//            isWriteThrough = true
//        }
//
//        cfg.setCacheConfiguration(clientCache)

        Ignition.loadSpringBean<ApplicationContext>("context.xml", "context")
        return Ignition.start("ignite.xml")
    }
}