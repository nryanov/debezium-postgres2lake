package com.hibernate.cache.configuration

import org.apache.log4j.Logger
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration

/**
 * Created by GrIfOn on 23.07.2017.
 */
@Configuration
@ComponentScan(basePackages = arrayOf("com.hibernate.cache"))
open class AppConfiguration {
    private val logger: Logger = Logger.getLogger(AppConfiguration::class.java)

//    @Bean
//    open fun ignite() : Ignite = Ignition.start("ignite-ehcache.xml")
}